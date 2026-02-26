/*
 * Copyright (C) 2026 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//! Contains the VSIDL VPM implementation, implementing the StatesClientSubscriber
//! trait and informing listeners of new VPM updates.

use crate::states::custom_modes::CustomModesHandler;
use crate::states::orchestration_comms::{OrchestrationComms, ServiceUnitChangeEvent};
use crate::states::states_subscriber_interface::StateUpdateRequest;
use crate::states::states_subscriber_interface::StatesClientSubscriber;
use crate::states::ORCH_POWER_READY_PROP;
use crate::states::ORCH_VEHICLE_READY_PROP;
use async_trait::async_trait;
use binder::Status;
use binder::Strong;
use binder_tokio::TokioRuntime;
use futures::StreamExt;
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateClientApi::{
    BpPowerStateClientApi, IPowerStateClientApi,
};
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateReportListener::{
    BnPowerStateReportListener, IPowerStateReportListener, IPowerStateReportListenerAsyncServer,
};
use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;
use google_sdv_vpm::binder;
use google_sdv_vpm::binder::BinderFeatures;
use log::{debug, error, info, warn};
use mockall::automock;
use persistent_binder::{BinderConnectionManager, PersistentBinderConnection};
use rustutils::android::system_properties;
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::UnitNameDiscoveryArgs;
use sdv_comms::ContextRef;
use sdv_mw_rs_comms::SubscribeOptions;
use sdv_mw_rs_comms::UnitName;
use sdv_mw_rs_comms::UnitQuery;
use sdv_mw_rs_sdv_comms::SdvComms;
use sdv_orchestration_common::fqin::Fqin;
use sdv_orchestration_common::mode::{Mode, UpdateEvent::StateChange};
use sdv_orchestration_common::vpm_states::power_state_to_string;
use sdv_orchestration_common::vpm_states::vehicle_state_to_string;
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use sdv_status_rs::SdvStatusCode;
use sdv_vpm_api::vehicle::VehicleStateChange;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::runtime::Handle;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{event, instrument, Level};

const VPM_SYSTEM_SERVICE_BUNDLE_NAME: &str = "VpmSystemServiceBundle";
const VPM_SYSTEM_SERVICE_PACKAGE_NAME: &str = "com.android.sdv.vpm";
const VPM_SYSTEM_SERVICE_SERVICE_UNIT_NAME: &str =
    "com-android-sdv-vpm-vehicle-vehicle-state-change-unique";
const POLLING_TIME: Duration = Duration::from_millis(50);

#[automock]
trait PowerStateReporter: Send + Sync {
    fn subscribe_to_power_reporter(
        &self,
        vpm_client_binder: &Strong<dyn IPowerStateClientApi>,
        vpm_client_binder_power_state: &Strong<dyn IPowerStateReportListener>,
    ) -> SdvResult<PowerStateReport>;
}

#[derive(Default)]
struct VpmPowerStateReporter {}

impl PowerStateReporter for VpmPowerStateReporter {
    fn subscribe_to_power_reporter(
        &self,
        vpm_client_binder: &Strong<dyn IPowerStateClientApi>,
        vpm_client_binder_power_state: &Strong<dyn IPowerStateReportListener>,
    ) -> SdvResult<PowerStateReport> {
        debug!("Subscribing to VPM power state reports");
        vpm_client_binder
            .subscribeToPowerStateReport(vpm_client_binder_power_state)
            .map_err(SdvStatus::from)
    }
}

#[derive(Clone)]
/// VPM client that uses the VPM VSIDL implementation.
pub struct OrchStateSubscriber {
    update_sender: Sender<Arc<StateUpdateRequest>>,
    context_ref: ContextRef,
    vpm_connection: Arc<dyn BinderConnectionManager<dyn IPowerStateClientApi>>,
    orch_comms: Arc<dyn OrchestrationComms>,
    power_state_reporter: Arc<dyn PowerStateReporter>,
    // We only subscribe to vehicle modes in the same VM so we just need to keep track of same vm subscription
    vehicle_subscription_state: Arc<(AsyncMutex<bool>, Notify)>,
    polling_time: Duration,
}

impl OrchStateSubscriber {
    /// Creates a new StatesClientSubscriber using the given sender to notify of new modes updates.
    pub fn new(
        update_sender: Sender<Arc<StateUpdateRequest>>,
        dm: Arc<sdv_dump::DumpManager>,
    ) -> Self {
        if let Err(ref err) = sdv_comms_ctx::wait_for_agents() {
            // If we could not wait for agents, orch will panic on ContextRef creation with a generic error.
            // Orch depends on comms for all its behavior, so if there was an initialization problem,
            // nothing will work. We prefer to provide a proper error when panicking.
            panic!("Failed to wait for sdv comms agents with error: {err:?}");
        }
        let fqin = ServiceFqin::builder()
            .sdv_vm_name("vm")
            .sdv_package_name("com.android.sdv.orchestrator")
            .service_bundle_name("OrchestratorServiceBundle")
            .service_instance_name("default")
            .build()
            .unwrap();
        let context_ref = ContextRef::create(fqin.clone());
        dm.set_self_fqin(context_ref.get_self_fqin().to_string());
        let comms = Arc::new(SdvComms { context: context_ref });
        let vpm_connection = PersistentBinderConnection::new(get_vpm_binder_service);

        let (subscription_lock, notify) = (AsyncMutex::new(false), Notify::new());
        Self::new_with(
            update_sender,
            comms,
            context_ref,
            Arc::new(VpmPowerStateReporter::default()),
            POLLING_TIME,
            vpm_connection,
            Arc::new((subscription_lock, notify)),
        )
    }

    fn new_with(
        update_sender: Sender<Arc<StateUpdateRequest>>,
        orch_comms: Arc<dyn OrchestrationComms>,
        context_ref: ContextRef,
        power_state_reporter: Arc<dyn PowerStateReporter>,
        polling_time: Duration,
        vpm_connection: Arc<dyn BinderConnectionManager<dyn IPowerStateClientApi>>,
        vehicle_subscription_state: Arc<(AsyncMutex<bool>, Notify)>,
    ) -> Self {
        Self {
            update_sender,
            context_ref,
            vpm_connection,
            orch_comms,
            power_state_reporter,
            vehicle_subscription_state,
            polling_time,
        }
    }

    async fn process_new_power_update(
        &self,
        power_state_str: &String,
        power_notifier_sender: oneshot::Sender<bool>,
        power_notifier_receiver: oneshot::Receiver<bool>,
        cancel_token: CancellationToken,
    ) -> binder::Result<()> {
        let res = self
            .update_sender
            .send(Arc::new(StateUpdateRequest {
                update_event: StateChange(Mode::Power, power_state_str.clone()),
                state_notifier: Mutex::new(Some(power_notifier_sender)),
                cancel_token: cancel_token.clone(),
                timestamp: None,
            }))
            .await;
        if let Err(ref err) = res {
            warn!(
                "Failed to send power state update '{}' with error '{:?}'.",
                power_state_str, err
            );
            // If we failed to send the update, then the receiver will not be notified.
            // So we just return an error.
            return Err(Status::new_service_specific_error_str(
                binder::StatusCode::FAILED_TRANSACTION as i32,
                Some("Failed to notify vpm of finished power transition."),
            ));
        }
        match power_notifier_receiver.await {
            Ok(_) => Ok(()),
            Err(_) => {
                // If the transition was cancelled, the receiver could drop before the cancelled in onPowerStateReportChanged
                // is triggered.
                // We should not return an error so we stay consistent of how we treat cancellations in onPowerStateReportChanged.
                if cancel_token.is_cancelled() {
                    debug!("Channel to notify VPM of finished transition was closed as part of a power transition cancellation.");
                    Ok(())
                } else {
                    Err(Status::new_service_specific_error_str(
                        binder::StatusCode::FAILED_TRANSACTION as i32,
                        Some("Failed to notify vpm of finished power transition: channel was closed.")
                    ))
                }
            }
        }
    }
}

async fn create_vehicle_modes_observer(
    vm_name: &str,
    comms: Arc<dyn OrchestrationComms>,
) -> SdvResult<
    std::pin::Pin<
        Box<dyn futures::stream::Stream<Item = SdvResult<Vec<VehicleStateChange>>> + Send>,
    >,
> {
    let publication = comms
        .get_publication(&UnitQuery::Name(
            UnitName::builder()
                .vm_name(vm_name)
                .package_name(VPM_SYSTEM_SERVICE_PACKAGE_NAME)
                .bundle_name(VPM_SYSTEM_SERVICE_BUNDLE_NAME)
                .service_unit_name(VPM_SYSTEM_SERVICE_SERVICE_UNIT_NAME)
                .build()?,
        ))
        .await?;
    Ok(sdv_mw_rs_comms::observe::<VehicleStateChange>(
        publication
            .observe(
                &SubscribeOptions::builder().read_on_subscribe(true).build().unwrap_or_default(),
            )
            .await?,
    ))
}

async fn vehicle_subscription_thread(
    sender: Sender<Arc<StateUpdateRequest>>,
    vm_name: String,
    orch_comms: Arc<dyn OrchestrationComms>,
    vehicle_subscription_state: Arc<(AsyncMutex<bool>, Notify)>,
    polling_time: Duration,
) {
    let (lock, notify) = &*vehicle_subscription_state;
    // The Loop will attempt to re-create the observer as we want to keep the vehicle subscription always alive.
    loop {
        // Lock should not be held when invoking handle_vehicle_modes_subscription so that we allow new (un)registrations.
        // Once the vehicle subscriptions ends (DT stream closes after an interruption or unregistration), we can read
        // the new value to decide if we need to subscribe again or not.
        // We do not cancel the ongoing subscription upon a new notification as we cannot close the DT stream.
        {
            let mut is_registered = lock.lock().await;
            // Verify if we already can subscribe and we do not need to wait for a registered event
            while !*is_registered {
                // We subscribe as soon as possible to not lose any notification, but only if we have not been registered yet.
                let notified = notify.notified();
                // Drop the lock so that a new (un)registration can overwrite the is_registered value in the listeners' API callbacks.
                drop(is_registered);
                notified.await;
                // Read the is_registered value now that it should have been updated after the notification wake up.
                is_registered = lock.lock().await;
            }
        }
        // We listen to the subscription until it ends or gets interrupted.
        let should_retry =
            handle_vehicle_modes_subscription(sender.clone(), vm_name.clone(), orch_comms.clone())
                .await;
        // We sleep a bit before trying to resubscribe (in case of a problem with the cross-VM DT agent/interruption).
        sleep(polling_time).await;

        // We only retry if the DT stream was closed
        if should_retry {
            continue;
        } else {
            // If we don't need to retry the connection, then we wait for a SD change.
            notify.notified().await;
        }
    }
}

async fn handle_vehicle_modes_subscription(
    sender: Sender<Arc<StateUpdateRequest>>,
    vm_name: String,
    orch_comms: Arc<dyn OrchestrationComms>,
) -> bool {
    let subscription = create_vehicle_modes_observer(&vm_name, orch_comms.clone()).await;
    match subscription {
        Ok(observer) => {
            info!("Successfully subscribed for vehicle modes");
            let subscription_success_log =
                "sdv_orchestration_agent vehicle subscription has started successfully";
            info!("{subscription_success_log}");
            tracing::event!(Level::DEBUG, subscription_success_log);
            info!("Setting orch vehicle ready property to complete.");
            if let Err(ref e) = system_properties::write(ORCH_VEHICLE_READY_PROP, "true") {
                error!("Couldn't write {ORCH_VEHICLE_READY_PROP} property: {e}");
            }
            observe_publications_while_publisher_alive(observer, &sender).await;
            warn!("Vehicle mode publisher was terminated.");
            true
        }
        Err(error) => {
            // If publisher has unregistered, creation will fail as the create_vehicle_modes_observer will query SD.
            // DT will not return an error as it will queue the request.
            // This means we do not need to retry this connection.
            warn!("Error subscribing to vehicle modes: {error:?}.");
            false
        }
    }
}

async fn observe_publications_while_publisher_alive(
    mut observer: std::pin::Pin<
        Box<
            dyn futures::stream::Stream<Item = sdv_status_rs::SdvResult<Vec<VehicleStateChange>>>
                + Send,
        >,
    >,
    sender: &Sender<Arc<StateUpdateRequest>>,
) {
    while let Some(Ok(vehicle_state_changes)) = observer.next().await {
        debug!("New mode received {vehicle_state_changes:?}");
        for vehicle_state_change in vehicle_state_changes {
            if let Ok(vehicle_state) = vehicle_state_change.vehicle_state.enum_value() {
                if let Some(vehicle_state_str) = vehicle_state_to_string(vehicle_state) {
                    event!(
                        tracing::Level::INFO,
                        "Received vehicle transition request {}",
                        vehicle_state_str
                    );
                    info!(
                        "Received vehicle state {vehicle_state_str} from OrchestratorServiceBundleCallbacks"
                    );
                    let res = sender
                        .send(Arc::new(StateUpdateRequest {
                            update_event: StateChange(Mode::Vehicle, vehicle_state_str.clone()),
                            state_notifier: Mutex::new(None),
                            cancel_token: CancellationToken::new(),
                            timestamp: None,
                        }))
                        .await;
                    if res.is_err() {
                        warn!("Failed to send vehicle state '{vehicle_state_str}'.");
                    }
                } else {
                    warn!("Unsupported vehicle state {vehicle_state_change:?}.");
                }
            }
        }
    }
}

impl binder::Interface for OrchStateSubscriber {}

#[async_trait]
impl StatesClientSubscriber for OrchStateSubscriber {
    #[instrument(skip(self))]
    async fn subscribe_to_vehicle_updates(&self) -> SdvResult<()> {
        let vm_name = self.context_ref.get_self_fqin().get_sdv_vm_name().to_string();
        let update_sender_clone = self.update_sender.clone();
        let orch_comms_clone = self.orch_comms.clone();
        // We want to listen to new registrations from VPM in this VM in case the publisher died and a new one was created.
        let discovery_args_builder = UnitNameDiscoveryArgs::new_builder()
            .set_sdv_package_name(VPM_SYSTEM_SERVICE_PACKAGE_NAME)
            .set_service_bundle_name(VPM_SYSTEM_SERVICE_BUNDLE_NAME)
            .set_service_unit_name(VPM_SYSTEM_SERVICE_SERVICE_UNIT_NAME)
            .set_sdv_vm_name(vm_name.clone())
            .build();
        if let Ok(args) = discovery_args_builder {
            let (service_unit_change_sender, mut service_unit_change_receiver) =
                channel::<ServiceUnitChangeEvent>(1);
            let orch_comms_clone_clone = orch_comms_clone.clone();
            // This will keep listening to new (un)registrations of service units and notify the service_unit_change_receiver.
            // If the ongoing subscription fails at any point, the sender will be dropped and the receiver will be closed.
            tokio::spawn(async move {
                let res = orch_comms_clone_clone
                    .subscribe_service_unit_change_by_name(service_unit_change_sender, args)
                    .await;
                if let Err(ref err) = res {
                    warn!(
                        "Error initialising Service discovery Manager for vehicle modes with error: {err:?}"
                    );
                }
            });

            let orch_comms_clone = orch_comms_clone.clone();
            let vm_clone = vm_name.clone();
            let update_sender_clone = update_sender_clone.clone();
            let polling = self.polling_time;
            let shared_vehicle_subscription_state_clone = self.vehicle_subscription_state.clone();
            let handle = tokio::spawn(async move {
                vehicle_subscription_thread(
                    update_sender_clone,
                    vm_clone,
                    orch_comms_clone,
                    shared_vehicle_subscription_state_clone,
                    polling,
                )
                .await;
            });

            while let Some(unit_change_event) = service_unit_change_receiver.recv().await {
                match unit_change_event {
                    ServiceUnitChangeEvent::Registered(_) => {
                        let (lock, notify) = &*self.vehicle_subscription_state;
                        let mut is_registered = lock.lock().await;
                        *is_registered = true;
                        notify.notify_waiters();
                    }
                    ServiceUnitChangeEvent::Unregistered(_) => {
                        let (lock, notify) = &*self.vehicle_subscription_state;
                        let mut is_registered = lock.lock().await;
                        *is_registered = false;
                        notify.notify_waiters();
                    }
                }
            }
            error!("Service unit registration listener was terminated. No more active vehicle subscription.");
            handle.abort();
        } else {
            error!("Failed to create unit discovery args for vehicle modes subscription.");
        }
        // The subscribe_to_vehicle_updates should keep listening to new service unit registered.
        // If we reach this point, there was some error in the registration process.
        // We are only reading this value in unit tests so we don't provide exact error information.
        let err_msg = format!("Error subscribing to vehicle modes in VM {vm_name:?}.");
        Err(SdvStatus::with_message(SdvStatusCode::Unknown, err_msg))
    }

    async fn subscribe_to_custom_updates(
        &self,
        custom_modes_allowed: Arc<Mutex<HashMap<Fqin, HashSet<String>>>>,
    ) -> SdvResult<()> {
        let comms_clone = self.orch_comms.clone();
        CustomModesHandler::new(custom_modes_allowed, self.update_sender.clone(), comms_clone)
            .await;
        Ok(())
    }

    /// Registers a new listener for power updates
    async fn subscribe_to_power_updates(&self) -> SdvResult<PowerStateReport> {
        let rt = Handle::current();
        let server_impl = self.clone();
        let vpm_client_binder_power_state = BnPowerStateReportListener::new_async_binder(
            server_impl.clone(),
            TokioRuntime(rt.clone()),
            BinderFeatures::default(),
        );

        let vpm_binder_mutex = self.vpm_connection.get_binder();
        let vpm_binder = vpm_binder_mutex.lock().unwrap();
        let result = self
            .power_state_reporter
            .subscribe_to_power_reporter(&vpm_binder, &vpm_client_binder_power_state);
        if result.is_ok() {
            let subscription_success_log =
                "sdv_orchestration_agent power subscription has started successfully";
            debug!("{subscription_success_log}");
            tracing::event!(Level::DEBUG, subscription_success_log);
            debug!("Setting orch power ready property to complete.");
            if let Err(ref e) = system_properties::write(ORCH_POWER_READY_PROP, "true") {
                error!("Couldn't write {ORCH_POWER_READY_PROP} property: {e}");
            }
        }
        let power_state_reporter = self.power_state_reporter.clone();

        let update_sender_clone = self.update_sender.clone();
        self.vpm_connection.set_reconnect_listener(Box::new(move |vpm_binder| {
            let vpm_client_binder_power_state = BnPowerStateReportListener::new_async_binder(
                server_impl.clone(),
                TokioRuntime(rt.clone()),
                BinderFeatures::default(),
            );

            // We set the power state in case it changed while trying to connect again with VPM
            let current_power_state_res = power_state_reporter
                .subscribe_to_power_reporter(vpm_binder, &vpm_client_binder_power_state);
            if let Ok(current_power_state) = current_power_state_res {
                debug!("Resubscribing to power modes after binder death successfully.");
                if let Some(power_state_string) = power_state_to_string(current_power_state) {
                    debug!("Setting current power state {power_state_string:?}.");
                    let res = update_sender_clone.blocking_send(Arc::new(StateUpdateRequest {
                        update_event: StateChange(Mode::Power, power_state_string.to_string()),
                        state_notifier: Mutex::new(None), // No way to notify completion of task as there's no callback
                        cancel_token: CancellationToken::new(),
                        timestamp: None,
                    }));
                    if res.is_err() {
                        warn!("Failed to set initial power state {power_state_string:?}.");
                    }
                } else {
                    warn!("Invalid power_state_state {current_power_state_res:?}");
                }
            } else if let Err(ref err) = current_power_state_res {
                warn!("Failed to resubscribe to VPM power states with error: {err:?}");
            }
        }));

        result
    }
}

#[allow(non_snake_case)]
#[async_trait]
impl IPowerStateReportListenerAsyncServer for OrchStateSubscriber {
    async fn onPowerStateReportChanged(&self, power_state: PowerStateReport) -> binder::Result<()> {
        info!("VPM binder client received power state change {:?}", power_state);
        if let Some(power_state_str) = power_state_to_string(power_state) {
            event!(tracing::Level::INFO, "Received power transition request {}", power_state_str);
            let (power_notifier_sender, power_notifier_receiver) = oneshot::channel();
            let cancel_token = CancellationToken::new();
            let res = tokio::select! {
                 _ = cancel_token.cancelled() => {
                    debug!("Power state transition was cancelled");
                    Ok(())
                },
                res = self.process_new_power_update(&power_state_str, power_notifier_sender, power_notifier_receiver, cancel_token.clone()) => {
                    debug!("Power transition {power_state:?} finished with {res:?}");
                    res
                },
            };
            return res;
        }
        // Invalid power state
        Err(Status::new_service_specific_error_str(
            binder::StatusCode::BAD_VALUE as i32,
            Some(format!("Failed to process power state: {power_state:?}")),
        ))
    }
}

fn get_vpm_binder_service() -> Strong<dyn IPowerStateClientApi> {
    debug!("Fetching VPM binder");
    let descriptor =
        <BpPowerStateClientApi as IPowerStateClientApi>::get_descriptor().to_owned() + "/default";
    let result = binder::wait_for_interface(&descriptor).unwrap_or_else(|error| {
        warn!("Retrying connection to VPM: {error}");
        get_vpm_binder_service()
    });
    debug!("Successfully fetched VPM binder");

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::states::orchestration_comms::MockOrchestrationComms;
    use crate::states::orchestration_comms::ServiceUnitIdentity;
    use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateClientApi::{
        BnPowerStateClientApi, MockIPowerStateClientApi,
    };
    use named_lock::NamedLock;
    use persistent_binder::MockBinderConnectionManager;
    use sdv_comms::id::ServiceFqin;
    use sdv_orch_test_mocks::vehicle_modes_publication::*;
    use sdv_orchestration_common::mode::Mode::{Power, Vehicle};
    use sdv_vpm_api::vehicle::VpmVehicleState;
    use tokio::sync::mpsc::Receiver;
    use tokio::sync::oneshot;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_received_vehicle_mode_should_be_propagated() {
        // GIVEN
        let lock = NamedLock::create("vehicle_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("vehicle-mode-test");
        let context_ref = ContextRef::create(fqin.clone());
        let (mut mock_orch_comms, mut publisher, _) = get_mock_orch_comms();
        let _ = mock_orch_sd_registration(&mut mock_orch_comms);
        let (sender, mut receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        // Simulate case when we first initialize the class and property is false
        let set_prop = system_properties::write(ORCH_VEHICLE_READY_PROP, "false");
        assert!(set_prop.is_ok());
        tokio::spawn(async move {
            let res = subscriber.subscribe_to_vehicle_updates().await;
            // The subscription only ends with error. We fail the test if it ended.
            assert!(res.is_ok());
        });

        // WHEN
        let mut vehicle_state_change_message = VehicleStateChange::new();
        vehicle_state_change_message.vehicle_state = VpmVehicleState::LOW_POWER.into();
        let res = publisher.publish(&vehicle_state_change_message).await;
        assert!(res.is_ok());

        // THEN
        let upd = receiver.recv().await;
        assert!(system_properties::read(ORCH_VEHICLE_READY_PROP).unwrap().unwrap() == "true");
        assert!(upd.is_some());
        let state_update_request = upd.expect("State update request error");
        assert!(
            state_update_request.update_event
                == StateChange(
                    Vehicle,
                    vehicle_state_to_string(VpmVehicleState::LOW_POWER).unwrap()
                )
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_invalid_vehicle_mode_should_not_be_propagated() {
        // GIVEN
        let lock = NamedLock::create("vehicle_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("invalid-vehicle-mode");
        let context_ref = ContextRef::create(fqin.clone());
        let (mut mock_orch_comms, mut publisher, mock_message_queue) = get_mock_orch_comms();
        let _ = mock_orch_sd_registration(&mut mock_orch_comms);
        let mut message_queue_pub_recv = mock_message_queue.lock().unwrap().observe();
        let (sender, receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        tokio::spawn(async move {
            let res = subscriber.subscribe_to_vehicle_updates().await;
            // The subscription only ends with error. We fail the test if it ended.
            assert!(res.is_ok());
        });
        // WHEN
        let mut vehicle_state_change_message = VehicleStateChange::new();
        vehicle_state_change_message.vehicle_state =
            VpmVehicleState::VEHICLE_STATE_UNSPECIFIED.into();
        let res = publisher.publish(&vehicle_state_change_message).await;
        assert!(res.is_ok());
        // THEN
        // Wait until the message was published
        let message = message_queue_pub_recv.next().await;
        assert!(message.is_some());
        // And check that the receiver did not get it
        assert!(receiver.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vehicle_modes_should_return_error_if_unit_registration_failed() {
        // GIVEN
        let fqin = get_fqin("vehicle-mode-fail");
        let context_ref = ContextRef::create(fqin.clone());
        let mut mock_orch_comms = MockOrchestrationComms::default();
        mock_orch_comms
            .expect_subscribe_service_unit_change_by_name()
            .returning(|_, _| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        let (sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        // WHEN
        let res = subscriber.subscribe_to_vehicle_updates().await;
        // THEN
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_should_not_subscribe_if_no_publication() {
        // GIVEN
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let mut mock_orch_comms = MockOrchestrationComms::default();
        mock_orch_comms
            .expect_get_publication()
            .returning(move |_| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        // WHEN
        let handle = tokio::spawn(async move {
            handle_vehicle_modes_subscription(
                update_sender,
                "vm".to_string(),
                Arc::new(mock_orch_comms),
            )
            .await;
        });
        // THEN the task should end as there's no publication
        let res = handle.await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_should_not_subscribe_if_failed_to_observe() {
        // GIVEN
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let mut mock_orch_comms = MockOrchestrationComms::default();
        mock_orch_comms
            .expect_get_publication()
            .returning(move |_| Ok(Arc::new(FailureMockPublication::default())));
        // WHEN
        let handle = tokio::spawn(async move {
            handle_vehicle_modes_subscription(
                update_sender,
                "vm".to_string(),
                Arc::new(mock_orch_comms),
            )
            .await;
        });
        // THEN the task should end as there's no publication
        let res = handle.await;
        assert!(res.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_have_only_one_active_subscription() {
        // GIVEN
        let lock = NamedLock::create("vehicle_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("vehicle-one-subscription-test");
        let context_ref = ContextRef::create(fqin.clone());

        let mut mock_orch_comms = MockOrchestrationComms::default();
        let (publication_sender, recv) = channel::<()>(1);
        let (_, message_queue_arc) = get_publisher(recv);
        let mock_publication = MockPublication::new(message_queue_arc.clone(), publication_sender);
        let mock_publication_arc = Arc::new(mock_publication.clone());
        let (registered_sender, mut registered_recv) = channel::<()>(1);
        let registered_sender_clone = registered_sender.clone();
        // THEN we only subscribe once
        mock_orch_comms.expect_get_publication().times(1).returning(move |_| {
            let registered_sender_clone = registered_sender_clone.clone();
            tokio::task::spawn_blocking(move || {
                let res = registered_sender_clone.blocking_send(());
                assert!(res.is_ok());
            });
            Ok(mock_publication_arc.clone())
        });

        // We intercept the sender to register later in the test
        let unit_change_sender: Arc<AsyncMutex<Option<Sender<ServiceUnitChangeEvent>>>> =
            Arc::new(AsyncMutex::new(None));
        let unit_change_sender_clone = unit_change_sender.clone();
        mock_orch_comms.expect_subscribe_service_unit_change_by_name().returning(
            move |sender, _| {
                let sender_clone = sender.clone();
                let unit_change_sender_clone = unit_change_sender_clone.clone();
                tokio::spawn(async move {
                    unit_change_sender_clone.lock().await.replace(sender_clone.clone());
                    // First registered event
                    let res = sender_clone
                        .send(ServiceUnitChangeEvent::Registered(ServiceUnitIdentity {
                            name: "service-unit-name".to_string(),
                            fqin: get_fqin("fake-publisher"),
                        }))
                        .await;
                    assert!(res.is_ok());
                });
                Ok(())
            },
        );

        let (sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let state_subscription = Arc::new((AsyncMutex::new(false), Notify::new()));
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            state_subscription.clone(),
        );
        tokio::spawn(async move {
            let res = subscriber.subscribe_to_vehicle_updates().await;
            // The subscription only ends with error. We fail the test if it ended.
            assert!(res.is_ok());
        });

        // We need to wait until the observer was created to subscribe again
        let res = registered_recv.recv().await;
        assert!(res.is_some());

        let (tx, rx) = oneshot::channel::<()>();
        let state_subscription_clone = state_subscription.clone();
        let wait_handle = tokio::spawn(async move {
            let (_lock, notify) = &*state_subscription_clone;
            // Notify that we are about to wait on the notify.
            let res = tx.send(());
            assert!(res.is_ok());
            // Wait for the notification that second registration happened.
            // This is when the second subscription would happen if functionality gets broken.
            // In working scenario, there's no second subscription after sending another registration.
            let res = tokio::time::timeout(Duration::from_millis(500), notify.notified()).await;
            assert!(res.is_ok(), "Timed out waiting for registration.");
        });

        // Wait for the signal that the other task is about to wait on the notify.
        // This prevents a race condition where we send the event before the other task is waiting.
        rx.await.unwrap();

        // We need to sleep a bit to prevent the registered to be sent before the notify.notified().await is invoked.
        // Otherwise, the Registered event could have been sent after it and the event would be missed as the notification
        // is only sent to those already registered waiters for the notify_waiters.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // WHEN registered again
        let res = unit_change_sender
            .lock()
            .await
            .as_ref()
            .unwrap()
            .send(ServiceUnitChangeEvent::Registered(ServiceUnitIdentity {
                name: "service-unit-name".to_string(),
                fqin: get_fqin("fake-publisher"),
            }))
            .await;
        assert!(res.is_ok());
        // Wait for the registration to be notified and task completion.
        let res = tokio::time::timeout(Duration::from_millis(500), wait_handle).await;
        assert!(res.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_subscribe_again_if_observer_dropped() {
        // GIVEN
        let lock = NamedLock::create("vehicle_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("vehicle-resubscribe-test");
        let context_ref = ContextRef::create(fqin.clone());

        let mut mock_orch_comms = MockOrchestrationComms::default();
        let (publication_sender, recv) = channel::<()>(1);
        let (_, message_queue_arc) = get_publisher(recv);
        let mock_publication = MockPublication::new(message_queue_arc.clone(), publication_sender);
        let mock_publication_arc = Arc::new(mock_publication.clone());
        let (registered_sender, mut registered_recv) = channel::<()>(1);
        let registered_sender_clone = registered_sender.clone();
        // We subscribe twice because the observer was dropped but it is still registered
        mock_orch_comms.expect_get_publication().times(2).returning(move |_| {
            let registered_sender_clone = registered_sender_clone.clone();
            tokio::task::spawn_blocking(move || {
                let res = registered_sender_clone.blocking_send(());
                assert!(res.is_ok());
            });
            Ok(mock_publication_arc.clone())
        });
        let _ = mock_orch_sd_registration(&mut mock_orch_comms);

        let (sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        tokio::spawn(async move {
            let res = subscriber.subscribe_to_vehicle_updates().await;
            // The subscription only ends with error. We fail the test if it ended.
            assert!(res.is_ok());
        });

        // We need to wait until the observer was created for the first time to later drop it
        let res = registered_recv.recv().await;
        assert!(res.is_some());

        // WHEN
        // The stream gets closed if we drop all the senders
        message_queue_arc.lock().unwrap().drop_stream();

        // THEN we should subscribe again
        let res = registered_recv.recv().await;
        assert!(res.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_register_and_unregister_from_publisher() {
        // GIVEN
        let lock = NamedLock::create("vehicle_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("vehicle-register-unregister-test");
        let context_ref = ContextRef::create(fqin.clone());

        let mut mock_orch_comms = MockOrchestrationComms::default();
        let (publication_sender, recv) = channel::<()>(1);
        let (_, message_queue_arc) = get_publisher(recv);
        let mock_publication = MockPublication::new(message_queue_arc.clone(), publication_sender);
        let mut publication_recv = message_queue_arc.lock().unwrap().observe();
        let mock_publication_arc = Arc::new(mock_publication.clone());
        let (registered_sender, mut registered_recv) = channel::<()>(1);
        let registered_sender_clone = registered_sender.clone();
        // We subscribe once because there's an unregistration before the stream was closed
        mock_orch_comms.expect_get_publication().times(1).returning(move |_| {
            let registered_sender_clone = registered_sender_clone.clone();
            tokio::task::spawn_blocking(move || {
                let res = registered_sender_clone.blocking_send(());
                assert!(res.is_ok());
            });
            Ok(mock_publication_arc.clone())
        });

        // We intercept the sender to unregister later in the test
        let unit_change_sender: Arc<AsyncMutex<Option<Sender<ServiceUnitChangeEvent>>>> =
            Arc::new(AsyncMutex::new(None));
        let unit_change_sender_clone = unit_change_sender.clone();
        mock_orch_comms.expect_subscribe_service_unit_change_by_name().returning(
            move |sender, _| {
                let sender_clone = sender.clone();
                let unit_change_sender_clone = unit_change_sender_clone.clone();
                tokio::spawn(async move {
                    unit_change_sender_clone.lock().await.replace(sender_clone.clone());
                    // Register the custom mode publisher for the test so it can be observed using MW
                    let res = sender_clone
                        .send(ServiceUnitChangeEvent::Registered(ServiceUnitIdentity {
                            name: "service-unit-name".to_string(),
                            fqin: get_fqin("fake-publisher"),
                        }))
                        .await;
                    assert!(res.is_ok());
                });
                Ok(())
            },
        );

        let state_subscription = Arc::new((AsyncMutex::new(false), Notify::new()));
        let (sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            state_subscription.clone(),
        );

        tokio::spawn(async move {
            let res = subscriber.subscribe_to_vehicle_updates().await;
            // The subscription only ends with error. We fail the test if it ended.
            assert!(res.is_ok());
        });

        // We need to wait until the observer was created for the first time to later drop it
        let res = registered_recv.recv().await;
        assert!(res.is_some());

        // We unregister the publisher
        let res = unit_change_sender
            .lock()
            .await
            .as_ref()
            .unwrap()
            .send(ServiceUnitChangeEvent::Unregistered(ServiceUnitIdentity {
                name: "service-unit-name".to_string(),
                fqin: get_fqin("fake-publisher"),
            }))
            .await;
        assert!(res.is_ok());
        // Wait until the unregistered was received
        {
            let (lock, notify) = &*state_subscription;
            let mut is_registered = lock.lock().await;
            // Verify that the subscription did not unregister before exiting the while
            while *is_registered {
                let notified = notify.notified();
                drop(is_registered);
                let res = tokio::time::timeout(Duration::from_millis(500), notified).await;
                assert!(res.is_ok(), "Timed out waiting for unregistration");
                is_registered = lock.lock().await;
            }
        }
        // WHEN
        // The stream gets closed if we drop all the senders
        message_queue_arc.lock().unwrap().drop_stream();

        // THEN
        // The channel returns None if the channel has been closed
        let next_item = publication_recv.next().await;
        assert!(next_item.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_received_power_mode_should_be_propagated() {
        // GIVEN
        let fqin = get_fqin("power-mode-test");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, mut receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );

        tokio::spawn(async move {
            // THEN
            let upd = receiver.recv().await;
            assert!(upd.is_some());
            let state_update_request = upd.expect("State update request error");
            assert!(
                state_update_request.update_event
                    == StateChange(
                        Power,
                        power_state_to_string(PowerStateReport::SUSPEND_TO_DISK_ENTER).unwrap()
                    )
            );
            // We need to notify that the update was finished so that onPowerStateReportChanged call finishes.
            let res = state_update_request
                .state_notifier
                .lock()
                .unwrap()
                .take()
                .expect("Sender was set")
                .send(true);
            assert!(res.is_ok());
        });

        // WHEN
        let res =
            subscriber.onPowerStateReportChanged(PowerStateReport::SUSPEND_TO_DISK_ENTER).await;
        assert!(res.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cancelling_power_request_transition_should_return_success() {
        // GIVEN
        let fqin = get_fqin("cancel-test");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, mut receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        tokio::spawn(async move {
            let upd = receiver.recv().await;
            assert!(upd.is_some());
            let state_update_request = upd.expect("State update request error");
            // WHEN
            // We cancel the request so that onPowerStateReportChanged finishes.
            state_update_request.cancel_token.cancel();
        });

        // THEN
        let res = subscriber.onPowerStateReportChanged(PowerStateReport::SHUTDOWN_CANCELLED).await;
        assert!(res.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_power_transition_should_fail_if_update_receiver_is_dead() {
        // GIVEN
        let fqin = get_fqin("upd-recv-dead-test");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, mut receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        // WHEN
        receiver.close();
        // THEN
        let res =
            subscriber.onPowerStateReportChanged(PowerStateReport::SUSPEND_TO_DISK_ENTER).await;
        assert!(res.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_invalid_power_mode_should_not_be_propagated() {
        // GIVEN
        let fqin = get_fqin("invalid-power");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        // WHEN
        let res = subscriber.onPowerStateReportChanged(PowerStateReport::REPORT_UNSPECIFIED).await;
        // THEN
        assert!(res.is_err());
        assert!(receiver.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_subscribing_to_power_updates_should_return_power_report() {
        // GIVEN
        let lock = NamedLock::create("power_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("power-subscription");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let mut mock_power_state_reporter = MockPowerStateReporter::default();
        mock_power_state_reporter
            .expect_subscribe_to_power_reporter()
            .returning(move |_, _| Ok(PowerStateReport::POWER_OFF_EXIT));
        let mut vpm_mock_binder = MockBinderConnectionManager::default();
        vpm_mock_binder.expect_get_binder().return_const(Arc::new(Mutex::new(
            BnPowerStateClientApi::new_binder(
                MockIPowerStateClientApi::new(),
                BinderFeatures::default(),
            ),
        )));
        vpm_mock_binder.expect_set_reconnect_listener().returning(|_| ());
        // Simulate case when we first initialize the class and property is false
        let set_prop = system_properties::write(ORCH_POWER_READY_PROP, "false");
        assert!(set_prop.is_ok());
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(mock_power_state_reporter),
            Duration::ZERO,
            Arc::new(vpm_mock_binder),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        // WHEN
        let power_report = subscriber.subscribe_to_power_updates().await;
        // THEN
        assert!(system_properties::read(ORCH_POWER_READY_PROP).unwrap().unwrap() == "true");
        assert!(power_report.is_ok());
        assert!(power_report.unwrap() == PowerStateReport::POWER_OFF_EXIT);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_subscribe_to_power_modes_on_reconnection() {
        // GIVEN
        let lock = NamedLock::create("power_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("power-subscription-on-reconnect");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, mut receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let mut mock_power_state_reporter = MockPowerStateReporter::default();
        mock_power_state_reporter
            .expect_subscribe_to_power_reporter()
            // Once in subscription and once in the reconnect_listener callback
            .times(2)
            .returning(move |_, _| Ok(PowerStateReport::POWER_OFF_EXIT));
        let mut vpm_mock_binder = MockBinderConnectionManager::default();
        vpm_mock_binder.expect_get_binder().return_const(Arc::new(Mutex::new(
            BnPowerStateClientApi::new_binder(
                MockIPowerStateClientApi::new(),
                BinderFeatures::default(),
            ),
        )));
        vpm_mock_binder.expect_set_reconnect_listener().returning(move |listener| {
            // We need spawn_blocking because the listener executes a blocking_send
            tokio::task::spawn_blocking(move || {
                let vpm_binder: Strong<dyn IPowerStateClientApi> =
                    BnPowerStateClientApi::new_binder(
                        MockIPowerStateClientApi::new(),
                        BinderFeatures::default(),
                    );
                listener(&vpm_binder);
            });
        });
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(mock_power_state_reporter),
            Duration::ZERO,
            Arc::new(vpm_mock_binder),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        // WHEN
        // We don't care about the initial subscription value because it is already covered by another test
        let _ = subscriber.subscribe_to_power_updates().await;
        // THEN
        let upd = receiver.recv().await;
        assert!(upd.is_some());
        let state_update_request = upd.expect("State update request error");
        assert!(
            state_update_request.update_event
                == StateChange(
                    Power,
                    power_state_to_string(PowerStateReport::POWER_OFF_EXIT).unwrap()
                )
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_not_propagate_invalid_power_mode_on_reconnection() {
        // GIVEN
        let lock = NamedLock::create("power_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("invalid-power-on-reconnect");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let mut mock_power_state_reporter = MockPowerStateReporter::default();
        mock_power_state_reporter
            .expect_subscribe_to_power_reporter()
            // Once in subscription and once in the reconnect_listener callback
            .times(2)
            .returning(move |_, _| Ok(PowerStateReport::REPORT_UNSPECIFIED));
        let mut vpm_mock_binder = MockBinderConnectionManager::default();
        vpm_mock_binder.expect_get_binder().return_const(Arc::new(Mutex::new(
            BnPowerStateClientApi::new_binder(
                MockIPowerStateClientApi::new(),
                BinderFeatures::default(),
            ),
        )));
        vpm_mock_binder.expect_set_reconnect_listener().returning(move |listener| {
            // We need spawn_blocking because the listener executes a blocking_send
            tokio::task::spawn_blocking(move || {
                let vpm_binder: Strong<dyn IPowerStateClientApi> =
                    BnPowerStateClientApi::new_binder(
                        MockIPowerStateClientApi::new(),
                        BinderFeatures::default(),
                    );
                listener(&vpm_binder);
            });
        });
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(mock_power_state_reporter),
            Duration::ZERO,
            Arc::new(vpm_mock_binder),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        // WHEN
        // We don't care about the initial subscription value because it is already covered by another test
        let _ = subscriber.subscribe_to_power_updates().await;
        // THEN
        // No message was received because power mode was invalid
        assert!(receiver.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_not_panic_on_closed_receiver_when_lm_reconnects() {
        // GIVEN
        let lock = NamedLock::create("power_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("reconnection-closed-upd-receiver");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, mut receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let mut mock_power_state_reporter = MockPowerStateReporter::default();
        mock_power_state_reporter
            .expect_subscribe_to_power_reporter()
            // Once in subscription and once in the reconnect_listener callback
            .times(2)
            .returning(move |_, _| Ok(PowerStateReport::POWER_OFF_EXIT));
        let mut vpm_mock_binder = MockBinderConnectionManager::default();
        vpm_mock_binder.expect_get_binder().return_const(Arc::new(Mutex::new(
            BnPowerStateClientApi::new_binder(
                MockIPowerStateClientApi::new(),
                BinderFeatures::default(),
            ),
        )));
        let captured_listener: Arc<
            Mutex<Option<Box<dyn Fn(&Strong<dyn IPowerStateClientApi>) + Send + Sync + 'static>>>,
        > = Arc::new(Mutex::new(None));
        let captured_listener_clone = Arc::clone(&captured_listener);
        vpm_mock_binder.expect_set_reconnect_listener().returning(move |listener| {
            *captured_listener_clone.lock().unwrap() = Some(listener);
        });
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(mock_power_state_reporter),
            Duration::ZERO,
            Arc::new(vpm_mock_binder),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        let _ = subscriber.subscribe_to_power_updates().await;
        // WHEN
        receiver.close();
        let res = tokio::task::spawn_blocking(move || {
            let listener_guard = captured_listener.lock().unwrap();
            let listener = listener_guard.as_ref().unwrap();
            let vpm_binder: Strong<dyn IPowerStateClientApi> = BnPowerStateClientApi::new_binder(
                MockIPowerStateClientApi::new(),
                BinderFeatures::default(),
            );
            listener(&vpm_binder);
        })
        .await;
        assert!(res.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_not_propagate_any_power_mode_if_failed_to_reconnect() {
        // GIVEN
        let lock = NamedLock::create("power_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("reconnect-to-invalid-power-subscription");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let mut mock_power_state_reporter = MockPowerStateReporter::default();
        mock_power_state_reporter
            .expect_subscribe_to_power_reporter()
            .returning(move |_, _| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        let mut vpm_mock_binder = MockBinderConnectionManager::default();
        vpm_mock_binder.expect_get_binder().return_const(Arc::new(Mutex::new(
            BnPowerStateClientApi::new_binder(
                MockIPowerStateClientApi::new(),
                BinderFeatures::default(),
            ),
        )));
        vpm_mock_binder.expect_set_reconnect_listener().returning(move |listener| {
            // We need spawn_blocking because the listener executes a blocking_send
            tokio::task::spawn_blocking(move || {
                let vpm_binder: Strong<dyn IPowerStateClientApi> =
                    BnPowerStateClientApi::new_binder(
                        MockIPowerStateClientApi::new(),
                        BinderFeatures::default(),
                    );
                listener(&vpm_binder);
            });
        });
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(mock_power_state_reporter),
            Duration::ZERO,
            Arc::new(vpm_mock_binder),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        // WHEN
        // We don't care about the initial subscription value because it is already covered by another test
        let _ = subscriber.subscribe_to_power_updates().await;
        // THEN
        // No message was received because reconnection was invalid
        assert!(receiver.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_subscribing_to_power_updates_should_not_set_power_prop_if_failed() {
        // GIVEN
        let lock = NamedLock::create("power_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("power-subscr-fails-prop");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let mut mock_power_state_reporter = MockPowerStateReporter::default();
        mock_power_state_reporter
            .expect_subscribe_to_power_reporter()
            .returning(move |_, _| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        let mut vpm_mock_binder = MockBinderConnectionManager::default();
        vpm_mock_binder.expect_get_binder().return_const(Arc::new(Mutex::new(
            BnPowerStateClientApi::new_binder(
                MockIPowerStateClientApi::new(),
                BinderFeatures::default(),
            ),
        )));
        vpm_mock_binder.expect_set_reconnect_listener().returning(|_| ());
        // Simulate case when we first initialize the class and property is false
        let set_prop = system_properties::write(ORCH_POWER_READY_PROP, "false");
        assert!(set_prop.is_ok());
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(mock_power_state_reporter),
            Duration::ZERO,
            Arc::new(vpm_mock_binder),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        // WHEN
        let _ = subscriber.subscribe_to_power_updates().await;
        // THEN
        assert!(system_properties::read(ORCH_POWER_READY_PROP).unwrap().unwrap() == "false");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_subscribing_to_power_updates_should_return_error_if_failed() {
        // GIVEN
        let lock = NamedLock::create("power_prop_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let fqin = get_fqin("power-subscr-fails");
        let context_ref = ContextRef::create(fqin.clone());
        let mock_orch_comms = MockOrchestrationComms::default();
        let (sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let mut mock_power_state_reporter = MockPowerStateReporter::default();
        mock_power_state_reporter
            .expect_subscribe_to_power_reporter()
            .returning(move |_, _| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        let mut vpm_mock_binder = MockBinderConnectionManager::default();
        vpm_mock_binder.expect_set_reconnect_listener().returning(|_| ());
        vpm_mock_binder.expect_get_binder().return_const(Arc::new(Mutex::new(
            BnPowerStateClientApi::new_binder(
                MockIPowerStateClientApi::new(),
                BinderFeatures::default(),
            ),
        )));
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(mock_power_state_reporter),
            Duration::ZERO,
            Arc::new(vpm_mock_binder),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        // WHEN
        let power_report = subscriber.subscribe_to_power_updates().await;
        // THEN
        assert!(power_report.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_custom_modes_subscription() {
        // GIVEN
        let fqin = get_fqin("custom-subscription");
        let context_ref = ContextRef::create(fqin.clone());
        let mut mock_orch_comms = MockOrchestrationComms::default();
        let (sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        mock_orch_comms
            .expect_get_orch_service_bundle()
            .returning(|_| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        mock_orch_comms
            .expect_subscribe_service_unit_change_by_type()
            .returning(|_, _| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        let subscriber = OrchStateSubscriber::new_with(
            sender,
            Arc::new(mock_orch_comms),
            context_ref,
            Arc::new(MockPowerStateReporter::default()),
            Duration::ZERO,
            Arc::new(MockBinderConnectionManager::new()),
            Arc::new((AsyncMutex::new(false), Notify::new())),
        );
        let custom_modes_config = Arc::new(Mutex::new(HashMap::new()));
        // WHEN
        let res = subscriber.subscribe_to_custom_updates(custom_modes_config).await;
        // THEN
        // We just care that the CustomModesHandler was created. The custom mode behaviour is tested in the custom_mode.rs tests.
        assert!(res.is_ok());
    }

    fn get_publisher(
        recv: Receiver<()>,
    ) -> (MockPublisher<VehicleStateChange>, Arc<Mutex<MockMessageQueue>>) {
        let message_queue = MockMessageQueue::default();
        let message_queue_arc = Arc::new(Mutex::new(message_queue));
        let msg_queue_clone = message_queue_arc.clone();

        (
            MockPublisher::<VehicleStateChange>::new(
                Box::new(MockPublish::new(msg_queue_clone)),
                recv,
            ),
            message_queue_arc.clone(),
        )
    }

    fn mock_orch_sd_registration(
        mock_orch_comms: &mut MockOrchestrationComms,
    ) -> Arc<Mutex<Option<Sender<ServiceUnitChangeEvent>>>> {
        let sender_wrapper: Arc<Mutex<Option<Sender<ServiceUnitChangeEvent>>>> =
            Arc::new(Mutex::new(None));

        let sender_wrapper_clone = sender_wrapper.clone();
        mock_orch_comms.expect_subscribe_service_unit_change_by_name().returning(
            move |sender, _| {
                sender_wrapper_clone.lock().unwrap().replace(sender.clone());
                let sender_clone = sender.clone();
                tokio::spawn(async move {
                    // Register the vehicle mode publisher for the test so it can be observed using MW
                    let fqin = get_fqin("fake-instance");
                    let res = sender_clone
                        .send(ServiceUnitChangeEvent::Registered(ServiceUnitIdentity {
                            name: "service-vehicle".to_string(),
                            fqin,
                        }))
                        .await;
                    assert!(res.is_ok());
                });
                Ok(())
            },
        );
        sender_wrapper
    }

    fn get_mock_orch_comms(
    ) -> (MockOrchestrationComms, MockPublisher<VehicleStateChange>, Arc<Mutex<MockMessageQueue>>)
    {
        let mut mock_orch_comms = MockOrchestrationComms::default();
        let (sender, recv) = channel::<()>(1);
        let (publisher, message_queue_arc) = get_publisher(recv);
        let mock_publication = MockPublication::new(message_queue_arc.clone(), sender);
        let mock_publication_arc = Arc::new(mock_publication.clone());
        mock_orch_comms
            .expect_get_publication()
            .times(1)
            .returning(move |_| Ok(mock_publication_arc.clone()));
        (mock_orch_comms, publisher, message_queue_arc)
    }

    fn get_fqin(instance_name: &str) -> ServiceFqin {
        ServiceFqin::builder()
            .sdv_vm_name("vm")
            .sdv_package_name("sdv.orch.test")
            .service_bundle_name("FakeVehicleBundle")
            .service_instance_name(instance_name)
            .build()
            .unwrap()
    }
}
