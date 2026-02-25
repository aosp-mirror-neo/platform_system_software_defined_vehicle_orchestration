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
//! Implementation of custom modes. It uses RPC server to receive custom mode updates from
//! service bundles, which are then propagated and consumed by the orchestrators through pub/sub.
//! Uses MW generated bundles for RPC and DT.

use crate::states::orchestration_comms::{OrchestrationComms, ServiceUnitChangeEvent};
use crate::states::states_subscriber_interface::StateUpdateRequest;
use dashmap::DashMap;
use futures::StreamExt;
use lazy_static::lazy_static;
use log::{debug, error, info, warn};
use mockall::automock;
use regex::Error as RegexError;
use regex::Regex;
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::UnitType;
use sdv_mw_rs_com_android_sdv_orchestrator_orchestrator_service_bundle::publisher;
use sdv_mw_rs_com_android_sdv_orchestrator_orchestrator_service_bundle::OrchestratorServiceBundle;
use sdv_mw_rs_comms::Publisher;
use sdv_mw_rs_comms::{SubscribeOptions, UnitName, UnitQuery};
use sdv_orch_api_stable::global_custom_modes::GlobalCustomStateChange;
use sdv_orch_api_stable::local_custom_modes::{SetCustomStateRequest, SetCustomStateResponse};
use sdv_orchestration_common::fqin::Fqin;
use sdv_orchestration_common::mode::{Mode, UpdateEvent::StateChange};
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use sdv_status_rs::SdvStatusCode;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::sleep;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::event;
use tracing::Level;

lazy_static! {
    /// Custom mode name and value must be a string that can not be longer than 56 characters and
    /// can only contain letters, numbers, dashes, dots and underscores: [a-zA-Z0-9_-.].
    /// We use this regex to validate this condition on custom mode value and name.
    static ref CUSTOM_MODE_VALUE_REGEX: Result<Regex, RegexError> = Regex::new(r"^[\w_\-.]{1,56}$");
}
const POLLING_TIME: Duration = Duration::from_millis(100);

/// Used to mock in tests the pubsub publisher from mw library.
#[automock]
trait OrchServiceBundle: Send {
    fn get_custom_modes_publisher(&mut self) -> Option<Publisher<GlobalCustomStateChange>>;
}

impl OrchServiceBundle for OrchestratorServiceBundle {
    fn get_custom_modes_publisher(&mut self) -> Option<Publisher<GlobalCustomStateChange>> {
        self.take_publisher(publisher::Variant::UNIQUE)
    }
}

#[derive(Clone)]
/// Creates an RPC server that listens to custom modes updates and then propagates (and consumes) them
/// with pub/sub. Once a value is received through DT, it forwards the value to the listener.
pub(crate) struct CustomModesHandler {
    custom_modes: Arc<Mutex<HashMap<Fqin, HashSet<String>>>>,
    sender: Sender<SetCustomStateRequest>,
}

#[async_trait::async_trait]
impl com_android_sdv_orchestrator_custom_state_service_rpc::Interface for CustomModesHandler {
    async fn SetCustomState(
        &self,
        caller_id: sdv_comms::id::ServiceFqin,
        message: &sdv_orch_api_stable::local_custom_modes::SetCustomStateRequest,
    ) -> sdv_status_rs::SdvResult<sdv_orch_api_stable::local_custom_modes::SetCustomStateResponse>
    {
        info!("RPC custom state server received value {:?}", message);
        event!(
            tracing::Level::INFO,
            "Received custom transition request (through RPC) {}",
            message
        );

        match &*CUSTOM_MODE_VALUE_REGEX {
            Ok(regex) => {
                if !regex.is_match(&message.mode) {
                    error!("Invalid custom mode entered: {:?}.", &message.mode);
                    return Err(SdvStatus::with_message(
                        SdvStatusCode::InvalidArgument,
                        "Invalid custom mode entered.",
                    ));
                }
                if !regex.is_match(&message.value) {
                    error!("Invalid custom state value entered: {:?}.", &message.value);
                    return Err(SdvStatus::with_message(
                        SdvStatusCode::InvalidArgument,
                        "Invalid custom state value entered.",
                    ));
                }
            }
            Err(err) => {
                return Err(SdvStatus::with_message(
                    SdvStatusCode::Internal,
                    format!("Failed to create REGEX to validate custom modes: {err:?}"),
                ));
            }
        }

        if self
            .custom_modes
            .lock()
            .unwrap()
            .get(&Fqin::new(
                caller_id.get_sdv_package_name(),
                caller_id.get_service_bundle_name(),
                "", // We do not store instances in the config because the allowed publishers are per service bundle, not instance.
            ))
            .unwrap_or(&HashSet::new())
            .contains(&message.mode)
        {
            event!(
                tracing::Level::INFO,
                "Sending custom transition request for propagation {}",
                message
            );
            let res = self.sender.send(message.clone()).await;
            if let Err(err) = res {
                error!("Error while propagating custom mode value: {err:?}.");
                return Err(SdvStatus::with_message(
                    SdvStatusCode::Unknown,
                    "Error while processing custom mode value.",
                ));
            } else {
                return Ok(SetCustomStateResponse { ..Default::default() });
            }
        } else {
            error!(
                "Service bundle with name '{}' and package '{}' is not allowed to publish custom mode '{:?}'.",
                caller_id.get_service_bundle_name(),
                caller_id.get_sdv_package_name(),
                message
            );
            return Err(SdvStatus::with_message(
                SdvStatusCode::PermissionDenied,
                "Service bundle is not allowed to publish to the custom mode.",
            ));
        }
    }
}

impl CustomModesHandler {
    /// Creates a new state subscribers with the given orchestrator configuration for reading custom modes
    pub async fn new(
        custom_modes: Arc<Mutex<HashMap<Fqin, HashSet<String>>>>,
        update_sender: Sender<Arc<StateUpdateRequest>>,
        orchestration_comms: Arc<dyn OrchestrationComms>,
    ) -> CustomModesHandler {
        let (sender, receiver) = channel::<SetCustomStateRequest>(1);
        let handler = Self { custom_modes, sender };
        let server: Arc<CustomModesHandler> = Arc::new(handler.clone());
        let orch_bundle = orchestration_comms.get_orch_service_bundle((server,)).await;

        Self::set_up_custom_modes(
            update_sender,
            orch_bundle.map(|bundle| Box::new(bundle) as Box<dyn OrchServiceBundle>),
            receiver,
            orchestration_comms,
            Arc::new(DashMap::new()),
            POLLING_TIME,
        )
        .await;

        handler
    }

    #[cfg(test)]
    async fn new_with_bundle(
        custom_modes: Arc<Mutex<HashMap<Fqin, HashSet<String>>>>,
        update_sender: Sender<Arc<StateUpdateRequest>>,
        orchestration_comms: Arc<dyn OrchestrationComms>,
        bundle: SdvResult<Box<dyn OrchServiceBundle>>,
        custom_mode_subscriptions: Arc<DashMap<String, Arc<CancellationToken>>>,
    ) -> Self {
        let (custom_state_request_sender, custom_state_request_receiver) =
            channel::<SetCustomStateRequest>(1);
        let handler: CustomModesHandler =
            Self { custom_modes, sender: custom_state_request_sender };

        Self::set_up_custom_modes(
            update_sender,
            bundle,
            custom_state_request_receiver,
            orchestration_comms,
            custom_mode_subscriptions,
            Duration::ZERO,
        )
        .await;

        handler
    }

    async fn set_up_custom_modes(
        update_sender: Sender<Arc<StateUpdateRequest>>,
        orch_bundle: SdvResult<Box<dyn OrchServiceBundle>>,
        receiver: Receiver<SetCustomStateRequest>,
        orchestration_comms: Arc<dyn OrchestrationComms>,
        custom_mode_subscriptions: Arc<DashMap<String, Arc<CancellationToken>>>,
        polling_time: Duration,
    ) {
        let orch_comms_clone = orchestration_comms.clone();
        tokio::spawn(async move {
            subscribe_to_custom_modes(
                update_sender,
                orch_comms_clone,
                custom_mode_subscriptions,
                polling_time,
            )
            .await;
        });

        if let Ok(bundle) = orch_bundle {
            tokio::spawn(async move {
                wait_for_mode_update(bundle, receiver).await;
            });
        } else if let Err(ref err) = orch_bundle {
            error!("Failed to create subscriber for custom modes propagation {err:?}");
        }
    }
}

async fn wait_for_mode_update(
    mut bundle: Box<dyn OrchServiceBundle>,
    mut receiver: Receiver<SetCustomStateRequest>,
) {
    if let Some(publisher) = bundle.get_custom_modes_publisher() {
        while let Some(custom_state_request) = receiver.recv().await {
            event!(
                tracing::Level::INFO,
                "Propagating custom transition request to orchestrators {}",
                custom_state_request
            );
            let res = publisher.publish(&GlobalCustomStateChange {
                name: custom_state_request.mode.to_string(),
                value: custom_state_request.value.to_string(),
                timestamp: custom_state_request.timestamp,
                ..Default::default()
            });
            info!(
                "Propagated custom mode {:?} with value {:?} and with result {:?}",
                custom_state_request.mode, custom_state_request.value, res
            );
        }
    } else {
        error!("Error creating custom mode publisher");
    }
}

async fn subscribe_to_custom_modes(
    update_sender: Sender<Arc<StateUpdateRequest>>,
    orchestration_comms: Arc<dyn OrchestrationComms>,
    custom_mode_subscriptions: Arc<DashMap<String, Arc<CancellationToken>>>,
    polling_time: Duration,
) {
    if let Ok(unit_type) = UnitType::builder()
        .set_sdv_package_name("com.android.sdv.orchestrator")
        .set_service_bundle_name("OrchestratorServiceBundle")
        .set_unit_type_name("GlobalCustomStateChange")
        .build()
    {
        let (sender, mut receiver) = channel::<ServiceUnitChangeEvent>(1);
        let comms_clone = orchestration_comms.clone();
        tokio::spawn(async move {
            let res = comms_clone.subscribe_service_unit_change_by_type(sender, unit_type).await;
            if let Err(err) = res {
                warn!("Error initialising Service discovery Manager for custom modes: {err:?}");
            }
        });
        while let Some(unit_change_event) = receiver.recv().await {
            match unit_change_event {
                ServiceUnitChangeEvent::Registered(service_identity) => {
                    info!(
                        "New custom mode publisher registered in vm {:?}",
                        service_identity.fqin.get_sdv_vm_name()
                    );
                    handle_individual_subscription(
                        service_identity.name,
                        service_identity.fqin,
                        orchestration_comms.clone(),
                        update_sender.clone(),
                        Arc::clone(&custom_mode_subscriptions),
                        polling_time,
                    );
                }
                ServiceUnitChangeEvent::Unregistered(service_identity) => {
                    let vm_name = service_identity.fqin.get_sdv_vm_name();
                    info!("Custom mode publisher unregistered in vm {vm_name:?}");
                    if let Some(entry) = custom_mode_subscriptions.get(vm_name) {
                        entry.value().cancel();
                    }
                }
            }
        }
    } else {
        error!("Failed to create unit type for GlobalCustomStateChange");
    }
}

#[allow(non_snake_case)]
// The returned JoinHandle is used only in tests to verify that the spawned tasks exits on same scenarios.
fn handle_individual_subscription(
    unit_name: String,
    fqin: ServiceFqin,
    orchestration_comms: Arc<dyn OrchestrationComms>,
    update_sender: Sender<Arc<StateUpdateRequest>>,
    custom_mode_subscriptions: Arc<DashMap<String, Arc<CancellationToken>>>,
    polling_time: Duration,
) -> tokio::task::JoinHandle<()> {
    let vm_name = fqin.get_sdv_vm_name().to_owned();
    debug!("Subscribing to new Service Unit Definition with unit name {:?}, bundle name {:?}, package {:?} and vm {:?}",
    unit_name,
    fqin.get_service_bundle_name(),
    fqin.get_sdv_package_name(),
    vm_name.clone()
    );

    if let Some(entry) = custom_mode_subscriptions.get(&vm_name) {
        // We should never encounter this case as it means we got two registrations without an unregistration in the middle.
        // But in case we do, we proceed to cancel the previous subscription and start a new one.
        if !entry.is_cancelled() {
            warn!("Previous subscription for custom modes in vm {vm_name:?} is still active. Cancelling old subscription and creating a new one.");
            entry.cancel();
        }
    }

    // At this point there's either no entry or the existing one was cancelled. So we create a new subscription task.
    let cancellation_token = Arc::new(CancellationToken::new());
    custom_mode_subscriptions.insert(vm_name.clone(), cancellation_token.clone());

    tokio::spawn(async move {
        debug!("Starting new custom mode subscription for vm {vm_name:?}");
        tokio::select! {
            _ = start_custom_mode_subscription(
                update_sender,
                orchestration_comms,
                &unit_name,
                &vm_name,
                polling_time,
            ) => {
                debug!("Custom mode subscription for vm {vm_name:?} finished.");
            },
            // When cancelled, the tokio::select will drop the executing task in the other branch so
            // we don't need to worry about explicitly aborting the task to avoid leaked background tasks.
            _ = cancellation_token.cancelled() => {
                info!("Custom mode subscription for vm {vm_name:?} was cancelled.");
            }
        }
    })
}

async fn start_custom_mode_subscription(
    update_sender: Sender<Arc<StateUpdateRequest>>,
    orchestration_comms: Arc<dyn OrchestrationComms>,
    unit_name: &str,
    vm_name: &str,
    polling_time: Duration,
) {
    loop {
        let observer_result =
            create_observer(orchestration_comms.clone(), unit_name, vm_name).await;

        match observer_result {
            Ok(observer) => {
                info!("Successfully subscribed for custom modes in vm {vm_name:?}");
                let subscr_success_log = format!(
                    "sdv_orchestration_agent custom subscription vm {vm_name} has started successfully"
                );
                info!("{subscr_success_log}");
                tracing::event!(Level::DEBUG, subscr_success_log);

                observe_publications_while_publisher_alive(observer, update_sender.clone()).await;
                info!(
                    "Custom mode publisher from vm {vm_name:} was terminated. Attempting to re-subscribe."
                );

                // We sleep a while before trying to resubscribe (in case of a problem with the cross-VM DT agent/interruption).
                // The loop will attempt to re-create the observer.
                // If publisher has unregistered, creation will fail as the token will be cancelled.
                // If the publisher is still alive, it should succeed as DT queues the subscriptions.
                sleep(polling_time).await;
            }
            Err(err) => {
                // DT will not return an error as it will queue the request. This means we do not need to retry the connection.
                warn!("Error subscribing to custom modes in vm {vm_name:?} with error {err:?}.");
                return;
            }
        }
    }
}

async fn observe_publications_while_publisher_alive(
    mut observer: std::pin::Pin<
        Box<
            dyn futures::stream::Stream<
                    Item = sdv_status_rs::SdvResult<Vec<GlobalCustomStateChange>>,
                > + Send,
        >,
    >,
    update_sender: Sender<Arc<StateUpdateRequest>>,
) {
    while let Some(Ok(messages)) = observer.next().await {
        debug!("Received new custom mode publication: {messages:?}");
        for message in messages {
            event!(tracing::Level::INFO, "Processing custom transition request {}", message);
            let res = update_sender
                .send(Arc::new(StateUpdateRequest {
                    update_event: StateChange(
                        Mode::Custom(message.name.clone()),
                        message.value.clone(),
                    ),
                    state_notifier: Mutex::new(None),
                    cancel_token: CancellationToken::new(),
                    timestamp: message.timestamp.into_option(),
                }))
                .await;
            if res.is_err() {
                warn!(
                    "Failed to send custom state update '{}' with value '{}'.",
                    message.name, message.value
                );
            }
        }
    }
}

async fn create_observer(
    orchestration_comms: Arc<dyn OrchestrationComms>,
    unit_name: &str,
    vm_name: &str,
) -> sdv_status_rs::SdvResult<
    std::pin::Pin<
        Box<
            dyn futures::stream::Stream<
                    Item = sdv_status_rs::SdvResult<Vec<GlobalCustomStateChange>>,
                > + Send,
        >,
    >,
> {
    let publication = orchestration_comms
        .get_publication(&UnitQuery::Name(
            UnitName::builder()
                .vm_name(vm_name)
                .package_name("com.android.sdv.orchestrator")
                .bundle_name("OrchestratorServiceBundle")
                .service_unit_name(unit_name)
                .build()?,
        ))
        .await;
    if let Ok(public) = publication {
        if let Ok(observer) = public.observe(&SubscribeOptions::default()).await {
            Ok(sdv_mw_rs_comms::observe::<GlobalCustomStateChange>(observer))
        } else {
            warn!("Failed to find publication observer");
            Err(SdvStatus::with_message(
                SdvStatusCode::Unknown,
                "Error finding custom modes publication.",
            ))
        }
    } else {
        warn!("Failed to find publication observer");
        Err(SdvStatus::with_message(
            SdvStatusCode::Unknown,
            "Error finding custom modes publication.",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::states::orchestration_comms::MockOrchestrationComms;
    use crate::states::orchestration_comms::ServiceUnitIdentity;
    use com_android_sdv_orchestrator_custom_state_service_rpc::Interface;
    use futures::stream;
    use protobuf::well_known_types::timestamp::Timestamp;
    use protobuf::MessageField;
    use sdv_comms::id::ServiceFqin;
    use sdv_orch_test_mocks::custom_modes_publication::*;
    use sdv_orchestration_common::mode::Mode::Custom;
    use tokio::sync::Mutex as AsyncMutex;

    const PACKAGE: &str = "sdv.package.test";
    const BUNDLE: &str = "CustomModeBundle";

    fn get_fqin(instance: &str) -> ServiceFqin {
        ServiceFqin::builder()
            .sdv_vm_name("vm")
            .sdv_package_name(PACKAGE)
            .service_bundle_name(BUNDLE)
            .service_instance_name(instance)
            .build()
            .unwrap()
    }

    fn get_config(mode: String) -> Arc<Mutex<HashMap<Fqin, HashSet<String>>>> {
        let mut config = HashMap::new();
        let mut modes = HashSet::new();
        modes.insert(mode);
        config.insert(Fqin::new(PACKAGE, BUNDLE, ""), modes);
        Arc::new(Mutex::new(config))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sending_invalid_custom_mode_name_should_return_error() {
        // GIVEN
        let (mock_service_bundle, mock_orch_comms) = get_mocks();
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let mode = "$#*&^$".to_string();
        let config = get_config(mode.clone());
        let handler = CustomModesHandler::new_with_bundle(
            config,
            update_sender,
            Arc::new(mock_orch_comms),
            Ok(Box::new(mock_service_bundle)),
            Arc::new(DashMap::new()),
        )
        .await;
        // WHEN
        let result = handler
            .SetCustomState(
                get_fqin("test-invalid-custom-name"),
                &SetCustomStateRequest {
                    mode,
                    value: "TEST".to_string(),
                    timestamp: MessageField::some(Timestamp::now()),
                    ..Default::default()
                },
            )
            .await;
        // THEN
        assert!(result.is_err());
        assert!(result.unwrap_err().status_code() == SdvStatusCode::InvalidArgument);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sending_invalid_custom_mode_value_should_return_error() {
        // GIVEN
        let (mock_service_bundle, mock_orch_comms) = get_mocks();
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let mode = "TESTING".to_string();
        let config = get_config(mode.clone());
        let handler = CustomModesHandler::new_with_bundle(
            config,
            update_sender,
            Arc::new(mock_orch_comms),
            Ok(Box::new(mock_service_bundle)),
            Arc::new(DashMap::new()),
        )
        .await;
        // WHEN
        let result = handler
            .SetCustomState(
                get_fqin("test-invalid-custom-value"),
                &SetCustomStateRequest {
                    mode,
                    value: "@#$SD".to_string(),
                    timestamp: MessageField::some(Timestamp::now()),
                    ..Default::default()
                },
            )
            .await;
        // THEN
        assert!(result.is_err());
        assert!(result.unwrap_err().status_code() == SdvStatusCode::InvalidArgument);
    }

    // We consider a bundle to be unauthorized in the orchestrator context if it's trying to publish
    // to a custom modes, but it has not defined in the orch config that it's allowed to publish
    // to that custom mode. For this test, the mode is name is UNSUPPORTED_MODE and it has not be configured
    // that the test bundle is authorized to publish to it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sending_custom_update_from_unauthorized_bundle_should_return_error() {
        // GIVEN
        let (mock_service_bundle, mock_orch_comms) = get_mocks();
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let mode = "UNSUPPORTED_MODE".to_string();
        let config = get_config("TESTING".to_string());
        let handler = CustomModesHandler::new_with_bundle(
            config,
            update_sender,
            Arc::new(mock_orch_comms),
            Ok(Box::new(mock_service_bundle)),
            Arc::new(DashMap::new()),
        )
        .await;
        // WHEN
        let result = handler
            .SetCustomState(
                get_fqin("test-unauthorized-bundle"),
                &SetCustomStateRequest {
                    mode,
                    value: "TEST".to_string(),
                    timestamp: MessageField::some(Timestamp::now()),
                    ..Default::default()
                },
            )
            .await;
        // THEN
        assert!(result.is_err());
        assert!(result.unwrap_err().status_code() == SdvStatusCode::PermissionDenied);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sending_custom_mode_should_succeed_and_propagate_value() {
        // GIVEN
        let (mock_service_bundle, mock_orch_comms) = get_mocks();
        let (update_sender, mut update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let mode = "TEST_PROPAGATE".to_string();
        let value = "PROPAGATION".to_string();
        let config = get_config(mode.clone());
        let handler = CustomModesHandler::new_with_bundle(
            config,
            update_sender,
            Arc::new(mock_orch_comms),
            Ok(Box::new(mock_service_bundle)),
            Arc::new(DashMap::new()),
        )
        .await;
        // WHEN a new custom mode is set through RPC
        let result = handler
            .SetCustomState(
                get_fqin("test-propagate-value"),
                &SetCustomStateRequest {
                    mode: mode.clone(),
                    value: value.clone(),
                    timestamp: MessageField::some(Timestamp::now()),
                    ..Default::default()
                },
            )
            .await;
        // THEN the update_receiver should receive the new value
        assert!(result.is_ok());
        let upd = update_receiver.recv().await;
        assert!(upd.is_some());
        let state_update_request = upd.expect("State update request error");
        assert!(state_update_request.update_event == StateChange(Custom(mode), value));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_value_should_not_be_propagated_if_failed_to_get_publication() {
        // GIVEN
        let (update_sender, update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let mode = "TEST_NO_OBSERVER".to_string();
        let config = get_config(mode.clone());
        let (mock_service_bundle, mock_message_queue) = get_mock_orch_bundle();
        let mut publication_recv = mock_message_queue.lock().unwrap().observe();
        let mut mock_orch_comms = MockOrchestrationComms::default();
        mock_orch_comms
            .expect_get_publication()
            .returning(|_| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        mock_orch_sd_registration(&mut mock_orch_comms);
        let handler = CustomModesHandler::new_with_bundle(
            config,
            update_sender,
            Arc::new(mock_orch_comms),
            Ok(Box::new(mock_service_bundle)),
            Arc::new(DashMap::new()),
        )
        .await;
        // WHEN a new custom mode is set through RPC
        let result = handler
            .SetCustomState(
                get_fqin("test-propagate-value"),
                &SetCustomStateRequest {
                    mode: mode.clone(),
                    value: "TEST".to_string(),
                    timestamp: MessageField::some(Timestamp::now()),
                    ..Default::default()
                },
            )
            .await;
        assert!(result.is_ok());
        // THEN
        // Wait until the message was published
        publication_recv.next().await;
        // And check that the receiver did not get it
        assert!(update_receiver.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_value_should_not_be_propagated_if_failed_to_observe_publication() {
        // GIVEN
        let (update_sender, update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let mode = "TEST_FAIL_OBSERVER".to_string();
        let config = get_config(mode.clone());

        let (mock_service_bundle, mock_message_queue) = get_mock_orch_bundle();
        let mut mock_orch_comms = MockOrchestrationComms::default();
        mock_orch_comms
            .expect_get_publication()
            .returning(move |_| Ok(Arc::new(FailureMockPublication::default())));
        mock_orch_sd_registration(&mut mock_orch_comms);

        let mut publication_recv = mock_message_queue.lock().unwrap().observe();

        let handler = CustomModesHandler::new_with_bundle(
            config,
            update_sender,
            Arc::new(mock_orch_comms),
            Ok(Box::new(mock_service_bundle)),
            Arc::new(DashMap::new()),
        )
        .await;
        // WHEN a new custom mode is set through RPC
        let result = handler
            .SetCustomState(
                get_fqin("test-propagate-value"),
                &SetCustomStateRequest {
                    mode: mode.clone(),
                    value: "TEST".to_string(),
                    timestamp: MessageField::some(Timestamp::now()),
                    ..Default::default()
                },
            )
            .await;
        assert!(result.is_ok());
        // THEN
        // Wait until the message was published
        publication_recv.next().await;
        // And check that the receiver did not get it
        assert!(update_receiver.is_empty());
    }

    // This test checks that the SetCustomState request fails if the orch service bundle
    // failed to provide the publisher that will propagate custom modes through DT.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sending_custom_mode_should_fail_if_no_publisher() {
        // GIVEN
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let mode = "TEST_NO_PUBLISHER".to_string();
        let config = get_config(mode.clone());
        let mut mock_service_bundle = MockOrchServiceBundle::default();
        mock_service_bundle.expect_get_custom_modes_publisher().times(1).returning(move || None);
        let mut mock_orch_comms = MockOrchestrationComms::default();
        mock_orch_comms
            .expect_subscribe_service_unit_change_by_type()
            .returning(|_, _| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        let handler = CustomModesHandler::new_with_bundle(
            config,
            update_sender,
            Arc::new(mock_orch_comms),
            Ok(Box::new(mock_service_bundle)),
            Arc::new(DashMap::new()),
        )
        .await;
        // WHEN a new custom mode is set through RPC
        let result = handler
            .SetCustomState(
                get_fqin("test-propagate-value"),
                &SetCustomStateRequest {
                    mode: mode.clone(),
                    value: "TEST".to_string(),
                    timestamp: MessageField::some(Timestamp::now()),
                    ..Default::default()
                },
            )
            .await;
        // THEN the request should fail as there's no custom mode publisher (so the receiver was dropped)
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sending_custom_mode_should_fail_if_no_orch_bundle() {
        // GIVEN
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let mode = "TEST_NO_BUNDLE".to_string();
        let config = get_config(mode.clone());
        let mut mock_orch_comms = MockOrchestrationComms::default();
        mock_orch_comms
            .expect_subscribe_service_unit_change_by_type()
            .returning(|_, _| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        let handler = CustomModesHandler::new_with_bundle(
            config,
            update_sender,
            Arc::new(mock_orch_comms),
            Err(SdvStatus::new(SdvStatusCode::Unknown)),
            Arc::new(DashMap::new()),
        )
        .await;
        // WHEN a new custom mode is set through RPC
        let result = handler
            .SetCustomState(
                get_fqin("test-propagate-value"),
                &SetCustomStateRequest {
                    mode: mode.clone(),
                    value: "TEST".to_string(),
                    timestamp: MessageField::some(Timestamp::now()),
                    ..Default::default()
                },
            )
            .await;
        // THEN the request should fail as there's no orch bundle (so the receiver was dropped)
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_should_cancel_subscription_when_subscribing_multiple_times() {
        // GIVEN
        let mock_publication_arc =
            Arc::new(MockPublication::new(Arc::new(Mutex::new(MockMessageQueue::default()))));
        let mut mock_orch_comms = MockOrchestrationComms::default();

        // We use this channel to notify when the observer is trying to be created
        let (registered_sender, mut registered_recv) = channel::<()>(1);

        // THEN get_publication should have been called twice (once for each subscription)
        mock_orch_comms.expect_get_publication().times(2).returning(move |_| {
            let res = registered_sender.try_send(());
            assert!(res.is_ok());
            Ok(mock_publication_arc.clone())
        });

        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let custom_mode_subscriptions = Arc::new(DashMap::new());
        let fqin = get_fqin("fake-publisher");
        let orch_comms_arc = Arc::new(mock_orch_comms);

        // Subscribe for the first time
        let first_task_handle = handle_individual_subscription(
            "service-unit-name".to_string(),
            fqin.clone(),
            orch_comms_arc.clone(),
            update_sender.clone(),
            custom_mode_subscriptions.clone(),
            Duration::ZERO,
        );

        // Wait until the first subscription is active.
        registered_recv.recv().await.expect("First subscription should be active");

        let first_cancellation_token = custom_mode_subscriptions.get("vm").unwrap().clone();

        // WHEN subscribing for the second time for the same VM
        let second_task_handle = handle_individual_subscription(
            "service-unit-name".to_string(),
            fqin.clone(),
            orch_comms_arc.clone(),
            update_sender.clone(),
            custom_mode_subscriptions.clone(),
            Duration::ZERO,
        );

        // First token should have been cancelled when we subscribe for the second time.
        // We don't need to add a delay or wait for the cancel because that is done synchronously in the
        // handle_individual_subscription before spawing the new tokio task.
        assert!(first_cancellation_token.is_cancelled());
        // Wait until first task exits.
        let result = tokio::time::timeout(Duration::from_secs(1), first_task_handle).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());

        // Now wait until the second subscription is active.
        registered_recv.recv().await.expect("First subscription should be active");

        // THEN the test should complete without panicking.
        // We cancel the second task to allow the test to finish gracefully.
        let token = custom_mode_subscriptions.get("vm").unwrap().clone();
        token.cancel();
        // Wait until second task exits.
        let result = tokio::time::timeout(Duration::from_secs(1), second_task_handle).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_subscribe_again_if_observer_dropped() {
        // GIVEN
        let (mock_service_bundle, message_queue_arc) = get_mock_orch_bundle();
        let mock_publication = MockPublication::new(message_queue_arc.clone());
        let mock_publication_arc = Arc::new(mock_publication.clone());
        let mut mock_orch_comms = MockOrchestrationComms::default();

        // We use this channel to notify when the observer is trying to be created
        let (registered_sender, mut registered_recv) = channel::<()>(1);
        let registered_sender_clone = registered_sender.clone();
        // We should have subscribed twice: one initially and one after observed dropped.
        mock_orch_comms.expect_get_publication().times(2).returning(move |_| {
            let registered_sender_clone = registered_sender_clone.clone();
            tokio::spawn(async move {
                let res = registered_sender_clone.send(()).await;
                assert!(res.is_ok());
            });
            Ok(mock_publication_arc.clone())
        });
        mock_orch_sd_registration(&mut mock_orch_comms);
        // Create the handler
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        CustomModesHandler::new_with_bundle(
            Default::default(),
            update_sender,
            Arc::new(mock_orch_comms),
            Ok(Box::new(mock_service_bundle)),
            Arc::new(DashMap::new()),
        )
        .await;
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
        let (mock_service_bundle, message_queue_arc) = get_mock_orch_bundle();
        let mock_publication = MockPublication::new(message_queue_arc.clone());
        let mut publication_recv = message_queue_arc.lock().unwrap().observe();
        let mock_publication_arc = Arc::new(mock_publication.clone());
        let mut mock_orch_comms = MockOrchestrationComms::default();

        // We use this channel to notify when the observer is trying to be created
        let (registered_sender, mut registered_recv) = channel::<()>(1);
        let registered_sender_clone = registered_sender.clone();
        // We only subscribe to the publisher once on initial registration.
        // After we the observer gets dropped, we should have already unregistered so there should
        // not be any other subscription made.
        mock_orch_comms.expect_get_publication().times(1).returning(move |_| {
            let registered_sender_clone = registered_sender_clone.clone();
            tokio::spawn(async move {
                let res = registered_sender_clone.send(()).await;
                assert!(res.is_ok());
            });
            Ok(mock_publication_arc.clone())
        });

        // We intercept the sender to unregister later in the test
        let unit_change_sender: Arc<AsyncMutex<Option<Sender<ServiceUnitChangeEvent>>>> =
            Arc::new(AsyncMutex::new(None));
        let unit_change_sender_clone = unit_change_sender.clone();
        mock_orch_comms.expect_subscribe_service_unit_change_by_type().returning(
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
        let custom_subscription = Arc::new(DashMap::new());
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        CustomModesHandler::new_with_bundle(
            Default::default(),
            update_sender,
            Arc::new(mock_orch_comms),
            Ok(Box::new(mock_service_bundle)),
            custom_subscription.clone(),
        )
        .await;

        // We need to wait until the observer was created to later drop it
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
        let vm_name_to_check = "vm".to_string();
        // Unregister should be fast, so we timeout and fail the test if it did not happen.
        let timeout_duration = tokio::time::Duration::from_millis(500);
        let wait_result = tokio::time::timeout(timeout_duration, async {
            loop {
                // Wait until the token was cancelled to indicate that the subscription ended.
                if custom_subscription.get(&vm_name_to_check).unwrap().is_cancelled() {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(wait_result.is_ok());

        // WHEN
        // The stream gets closed if we drop all the senders
        message_queue_arc.lock().unwrap().drop_stream();

        // THEN
        // The channel returns None if the channel has been closed
        let next_item = publication_recv.next().await;
        assert!(next_item.is_none());
    }

    #[tokio::test]
    async fn test_observe_publications_finishes_on_cancellation() {
        // GIVEN a stream that never ends
        let observer = stream::pending::<SdvResult<Vec<GlobalCustomStateChange>>>();
        let pinned_observer: std::pin::Pin<
            Box<dyn futures::stream::Stream<Item = SdvResult<Vec<GlobalCustomStateChange>>> + Send>,
        > = Box::pin(observer);
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let handle = tokio::spawn(async move {
            observe_publications_while_publisher_alive(pinned_observer, update_sender).await;
        });
        // Wait a bit to ensure the task is awaiting for the observer
        tokio::time::sleep(Duration::from_millis(10)).await;
        // WHEN aborting the task
        handle.abort();
        // THEN the task should finish observing
        let result = handle.await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn test_custom_mode_subscription_should_return_on_cancellation() {
        let mock_publication_arc =
            Arc::new(MockPublication::new(Arc::new(Mutex::new(MockMessageQueue::default()))));
        let mut mock_orch_comms = MockOrchestrationComms::default();
        // We use this channel to notify when the observer is trying to be created
        let (registered_sender, mut registered_recv) = channel::<()>(1);
        mock_orch_comms.expect_get_publication().returning(move |_| {
            let res = registered_sender.try_send(());
            assert!(res.is_ok());
            Ok(mock_publication_arc.clone())
        });

        let orch_comms_arc = Arc::new(mock_orch_comms);
        let (update_sender, _) = channel::<Arc<StateUpdateRequest>>(1);
        let fqin = get_fqin("fake-publisher");
        let subscriptions = Arc::new(DashMap::new());
        // WHEN starting a custom mode subscription
        let handle = handle_individual_subscription(
            "service-unit-name".to_string(),
            fqin.clone(),
            orch_comms_arc.clone(),
            update_sender.clone(),
            Arc::clone(&subscriptions),
            Duration::ZERO,
        );
        // Wait until the observer was registered before cancelling.
        let res = registered_recv.recv().await;
        assert!(res.is_some());
        // Wait a bit longer to ensure the observe_publications_while_publisher_alive is awaiting the observer.
        tokio::time::sleep(Duration::from_millis(10)).await;
        // We assume at this point that we are awaiting the observer, so we cancel to check that the fn returns.
        subscriptions.get("vm").unwrap().cancel();

        // THEN the task finishes gracefully instead of running forever.
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok());
        // Check that the task did not panic
        assert!(result.unwrap().is_ok());
    }

    fn get_mock_orch_bundle() -> (MockOrchServiceBundle, Arc<Mutex<MockMessageQueue>>) {
        let message_queue = MockMessageQueue::default();
        let message_queue_arc = Arc::new(Mutex::new(message_queue));
        let msg_queue_clone = message_queue_arc.clone();
        let mut mock_service_bundle = MockOrchServiceBundle::default();
        mock_service_bundle.expect_get_custom_modes_publisher().times(1).returning(move || {
            Some(Publisher::new(Box::new(MockPublisher::new(msg_queue_clone.clone()))))
        });
        (mock_service_bundle, message_queue_arc)
    }

    fn mock_orch_sd_registration(mock_orch_comms: &mut MockOrchestrationComms) {
        mock_orch_comms.expect_subscribe_service_unit_change_by_type().returning(
            move |sender, _| {
                let sender_clone = sender.clone();
                tokio::spawn(async move {
                    // Register the custom mode publisher for the test so it can be observed using MW
                    let fqin = get_fqin("fake-publisher");
                    let res = sender_clone
                        .send(ServiceUnitChangeEvent::Registered(ServiceUnitIdentity {
                            name: "service-unit-name".to_string(),
                            fqin,
                        }))
                        .await;
                    assert!(res.is_ok());
                });
                Ok(())
            },
        );
    }

    // Returns all the mocks needed to mock pubsub (through MW) and Service Discovery.
    fn get_mocks() -> (MockOrchServiceBundle, MockOrchestrationComms) {
        // Mocks for simulating communication using pubsub
        let (mock_service_bundle, message_queue_arc) = get_mock_orch_bundle();
        let mock_publication = MockPublication::new(message_queue_arc);
        let mock_publication_arc = Arc::new(mock_publication.clone());
        let mut mock_orch_comms = MockOrchestrationComms::default();
        mock_orch_comms
            .expect_get_publication()
            .times(1)
            .returning(move |_| Ok(mock_publication_arc.clone()));
        mock_orch_sd_registration(&mut mock_orch_comms);

        (mock_service_bundle, mock_orch_comms)
    }
}
