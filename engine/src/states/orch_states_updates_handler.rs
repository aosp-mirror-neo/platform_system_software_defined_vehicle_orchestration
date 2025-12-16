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
//! Orchestrator lib that contains the logic of subscribing to VPM updates, and
//! forwarding the updates to the engine.

use crate::states::orch_states_subscriber::OrchStateSubscriber;
use crate::states::states_subscriber_interface::StateUpdateRequest;
use crate::StatesClientSubscriber;
use crate::StatesUpdateHandler;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::ILifecycleManager;
use log::{debug, info, warn};
use persistent_binder::BinderConnectionManager;
use rustutils::android::system_properties;
use sdv_dump::DumpStateReporter;
use sdv_orchestration_common::fqin::Fqin;
use sdv_orchestration_common::mode::UpdateEvent::StateChange;
use sdv_orchestration_common::mode::{Mode, UpdateEvent};
use sdv_orchestration_common::vpm_states::power_state_to_string;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const POWER_STATE_PROP: &str = "sdv.orchestrator.power_state";

/// Handle the subscriptions to vehicle, power and custom modes.
/// It also sets the initial power state through the state_update_sender.
#[derive(Clone)]
struct ModeSubscriptionHandler {
    state_update_sender: Sender<Arc<StateUpdateRequest>>,
    client_subscriber: Arc<dyn StatesClientSubscriber>,
    custom_modes: Arc<Mutex<HashMap<Fqin, HashSet<String>>>>,
}

impl ModeSubscriptionHandler {
    fn new(
        state_update_sender: Sender<Arc<StateUpdateRequest>>,
        client_subscriber: Arc<dyn StatesClientSubscriber>,
        custom_modes: Arc<Mutex<HashMap<Fqin, HashSet<String>>>>,
    ) -> Self {
        Self { state_update_sender, client_subscriber, custom_modes }
    }

    async fn start_subscriptions(&self) {
        tokio::join!(
            self.subscribe_to_vehicle_updates(),
            self.subscribe_to_custom_updates(),
            self.subscribe_to_power_updates()
        );
    }

    async fn subscribe_to_power_updates(&self) {
        let initial_power_state_res = self.client_subscriber.subscribe_to_power_updates().await;
        if let Ok(initial_power_state) = initial_power_state_res {
            info!("Successfully subscribed to VPM power states");
            if let Some(power_state_string) = power_state_to_string(initial_power_state) {
                debug!("Setting initial power state {power_state_string:?}.");
                let res = self
                    .state_update_sender
                    .send(Arc::new(StateUpdateRequest {
                        update_event: StateChange(Mode::Power, power_state_string.to_string()),
                        state_notifier: Mutex::new(None),
                        cancel_token: CancellationToken::new(),
                        timestamp: None,
                    }))
                    .await;
                if res.is_err() {
                    warn!("Failed to set initial power state {power_state_string:?}.");
                }
            } else {
                warn!("Invalid initial power state received: {initial_power_state_res:?}");
            }
        } else if let Err(err) = initial_power_state_res {
            warn!("Subscription for VPM power states failed with error: {err:?}");
        }
    }

    async fn subscribe_to_vehicle_updates(&self) {
        let res = self.client_subscriber.subscribe_to_vehicle_updates().await;
        if let Err(err) = res {
            warn!("Error subscribing to vehicle modes: {err:?}");
        }
    }

    async fn subscribe_to_custom_updates(&self) {
        let res =
            self.client_subscriber.subscribe_to_custom_updates(self.custom_modes.clone()).await;
        if let Err(err) = res {
            warn!("Error subscribing to custom modes: {err:?}");
        }
    }
}

/// Uses a channel receiver state_update_receiver to get notified of vehicle, power and custom modes, and adds them to the
/// state_update_queue queue.
/// If the new mode is a power mode, it checks whether it's a cancel one to either remove the last power transition
/// or the current in progress through the current_transaction (if it's a power mode).
/// Once it has processed the new mode, it notifies the StateUpdater through the sender state_update_sender.
struct TransitionQueueHandler {
    /// Tracks the list of modes that need to be processed. Modes are processed one by one,
    /// in the order they were received. This queue is also used for cancelling power transitions.
    /// The TransitionQueueHandler updates this value while the StateUpdater consumes it.
    state_update_queue: Arc<Mutex<VecDeque<Arc<StateUpdateRequest>>>>,
    /// Receiver used to wait for new power, vehicle and custom mode transitions sent by the ModeSubscriptionHandler.
    state_update_receiver: Receiver<Arc<StateUpdateRequest>>,
    /// Sender used to notify to the StateUpdater that there's a new transition to process.
    state_update_sender: Sender<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>,
    /// Contains information about the current transition in progress: the cancellation token of the currently executing task
    /// and which is the current processing mode. This is used for cancelling power transition if requested.
    /// This value is set by the StateUpdater and consumed by this TransitionQueueHandler.
    current_transaction: Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>,
}

/// Queues new state transition requests and decides whether we need to cancel ongoing work based on new modes received.
impl TransitionQueueHandler {
    fn new(
        state_update_queue: Arc<Mutex<VecDeque<Arc<StateUpdateRequest>>>>,
        state_update_receiver: Receiver<Arc<StateUpdateRequest>>,
        state_update_sender: Sender<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>,
    ) -> Self {
        Self {
            state_update_queue,
            state_update_receiver,
            state_update_sender,
            current_transaction: Arc::new(Mutex::new(None)),
        }
    }

    fn set_recover_for_lm_crash(
        &self,
        lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
    ) {
        let updates_queue = self.state_update_queue.clone();
        let state_update_sender = self.state_update_sender.clone();
        let current_transaction = self.current_transaction.clone();
        lm_connection.set_reconnect_listener(Box::new(move |_| {
            // Lock the current transaction so we do not proceed with a new transition.
            let current_transaction_guard = current_transaction.lock().unwrap();
            // We push to the front so that the next mode is the recover of the states of bundles at the moment of crash.
            updates_queue.lock().unwrap().push_front(Arc::new(StateUpdateRequest {
                update_event: UpdateEvent::Recovery,
                state_notifier: Mutex::new(None),
                cancel_token: CancellationToken::new(),
                timestamp: None,
            }));
            // We cancel the current transaction so we can proceed with the recovery as soon as possible
            current_transaction_guard.as_ref().inspect(|(_, cancellation_token)| {
                cancellation_token.cancel();
            });
            drop(current_transaction_guard);
            let res = state_update_sender.blocking_send(current_transaction.clone());
            if res.is_err() {
                warn!("Failed to recover after LM crash.");
            }
        }));
    }

    async fn wait_for_state_updates(&mut self) {
        while let Some(state_update) = self.state_update_receiver.recv().await {
            if let UpdateEvent::StateChange(Mode::Power, state) = &state_update.update_event {
                if state == "SHUTDOWN_CANCELLED" {
                    // Cancel the cancellation token to stop the progress of ongoing request.
                    let mut list_binding = self.state_update_queue.lock().unwrap();
                    let len = list_binding.len();
                    let mut found = false;
                    // We need to iterate through newest to oldest (reverse order), in order to remove the last power transition received.
                    for (pos, state) in list_binding.iter().rev().enumerate() {
                        if let UpdateEvent::StateChange(Mode::Power, _) = &state.update_event {
                            list_binding.remove(len - pos - 1);
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        self.current_transaction.lock().unwrap().as_ref().inspect(
                            |(event, cancellation_token)| {
                                // If we were currently processing a power transition and we haven't found one in the queue,
                                // then it was the one that needs to be canceled.
                                if let UpdateEvent::StateChange(Mode::Power, _) = event {
                                    cancellation_token.cancel();
                                }
                            },
                        );
                    }
                    list_binding.push_back(state_update.clone());
                } else {
                    self.state_update_queue.lock().unwrap().push_back(state_update.clone());
                }
            } else {
                self.state_update_queue.lock().unwrap().push_back(state_update.clone());
            }
            let res = self.state_update_sender.send(self.current_transaction.clone()).await;
            if res.is_err() {
                warn!("Failed to process new update {state_update:?}");
            }
        }
    }
}

struct OrchStateDumper {
    /// Queue of the new state updates to process. The queue is updated by the TransitionQueueHandler.
    state_update_queue: Arc<Mutex<VecDeque<Arc<StateUpdateRequest>>>>,
    /// Stores information about enforced modes for dumpsys.
    process_dump_info: Arc<Mutex<Option<String>>>,
}

impl OrchStateDumper {
    fn new(
        state_update_queue: Arc<Mutex<VecDeque<Arc<StateUpdateRequest>>>>,
        process_dump_info: Arc<Mutex<Option<String>>>,
    ) -> Self {
        Self { state_update_queue, process_dump_info }
    }
}

/// Dumps the state of the mode subscriptions with the following format:
///
/// *NAME: Engine state
/// *REPORT:
/// Last mode enforced was Custom("TIRE_PRESSURE") with value "front-left"
///
/// Next modes to process: [Power: "ON"]
///
impl DumpStateReporter for OrchStateDumper {
    fn dump(&self, writer: &mut dyn std::io::Write, _: &[&std::ffi::CStr]) -> Result<(), Error> {
        if let Some(state) = self.process_dump_info.lock().unwrap().as_ref() {
            writeln!(writer, "{state}\n")?;
        }
        writeln!(
            writer,
            "Next modes to process: [{}]",
            self.state_update_queue
                .lock()
                .unwrap()
                .iter()
                .map(|state_update| format!("{}", state_update.update_event))
                .collect::<Vec<String>>()
                .join(", ")
        )?;
        Result::Ok(())
    }
}

/// Waits for new state updates (from the TransitionQueueHandler) and dispatches them to the engine through the StatesUpdateHandler.
struct StateUpdater {
    /// Handler used to notify the orchestrator engine of new vehicle, power and custom mode updates.
    state_update_handler: Arc<dyn StatesUpdateHandler + Send>,
    /// Waits for notifications of new state transitions to process, fetching them from the state_update_queue.
    /// The value received is used to update which is the current transition in progress in case the TransitionQueueHandler needs to cancel it.
    state_update_receiver: Receiver<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>,
    /// JoinHandle with the current task (state update) in progress.
    current_task_handle: Option<JoinHandle<anyhow::Result<()>>>,
    /// Queue of the new state updates to process. The queue is updated by the TransitionQueueHandler.
    state_update_queue: Arc<Mutex<VecDeque<Arc<StateUpdateRequest>>>>,
}

impl StateUpdater {
    fn new(
        state_update_queue: Arc<Mutex<VecDeque<Arc<StateUpdateRequest>>>>,
        state_update_handler: Arc<dyn StatesUpdateHandler + Send>,
        state_update_receiver: Receiver<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>,
    ) -> Self {
        Self {
            state_update_queue,
            state_update_handler,
            current_task_handle: None,
            state_update_receiver,
        }
    }

    async fn process_updates(&mut self) {
        while let Some(current_transaction) = self.state_update_receiver.recv().await {
            if let Some(state_process) = self.current_task_handle.take() {
                debug!("Waiting for previous state update to complete before moving to next one.");
                let res: Result<_, tokio::task::JoinError> = state_process.await;
                debug!(
                    "Previous state update {current_transaction:?} finished with result '{res:?}'"
                );
            }
            debug!("Queue contains: {:?}", self.state_update_queue.lock().unwrap());

            let Some(state) = self.state_update_queue.lock().unwrap().pop_front() else {
                debug!("Update receiver triggered, but the state update queue is empty.");
                continue;
            };

            let new_cancel = CancellationToken::new();
            *current_transaction.lock().unwrap() =
                Some((state.update_event.clone(), new_cancel.clone()));
            self.current_task_handle =
                Some(self.dispatch_state_update_request(state, new_cancel.clone()).await);
            info!("Finished dispatching new value received.");
        }
    }

    #[allow(non_snake_case)]
    async fn dispatch_state_update_request(
        &self,
        state_update_request: Arc<StateUpdateRequest>,
        cancel_token: CancellationToken,
    ) -> JoinHandle<anyhow::Result<()>> {
        let state_update_request_clone = state_update_request.clone();
        let handler = self.state_update_handler.clone();
        tokio::spawn(async move {
            let state_update_request_clone_clone = state_update_request_clone.clone();

            // Cancellation boolean used in the engine to inform that a cancellation happen and that it should not
            // dispatch any new calls to LM. This should later be replaced by tasks.
            let cancel_bool = Arc::new(AtomicBool::new(false));
            let cancel_clone = cancel_bool.clone();
            let cancellable_task = tokio::spawn(async move {
                let state_req = state_update_request_clone_clone.clone();
                match &state_req.update_event {
                    StateChange(mode, state) => {
                        handler
                            .on_state_update(mode, state, cancel_clone, &state_req.timestamp)
                            .await;
                    }
                    event => {
                        handler.on_event_update(event, cancel_clone).await;
                    }
                }
            });
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    cancel_bool.store(true, Ordering::Relaxed);
                    debug!("Current processing  mode update {} was cancelled.", state_update_request_clone.update_event);
                    state_update_request_clone.cancel_token.cancel();
                },
                _ = cancellable_task => {
                    info!("Finished processing mode update {}", state_update_request_clone.update_event);
                    // We need the power state as system property to enable stopping boot tracing
                    // when the boot completes. State of other modes is not stored.
                    if let UpdateEvent::StateChange(Mode::Power, state) = &state_update_request_clone.update_event {
                        if let Err(e) = system_properties::write(POWER_STATE_PROP, &state.to_string()) {
                            warn!("failed to set {POWER_STATE_PROP}: {e:?}");
                        }
                    }
                    if let Some(sender) = state_update_request_clone.state_notifier.lock().unwrap().take() {
                        let res = sender.send(true);
                        if let Err(err) = res {
                            warn!("Failed to notify completion of mode update {} and error {:?}. This could have happened if VPM crashed.", state_update_request_clone.update_event, err);
                        }
                    }
                }
            };

            Ok(())
        })
    }
}

/// Handles different types of state updates for orchestrator
pub struct OrchestratorStateUpdatesHandler {
    /// Field retrieved only in testing
    _state_update_sender: Sender<Arc<StateUpdateRequest>>,
    /// Keep reference to enable binder reconnection handling
    _subscription_handler: ModeSubscriptionHandler,
    /// State dumper used in tests to do dumps
    _orch_state_dumper: Arc<OrchStateDumper>,
}

impl OrchestratorStateUpdatesHandler {
    /// Used to create a new OrchestratorStateUpdatesHandler with a mocked StatesClientSubscriber.
    #[cfg(test)]
    async fn new_with_subscriber(
        client_subscriber: Arc<dyn StatesClientSubscriber>,
        observer: Arc<dyn StatesUpdateHandler + Send>,
        custom_modes: Arc<Mutex<HashMap<Fqin, HashSet<String>>>>,
        lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
    ) -> Self {
        let (state_update_sender, dump_reporter) = Self::set_up_states_handler(
            observer,
            None,
            Arc::new(Mutex::new(None)),
            lm_connection.clone(),
        )
        .await;
        let subscription_handler = ModeSubscriptionHandler::new(
            state_update_sender.clone(),
            client_subscriber,
            custom_modes,
        );
        subscription_handler.start_subscriptions().await;
        Self {
            _state_update_sender: state_update_sender.clone(),
            _subscription_handler: subscription_handler,
            _orch_state_dumper: dump_reporter,
        }
    }

    /// Creates a new orchestrator vpm client and subscribes to VPM updates
    pub async fn new(
        observer: Arc<dyn StatesUpdateHandler + Send>,
        custom_modes: Arc<Mutex<HashMap<Fqin, HashSet<String>>>>,
        dm: Arc<sdv_dump::DumpManager>,
        process_dump_info: Arc<Mutex<Option<String>>>,
        lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
    ) -> Self {
        let (state_update_sender, dump_reporter) = Self::set_up_states_handler(
            observer,
            Some(dm.clone()),
            process_dump_info.clone(),
            lm_connection.clone(),
        )
        .await;
        let client_subscriber = Arc::new(OrchStateSubscriber::new(state_update_sender.clone(), dm));
        let subscription_handler = ModeSubscriptionHandler::new(
            state_update_sender.clone(),
            client_subscriber,
            custom_modes,
        );
        subscription_handler.start_subscriptions().await;
        Self {
            _state_update_sender: state_update_sender,
            _subscription_handler: subscription_handler,
            _orch_state_dumper: dump_reporter,
        }
    }

    async fn set_up_states_handler(
        observer: Arc<dyn StatesUpdateHandler + Send>,
        dm: Option<Arc<sdv_dump::DumpManager>>,
        process_dump_info: Arc<Mutex<Option<String>>>,
        lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
    ) -> (Sender<Arc<StateUpdateRequest>>, Arc<OrchStateDumper>) {
        // Channel to forward state updates from the ModeSubscriptionHandler to the TransitionQueueHandler.
        // Size is 1 to process only one at a time.
        let (transition_state_update_sender, transition_state_update_receiver) =
            channel::<Arc<StateUpdateRequest>>(1);

        // Channel to forward state updates from the TransitionQueueHandler to the StateUpdater.
        // Size is 1 to process only one at a time.
        let (state_update_sender, state_update_receiver) =
            channel::<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>(1);

        let state_update_queue = Arc::new(Mutex::new(VecDeque::new()));

        let dump_reporter =
            Arc::new(OrchStateDumper::new(state_update_queue.clone(), process_dump_info.clone()));
        if let Some(dump) = dm {
            dump.add_reporter("Engine state", &dump_reporter.clone());
        }

        let mut queue_handler = TransitionQueueHandler::new(
            state_update_queue.clone(),
            transition_state_update_receiver,
            state_update_sender,
        );
        queue_handler.set_recover_for_lm_crash(lm_connection.clone());
        let mut state_updater =
            StateUpdater::new(state_update_queue, observer.clone(), state_update_receiver);
        tokio::spawn(async move {
            state_updater.process_updates().await;
        });
        tokio::spawn(async move {
            queue_handler.wait_for_state_updates().await;
        });

        // Trigger the default state of orchestrator before we subscribe to power modes.
        // mpsc::bounded ensures that the sent messages will be processed in the order they were sent.
        let res = transition_state_update_sender
            .send(Arc::new(StateUpdateRequest {
                update_event: UpdateEvent::Default,
                state_notifier: Mutex::new(None),
                cancel_token: CancellationToken::new(),
                timestamp: None,
            }))
            .await;
        if res.is_err() {
            warn!("Failed to set default state of orchestrator");
        }
        (transition_state_update_sender, dump_reporter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::states::states_subscriber_interface::MockStatesClientSubscriber;
    use crate::MockStatesUpdateHandler;
    use crate::OrchestratorStateUpdatesHandler;
    use binder::BinderFeatures;
    use binder::Strong;
    use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::{
        BnLifecycleManager, MockILifecycleManager,
    };
    use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;
    use persistent_binder::MockBinderConnectionManager;
    use persistent_binder::PersistentBinderConnection;
    use regex::Regex;
    use sdv_orchestration_common::mode::Mode;
    use sdv_orchestration_common::vpm_states::power_state_to_string;
    use sdv_status_rs::SdvStatus;
    use sdv_status_rs::SdvStatusCode;
    use std::thread;
    use std::time::Duration;
    use tokio::sync::oneshot;

    const SHUTDOWN_CANCELLED: &str = "SHUTDOWN_CANCELLED";

    #[tokio::test]
    async fn test_should_subscribe_to_power_updates_on_create() {
        // GIVEN
        let mut vpm_subscrib = MockStatesClientSubscriber::default();
        vpm_subscrib.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        vpm_subscrib.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        let mut vpm_listener = MockStatesUpdateHandler::default();
        catch_default_mode(&mut vpm_listener);

        // THEN expect subscribe to power updates is called 1 time
        vpm_subscrib
            .expect_subscribe_to_power_updates()
            .times(1)
            .returning(move || Ok(PowerStateReport::POWER_OFF_EXIT));
        // WHEN
        let _ = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscrib),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_should_not_panic_when_subscribe_to_power_state_fails() {
        // GIVEN
        let mut vpm_subscrib = MockStatesClientSubscriber::default();
        vpm_subscrib.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        vpm_subscrib.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        let mut vpm_listener = MockStatesUpdateHandler::default();
        catch_default_mode(&mut vpm_listener);

        // WHEN subscribe to power modes returns an error
        vpm_subscrib.expect_subscribe_to_power_updates().returning(|| {
            Err(SdvStatus::with_message(
                SdvStatusCode::Unknown,
                "Failed to subscribe to power modes.",
            ))
        });
        // THEN
        // We just need to check that it does not panic
        let _ = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscrib),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_should_send_state_updates_on_initial_power_state() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(move || Ok(PowerStateReport::POWER_OFF_EXIT));
        catch_default_mode(&mut vpm_listener);

        // THEN expect the initial power state was sent
        vpm_listener
            .expect_on_state_update()
            .withf(|mode, value, _, _| {
                *mode == Mode::Power
                    && value == power_state_to_string(PowerStateReport::POWER_OFF_EXIT).unwrap()
            })
            .returning(|_, _, _, _| ());

        // WHEN
        let _ = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_should_not_set_initial_power_update_if_invalid_power_state() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(move || Ok(PowerStateReport::REPORT_UNSPECIFIED));
        catch_default_mode(&mut vpm_listener);

        // THEN no initial power mode should have been set
        vpm_listener.expect_on_state_update().never();

        // WHEN
        let _ = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_should_not_panic_if_recv_gone_when_setting_initial_power_mode() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(move || Ok(PowerStateReport::POWER_OFF_EXIT));
        let (state_update_sender, mut state_update_receiver) =
            channel::<Arc<StateUpdateRequest>>(1);
        let handler = ModeSubscriptionHandler::new(
            state_update_sender.clone(),
            Arc::new(vpm_subscriber),
            Arc::new(Mutex::new(HashMap::new())),
        );
        // WHEN/THEN
        state_update_receiver.close();
        handler.subscribe_to_power_updates().await;
    }

    #[tokio::test]
    async fn test_should_not_set_initial_power_update_if_subscription_failed() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(move || Err(SdvStatus::new(SdvStatusCode::Unknown)));
        catch_default_mode(&mut vpm_listener);

        // THEN no initial power mode should have been set
        vpm_listener.expect_on_state_update().never();

        // WHEN
        let _ = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_should_send_state_updates_on_power_update() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        catch_default_mode(&mut vpm_listener);

        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(move || Ok(PowerStateReport::POWER_OFF_EXIT));
        let power_state = "ON";

        // THEN expect that the ON value was sent to the receiver.
        vpm_listener
            .expect_on_state_update()
            .withf(move |mode, value, _, _| *mode == Mode::Power && value == power_state)
            .returning(|_, _, _, _| ());

        // Catch initial power mode
        vpm_listener.expect_on_state_update().returning(|_, _, _, _| ());
        // WHEN
        let client_subscriber = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
        let result = client_subscriber
            ._state_update_sender
            .send(get_state_request_update(Mode::Power, power_state.to_string()))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_should_notify_on_update_finished_if_notifier_present() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        catch_default_mode(&mut vpm_listener);

        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(move || Ok(PowerStateReport::POWER_OFF_EXIT));
        let power_state = "ON";
        let (power_notifier_sender, power_notifier_receiver) = oneshot::channel();

        // Catch initial power mode
        vpm_listener.expect_on_state_update().returning(|_, _, _, _| ());
        // WHEN
        let client_subscriber = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
        let result = client_subscriber
            ._state_update_sender
            .send(Arc::new(StateUpdateRequest {
                update_event: StateChange(Mode::Power, power_state.to_string()),
                state_notifier: Mutex::new(Some(power_notifier_sender)),
                cancel_token: CancellationToken::new(),
                timestamp: None,
            }))
            .await;
        assert!(result.is_ok());

        // Wait for the value to be received
        let value_received = power_notifier_receiver.await;
        assert!(value_received.is_ok());
    }

    #[tokio::test]
    async fn test_should_subscribe_to_vehicle_updates_on_create() {
        // GIVEN
        let mut vpm_subscrib = MockStatesClientSubscriber::default();
        vpm_subscrib
            .expect_subscribe_to_power_updates()
            .returning(move || Ok(PowerStateReport::POWER_OFF_EXIT));
        vpm_subscrib.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        let mut vpm_listener = MockStatesUpdateHandler::default();
        catch_default_mode(&mut vpm_listener);

        // THEN expect subscribe to power updates is called 1 time
        vpm_subscrib.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));

        // WHEN
        let _ = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscrib),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_should_send_state_updates_on_vehicle_update() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(|| Ok(PowerStateReport::POWER_OFF_EXIT));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        catch_default_mode(&mut vpm_listener);
        let vehicle_state = "VEHICLE_ON";

        // THEN expect that the VEHICLE_ON value was sent to the receiver.
        vpm_listener
            .expect_on_state_update()
            .withf(move |mode, value, _, _| *mode == Mode::Vehicle && value == vehicle_state)
            .returning(|_, _, _, _| ());

        // Catch initial power mode
        vpm_listener.expect_on_state_update().returning(|_, _, _, _| ());
        // WHEN
        let client_subscriber = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
        let result = client_subscriber
            ._state_update_sender
            .send(get_state_request_update(Mode::Vehicle, vehicle_state.to_string()))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_should_send_default_state_updates_on_create() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(|| Ok(PowerStateReport::POWER_OFF_EXIT));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));

        // THEN expect that the default mode was set
        catch_default_mode(&mut vpm_listener);

        // Catch initial power mode
        vpm_listener.expect_on_state_update().returning(|_, _, _, _| ());
        // WHEN
        let _ = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_send_recover_lm_mode_on_lm_reconnection() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(|| Ok(PowerStateReport::POWER_OFF_EXIT));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        catch_default_mode(&mut vpm_listener);

        let (power_sender, mut power_recv) = channel::<()>(1);

        let mut lm_mock_binder = MockBinderConnectionManager::default();
        let captured_listener: Arc<
            Mutex<Option<Box<dyn Fn(&Strong<dyn ILifecycleManager>) + Send + Sync + 'static>>>,
        > = Arc::new(Mutex::new(None));
        let captured_listener_clone = Arc::clone(&captured_listener);
        lm_mock_binder.expect_set_reconnect_listener().returning(move |listener| {
            *captured_listener_clone.lock().unwrap() = Some(listener);
        });

        let power_sender_clone = power_sender.clone();
        // Catch initial POWER_OFF_EXIT
        vpm_listener
            .expect_on_state_update()
            .times(1)
            .withf(|mode, val, _, _| {
                *mode == Mode::Power
                    && val == power_state_to_string(PowerStateReport::POWER_OFF_EXIT).unwrap()
            })
            .returning(move |_, _, _, _| {
                let power_sender_clone = power_sender_clone.clone();
                tokio::spawn(async move {
                    let _ = power_sender_clone.send(()).await;
                });
            });

        let (finish_test_sender, mut finish_test_receiver) = channel::<()>(1);
        let finish_test_sender_clone = finish_test_sender.clone();
        // THEN catch the Recovery for reconnect
        vpm_listener
            .expect_on_event_update()
            .withf(|event, _| *event == UpdateEvent::Recovery)
            .returning(move |_, _| {
                let finish_test_sender_clone = finish_test_sender_clone.clone();
                tokio::spawn(async move {
                    let _ = finish_test_sender_clone.send(()).await;
                });
            });

        OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(lm_mock_binder),
        )
        .await;

        // Wait until the initial POWER_OFF_EXIT was sent so that we collect all expectations.
        // Otherwise, the initial power mode could have been sent after the listener is called and
        // we will have an expectation which conditions are never met.
        let _ = power_recv.recv().await;

        // WHEN the LM reconnection is triggered
        // We need spawn_blocking because the listener executes a blocking_send
        let res = tokio::task::spawn_blocking(move || {
            let listener_guard = captured_listener.lock().unwrap();
            let listener = listener_guard.as_ref().unwrap();
            let lm_binder: Strong<dyn ILifecycleManager> = BnLifecycleManager::new_binder(
                MockILifecycleManager::new(),
                BinderFeatures::default(),
            );
            // WHEN
            listener(&lm_binder);
        })
        .await;
        assert!(res.is_ok());
        // Wait for the Default mode with reconnection value was triggered to finish the test.
        let res = finish_test_receiver.recv().await;
        assert!(res.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_not_panic_on_closed_receiver_when_lm_reconnects() {
        // GIVEN
        let (_, state_update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let (process_sender, mut process_receiver) =
            channel::<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>(1);
        let mut handler = TransitionQueueHandler::new(
            Arc::new(Mutex::new(VecDeque::new())),
            state_update_receiver,
            process_sender,
        );

        let mut lm_mock_binder = MockBinderConnectionManager::default();
        let captured_listener: Arc<
            Mutex<Option<Box<dyn Fn(&Strong<dyn ILifecycleManager>) + Send + Sync + 'static>>>,
        > = Arc::new(Mutex::new(None));
        let captured_listener_clone = Arc::clone(&captured_listener);
        lm_mock_binder.expect_set_reconnect_listener().returning(move |listener| {
            *captured_listener_clone.lock().unwrap() = Some(listener);
        });
        handler.set_recover_for_lm_crash(Arc::new(lm_mock_binder));

        tokio::spawn(async move {
            let _ = handler.wait_for_state_updates().await;
        });

        process_receiver.close();

        // We need spawn_blocking because the listener executes a blocking_send
        let res = tokio::task::spawn_blocking(move || {
            let listener_guard = captured_listener.lock().unwrap();
            let listener = listener_guard.as_ref().unwrap();
            let lm_binder: Strong<dyn ILifecycleManager> = BnLifecycleManager::new_binder(
                MockILifecycleManager::new(),
                BinderFeatures::default(),
            );
            // WHEN
            listener(&lm_binder);
        })
        .await;
        assert!(res.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recover_after_lm_crash_should_remain_first_in_transaction_queue() {
        // GIVEN
        let (state_update_sender, state_update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let (process_sender, mut process_receiver) =
            channel::<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>(1);

        // We start the queue with one value to verify that Recovery is added first
        let mode = Mode::Power;
        let value = "POWER_ON".to_string();
        let vehicle_request = get_state_request_update(mode.clone(), value.clone());
        let state_update_queue = Arc::new(Mutex::new(VecDeque::from([vehicle_request])));
        let mut handler = TransitionQueueHandler::new(
            state_update_queue.clone(),
            state_update_receiver,
            process_sender,
        );

        let mut lm_mock_binder = MockBinderConnectionManager::default();
        let captured_listener: Arc<
            Mutex<Option<Box<dyn Fn(&Strong<dyn ILifecycleManager>) + Send + Sync + 'static>>>,
        > = Arc::new(Mutex::new(None));
        let captured_listener_clone = Arc::clone(&captured_listener);
        lm_mock_binder.expect_set_reconnect_listener().returning(move |listener| {
            *captured_listener_clone.lock().unwrap() = Some(listener);
        });
        handler.set_recover_for_lm_crash(Arc::new(lm_mock_binder));

        tokio::spawn(async move {
            let _ = handler.wait_for_state_updates().await;
        });

        // We need spawn_blocking because the listener executes a blocking_send
        tokio::task::spawn_blocking(move || {
            let listener_guard = captured_listener.lock().unwrap();
            let listener = listener_guard.as_ref().unwrap();
            let lm_binder: Strong<dyn ILifecycleManager> = BnLifecycleManager::new_binder(
                MockILifecycleManager::new(),
                BinderFeatures::default(),
            );
            // WHEN
            listener(&lm_binder);
        });

        // Wait for recovery
        let _ = process_receiver.recv().await;
        // THEN verify that the recovery was added to beginning of queue
        assert!(state_update_queue.lock().unwrap().len() == 2);
        assert!(state_update_queue.lock().unwrap()[0].update_event == UpdateEvent::Recovery);
        // Check also that the queued transaction was pushed to second place
        assert!(
            state_update_queue.lock().unwrap()[1].update_event
                == StateChange(Mode::Power, "POWER_ON".to_string())
        );

        // WHEN cancelling a power transition
        let _ = state_update_sender
            .send(get_state_request_update(Mode::Power, SHUTDOWN_CANCELLED.to_string()))
            .await;
        // THEN the Recovery mode should not have been removed from the queue
        // But Power::POWER_ON mode should been removed from queue
        let _ = process_receiver.recv().await;
        assert!(state_update_queue.lock().unwrap().len() == 2);
        assert!(state_update_queue.lock().unwrap()[0].update_event == UpdateEvent::Recovery);
        // Check also that the new transaction was queued
        assert!(
            state_update_queue.lock().unwrap()[1].update_event
                == StateChange(Mode::Power, SHUTDOWN_CANCELLED.to_string())
        );

        // WHEN sending a non cancellation mode
        let _ = state_update_sender
            .send(get_state_request_update(Mode::Vehicle, "LOW_POWER".to_string()))
            .await;
        // THEN the Recovery should still be first in the list
        let _ = process_receiver.recv().await;
        assert!(state_update_queue.lock().unwrap().len() == 3);
        assert!(state_update_queue.lock().unwrap()[0].update_event == UpdateEvent::Recovery);
        // Check also that the SHUTDOWN_CANCELLED is still second
        assert!(
            state_update_queue.lock().unwrap()[1].update_event
                == StateChange(Mode::Power, SHUTDOWN_CANCELLED.to_string())
        );
        // Check also that the new transaction was queued last
        assert!(
            state_update_queue.lock().unwrap()[2].update_event
                == StateChange(Mode::Vehicle, "LOW_POWER".to_string())
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recover_after_lm_crash_should_cancel_ongoing_transition() {
        // GIVEN
        let (_, state_update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let (process_sender, mut process_receiver) =
            channel::<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>(1);

        let handler = TransitionQueueHandler::new(
            Arc::new(Mutex::new(VecDeque::new())),
            state_update_receiver,
            process_sender,
        );
        // The cancellation that is expected to be cancelled on LM reconnection
        let new_cancel = CancellationToken::new();
        *handler.current_transaction.lock().unwrap() =
            Some((StateChange(Mode::Power, "ON".to_string()), new_cancel.clone()));

        let mut lm_mock_binder = MockBinderConnectionManager::default();
        let captured_listener: Arc<
            Mutex<Option<Box<dyn Fn(&Strong<dyn ILifecycleManager>) + Send + Sync + 'static>>>,
        > = Arc::new(Mutex::new(None));
        let captured_listener_clone = Arc::clone(&captured_listener);
        lm_mock_binder.expect_set_reconnect_listener().returning(move |listener| {
            *captured_listener_clone.lock().unwrap() = Some(listener);
        });
        handler.set_recover_for_lm_crash(Arc::new(lm_mock_binder));

        // We need spawn_blocking because the listener executes a blocking_send
        tokio::task::spawn_blocking(move || {
            let listener_guard = captured_listener.lock().unwrap();
            let listener = listener_guard.as_ref().unwrap();
            let lm_binder: Strong<dyn ILifecycleManager> = BnLifecycleManager::new_binder(
                MockILifecycleManager::new(),
                BinderFeatures::default(),
            );
            // WHEN the reconnect listener is invoked
            listener(&lm_binder);
        });

        // Wait for Recovery to be send
        let _ = process_receiver.recv().await;
        // THEN verify that the current task was cancelled
        assert!(new_cancel.is_cancelled());
    }

    #[tokio::test]
    async fn test_should_subscribe_to_custom_updates_on_create() {
        // GIVEN
        let mut vpm_subscrib = MockStatesClientSubscriber::default();
        vpm_subscrib
            .expect_subscribe_to_power_updates()
            .returning(move || Ok(PowerStateReport::POWER_OFF_EXIT));
        vpm_subscrib.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        let mut vpm_listener = MockStatesUpdateHandler::default();
        catch_default_mode(&mut vpm_listener);

        // THEN expect subscribe to power updates is called 1 time
        vpm_subscrib.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));

        // WHEN
        let _ = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscrib),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_should_send_state_updates_on_custom_update() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(|| Ok(PowerStateReport::POWER_OFF_EXIT));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        catch_default_mode(&mut vpm_listener);

        let custom_state_name = "CHARGING";
        let custom_state_value = "ON";

        // THEN expect that the CUSTOM CHARGING ON value was sent to the receiver.
        vpm_listener
            .expect_on_state_update()
            .withf(move |mode, value, _, _| {
                *mode == Mode::Custom(custom_state_name.to_string()) && value == custom_state_value
            })
            .returning(|_, _, _, _| ());

        // Catch initial power mode
        vpm_listener.expect_on_state_update().returning(|_, _, _, _| ());
        // WHEN
        let client_subscriber = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
        let result = client_subscriber
            ._state_update_sender
            .send(get_state_request_update(
                Mode::Custom(custom_state_name.to_string()),
                custom_state_value.to_string(),
            ))
            .await;
        assert!(result.is_ok());
    }

    // Test that cancellations are proceess correctly by the TransitionQueueHandler.

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handler_should_not_remove_custom_modes_from_queue_on_shutdown_cancelled() {
        // GIVEN
        let (state_update_sender, state_update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let (process_sender, mut process_receiver) =
            channel::<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>(1);
        let mode = Mode::Custom("Mode".to_string());
        let value = "some_value".to_string();
        let vehicle_request = get_state_request_update(mode.clone(), value.clone());
        let state_update_queue = Arc::new(Mutex::new(VecDeque::from([vehicle_request])));
        let mut handler = TransitionQueueHandler::new(
            state_update_queue.clone(),
            state_update_receiver,
            process_sender,
        );
        tokio::spawn(async move {
            let _ = handler.wait_for_state_updates().await;
        });
        // WHEN
        let _ = state_update_sender
            .send(get_state_request_update(Mode::Power, SHUTDOWN_CANCELLED.to_string()))
            .await;
        // THEN the custom mode should not have been removed from the queue
        let _ = process_receiver.recv().await;
        assert!(state_update_queue.lock().unwrap().len() == 2);
        assert!(
            state_update_queue.lock().unwrap()[0].update_event
                == StateChange(mode, value.to_string())
        );
        // Check also that the new transaction was queued
        assert!(
            state_update_queue.lock().unwrap()[1].update_event
                == StateChange(Mode::Power, SHUTDOWN_CANCELLED.to_string())
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handler_should_not_remove_vehicle_modes_from_queue_on_shutdown_cancelled() {
        // GIVEN
        let (state_update_sender, state_update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let (process_sender, mut process_receiver) =
            channel::<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>(1);
        let mode = Mode::Vehicle;
        let value = "LOW_POWER".to_string();
        let vehicle_request = get_state_request_update(mode.clone(), value.clone());
        let state_update_queue = Arc::new(Mutex::new(VecDeque::from([vehicle_request])));
        let mut handler = TransitionQueueHandler::new(
            state_update_queue.clone(),
            state_update_receiver,
            process_sender,
        );
        tokio::spawn(async move {
            let _ = handler.wait_for_state_updates().await;
        });
        // WHEN
        let _ = state_update_sender
            .send(get_state_request_update(Mode::Power, SHUTDOWN_CANCELLED.to_string()))
            .await;
        // THEN the vehicle mode should not have been removed from the queue
        let _ = process_receiver.recv().await;
        assert!(state_update_queue.lock().unwrap().len() == 2);
        assert!(
            state_update_queue.lock().unwrap()[0].update_event
                == StateChange(mode, value.to_string())
        );
        // Check also that the new transaction was queued
        assert!(
            state_update_queue.lock().unwrap()[1].update_event
                == StateChange(Mode::Power, SHUTDOWN_CANCELLED.to_string())
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handler_should_remove_newest_power_mode_from_queue_on_shutdown_cancelled() {
        // GIVEN
        let (state_update_sender, state_update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let (process_sender, mut process_receiver) =
            channel::<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>(1);
        let newest_vehicle_request = get_state_request_update(Mode::Power, "ON".to_string());
        let old_power_state = "POWER_OFF_EXIT".to_string();
        let oldest_vehicle_request = get_state_request_update(Mode::Power, old_power_state.clone());
        let state_update_queue =
            Arc::new(Mutex::new(VecDeque::from([oldest_vehicle_request, newest_vehicle_request])));
        let mut handler = TransitionQueueHandler::new(
            state_update_queue.clone(),
            state_update_receiver,
            process_sender,
        );
        tokio::spawn(async move {
            let _ = handler.wait_for_state_updates().await;
        });
        // WHEN
        let _ = state_update_sender
            .send(get_state_request_update(Mode::Power, SHUTDOWN_CANCELLED.to_string()))
            .await;
        // THEN
        let _ = process_receiver.recv().await;
        assert!(state_update_queue.lock().unwrap().len() == 2);
        // The first item on the queue was the oldest one that wasn't removed
        assert!(
            state_update_queue.lock().unwrap()[0].update_event
                == StateChange(Mode::Power, old_power_state)
        );
        // The SHUTDOWN_CANCELLED should have been added to the end of the queue
        assert!(
            state_update_queue.lock().unwrap()[1].update_event
                == StateChange(Mode::Power, SHUTDOWN_CANCELLED.to_string())
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handler_should_cancel_in_progress_power_transaction_on_shutdown_cancelled() {
        // GIVEN
        let (state_update_sender, state_update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let (process_sender, mut process_receiver) =
            channel::<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>(1);
        let state_update_queue = Arc::new(Mutex::new(VecDeque::new()));
        let cancel_t = CancellationToken::new();
        let mut handler = TransitionQueueHandler::new(
            state_update_queue.clone(),
            state_update_receiver,
            process_sender,
        );
        *handler.current_transaction.lock().unwrap() =
            Some((StateChange(Mode::Power, "ON".to_string()), cancel_t.clone()));
        tokio::spawn(async move {
            let _ = handler.wait_for_state_updates().await;
        });
        // WHEN
        let _ = state_update_sender
            .send(get_state_request_update(Mode::Power, SHUTDOWN_CANCELLED.to_string()))
            .await;
        // THEN
        let _ = process_receiver.recv().await;
        assert!(state_update_queue.lock().unwrap().len() == 1);
        // The SHUTDOWN_CANCELLED should have been added to the end of the queue
        assert!(
            state_update_queue.lock().unwrap()[0].update_event
                == StateChange(Mode::Power, SHUTDOWN_CANCELLED.to_string())
        );
        // The ongoing transaction should have been cancelled
        assert!(cancel_t.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handler_should_not_cancel_in_progress_vehicle_transaction_on_shutdown_cancelled()
    {
        // GIVEN
        let (state_update_sender, state_update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let (process_sender, mut process_receiver) =
            channel::<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>(1);
        let state_update_queue = Arc::new(Mutex::new(VecDeque::new()));
        let cancel_t = CancellationToken::new();
        let mut handler = TransitionQueueHandler::new(
            state_update_queue.clone(),
            state_update_receiver,
            process_sender,
        );
        *handler.current_transaction.lock().unwrap() =
            Some((StateChange(Mode::Vehicle, "PARK".to_string()), cancel_t.clone()));
        tokio::spawn(async move {
            let _ = handler.wait_for_state_updates().await;
        });
        // WHEN
        let _ = state_update_sender
            .send(get_state_request_update(Mode::Power, SHUTDOWN_CANCELLED.to_string()))
            .await;
        // THEN
        let _ = process_receiver.recv().await;
        assert!(state_update_queue.lock().unwrap().len() == 1);
        // The SHUTDOWN_CANCELLED should have been added to the end of the queue
        assert!(
            state_update_queue.lock().unwrap()[0].update_event
                == StateChange(Mode::Power, SHUTDOWN_CANCELLED.to_string())
        );
        // The ongoing transaction should NOT have been cancelled
        assert!(!cancel_t.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handler_should_not_cancel_in_progress_custom_transaction_on_shutdown_cancelled() {
        // GIVEN
        let (state_update_sender, state_update_receiver) = channel::<Arc<StateUpdateRequest>>(1);
        let (process_sender, mut process_receiver) =
            channel::<Arc<Mutex<Option<(UpdateEvent, CancellationToken)>>>>(1);
        let state_update_queue = Arc::new(Mutex::new(VecDeque::new()));
        let cancel_t = CancellationToken::new();
        let mut handler = TransitionQueueHandler::new(
            state_update_queue.clone(),
            state_update_receiver,
            process_sender,
        );
        *handler.current_transaction.lock().unwrap() = Some((
            StateChange(Mode::Custom("Mode".to_string()), "some_value".to_string()),
            cancel_t.clone(),
        ));
        tokio::spawn(async move {
            let _ = handler.wait_for_state_updates().await;
        });
        // WHEN
        let _ = state_update_sender
            .send(get_state_request_update(Mode::Power, SHUTDOWN_CANCELLED.to_string()))
            .await;
        // THEN
        let _ = process_receiver.recv().await;
        assert!(state_update_queue.lock().unwrap().len() == 1);
        // The SHUTDOWN_CANCELLED should have been added to the end of the queue
        assert!(
            state_update_queue.lock().unwrap()[0].update_event
                == StateChange(Mode::Power, SHUTDOWN_CANCELLED.to_string())
        );
        // The ongoing transaction should NOT have been cancelled
        assert!(!cancel_t.is_cancelled());
    }

    // The following tests check that the interactions between the TransitionQueueHandler and the StateUpdater
    // work as expected during a cancellation request.

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_cancel_in_progress_transaction_on_shutdown_cancelled() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(|| Err(SdvStatus::new(SdvStatusCode::Unknown)));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        catch_default_mode(&mut vpm_listener);

        let (test_sender, mut test_receiver) = channel::<()>(1);
        let cancel_t = CancellationToken::new();
        vpm_listener
            .expect_on_state_update()
            .withf(move |mode, value, _, _| *mode == Mode::Power && value == "ON")
            .returning(move |_, _, cancel_boolean, _| {
                let res = test_sender.try_send(());
                assert!(res.is_ok());
                // We want to keep this thread busy (simulating long work) waiting for it to be cancelled.
                while !cancel_boolean.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(10));
                }
            });

        // Catch SHUTDOWN_CANCELLED
        vpm_listener.expect_on_state_update().returning(|_, _, _, _| ());

        let client_subscriber = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
        let res = client_subscriber
            ._state_update_sender
            .send(Arc::new(StateUpdateRequest {
                update_event: StateChange(Mode::Power, "ON".to_string()),
                state_notifier: Mutex::new(None),
                cancel_token: cancel_t.clone(),
                timestamp: None,
            }))
            .await;
        assert!(res.is_ok());
        // Wait until the Power::ON is being executed so we proceed to cancel it.
        // The test becomes flaky if we don't do this wait.
        let res = test_receiver.recv().await;
        assert!(res.is_some());

        // WHEN we request to cancel the POWER ON transaction
        let res = client_subscriber
            ._state_update_sender
            .send(get_state_request_update(Mode::Power, SHUTDOWN_CANCELLED.to_string()))
            .await;
        assert!(res.is_ok());

        // THEN we wait until the transaction was cancelled
        cancel_t.cancelled().await;
    }

    // This test works as follows:
    // Initially, the POWER_OFF_EXIT will be set as part of the subscription to power modes.
    // While the POWER_OFF_EXIT is being executed, an ON mode is received and is queued to executed.
    // While the POWER_OFF_EXIT keeps being executed, a SHUTDOWN_CANCELLED is received, which means that the
    // ON transaction needs to be removed from the queue.
    // Once the POWER_OFF_EXIT finishes, the SHUTDOWN_CANCELLED transaction should be executed as the ON should have been removed.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_remove_from_queue_transactions_on_shutdown_cancelled() {
        // GIVEN
        let mut vpm_subscriber = MockStatesClientSubscriber::default();
        let mut vpm_listener = MockStatesUpdateHandler::default();
        vpm_subscriber
            .expect_subscribe_to_power_updates()
            .returning(|| Ok(PowerStateReport::POWER_OFF_EXIT));
        vpm_subscriber.expect_subscribe_to_custom_updates().times(1).returning(|_| Ok(()));
        vpm_subscriber.expect_subscribe_to_vehicle_updates().times(1).returning(|| Ok(()));
        catch_default_mode(&mut vpm_listener);

        // THEN the POWER::ON should never be called because the POWER_OFF_EXIT should be running when
        // we received the SHUTDOWN_CANCELLED
        vpm_listener
            .expect_on_state_update()
            .never()
            .withf(move |mode, value, _, _| *mode == Mode::Power && value == "ON")
            .returning(|_, _, _, _| ());

        vpm_listener
            .expect_on_state_update()
            .withf(move |mode, value, _, _| *mode == Mode::Power && value == "POWER_OFF_EXIT")
            .returning(move |_, _, _, _| {
                // We need to have the POWER_OFF_EXIT being executed while we send the ON and SHUTDOWN_CANCELLED.
                // There's currently no way to stop this execution because we need to wait until the SHUTDOWN_CANCELLED
                // was processed and the ON removed from the queue.
                // One second should be enough. If test becomes flaky, we need to increase this value.
                thread::sleep(Duration::from_secs(1));
            });

        let (sender, mut finish_test_receiver) = channel::<()>(1);
        vpm_listener
            .expect_on_state_update()
            .withf(move |mode, value, _, _| *mode == Mode::Power && value == SHUTDOWN_CANCELLED)
            .returning(move |_, _, _, _| {
                let sender_clone = sender.clone();
                tokio::spawn(async move {
                    let res = sender_clone.send(()).await;
                    assert!(res.is_ok());
                });
            });
        let client_subscriber = OrchestratorStateUpdatesHandler::new_with_subscriber(
            Arc::new(vpm_subscriber),
            Arc::new(vpm_listener),
            Arc::new(Mutex::new(HashMap::new())),
            get_mock_persistent_lm_connection(),
        )
        .await;
        let cancel_t = CancellationToken::new();
        // We put the POWER::ON transaction in the queue, but it should be cancelled and never executed.
        let _ = client_subscriber
            ._state_update_sender
            .send(Arc::new(StateUpdateRequest {
                update_event: StateChange(Mode::Power, "ON".to_string()),
                state_notifier: Mutex::new(None),
                cancel_token: cancel_t.clone(),
                timestamp: None,
            }))
            .await;

        // WHEN
        let res = client_subscriber
            ._state_update_sender
            .send(get_state_request_update(Mode::Power, SHUTDOWN_CANCELLED.to_string()))
            .await;
        assert!(res.is_ok());

        // THEN the SHUTDOWN_CANCELLED was processed without the POWER::ON being executed
        let res = finish_test_receiver.recv().await;
        assert!(res.is_some());
    }

    // Dump tests

    #[tokio::test]
    async fn test_dumper_should_dump_queued_modes_in_received_order() {
        // GIVEN
        let state_update_queue = Arc::new(Mutex::new(VecDeque::from([
            Arc::new(StateUpdateRequest {
                update_event: StateChange(Mode::Power, "ON".to_string()),
                state_notifier: Mutex::new(None),
                cancel_token: CancellationToken::new(),
                timestamp: None,
            }),
            Arc::new(StateUpdateRequest {
                update_event: StateChange(Mode::Vehicle, "PARK".to_string()),
                state_notifier: Mutex::new(None),
                cancel_token: CancellationToken::new(),
                timestamp: None,
            }),
            Arc::new(StateUpdateRequest {
                update_event: StateChange(Mode::Custom("CHARGING".to_string()), "IDLE".to_string()),
                state_notifier: Mutex::new(None),
                cancel_token: CancellationToken::new(),
                timestamp: None,
            }),
        ])));
        let dump_info = String::from("Dumping info");
        let process_dump_info = Arc::new(Mutex::new(Some(dump_info.clone())));
        let dumper = OrchStateDumper::new(state_update_queue, process_dump_info);
        let mut buffer = Vec::new();
        // WHEN
        let res = dumper.dump(&mut buffer, &[]);
        let buf_str = String::from_utf8(buffer.clone()).unwrap();
        // THEN
        assert!(res.is_ok());
        assert!(buf_str.contains(&dump_info));
        assert!(Regex::new(
            r#"Next modes to process: \[Power: "ON", Vehicle: "PARK", Custom\("CHARGING"\): "IDLE"\]"#
        )
        .unwrap()
        .is_match(&buf_str));
    }

    fn get_mock_persistent_lm_connection() -> Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>
    {
        PersistentBinderConnection::new(move || {
            BnLifecycleManager::new_binder(MockILifecycleManager::new(), BinderFeatures::default())
        })
    }

    fn get_state_request_update(mode: Mode, state: String) -> Arc<StateUpdateRequest> {
        Arc::new(StateUpdateRequest {
            update_event: StateChange(mode, state),
            state_notifier: Mutex::new(None),
            cancel_token: CancellationToken::new(),
            timestamp: None,
        })
    }

    /// Catches the DEFAULT state sent when creating the OrchestratorStateUpdatesHandler
    fn catch_default_mode(vpm_listener: &mut MockStatesUpdateHandler) {
        vpm_listener
            .expect_on_event_update()
            .withf(|event, _| *event == UpdateEvent::Default)
            .returning(|_, _| ());
    }
}
