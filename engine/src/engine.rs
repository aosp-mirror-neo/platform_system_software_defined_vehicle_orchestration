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
//! Handles the internal orchestrator logic, starting and stopping services on VPM updates.

use crate::instance_manager_factory::IInstanceManagerFactory;
use crate::instance_manager::{IInstanceManager, RequestType};
use crate::StatesUpdateHandler;
use crate::instance_manager::TransitionRequest;
use futures::future;
use futures::{FutureExt, StreamExt};
use google_sdv_lifecycle_manager::binder;
use persistent_binder::BinderConnectionManager;
use log::{debug, error, info, warn};
use protobuf::well_known_types::timestamp::Timestamp;
use rustutils::android::system_properties::read;
use rustutils::android::system_properties;
use sdv_dump::DumpStateReporter;
use sdv_orch_dist_config::evaluator::Evaluator;
use sdv_orchestration_common::fqin::Fqin;
use google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin;
use sdv_orchestration_common::instance_state::InstanceState;
use sdv_orchestration_common::mode::{Mode, UpdateEvent};
use std::collections::HashMap;
use binder::BinderFeatures;
use async_trait::async_trait;
use binder_tokio::TokioRuntime;
use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::IServiceBundleInstanceStateChangeListener;
use google_sdv_lifecycle_crash_notification_aidl::aidl::google::sdv::lifecycle::internal::crash_notification::IServiceBundleCrashNotificationCallback::{BnServiceBundleCrashNotificationCallback, IServiceBundleCrashNotificationCallbackAsyncServer};
use google_sdv_lifecycle_crash_notification_aidl::aidl::google::sdv::lifecycle::internal::crash_notification::IServiceBundleCrashNotificationService::IServiceBundleCrashNotificationService;
use std::io::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use tokio::runtime::Handle;
use tokio::sync::mpsc::Receiver;
use tracing::instrument;

static LM_CALLS_MAX_THREAD_LIMIT: LazyLock<usize> =
    LazyLock::new(|| read_lm_binder_max_parallelism_sys_prop_or_default(read));

const FALLBACK_LM_CALLS_MAX_THREAD: usize = 12;
const ORCH_STATE_READY_PROP: &str = "ro.sdv.orchestrator.state.ready";
const LM_CALLS_MAX_THREAD_LIMIT_PROPERTY: &str = "ro.boot.sdv.max_bundles_management_threads";

fn read_lm_binder_max_parallelism_sys_prop_or_default(
    read: fn(&str) -> system_properties::error::Result<Option<String>>,
) -> usize {
    read(LM_CALLS_MAX_THREAD_LIMIT_PROPERTY).ok().flatten().map_or_else(
        || {
            error!(
                "Could not read orchestration parallelism property, check sys config. \
                Using default max threads: {FALLBACK_LM_CALLS_MAX_THREAD}"
            );
            FALLBACK_LM_CALLS_MAX_THREAD
        },
        |prop_str| {
            prop_str.parse::<usize>().unwrap_or_else(|_| {
                error!(
                    "Could not parse orchestration parallelism property, check sys config. \
                    Using default max thread: {FALLBACK_LM_CALLS_MAX_THREAD}"
                );
                FALLBACK_LM_CALLS_MAX_THREAD
            })
        },
    )
}

/// Sets the orchestrator ready property to true if it's not already set.
/// This property is read-only after being set once, so we avoid trying to write it again
/// to prevent unnecessary errors in the log.
fn set_orchestrator_ready_prop() {
    if read(ORCH_STATE_READY_PROP).ok().flatten().as_deref() != Some("true") {
        if let Err(ref e) = system_properties::write(ORCH_STATE_READY_PROP, "true") {
            error!("Couldn't write property '{ORCH_STATE_READY_PROP}': {e}");
        }
    }
}

#[derive(Clone)]
/// Internal orch functionality
pub struct OrchestratorEngine {
    config: Arc<Mutex<Evaluator>>,
    dump_state: Arc<Mutex<Option<String>>>,
    instance_manager_factory: Arc<dyn IInstanceManagerFactory>,
    // Avoids creating a new instance manager for already enforced bundles
    enforcing_bundles: Arc<Mutex<HashMap<Fqin, Arc<dyn IInstanceManager>>>>,
}

impl DumpStateReporter for OrchestratorEngine {
    fn dump(&self, writer: &mut dyn std::io::Write, args: &[&std::ffi::CStr]) -> Result<(), Error> {
        // TODO(b/408147268): find a way to dump the config without using the engine class
        self.config.lock().unwrap().dump(writer, args)?;
        Result::Ok(())
    }
}

impl binder::Interface for OrchestratorEngine {}

#[allow(non_snake_case)]
#[async_trait]
impl IServiceBundleCrashNotificationCallbackAsyncServer for OrchestratorEngine {
    async fn onServiceBundleCrash(&self, fqin: &ServiceFqin) -> binder::Result<()> {
        // Do not change this log as it's used in the SdvE2EOrchestrationBundleRestartedAfterCrashNotificationTest e2e test
        // to verify that the LM crash notification binder API works as expected.
        info!("Service bundle crashed for fqin: {fqin:?}");

        let instance = Fqin::from_lm_fqin(fqin);
        let instance_manager = {
            let mut binding = self.enforcing_bundles.lock().unwrap();
            binding
                .entry(instance.clone())
                .or_insert_with(|| self.instance_manager_factory.create(&instance))
                .clone()
        };
        // If the bundle has crashed, the state we have assigned previously is not valid anymore.
        instance_manager.invalidate_state();
        if !instance_manager.is_restartable() {
            instance_manager.handle_non_restartable_bundle_crash();
            // Log is used in e2e tests. Please do not change, or update e2e tests if necessary.
            warn!("Bundle with {:?} has crashed, but it is not restartable.", instance);
        } else {
            instance_manager.orchestrate_restart();
        }
        Ok(())
    }
}

impl OrchestratorEngine {
    /// Creates a new orchestrator engine that starts and stops service bundles.
    /// Configured service bundles with no conditions attached will be queued to start at this point.
    pub fn new(
        config: Arc<Mutex<Evaluator>>,
        dump_state: Arc<Mutex<Option<String>>>,
        instance_manager_factory: Arc<dyn IInstanceManagerFactory>,
        crash_notifier_connection: Arc<
            dyn BinderConnectionManager<dyn IServiceBundleCrashNotificationService>,
        >,
        mut new_listener_receiver: Receiver<
            binder::Strong<dyn IServiceBundleInstanceStateChangeListener>,
        >,
    ) -> Self {
        let engine = Self {
            config,
            dump_state,
            instance_manager_factory,
            enforcing_bundles: Arc::new(Mutex::new(HashMap::new())),
        };
        // We need to get the tokio handle to recreate the crash notification connection
        // in `subscribe_to_crash_notification`.
        // Otherwise it's called from a binder thread (on death) instead of tokio one, and panics.
        let tokio_handle = Handle::current();
        let engine_clone = engine.clone();
        crash_notifier_connection.set_reconnect_listener(Box::new(move |binder| {
            debug!("LM Crash Notifier binder has died, connecting and subscribing again.");
            subscribe_to_crash_notification(engine_clone.clone(), binder, tokio_handle.clone());
        }));
        subscribe_to_crash_notification(
            engine.clone(),
            &crash_notifier_connection.get_binder().lock().unwrap(),
            Handle::current(),
        );

        // Spawn task to handle new listeners
        let engine_clone = engine.clone();
        tokio::spawn(async move {
            while let Some(listener) = new_listener_receiver.recv().await {
                engine_clone.report_all_bundle_states_to_listener(listener);
            }
        });
        engine
    }

    #[instrument(skip(self, cancellation), ret)]
    /// Returns a boolean indicating if the transaction was cancelled or not.
    async fn enforce_instances_states(
        &self,
        mut instances_states: HashMap<Fqin, InstanceState>,
        cancellation: Arc<AtomicBool>,
    ) -> bool {
        // Ensure that obsolete services are destroyed.
        self.enforcing_bundles.lock().unwrap().iter().for_each(|(fqin, instance)| {
            if !instances_states.contains_key(fqin) && instance.is_not_destroyed() {
                instances_states.insert(fqin.clone(), InstanceState::Destroyed);
            }
        });

        futures::stream::iter(instances_states)
            // Stop processing if cancellation is requested
            .take_while(|_| future::ready(!cancellation.load(Ordering::Relaxed)))
            // Process each instance state concurrently
            .for_each_concurrent(*LM_CALLS_MAX_THREAD_LIMIT, |(instance, state)| {
                let enforcing_bundles = self.enforcing_bundles.clone();
                let lm_cancel_clone = cancellation.clone();
                let instance_manager_factory = self.instance_manager_factory.clone();
                tokio::task::spawn_blocking(move || {
                    let instance_manager = {
                        let mut binding = enforcing_bundles.lock().unwrap();
                        binding
                            .entry(instance.clone())
                            .or_insert_with(|| instance_manager_factory.create(&instance))
                            .clone()
                    };
                    let (responder, receiver) = tokio::sync::oneshot::channel();
                    let request = TransitionRequest {
                        cancel: lm_cancel_clone,
                        responder: Some(responder),
                        request_type: RequestType::NewMode(state.clone()),
                    };
                    instance_manager.orchestrate_transition_request(request);
                    if receiver.blocking_recv().is_ok() {
                        info!("Finished transitioning {:?} to {:?}.", instance, state);
                    } else {
                        warn!("Failed to transition {:?} to {:?}.", instance, state);
                    }
                })
                .map(|result| result.unwrap_or_default())
            })
            .await;

        info!("Finished enforcing instances states.");
        cancellation.load(Ordering::Relaxed)
    }

    fn report_all_bundle_states_to_listener(
        &self,
        listener: binder::Strong<dyn IServiceBundleInstanceStateChangeListener>,
    ) {
        self.enforcing_bundles.lock().unwrap().iter().for_each(|(_, instance)| {
            instance.report_current_state_to_listener(listener.clone());
        });
    }
}

/// Subscribe to crash notifications from LM setting the engine to receive notifications
pub fn subscribe_to_crash_notification(
    engine: OrchestratorEngine,
    crash_binder: &binder::Strong<dyn IServiceBundleCrashNotificationService>,
    tokio_handle: Handle,
) {
    let crash_callback = BnServiceBundleCrashNotificationCallback::new_async_binder(
        engine,
        TokioRuntime(tokio_handle),
        BinderFeatures::default(),
    );
    let res = crash_binder.subscribe(&crash_callback);
    if let Err(ref err) = res {
        warn!("Failed to subscribe to crash notifications with error: {:?}", err);
    }
}

#[async_trait::async_trait]
impl StatesUpdateHandler for OrchestratorEngine {
    #[instrument(skip(self, cancellation, timestamp))]
    async fn on_state_update(
        &self,
        mode: &Mode,
        state: &str,
        cancellation: Arc<AtomicBool>,
        timestamp: &Option<Timestamp>,
    ) {
        let last_dump = self.dump_state.lock().unwrap().clone();
        *self.dump_state.lock().unwrap() =
            Some(format!("Processing mode {mode:?} with value {state:?}"));

        let instances_states = {
            info!("VPM callback received vpm update '{mode:?}' with state '{state:?}'.");
            set_orchestrator_ready_prop();
            let mut config = self.config.lock().unwrap();
            debug!("Updating VPM state {mode:?} in config");
            // When setting the initial state of orchestrator, we don't have any value to update.
            // In this case, we just want to evaluate the initial state of the config and enforce the corresponding states.
            let is_state_updated = config.update_orchestration_state(mode, state, timestamp);
            if is_state_updated.is_none() {
                warn!("Update with mode {mode:?} and state {state:?} was prevented due to outdated timestamp");
                // As we didn't enforce any new state, we set the dump to the previously enforced mode.
                *self.dump_state.lock().unwrap() = last_dump;
                return;
            }
            // Triggering service bundle lifecycle changes
            config.get_instances_states()
        };

        debug!("Required service instances states are {instances_states:?}");
        let cancelled = self.enforce_instances_states(instances_states, cancellation).await;
        if cancelled {
            info!("Finished enforcing states after cancellation.");
        } else {
            info!("Finished enforcing mode '{mode:?}' with state '{state:?}'. successfully.");
        }

        *self.dump_state.lock().unwrap() =
            Some(format!("Last mode enforced was {mode:?} with value {state:?}"));
    }

    #[instrument(skip(self))]
    async fn on_event_update(&self, event: &UpdateEvent, cancellation: Arc<AtomicBool>) {
        if let UpdateEvent::StateChange(_, _) = event {
            error!("Received mode update instead of event update, aborting update.");
            return;
        }

        if *event == UpdateEvent::Recovery {
            self.enforcing_bundles.lock().unwrap().iter().for_each(|(_, instance)| {
                instance.invalidate_state();
            });
        }

        let state = event.to_string();
        *self.dump_state.lock().unwrap() = Some(state.clone());

        info!("VPM callback received vpm update '{event:?}' with state '{state}'.");
        set_orchestrator_ready_prop();

        debug!("Starting service bundles after '{event:?}' event.");
        let instances_states = self.config.lock().unwrap().get_instances_states();
        debug!("Required service instances states are {instances_states:?}");
        // UpdateEvents can be cancelled in case LM crashes and we want to do a recovery.
        // If we were already doing a recovery or a Default mode, we can still cancel and proceed with  new recovery.
        let cancelled = self.enforce_instances_states(instances_states, cancellation).await;
        if cancelled {
            info!("Finished enforcing states after cancellation.");
        } else {
            info!("Finished enforcing mode '{event:?}' with state '\"{state}\"'. successfully.");
        }

        *self.dump_state.lock().unwrap() = Some(format!("Finished: {state}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance_manager_factory::InstanceManagerFactory;
    use crate::instance_manager::MockIInstanceManager;
    use crate::LifecycleManagerFetcher;
    use crate::OrchestratorEngine;
    use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::ILifecycleManager;
    use binder::Status;
    use googletest::{
        assert_that,
        prelude::{anything, eq, err, ok},
    };
    use persistent_binder::PersistentBinderConnection;
    use persistent_binder::MockBinderConnectionManager;
    use sdv_orch_test_mocks::dumpsys_lm_mocks::*;
    use sdv_orch_test_mocks::mock_lm_with_interceptor::*;
    use sdv_orch_test_mocks::parallelization_lm_mock::*;
    use std::sync::Arc;
    use std::sync::Mutex;
    use google_sdv_lifecycle_crash_notification_aidl::aidl::google::sdv::lifecycle::internal::crash_notification::IServiceBundleCrashNotificationService::{MockIServiceBundleCrashNotificationService, BnServiceBundleCrashNotificationService};
    use crate::instance_manager_factory::IInstanceManagerFactory;
    use crate::instance_manager_factory::MockIInstanceManagerFactory;
    use tokio::sync::broadcast;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::MockIServiceBundleInstanceStateChangeListener;
    use std::time::Duration;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::BnServiceBundleInstanceStateChangeListener;
    use tokio::sync::mpsc;

    static TEST_PROTO_PATH: &str = "src/test_protos/unit_test_config_sample.textproto";
    static TEST_PROTO_NO_CONDITION_PATH: &str =
        "src/test_protos/unit_test_config_no_condition.textproto";

    #[derive(PartialEq)]
    enum LMCall {
        Launch,
        Start,
        Shutdown,
    }

    struct TestContext {
        lm_interceptor: Arc<LMInterceptor>,
        orch: OrchestratorEngine,
    }

    fn create_unused_new_subscriber_receiver(
    ) -> mpsc::Receiver<binder::Strong<dyn IServiceBundleInstanceStateChangeListener>> {
        mpsc::channel(1).1
    }

    impl TestContext {
        fn new(lm: LMInterceptor) -> Self {
            Self::new_with_config(lm, Evaluator::new(None, &[]))
        }

        fn new_with_config(lm: LMInterceptor, config: Evaluator) -> Self {
            let lm_interceptor = Arc::new(lm);
            let mock_lm = MockLifecycleManager::create(lm_interceptor.clone());
            let restart_config = Arc::new(config.get_retry_configuration());
            let evaluator = Arc::new(Mutex::new(config));
            let factory: Arc<dyn IInstanceManagerFactory> = Arc::new(InstanceManagerFactory::new(
                PersistentBinderConnection::new(move || mock_lm.get_lm()),
                restart_config,
                evaluator.clone(),
                Arc::new(Mutex::new(HashMap::new())),
            ));
            let orch = OrchestratorEngine::new(
                evaluator,
                Arc::new(Mutex::new(None)),
                factory,
                get_mock_crash_notifier_persistent_connection(),
                create_unused_new_subscriber_receiver(),
            );
            Self { lm_interceptor, orch }
        }

        // Assert that only calls from [calls] are intercepted by LMInterceptor, and no other calls.
        fn assert_lm_calls(&self, fqin: &Fqin, calls: Vec<LMCall>) {
            for call in [LMCall::Launch, LMCall::Start, LMCall::Shutdown] {
                let call_state = match call {
                    LMCall::Launch => self.lm_interceptor.launch_service_called.lock().unwrap(),
                    LMCall::Start => self.lm_interceptor.start_service_called.lock().unwrap(),
                    LMCall::Shutdown => self.lm_interceptor.shutdown_service_called.lock().unwrap(),
                }
                .clone();

                if calls.contains(&call) {
                    assert_that!(call_state, ok(eq(fqin)));
                } else {
                    assert_that!(call_state, err(anything()));
                }
            }
        }
    }

    fn get_mock_crash_notifier_persistent_connection(
    ) -> Arc<dyn BinderConnectionManager<dyn IServiceBundleCrashNotificationService>> {
        PersistentBinderConnection::new(move || {
            let mut mock = MockIServiceBundleCrashNotificationService::new();
            mock.expect_subscribe().returning(|_| Ok(()));
            BnServiceBundleCrashNotificationService::new_binder(mock, BinderFeatures::default())
        })
    }

    impl LifecycleManagerFetcher for MockLifecycleManager {
        fn get_lm(&self) -> binder::Strong<dyn ILifecycleManager> {
            binder::Strong::new(Box::new(self.clone()))
        }
    }

    impl LifecycleManagerFetcher for DumpsysLmMock {
        fn get_lm(&self) -> binder::Strong<dyn ILifecycleManager> {
            binder::Strong::new(Box::new(self.clone()))
        }
    }

    impl LifecycleManagerFetcher for ParallelizationLmFetcher {
        fn get_lm(&self) -> binder::Strong<dyn ILifecycleManager> {
            binder::Strong::new(Box::new(self.clone()))
        }
    }

    impl LifecycleManagerFetcher for FailingParallelizationLmFetcher {
        fn get_lm(&self) -> binder::Strong<dyn ILifecycleManager> {
            binder::Strong::new(Box::new(self.clone()))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 12)]
    async fn test_parallel_transitions_to_started() {
        test_parallel_transitions(InstanceState::Started).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 12)]
    async fn test_parallel_transitions_to_created() {
        test_parallel_transitions(InstanceState::Created).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 12)]
    async fn test_parallel_transitions_to_destroyed() {
        // GIVEN
        let mock_lm = Arc::new(ParallelizationLmMock::default());
        let fetcher = ParallelizationLmFetcher { lm: mock_lm.clone() };
        let evaluator = Arc::new(Mutex::new(Evaluator::new(None, &[])));
        let factory = Arc::new(InstanceManagerFactory::new(
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            Arc::new(HashMap::new()),
            evaluator.clone(),
            Arc::new(Mutex::new(HashMap::new())),
        ));
        let test_orch = OrchestratorEngine::new(
            evaluator,
            Arc::new(Mutex::new(None)),
            factory,
            get_mock_crash_notifier_persistent_connection(),
            create_unused_new_subscriber_receiver(),
        );
        // First we start the instances so we can destroy them after
        let mut new_states = HashMap::new();
        for fqin in (1..64_u8)
            .map(|i| Fqin::new(&format!("com.google.sdv.test{i}"), "ServiceName", "instance"))
        {
            new_states.insert(fqin.clone(), InstanceState::Started);
        }
        test_orch.enforce_instances_states(new_states, Arc::new(AtomicBool::new(false))).await;

        // Prepare the instances to be destroyed
        let mut new_states = HashMap::new();
        for fqin in (1..64_u8)
            .map(|i| Fqin::new(&format!("com.google.sdv.test{i}"), "ServiceName", "instance"))
        {
            new_states.insert(fqin.clone(), InstanceState::Destroyed);
        }

        // WHEN
        test_orch.enforce_instances_states(new_states, Arc::new(AtomicBool::new(false))).await;

        // THEN
        // With given test payload we should achieve maximum of 8 transactions in
        // parallel at some point in time.
        assert_that!(mock_lm.max_pending_transactions.load(Ordering::Relaxed), eq(12));

        // sanity checks: all pending transactions should be finished
        assert_that!(mock_lm.pending_transactions.load(Ordering::Relaxed), eq(0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 12)]
    async fn test_failing_parallel_transitions() {
        // GIVEN
        let mock_lm = Arc::new(FailingParallelizationLmMock::default());
        let fetcher = FailingParallelizationLmFetcher { lm: mock_lm.clone() };
        let evaluator = Arc::new(Mutex::new(Evaluator::new(None, &[])));
        let factory = Arc::new(InstanceManagerFactory::new(
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            Arc::new(HashMap::new()),
            evaluator.clone(),
            Arc::new(Mutex::new(HashMap::new())),
        ));
        let test_orch = OrchestratorEngine::new(
            evaluator,
            Arc::new(Mutex::new(None)),
            factory,
            get_mock_crash_notifier_persistent_connection(),
            create_unused_new_subscriber_receiver(),
        );

        let mut new_states = HashMap::new();
        for bundle_number in 1..32_u8 {
            for target_state in
                [InstanceState::Created, InstanceState::Started, InstanceState::Destroyed]
            {
                let fqin = Fqin::new(
                    &format!("com.google.sdv.test{bundle_number}"),
                    "ServiceName",
                    &format!("instance-{target_state:?}"),
                );
                new_states.insert(fqin, target_state);
            }
        }

        // WHEN
        test_orch.enforce_instances_states(new_states, Arc::new(AtomicBool::new(false))).await;

        // THEN
        // With given test payload we should achieve maximum of 8 transactions in
        // parallel at some point in time.
        assert_that!(mock_lm.max_pending_transactions.load(Ordering::Relaxed), eq(12));

        // sanity checks: all pending transactions should be finished
        assert_that!(mock_lm.pending_transactions.load(Ordering::Relaxed), eq(0));
    }

    async fn test_parallel_transitions(target_state: InstanceState) {
        // GIVEN
        let mock_lm = Arc::new(ParallelizationLmMock::default());
        let fetcher = ParallelizationLmFetcher { lm: mock_lm.clone() };
        let evaluator = Arc::new(Mutex::new(Evaluator::new(None, &[])));
        let factory = Arc::new(InstanceManagerFactory::new(
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            Arc::new(HashMap::new()),
            evaluator.clone(),
            Arc::new(Mutex::new(HashMap::new())),
        ));
        let test_orch = OrchestratorEngine::new(
            evaluator,
            Arc::new(Mutex::new(None)),
            factory,
            get_mock_crash_notifier_persistent_connection(),
            create_unused_new_subscriber_receiver(),
        );

        let fqins = (1..64_u8)
            .map(|i| Fqin::new(&format!("com.google.sdv.test{i}"), "ServiceName", "instance"));
        let mut new_states = HashMap::new();
        for fqin in fqins {
            new_states.insert(fqin.clone(), target_state.clone());
        }

        // WHEN
        test_orch.enforce_instances_states(new_states, Arc::new(AtomicBool::new(false))).await;

        // THEN
        // With given test payload we should achieve maximum of 8 transactions in
        // parallel at some point in time.
        assert_that!(mock_lm.max_pending_transactions.load(Ordering::Relaxed), eq(12));

        // sanity checks: all pending transactions should be finished
        assert_that!(mock_lm.pending_transactions.load(Ordering::Relaxed), eq(0));
    }

    #[tokio::test]
    async fn test_transition_to_created() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let test_context = TestContext::new(LMInterceptor::default());
        let mut new_states = HashMap::new();
        new_states.insert(fqin.clone(), InstanceState::Created);

        // WHEN
        test_context
            .orch
            .enforce_instances_states(new_states, Arc::new(AtomicBool::new(false)))
            .await;

        // THEN
        test_context.assert_lm_calls(&fqin, vec![LMCall::Launch]);
        assert_that!(test_context.orch.enforcing_bundles.lock().unwrap().len(), eq(1));
        assert!(test_context.orch.enforcing_bundles.lock().unwrap().contains_key(&fqin));
    }

    #[tokio::test]
    async fn test_transition_to_started() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let test_context = TestContext::new(LMInterceptor::default());
        let mut new_states = HashMap::new();
        new_states.insert(fqin.clone(), InstanceState::Started);

        // WHEN
        test_context
            .orch
            .enforce_instances_states(new_states, Arc::new(AtomicBool::new(false)))
            .await;

        // THEN
        test_context.assert_lm_calls(&fqin, vec![LMCall::Start]);
        assert_that!(test_context.orch.enforcing_bundles.lock().unwrap().len(), eq(1));
        assert!(test_context.orch.enforcing_bundles.lock().unwrap().contains_key(&fqin));
    }

    #[tokio::test]
    async fn test_transition_from_destroyed_to_destroyed() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let test_context = TestContext::new(LMInterceptor::default());
        let mut new_states = HashMap::new();
        new_states.insert(fqin.clone(), InstanceState::Destroyed);

        // WHEN
        test_context
            .orch
            .enforce_instances_states(new_states, Arc::new(AtomicBool::new(false)))
            .await;

        // THEN
        test_context.assert_lm_calls(&fqin, vec![LMCall::Shutdown]);
        // We don't remove the instance manager when destroying
        assert_that!(test_context.orch.enforcing_bundles.lock().unwrap().len(), eq(1));
    }

    #[tokio::test]
    async fn test_transition_to_destroyed() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let test_context = TestContext::new(LMInterceptor::default());
        let mut new_states = HashMap::new();
        // We need to start first the service to destroy it after
        new_states.insert(fqin.clone(), InstanceState::Started);
        test_context
            .orch
            .enforce_instances_states(new_states.clone(), Arc::new(AtomicBool::new(false)))
            .await;
        test_context.lm_interceptor.reset_calls();
        // WHEN
        new_states.insert(fqin.clone(), InstanceState::Destroyed);
        test_context
            .orch
            .enforce_instances_states(new_states, Arc::new(AtomicBool::new(false)))
            .await;

        // THEN
        test_context.assert_lm_calls(&fqin, vec![LMCall::Shutdown]);
        // We don't remove the instance manager when destroying
        assert_that!(test_context.orch.enforcing_bundles.lock().unwrap().len(), eq(1));
    }

    // Test that obsolete bundles are destroyed

    #[tokio::test]
    async fn test_transition_started_to_undefined() {
        // GIVEN / WHEN
        let (fqin, test_context) =
            given_transition_to_undefined_state(InstanceState::Started).await;

        // THEN only stop and shutdown are called
        test_context.assert_lm_calls(&fqin, vec![LMCall::Shutdown]);
    }

    #[tokio::test]
    async fn test_transition_created_to_undefined() {
        // GIVEN / WHEN
        let (fqin, test_context) =
            given_transition_to_undefined_state(InstanceState::Created).await;

        // THEN only stop and shutdown are called
        test_context.assert_lm_calls(&fqin, vec![LMCall::Shutdown]);
    }

    #[tokio::test]
    async fn test_transition_destroyed_to_undefined() {
        // GIVEN / WHEN
        let (fqin, test_context) =
            given_transition_to_undefined_state(InstanceState::Destroyed).await;

        // THEN it should not be destroyed a second time.
        test_context.assert_lm_calls(&fqin, vec![]);
    }

    async fn given_transition_to_undefined_state(old_state: InstanceState) -> (Fqin, TestContext) {
        // GIVEN current state is setup
        let (fqin, test_context) = given_initial_transition_to_state(old_state).await;

        // reset interceptor, to only capture calls during the transition
        test_context.lm_interceptor.reset_calls();

        // WHEN transitioning to state where no service bundle should be running
        test_context
            .orch
            .enforce_instances_states(HashMap::new(), Arc::new(AtomicBool::new(false)))
            .await;

        (fqin, test_context)
    }

    async fn given_initial_transition_to_state(new_state: InstanceState) -> (Fqin, TestContext) {
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let test_context = TestContext::new(LMInterceptor::default());

        // GIVEN transitioning to the initial state
        let mut new_states = HashMap::new();
        new_states.insert(fqin.clone(), new_state);
        test_context
            .orch
            .enforce_instances_states(new_states, Arc::new(AtomicBool::new(false)))
            .await;

        (fqin, test_context)
    }

    // On state update tests

    #[tokio::test]
    async fn test_on_state_update_should_start_required_services() {
        // GIVEN
        let fqin = Fqin::new("com.android.sdv.samples", "sample_service_name", "sample_instance");
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // WHEN
        test_context
            .orch
            .on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(false)), &None)
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![LMCall::Start]);
    }

    #[tokio::test]
    async fn test_on_state_update_should_destroy_obsolete_services() {
        // GIVEN
        let fqin = Fqin::new("com.android.sdv.samples", "sample_service_name", "sample_instance");
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // Start test service
        test_context
            .orch
            .on_state_update(
                &Mode::Power,
                &String::from("ON"),
                Arc::new(AtomicBool::new(false)),
                &None,
            )
            .await;
        test_context.lm_interceptor.reset_calls();
        // WHEN
        test_context
            .orch
            .on_state_update(
                &Mode::Power,
                "SHUTDOWN_PREPARE",
                Arc::new(AtomicBool::new(false)),
                &None,
            )
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![LMCall::Shutdown]);
    }

    #[tokio::test]
    async fn test_on_cancel_it_should_not_process_any_services() {
        // GIVEN
        let fqin = Fqin::new("com.android.sdv.samples", "sample_service_name", "sample_instance");
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // Start test service
        // WHEN
        test_context
            .orch
            .on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(true)), &None)
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![]);
    }

    // We verify that the orch config was updated as in the config we have the condition:
    //     - Power: ON && Vehicle: Park => Start sample_instance
    // We send the "Power: ON" with cancellation in true: only the orch config should be updated but no LM call is made.
    // We send the "Vehicle: PARK" with cancellation in false: orch config is updated and LM call should have been made (which would have started the bundle).
    // If the "Power: ON" would have not been set in the config, then the service bundle would not have been started as we have the AND condition.
    #[tokio::test]
    async fn test_on_cancel_it_should_update_orch_config() {
        // GIVEN
        let fqin = Fqin::new("com.android.sdv.samples", "sample_service_name", "sample_instance");
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // WHEN
        test_context
            .orch
            .on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(true)), &None)
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![]);
        // WHEN
        test_context
            .orch
            .on_state_update(&Mode::Vehicle, "PARK", Arc::new(AtomicBool::new(false)), &None)
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![LMCall::Start]);
    }

    #[tokio::test]
    async fn test_should_not_apply_update_on_outdated_timestamp_for_custom_mode() {
        // GIVEN
        let timestamp_older = Some(Timestamp { seconds: 10, nanos: 0, ..Timestamp::new() });
        let timestamp_newer = Some(Timestamp { seconds: 15, nanos: 0, ..Timestamp::new() });
        let fqin = Fqin::new("com.android.sdv.samples", "sample_service_name", "sample_instance");
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // WHEN
        // This state should not trigger any transition for the bundle
        test_context
            .orch
            .on_state_update(
                &Mode::Custom("TEST".to_string()),
                "some-state",
                Arc::new(AtomicBool::new(false)),
                &timestamp_newer,
            )
            .await;
        // This state should have triggered the start, but the timestamp is older than the previous one, so the bundle is not started.
        test_context
            .orch
            .on_state_update(
                &Mode::Custom("TEST".to_string()),
                "start",
                Arc::new(AtomicBool::new(false)),
                &timestamp_older,
            )
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![]);
        assert_that!(
            test_context.orch.dump_state.lock().unwrap().as_ref().unwrap(),
            eq("Last mode enforced was Custom(\"TEST\") with value \"some-state\"")
        );
    }

    #[tokio::test]
    async fn test_on_incorrect_update_event_type_it_should_not_process_any_updates() {
        // GIVEN
        let fqin = Fqin::new("com.android.sdv.samples", "sample_service_name", "sample_instance");
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // Set a state first so it is not started on the recovery
        test_context
            .orch
            .on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(false)), &None)
            .await;
        // Reset the call so that we verify that the Start is not called in the update event
        test_context.lm_interceptor.reset_calls();
        // WHEN
        test_context
            .orch
            .on_event_update(
                &UpdateEvent::StateChange(Mode::Power, "ON".to_string()),
                Arc::new(AtomicBool::new(true)),
            )
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![]);
    }

    #[tokio::test]
    async fn test_on_recovery_event_should_invalidate_instance_states() {
        // GIVEN
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);

        let mut mock_im = MockIInstanceManager::new();
        // We expect `invalidate_state` to be called exactly once.
        mock_im.expect_invalidate_state().times(1).return_const(());
        // We need to mock `is_not_destroyed` as it's called when destroying obsolete services.
        mock_im.expect_is_not_destroyed().returning(|| true);
        // We need to mock `orchestrate_transition_request` as it's called to enforce the new state.
        mock_im.expect_orchestrate_transition_request().return_const(());
        let mock_im = Arc::new(mock_im);

        let mut factory = MockIInstanceManagerFactory::new();
        factory.expect_create().returning(move |_| mock_im.clone());
        let orch = OrchestratorEngine::new(
            Arc::new(Mutex::new(config)),
            Arc::new(Mutex::new(None)),
            Arc::new(factory),
            get_mock_crash_notifier_persistent_connection(),
            create_unused_new_subscriber_receiver(),
        );

        // Enforce an initial state to populate `enforcing_bundles`.
        orch.on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(false)), &None).await;

        // WHEN a recovery event occurs.
        orch.on_event_update(&UpdateEvent::Recovery, Arc::new(AtomicBool::new(false))).await;
    }

    #[tokio::test]
    async fn test_on_default_event_should_not_invalidate_instance_states() {
        // GIVEN
        let config = Evaluator::new(Some(String::from(TEST_PROTO_NO_CONDITION_PATH)), &[]);

        let mut mock_im = MockIInstanceManager::new();
        // We expect `invalidate_state` to never be called.
        mock_im.expect_invalidate_state().never();
        mock_im.expect_is_not_destroyed().returning(|| false);
        mock_im.expect_orchestrate_transition_request().return_const(());
        let mock_im = Arc::new(mock_im);

        let mut factory = MockIInstanceManagerFactory::new();
        factory.expect_create().returning(move |_| mock_im.clone());
        let orch = OrchestratorEngine::new(
            Arc::new(Mutex::new(config)),
            Arc::new(Mutex::new(None)),
            Arc::new(factory),
            get_mock_crash_notifier_persistent_connection(),
            create_unused_new_subscriber_receiver(),
        );

        // WHEN a default event occurs.
        orch.on_event_update(&UpdateEvent::Default, Arc::new(AtomicBool::new(false))).await;
    }

    // On default state test

    #[tokio::test]
    async fn test_on_default_update_should_launch_services_with_no_condition() {
        // GIVEN / WHEN
        let fqin = Fqin::new(
            "com.android.sdv.samples",
            "sample_service_name_no_conditions",
            "sample_instance_no_conditions",
        );
        let config = Evaluator::new(Some(String::from(TEST_PROTO_NO_CONDITION_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        test_context
            .orch
            .on_event_update(&UpdateEvent::Default, Arc::new(AtomicBool::new(false)))
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![LMCall::Start]);
    }

    #[tokio::test]
    async fn test_on_default_cancelled_it_should_not_process_any_updates() {
        // GIVEN
        let fqin = Fqin::new("com.android.sdv.samples", "sample_service_name", "sample_instance");
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // Set a state first so it is not started on the recovery
        test_context
            .orch
            .on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(false)), &None)
            .await;
        // Reset the call so that we verify that the Start is not called in the update event
        test_context.lm_interceptor.reset_calls();
        // WHEN
        test_context
            .orch
            .on_event_update(&UpdateEvent::Default, Arc::new(AtomicBool::new(true)))
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![]);
    }

    // On recovery state test

    #[tokio::test]
    async fn test_on_recovery_update_should_start_required_services() {
        // GIVEN
        let fqin = Fqin::new("com.android.sdv.samples", "sample_service_name", "sample_instance");
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // Set a state first so it is started on the recovery
        test_context
            .orch
            .on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(false)), &None)
            .await;
        // Reset the call so that we verify that the Start is called on the Recovery
        test_context.lm_interceptor.reset_calls();
        // WHEN
        test_context
            .orch
            .on_event_update(&UpdateEvent::Recovery, Arc::new(AtomicBool::new(false)))
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![LMCall::Start]);
    }

    #[tokio::test]
    async fn test_on_recovery_cancelled_it_should_not_process_any_updates() {
        // GIVEN
        let fqin = Fqin::new("com.android.sdv.samples", "sample_service_name", "sample_instance");
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // Set a state first so it is not started on the recovery
        test_context
            .orch
            .on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(false)), &None)
            .await;
        // Reset the call so that we verify that the Start is not called in the update event
        test_context.lm_interceptor.reset_calls();
        // WHEN
        test_context
            .orch
            .on_event_update(&UpdateEvent::Recovery, Arc::new(AtomicBool::new(true)))
            .await;
        // THEN
        test_context.assert_lm_calls(&fqin, vec![]);
    }

    // Dump tests

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_dump_in_progress_mode_updates_with_default_value() {
        // GIVEN
        // This channel is used to simulate that there's a bundle that takes a long time to start.
        // So we can verify that if we dump while processing modes, the dump will inform which value is in progess.
        let (block_start_service_sender, block_start_service_recv) = broadcast::channel(1);
        // This channel is used to notify that the start_service was called and is being executed.
        // This informs us the first moment from which we can verify the value contained by the dump string.
        let (start_service_channel_sender, mut start_service_channel_recv) = broadcast::channel(1);
        let fetcher =
            DumpsysLmMock::new(start_service_channel_sender, Arc::new(block_start_service_recv));
        let dump_state = Arc::new(Mutex::new(None));
        let dump_state_clone = dump_state.clone();
        // WHEN
        tokio::spawn(async move {
            let evaluator = Arc::new(Mutex::new(Evaluator::new(
                Some(String::from(TEST_PROTO_NO_CONDITION_PATH)),
                &[],
            )));
            let factory = Arc::new(InstanceManagerFactory::new(
                PersistentBinderConnection::new(move || fetcher.get_lm()),
                Arc::new(HashMap::new()),
                evaluator.clone(),
                Arc::new(Mutex::new(HashMap::new())),
            ));
            let orch = OrchestratorEngine::new(
                evaluator,
                dump_state_clone,
                factory,
                get_mock_crash_notifier_persistent_connection(),
                create_unused_new_subscriber_receiver(),
            );
            orch.on_event_update(&UpdateEvent::Default, Arc::new(AtomicBool::new(false))).await;
        });
        // THEN
        // We wait until the start_service was called because at this point we know that the on_state_update
        // was called and the dump value was filled with the current processing mode
        let _ = start_service_channel_recv.recv().await;
        assert_that!(
            dump_state.lock().unwrap().as_ref().unwrap(),
            eq(&format!("{}", UpdateEvent::Default))
        );
        // Notify that the start_service can finish now so we do not block the test
        let _ = block_start_service_sender.send(());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_dump_in_progress_mode_updates_with_recovery_value() {
        // GIVEN
        // This channel is used to simulate that there's a bundle that takes a long time to start.
        // So we can verify that if we dump while processing modes, the dump will inform which value is in progess.
        let (block_start_service_sender, block_start_service_recv) = broadcast::channel(1);
        // This channel is used to notify that the start_service was called and is being executed.
        // This informs us the first moment from which we can verify the value contained by the dump string.
        let (start_service_channel_sender, mut start_service_channel_recv) = broadcast::channel(1);
        let fetcher =
            DumpsysLmMock::new(start_service_channel_sender, Arc::new(block_start_service_recv));
        let dump_state = Arc::new(Mutex::new(None));
        let dump_state_clone = dump_state.clone();
        // WHEN
        tokio::spawn(async move {
            let evaluator = Arc::new(Mutex::new(Evaluator::new(
                Some(String::from(TEST_PROTO_NO_CONDITION_PATH)),
                &[],
            )));
            let factory = Arc::new(InstanceManagerFactory::new(
                PersistentBinderConnection::new(move || fetcher.get_lm()),
                Arc::new(HashMap::new()),
                evaluator.clone(),
                Arc::new(Mutex::new(HashMap::new())),
            ));
            let orch = OrchestratorEngine::new(
                evaluator,
                dump_state_clone,
                factory,
                get_mock_crash_notifier_persistent_connection(),
                create_unused_new_subscriber_receiver(),
            );
            orch.on_event_update(&UpdateEvent::Recovery, Arc::new(AtomicBool::new(false))).await;
        });
        // THEN
        // We wait until the start_service was called because at this point we know that the on_state_update
        // was called and the dump value was filled with the current processing mode
        let _ = start_service_channel_recv.recv().await;
        assert_that!(
            dump_state.lock().unwrap().as_ref().unwrap(),
            eq(&format!("{}", UpdateEvent::Recovery))
        );
        // Notify that the start_service can finish now so we do not block the test
        let _ = block_start_service_sender.send(());
    }

    #[tokio::test]
    async fn test_on_default_update_should_dump_value_on_finish() {
        // GIVEN
        let config = Evaluator::new(Some(String::from(TEST_PROTO_NO_CONDITION_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // WHEN
        test_context
            .orch
            .on_event_update(&UpdateEvent::Default, Arc::new(AtomicBool::new(false)))
            .await;
        // THEN
        let dump_state = test_context.orch.dump_state.lock().unwrap();
        assert_that!(
            dump_state.as_ref().unwrap(),
            eq(&format!("Finished: {}", UpdateEvent::Default))
        );
    }

    #[tokio::test]
    async fn test_on_recovery_update_should_dump_value_on_finish() {
        // GIVEN
        let config = Evaluator::new(Some(String::from(TEST_PROTO_NO_CONDITION_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // WHEN
        test_context
            .orch
            .on_event_update(&UpdateEvent::Recovery, Arc::new(AtomicBool::new(false)))
            .await;
        // THEN
        let dump_state = test_context.orch.dump_state.lock().unwrap();
        assert_that!(
            dump_state.as_ref().unwrap(),
            eq(&format!("Finished: {}", UpdateEvent::Recovery))
        );
    }

    #[tokio::test]
    async fn test_should_dump_configuration() {
        // GIVEN
        let mut config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        config.update_orchestration_state(&Mode::Power, "ON", &None);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        let mut buffer = Vec::new();
        // WHEN
        let res = test_context.orch.dump(&mut buffer, &[]);
        let buf_str = String::from_utf8(buffer.clone()).unwrap();
        // THEN
        assert!(res.is_ok());
        // We just check that the dump is dumping contains some of the config information.
        // The specific information dumped is tested in the config lib.
        assert!(buf_str.contains("Power"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_dump_in_progress_mode_updates() {
        // GIVEN
        // This channel is used to simulate that there's a bundle that takes a long time to start.
        // So we can verify that if we dump while processing modes, the dump will inform which value is in progess.
        let (block_start_service_sender, block_start_service_recv) = broadcast::channel(1);
        // This channel is used to notify that the start_service was called and is being executed.
        // This informs us the first moment from which we can verify the value contained by the dump string.
        let (start_service_channel_sender, mut start_service_channel_recv) = broadcast::channel(1);
        let fetcher =
            DumpsysLmMock::new(start_service_channel_sender, Arc::new(block_start_service_recv));
        let dump_state = Arc::new(Mutex::new(None));
        let dump_state_clone = dump_state.clone();
        // WHEN
        tokio::spawn(async move {
            let evaluator =
                Arc::new(Mutex::new(Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[])));
            let factory = Arc::new(InstanceManagerFactory::new(
                PersistentBinderConnection::new(move || fetcher.get_lm()),
                Arc::new(HashMap::new()),
                evaluator.clone(),
                Arc::new(Mutex::new(HashMap::new())),
            ));
            let orch = OrchestratorEngine::new(
                evaluator,
                dump_state_clone,
                factory,
                get_mock_crash_notifier_persistent_connection(),
                create_unused_new_subscriber_receiver(),
            );
            orch.on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(false)), &None).await;
        });
        // THEN
        // We wait until the start_service was called because at this point we know that the on_state_update
        // was called and the dump value was filled with the current processing mode
        let _ = start_service_channel_recv.recv().await;
        assert_that!(
            dump_state.lock().unwrap().as_ref().unwrap(),
            eq("Processing mode Power with value \"ON\"")
        );
        // Notify that the start_service can finish now so we do not block the test
        let _ = block_start_service_sender.send(());
    }

    #[tokio::test]
    async fn test_should_dump_last_enforced_mode() {
        // GIVEN
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // WHEN
        test_context
            .orch
            .on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(false)), &None)
            .await;
        // THEN
        let dump_state = test_context.orch.dump_state.lock().unwrap();
        assert_that!(
            dump_state.as_ref().unwrap(),
            eq("Last mode enforced was Power with value \"ON\"")
        );
    }

    #[tokio::test]
    async fn test_should_dump_last_enforced_mode_when_cancelled() {
        // GIVEN
        let config = Evaluator::new(Some(String::from(TEST_PROTO_PATH)), &[]);
        let test_context = TestContext::new_with_config(LMInterceptor::default(), config);
        // WHEN
        test_context
            .orch
            .on_state_update(&Mode::Power, "ON", Arc::new(AtomicBool::new(true)), &None)
            .await;
        // THEN
        let dump_state = test_context.orch.dump_state.lock().unwrap();
        assert_that!(
            dump_state.as_ref().unwrap(),
            eq("Last mode enforced was Power with value \"ON\"")
        );
    }

    #[test]
    fn test_lazylock_returns_default_value_when_no_sysprop_modifications() {
        assert!(
            *LM_CALLS_MAX_THREAD_LIMIT == FALLBACK_LM_CALLS_MAX_THREAD,
            "lazy sysprop read unexpected value: {}",
            *LM_CALLS_MAX_THREAD_LIMIT
        );
    }

    #[tokio::test]
    async fn test_read_or_default_returns_set_sys_prop() {
        const EXPECTED_VALUE: usize = 99;
        let read_value = read_lm_binder_max_parallelism_sys_prop_or_default(|sys_prop_name| {
            assert!(sys_prop_name == LM_CALLS_MAX_THREAD_LIMIT_PROPERTY);
            Ok(Some(EXPECTED_VALUE.to_string()))
        });
        assert!(read_value == EXPECTED_VALUE);
    }

    #[tokio::test]
    async fn test_get_sys_prop_returns_fallback_if_missing_sys_prop() {
        let read_value = read_lm_binder_max_parallelism_sys_prop_or_default(|_| Ok(None));
        assert!(read_value == FALLBACK_LM_CALLS_MAX_THREAD);
    }

    // Crash notification subscription tests

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_subscribe_to_crash_notifications_on_reconnection() {
        // GIVEN
        let mut mock_crash_notifier = MockIServiceBundleCrashNotificationService::new();
        // THEN subscribe should be called twice: once on initial creation, and once on reconnection.
        mock_crash_notifier.expect_subscribe().times(2).returning(|_| Ok(()));

        let crash_binder = BnServiceBundleCrashNotificationService::new_binder(
            mock_crash_notifier,
            BinderFeatures::default(),
        );
        let mut mock_crash_notifier_binder = MockBinderConnectionManager::new();
        mock_crash_notifier_binder
            .expect_get_binder()
            .return_const(Arc::new(Mutex::new(crash_binder.clone())));

        mock_crash_notifier_binder.expect_set_reconnect_listener().returning(move |listener| {
            // WHEN crash notifier binder dies, then we should subscribe for the crash notification for the 2nd time
            listener(&crash_binder);
        });
        // WHEN creating the engine, we should subscribe for crash notification for the 1st time
        let evaluator = Arc::new(Mutex::new(Evaluator::new(None, &[])));
        let factory = Arc::new(InstanceManagerFactory::new(
            PersistentBinderConnection::new(move || {
                MockLifecycleManager::create(Arc::new(LMInterceptor::default())).get_lm()
            }),
            Arc::new(HashMap::new()),
            evaluator.clone(),
            Arc::new(Mutex::new(HashMap::new())),
        ));
        OrchestratorEngine::new(
            evaluator,
            Arc::new(Mutex::new(None)),
            factory,
            Arc::new(mock_crash_notifier_binder),
            create_unused_new_subscriber_receiver(),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_not_panic_if_subscription_to_crash_notification_fails() {
        // GIVEN
        let mut mock_crash_notifier = MockIServiceBundleCrashNotificationService::new();
        mock_crash_notifier.expect_subscribe().times(1).returning(|_| {
            // WHEN subscription returns error
            Err(Status::new_service_specific_error_str(
                binder::StatusCode::FAILED_TRANSACTION as i32,
                Some("Error."),
            ))
        });
        let mut mock_crash_notifier_binder = MockBinderConnectionManager::new();
        mock_crash_notifier_binder.expect_set_reconnect_listener().returning(|_| ());
        mock_crash_notifier_binder.expect_get_binder().return_const(Arc::new(Mutex::new(
            BnServiceBundleCrashNotificationService::new_binder(
                mock_crash_notifier,
                BinderFeatures::default(),
            ),
        )));
        // THEN engine should not panic
        let evaluator = Arc::new(Mutex::new(Evaluator::new(None, &[])));
        let factory = Arc::new(InstanceManagerFactory::new(
            PersistentBinderConnection::new(move || {
                MockLifecycleManager::create(Arc::new(LMInterceptor::default())).get_lm()
            }),
            Arc::new(HashMap::new()),
            evaluator.clone(),
            Arc::new(Mutex::new(HashMap::new())),
        ));
        OrchestratorEngine::new(
            evaluator,
            Arc::new(Mutex::new(None)),
            factory,
            Arc::new(mock_crash_notifier_binder),
            create_unused_new_subscriber_receiver(),
        );
    }

    // Crash notification tests

    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_return_ok_on_crash_notification() {
        // GIVEN
        let mut mock_crash_notifier = MockIServiceBundleCrashNotificationService::new();
        mock_crash_notifier.expect_subscribe().times(1).returning(|_| Ok(()));
        let mut mock_crash_notifier_binder = MockBinderConnectionManager::new();
        mock_crash_notifier_binder.expect_get_binder().return_const(Arc::new(Mutex::new(
            BnServiceBundleCrashNotificationService::new_binder(
                mock_crash_notifier,
                BinderFeatures::default(),
            ),
        )));
        mock_crash_notifier_binder.expect_set_reconnect_listener().returning(|_| ());
        let evaluator = Arc::new(Mutex::new(Evaluator::new(None, &[])));
        let factory = Arc::new(InstanceManagerFactory::new(
            PersistentBinderConnection::new(move || {
                MockLifecycleManager::create(Arc::new(LMInterceptor::default())).get_lm()
            }),
            Arc::new(HashMap::new()),
            evaluator.clone(),
            Arc::new(Mutex::new(HashMap::new())),
        ));
        let engine = OrchestratorEngine::new(
            evaluator,
            Arc::new(Mutex::new(None)),
            factory,
            Arc::new(mock_crash_notifier_binder),
            create_unused_new_subscriber_receiver(),
        );
        // WHEN
        let res = engine
            .onServiceBundleCrash(&ServiceFqin {
                sdvVmName: "local-vm".to_owned(),
                sdvPackageName: "com.google.sdv.test".to_owned(),
                serviceBundleName: "BundleName".to_owned(),
                serviceInstanceName: "InstanceName".to_owned(),
            })
            .await;
        // TODO(b/419239856): extend with expected crash functionality
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_on_crash_notification_it_invalidates_state_and_handles_non_restartable_bundle() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "BundleName", "InstanceName");
        let lm_fqin = fqin.to_lm_fqin();

        let mut mock_im = MockIInstanceManager::new();
        // THEN we expect invalidate_state to be called.
        mock_im.expect_invalidate_state().times(1).return_const(());
        // And since we'll mock it as not restartable, orchestrate_restart should not be called.
        mock_im.expect_is_restartable().times(1).returning(|| false);
        mock_im.expect_handle_non_restartable_bundle_crash().times(1).return_const(());
        mock_im.expect_orchestrate_restart().never();

        let mock_im = Arc::new(mock_im);
        let mut factory = MockIInstanceManagerFactory::new();
        factory.expect_create().returning(move |_| mock_im.clone());

        let engine = OrchestratorEngine::new(
            Arc::new(Mutex::new(Evaluator::new(None, &[]))),
            Arc::new(Mutex::new(None)),
            Arc::new(factory),
            get_mock_crash_notifier_persistent_connection(),
            create_unused_new_subscriber_receiver(),
        );

        // WHEN a crash notification is received for the bundle.
        let result = engine.onServiceBundleCrash(&lm_fqin).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_on_crash_notification_it_invalidates_state_and_restarts_restartable_bundle() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "BundleName", "InstanceName");
        let lm_fqin = fqin.to_lm_fqin();

        let mut mock_im = MockIInstanceManager::new();
        // THEN we expect invalidate_state to be called.
        mock_im.expect_invalidate_state().times(1).return_const(());
        // And since we'll mock it as not restartable, orchestrate_restart should not be called.
        mock_im.expect_is_restartable().times(1).returning(|| true);
        mock_im.expect_handle_non_restartable_bundle_crash().never();
        mock_im.expect_orchestrate_restart().times(1).returning(|| None);

        let mock_im = Arc::new(mock_im);
        let mut factory = MockIInstanceManagerFactory::new();
        factory.expect_create().returning(move |_| mock_im.clone());

        let engine = OrchestratorEngine::new(
            Arc::new(Mutex::new(Evaluator::new(None, &[]))),
            Arc::new(Mutex::new(None)),
            Arc::new(factory),
            get_mock_crash_notifier_persistent_connection(),
            create_unused_new_subscriber_receiver(),
        );

        // WHEN a crash notification is received for the bundle.
        let result = engine.onServiceBundleCrash(&lm_fqin).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_new_state_reporter_listener_added_triggers_state_reporting() {
        // GIVEN
        let (new_listener_registered_sender, new_listener_registered_receiver) = mpsc::channel(1);

        let engine = OrchestratorEngine::new(
            Arc::new(Mutex::new(Evaluator::new(None, &[]))),
            Arc::new(Mutex::new(None)),
            Arc::new(MockIInstanceManagerFactory::new()),
            get_mock_crash_notifier_persistent_connection(),
            new_listener_registered_receiver,
        );
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_im = MockIInstanceManager::new();
        let mock_listener_binder = BnServiceBundleInstanceStateChangeListener::new_binder(
            MockIServiceBundleInstanceStateChangeListener::new(),
            BinderFeatures::default(),
        );

        let (listener_invoked_sender, mut listener_invoked_receiver) = mpsc::channel::<()>(1);
        mock_im.expect_report_current_state_to_listener().times(1).returning(move |_| {
            listener_invoked_sender.try_send(()).unwrap();
        });

        // Inject the instance manager into the enforcing_bundles
        let mock_im = Arc::new(mock_im);
        engine.enforcing_bundles.lock().unwrap().insert(fqin.clone(), mock_im);

        // WHEN new listener is added
        new_listener_registered_sender.send(mock_listener_binder).await.unwrap();

        // THEN the report current state was invoked
        tokio::time::timeout(Duration::from_secs(1), listener_invoked_receiver.recv())
            .await
            .expect("Timeout waiting for receiver invocation")
            .unwrap();
    }
}
