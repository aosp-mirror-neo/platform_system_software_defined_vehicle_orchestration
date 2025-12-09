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
//! Handles the lifecycle state machine for one single service bundle instance.

use binder::Status;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::ILifecycleManager;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ResponseCode::ResponseCode;
use log::{debug, info, warn};
use persistent_binder::BinderConnectionManager;
use sdv_orchestration_common::fqin::Fqin;
use sdv_orchestration_common::instance_state::InstanceState;
use sdv_orchestration_common::retry_configuration::RetryConfiguration;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{oneshot, Notify};

#[derive(PartialEq)]
/// Defines the types of errors that can occur when interacting with the Lifecycle Manager (LM).
/// These error types determine the retry strategy.
enum ErrorType {
    /// A temporary error that might be resolved by simply retrying the operation.
    /// No special cleanup action is taken before the next attempt.
    TRANSIENT,
    /// An error that suggests the service bundle might be in a corrupted state.
    /// Before retrying, an attempt is made to kill the service instance to ensure a clean restart.
    PERSISTENT,
    /// A permanent error that is unlikely to be resolved by retrying (e.g., PermissionDenied).
    /// No further retry attempts are made.
    PERMANENT,
}

/// The last state set for the instance.
#[derive(PartialEq)]
enum LastState {
    /// This case means that we haven't finished applying any transition yet.
    UNKNOWN,
    /// Last state transition ended in an error, so we do not know the actual
    /// state for the instance.
    ERROR,
    /// The successfully set last state for the instance.
    STATE(InstanceState),
}

/// Request a new state for the instance.
pub struct TransitionRequest {
    /// The new state requested for the instance.
    pub new_state: InstanceState,
    /// A transition can be cancelled in the following cases:
    ///  - LM crashed and we cancel all ongoing transitions, followed by a new recover event.
    ///  - We are performing a crash recovery (restart) and a new mode is set, requiring the
    ///    cancellation of the current transition.
    pub cancel: Arc<AtomicBool>,
    /// Notify to the originator of the transition that the request was completed.
    /// This is needed for power modes to notify VPM of completion.
    pub responder: Option<oneshot::Sender<Result<(), Status>>>,
}

macro_rules! lm {
    ($owner:expr) => {{
        $owner.get_binder().lock().unwrap()
    }};
}

/// Handles the lifecycle of a single bundle instance
pub struct InstanceManager {
    /// Last enforced state is used to check whether we need to apply the transition or
    /// if we can skip it.
    last_enforced_state: Arc<Mutex<LastState>>,
    /// Queue of all pending transitions.
    enforce_queue: Arc<Mutex<std::collections::VecDeque<TransitionRequest>>>,
    /// The restart configuration for this instance.
    restart_config: RetryConfiguration,
    /// Used to notify the enforcement task loop that there's a new transition request.
    notifier: Arc<Notify>,
    /// The FQIN of the service bundle instance managed by this manager.
    fqin: Fqin,
    /// The LM binder connection to perform transitions.
    lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
}

impl InstanceManager {
    pub fn new(
        fqin: Fqin,
        lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
        restart_config: RetryConfiguration,
    ) -> Arc<Self> {
        let last_enforced_state = Arc::new(Mutex::new(LastState::UNKNOWN));
        let enforce_queue: Arc<Mutex<std::collections::VecDeque<TransitionRequest>>> =
            Arc::new(Mutex::new(std::collections::VecDeque::new()));
        let notifier = Arc::new(Notify::new());

        let manager = Arc::new(Self {
            last_enforced_state: last_enforced_state.clone(),
            enforce_queue: enforce_queue.clone(),
            restart_config,
            notifier: notifier.clone(),
            fqin: fqin.clone(),
            lm_connection: lm_connection.clone(),
        });

        // Spawn the enforcement task
        let enforcement_manager_clone = manager.clone();
        tokio::spawn(async move {
            enforcement_manager_clone.enforcement_task_loop().await;
        });

        manager
    }

    /// Waits until there's a new state to enforce for the instance, consuming the transition queue.
    async fn enforcement_task_loop(&self) {
        loop {
            // Waits to be notified of a new request
            self.notifier.notified().await;
            // Process all requests in the queue one by one
            while let Some(mut request) = self.enforce_queue.lock().unwrap().pop_front() {
                let result = self.enforce_transition(&request);

                if let Some(responder) = request.responder.take() {
                    let res = responder.send(result);
                    if let Err(err) = res {
                        warn!(
                            "Failed to notify completion of transition for {:?} to {:?}: {:?}",
                            self.fqin, request.new_state, err
                        );
                    }
                }
            }
        }
    }

    /// Enforces a transition request, handling retries and cancellation.
    /// Returns a result to be sent to the request's responder.
    fn enforce_transition(&self, request: &TransitionRequest) -> Result<(), Status> {
        // We are enforcing a new state for the instance, so we reset the retries number.
        // We add 1 for the initial enforcement before retrying.
        let mut retries = self.restart_config.max_retries + 1;

        let mut last_error: Option<Status> = None;

        while retries > 0 && !request.cancel.load(Ordering::Relaxed) {
            match Self::orchestrate_transition_internal(
                &self.fqin,
                self.lm_connection.clone(),
                request.new_state.clone(),
                request.cancel.clone(),
            ) {
                Ok(()) => {
                    *self.last_enforced_state.lock().unwrap() =
                        LastState::STATE(request.new_state.clone());
                    return Ok(());
                }
                Err((message, error_type)) => {
                    *self.last_enforced_state.lock().unwrap() = LastState::ERROR;
                    retries -= 1;
                    if error_type == ErrorType::PERMANENT {
                        return Err(message);
                    }
                    last_error = Some(message);
                }
            };
        }

        if let Some(err) = last_error {
            return Err(err);
        }

        if request.cancel.load(Ordering::Relaxed) {
            let error_msg =
                format!("Enforcement for {:?} to {:?} was cancelled", self.fqin, request.new_state);
            return Err(Status::new_service_specific_error(
                -1,
                Some(&std::ffi::CString::new(error_msg).unwrap()),
            ));
        }

        // This should not be reached as retries is always greater than 0 and last_error would
        // contain Some(err) after one iteration with error.
        Ok(())
    }

    /// Sends a new transition request to the instance manager.
    /// If the instance is already destroyed, it will skip the transition.
    /// We only check for destroy at this stage as we have not integrated the LM crash recovery yet, so we are not sure of last enforced state.
    /// If a responder is set for the request, it will be notified when we finished executing the transition (either successfully or not).
    // TODO(b/419239856): check for all states after last state is invalidated after LM crash.
    pub fn orchestrate_transition_request(&self, request: TransitionRequest) {
        // TODO(b/419239856): cancel current restart (from crash) in progress + remove restarts from queue

        let last_state_guard = self.last_enforced_state.lock().unwrap();
        // If it's already destroyed, we skip the enforcement.
        // If last state was an error, we don't know if it's actually destroy, so we don't skip the destroyment.
        if request.new_state == InstanceState::Destroyed
            && (*last_state_guard == LastState::STATE(InstanceState::Destroyed)
                || *last_state_guard == LastState::UNKNOWN)
        {
            info!("Bundle {:?} is already destroyed, skipping transition.", self.fqin);
            if let Some(responder) = request.responder {
                let res = responder.send(Ok(()));
                if let Err(err) = res {
                    warn!(
                        "Failed to notify skipped destroying of instance {:?}: {err:?}",
                        self.fqin
                    );
                }
                return;
            }
        }

        let mut enforce_queue_guard = self.enforce_queue.lock().unwrap();
        enforce_queue_guard.push_back(request);

        self.notifier.notify_one();
    }

    /// Returns true if the instance has a last enforced state of created or started.
    pub fn is_created_or_started(&self) -> bool {
        let last_state = self.last_enforced_state.lock().unwrap();
        *last_state == LastState::STATE(InstanceState::Created)
            || *last_state == LastState::STATE(InstanceState::Started)
    }

    fn orchestrate_transition_internal(
        fqin: &Fqin,
        lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
        new_state: InstanceState,
        cancel: Arc<AtomicBool>,
    ) -> Result<(), (Status, ErrorType)> {
        if cancel.load(Ordering::Relaxed) {
            let error_msg = format!("Enforcement for {:?} to {:?} was cancelled", fqin, new_state);
            return Err((
                Status::new_service_specific_error(
                    -1,
                    Some(&std::ffi::CString::new(error_msg).unwrap()),
                ),
                ErrorType::PERMANENT,
            ));
        }
        match Self::try_lifecycle_transition(lm_connection.clone(), fqin, &new_state.clone()) {
            Ok(()) => {
                // TODO(b/437891686): notify state to HM
                Ok(())
            }
            Err(message) => {
                // TODO(b/437891686): notify state to HM
                warn!(
                    "Failed to transition service instance {:?} to {:?}: {message}",
                    fqin, new_state
                );
                let lm_err_type = Self::get_lm_error(&message);
                match lm_err_type {
                    ErrorType::PERSISTENT => {
                        let lm = lm!(lm_connection).clone();
                        let res = lm.killService(&fqin.to_lm_fqin());
                        if let Err(err) = res {
                            warn!(
                                "Failed to kill the bundle instance {:?} when retrying: {err:?}",
                                fqin
                            );
                        }
                    }
                    _ => {
                        // We just return the error
                    }
                }
                Err((message, lm_err_type))
            }
        }
    }

    fn get_lm_error(status: &Status) -> ErrorType {
        let error = status.service_specific_error();
        if error == ResponseCode::SERVICE_NOT_FOUND.0
            || error == ResponseCode::OPERATION_FAILED.0
            || error == ResponseCode::INTERNAL_ERROR.0
        {
            ErrorType::TRANSIENT
        } else if error == ResponseCode::VALUE_CORRUPTED.0
            || error == ResponseCode::INVALID_ARGUMENT.0
        {
            ErrorType::PERSISTENT
        } else {
            // PERMISSION_DENIED
            ErrorType::PERMANENT
        }
    }

    fn try_lifecycle_transition(
        connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
        fqin: &Fqin,
        state: &InstanceState,
    ) -> Result<(), Status> {
        match state {
            InstanceState::Created => {
                debug!("Requesting created state for service bundle {fqin:?}");
                let lm = lm!(connection).clone();
                let res = lm.launchService(&fqin.to_lm_fqin());
                info!("Request for moving service bundle {fqin:?} to CREATED state was {res:?}");
                res
            }
            InstanceState::Started => {
                debug!("Requesting started state for service bundle {fqin:?}");
                let lm = lm!(connection).clone();
                let res = lm.startService(&fqin.to_lm_fqin());
                info!("Request for moving service bundle {fqin:?} to STARTED state was {res:?}");
                res
            }
            InstanceState::Destroyed => {
                debug!("Requesting destroyed state for service bundle {fqin:?}");
                let lm = lm!(connection).clone();
                let res = lm.shutdownService(&fqin.to_lm_fqin());
                info!("Request for moving service bundle {fqin:?} to DESTROYED state was {res:?}");
                res
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LifecycleManagerFetcher;
    use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::MockILifecycleManager;
    use persistent_binder::PersistentBinderConnection;
    use sdv_orch_test_mocks::mock_all_lm_manager_wrapper::MockAllLmManagerWrapper;

    impl LifecycleManagerFetcher for MockAllLmManagerWrapper {
        fn get_lm(&self) -> binder::Strong<dyn ILifecycleManager> {
            binder::Strong::new(Box::new(self.clone()))
        }
    }

    #[tokio::test]
    async fn test_launch_service_should_return_error_if_lm_fails_to_launch_service() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let expected_error_code = -1;
        let error_message = c"failed to start service";
        let mut mock_lm = MockILifecycleManager::new();
        mock_lm.expect_launchService().times(1).returning(move |_| {
            Err(Status::new_service_specific_error(expected_error_code, Some(error_message)))
        });
        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            RetryConfiguration::new(0),
        );
        // WHEN
        let (responder, receiver) = oneshot::channel();
        let request = TransitionRequest {
            new_state: InstanceState::Created,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(request);
        let res = receiver.await.unwrap();

        // THEN
        let err = res.unwrap_err();
        assert_eq!(err.service_specific_error(), expected_error_code);
        assert!(err.to_string().contains(error_message.to_str().unwrap()));
    }

    #[tokio::test]
    async fn test_start_service_should_return_error_if_lm_fails_to_start_service() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let expected_error_code = -1;
        let error_message = c"failed to start service";
        let mut mock_lm = MockILifecycleManager::new();
        mock_lm.expect_startService().times(1).returning(move |_| {
            Err(Status::new_service_specific_error(expected_error_code, Some(error_message)))
        });
        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            RetryConfiguration::new(0), // Original restart_config is moved here
        );
        // WHEN
        let (responder, receiver) = oneshot::channel();
        let request = TransitionRequest {
            new_state: InstanceState::Started,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(request);
        let res = receiver.await.unwrap();
        // THEN
        let err = res.unwrap_err();
        assert_eq!(err.service_specific_error(), expected_error_code);
        assert!(err.to_string().contains(error_message.to_str().unwrap()));
    }

    #[tokio::test]
    async fn test_destroy_should_return_error_if_lm_fails_to_shutdown_service() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let expected_error_code = -1;
        let error_message = c"failed to destroy service";
        let mut mock_lm = MockILifecycleManager::new();
        mock_lm.expect_shutdownService().times(1).returning(move |_| {
            Err(Status::new_service_specific_error(expected_error_code, Some(error_message)))
        });
        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            RetryConfiguration::new(0),
        );
        // Set that the instance was started before
        *manager.last_enforced_state.lock().unwrap() = LastState::STATE(InstanceState::Started);
        // WHEN
        let (responder, receiver) = oneshot::channel();
        let request = TransitionRequest {
            new_state: InstanceState::Destroyed,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(request);
        let res = receiver.await.unwrap();
        // THEN
        let err = res.unwrap_err();
        assert_eq!(err.service_specific_error(), expected_error_code);
        assert!(err.to_string().contains(error_message.to_str().unwrap()));
    }

    #[tokio::test]
    async fn test_enforce_should_retry_on_transient_error() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        let expected_error_code = ResponseCode::SERVICE_NOT_FOUND.0;
        mock_lm
            .expect_startService()
            .times(3)
            .returning(move |_| Err(Status::new_service_specific_error(expected_error_code, None)));
        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.clone().get_lm()),
            RetryConfiguration::new(2),
        );
        // WHEN
        let (responder, receiver) = oneshot::channel();
        let request = TransitionRequest {
            new_state: InstanceState::Started,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(request);
        let res = receiver.await.unwrap();
        // THEN
        let err = res.unwrap_err();
        assert_eq!(err.service_specific_error(), expected_error_code);
    }

    #[tokio::test]
    async fn test_enforce_should_retry_on_persistent_error_and_kill() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        let expected_error_code = ResponseCode::VALUE_CORRUPTED.0;
        mock_lm
            .expect_startService()
            .times(3)
            .returning(move |_| Err(Status::new_service_specific_error(expected_error_code, None)));
        mock_lm.expect_killService().times(3).returning(|_| Ok(()));
        let fetcher = MockAllLmManagerWrapper::new(mock_lm);

        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.clone().get_lm()),
            RetryConfiguration::new(2),
        );
        // WHEN
        let (responder, receiver) = oneshot::channel();
        let request = TransitionRequest {
            new_state: InstanceState::Started,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(request);
        let res = receiver.await.unwrap();
        // THEN
        let err = res.unwrap_err();
        assert_eq!(err.service_specific_error(), expected_error_code);
    }

    #[tokio::test]
    async fn test_enforce_should_not_retry_on_permanent_error() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        let expected_error_code = ResponseCode::PERMISSION_DENIED.0;
        mock_lm
            .expect_startService()
            .times(1)
            .returning(move |_| Err(Status::new_service_specific_error(expected_error_code, None)));
        mock_lm.expect_killService().times(0);
        let fetcher = MockAllLmManagerWrapper::new(mock_lm);

        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.clone().get_lm()),
            RetryConfiguration::new(5),
        );
        // WHEN
        let (responder, receiver) = oneshot::channel();
        let request = TransitionRequest {
            new_state: InstanceState::Started,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(request);
        let res = receiver.await.unwrap();
        // THEN
        let err = res.unwrap_err();
        assert_eq!(err.service_specific_error(), expected_error_code);
    }

    #[tokio::test]
    async fn test_enforce_should_succeed_on_first_try() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        mock_lm.expect_startService().times(1).returning(|_| Ok(()));
        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.clone().get_lm()),
            RetryConfiguration::new(3),
        );
        // WHEN
        let (responder, receiver) = oneshot::channel();
        let request = TransitionRequest {
            new_state: InstanceState::Started,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(request);
        let res = receiver.await.unwrap();
        // THEN
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_should_not_destroy_instance_with_no_state() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        mock_lm.expect_shutdownService().never();
        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.clone().get_lm()),
            RetryConfiguration::new(3),
        );
        // WHEN
        let (responder, receiver) = oneshot::channel();
        let request = TransitionRequest {
            new_state: InstanceState::Destroyed,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(request);
        let res = receiver.await.unwrap();
        // THEN
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_should_not_destroy_destroyed_instance() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        mock_lm.expect_shutdownService().never();
        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.clone().get_lm()),
            RetryConfiguration::new(3),
        );
        *manager.last_enforced_state.lock().unwrap() = LastState::STATE(InstanceState::Destroyed);
        // WHEN
        let (responder, receiver) = oneshot::channel();
        let request = TransitionRequest {
            new_state: InstanceState::Destroyed,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(request);
        let res = receiver.await.unwrap();
        // THEN
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_enforce_should_exit_on_cancellation() {
        // GIVEN
        let mut mock_lm = MockILifecycleManager::new();
        mock_lm.expect_startService().never();
        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            Fqin::new("com.google.sdv.test", "ServiceName", "instance"),
            PersistentBinderConnection::new(move || fetcher.clone().get_lm()),
            RetryConfiguration::new(3),
        );
        // WHEN
        let (responder, receiver) = oneshot::channel();
        let request = TransitionRequest {
            new_state: InstanceState::Started,
            cancel: Arc::new(AtomicBool::new(true)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(request);
        let res = receiver.await.unwrap();
        // THEN
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_update_request_clears_queue_and_executes_in_order() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        let mut seq = mockall::Sequence::new();

        // Expect launchService to be called first, then startService.
        mock_lm.expect_launchService().times(1).in_sequence(&mut seq).returning(|_| Ok(()));
        mock_lm.expect_startService().times(1).in_sequence(&mut seq).returning(|_| Ok(()));

        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            RetryConfiguration::new(0),
        );

        // Manually add a "create" request to the enforce_queue.
        // This request should be cleared when the new "start" request is sent.
        let create_request = TransitionRequest {
            new_state: InstanceState::Created,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: None,
        };
        manager.enforce_queue.lock().unwrap().push_back(create_request);

        // WHEN a "start" request is sent.
        let (responder, receiver) = oneshot::channel();
        let start_request = TransitionRequest {
            new_state: InstanceState::Started,
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
        };
        manager.orchestrate_transition_request(start_request);

        // THEN the request should be processed successfully, with calls in order.
        let res = receiver.await.unwrap();
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_transition_is_not_attempted_if_cancelled() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        // We expect no calls to LM because the request is cancelled.
        mock_lm.expect_launchService().never();
        mock_lm.expect_startService().never();
        mock_lm.expect_shutdownService().never();

        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let lm_connection = PersistentBinderConnection::new(move || fetcher.get_lm());

        // WHEN a cancelled request is sent.
        let result = InstanceManager::orchestrate_transition_internal(
            &fqin,
            lm_connection,
            InstanceState::Started,
            Arc::new(AtomicBool::new(true)), // The request is cancelled
        );

        // THEN the request should fail and no LM calls should have been made.
        assert!(result.is_err());
    }
}
