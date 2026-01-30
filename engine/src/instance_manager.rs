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
use mockall::automock;
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

/// Type of transition request.
#[derive(PartialEq, Clone, Debug)]
pub enum RequestType {
    /// Request comes from enforcing a new Mode.
    NewMode(InstanceState),
    /// A crash happened and we need to restart the instance.
    Crash,
}

/// Information about the current enforcement task being processed.
#[derive(Clone)]
struct CurrentTaskInfo {
    /// A flag to cancel the ongoing task.
    cancel: Arc<AtomicBool>,
    /// The type of the request being processed.
    request_type: RequestType,
}

/// Request a new state for the instance.
pub struct TransitionRequest {
    /// A transition can be cancelled in the following cases:
    ///  - LM crashed and we cancel all ongoing transitions, followed by a new recover event.
    ///  - We are performing a crash recovery (restart) and a new mode is set, requiring the
    ///    cancellation of the current transition.
    pub cancel: Arc<AtomicBool>,
    /// Notify to the originator of the transition that the request was completed.
    /// This is needed for power modes to notify VPM of completion.
    pub responder: Option<oneshot::Sender<Result<(), Status>>>,
    /// The type of the request.
    pub request_type: RequestType,
}

macro_rules! lm {
    ($owner:expr) => {{
        $owner.get_binder().lock().unwrap()
    }};
}

#[automock]
pub trait IInstanceManager: Send + Sync {
    /// Sends a new transition request to the instance manager.
    /// If the instance is already in the desired state, it will skip the transition.
    /// If a responder is set for the request, it will be notified when we finished
    /// executing the transition (either successfully or not).
    fn orchestrate_transition_request(&self, request: TransitionRequest);
    /// Sends a transition request to the instance manager after a crash to restart it.
    /// It will set it to the last requested mode.
    /// Returns a receiver to wait for the completion of the restart attempt.
    fn orchestrate_restart(&self) -> Option<oneshot::Receiver<Result<(), Status>>>;
    /// Returns whether the instance supports restarts based on its configuration.
    fn is_restartable(&self) -> bool;
    /// Returns true if the instance has a last enforced state of created or started.
    fn is_created_or_started(&self) -> bool;
}

/// Handles the lifecycle of a single bundle instance
pub struct InstanceManager {
    /// Last enforced state is used to check whether we need to apply the transition or
    /// if we can skip it.
    last_enforced_state: Arc<Mutex<LastState>>,
    /// The last requested mode is required when we restart the instance to avoid computing
    /// all the orch config state. This doesn't mean that the last requested state was
    /// enforced correctly.
    last_requested_state: Arc<Mutex<Option<InstanceState>>>,
    /// Queue of all pending transitions.
    enforce_queue: Arc<Mutex<std::collections::VecDeque<TransitionRequest>>>,
    /// The restart configuration for this instance.
    restart_config: RetryConfiguration,
    /// Used to notify the enforcement task loop that there's a new transition request.
    notifier: Arc<Notify>,
    /// Current task in progress to allow cancellation.
    current_enforcement_task: Arc<Mutex<Option<CurrentTaskInfo>>>,
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
        let last_requested_state = Arc::new(Mutex::new(None));
        let enforce_queue: Arc<Mutex<std::collections::VecDeque<TransitionRequest>>> =
            Arc::new(Mutex::new(std::collections::VecDeque::new()));
        let notifier = Arc::new(Notify::new());
        let current_enforcement_task = Arc::new(Mutex::new(None));

        let manager = Arc::new(Self {
            last_enforced_state: last_enforced_state.clone(),
            last_requested_state: last_requested_state.clone(),
            enforce_queue: enforce_queue.clone(),
            restart_config,
            notifier: notifier.clone(),
            current_enforcement_task: current_enforcement_task.clone(),
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
            // We use a loop to prevent locking the enforce_queue mutex, allowing other threads
            // to push into the queue while processing a transition, and preventing deadlocks.
            loop {
                // Acquire the lock, pop a request, and immediately release the lock.
                let request_option = {
                    let mut enforce_queue_guard = self.enforce_queue.lock().unwrap();
                    enforce_queue_guard.pop_front()
                };

                let Some(mut request) = request_option else {
                    // No more requests in the queue, break to wait for notification.
                    break;
                };

                if let RequestType::NewMode(state) = &request.request_type {
                    *self.last_requested_state.lock().unwrap() = Some(state.clone());
                }

                *self.current_enforcement_task.lock().unwrap() = Some(CurrentTaskInfo {
                    cancel: request.cancel.clone(),
                    request_type: request.request_type.clone(),
                });

                let result = self.enforce_transition(&request);

                if let Some(responder) = request.responder.take() {
                    let res = responder.send(result);
                    if let Err(err) = res {
                        warn!(
                            "Failed to notify completion of transition for {:?} to {:?}: {:?}",
                            self.fqin, request.request_type, err
                        );
                    }
                }

                // We finished enforcing, so we remove it from the queue
                self.current_enforcement_task.lock().unwrap().take();
            }
        }
    }

    /// Enforces a transition request, handling retries and cancellation.
    /// Returns a result to be sent to the request's responder.
    fn enforce_transition(&self, request: &TransitionRequest) -> Result<(), Status> {
        let mut retries = match request.request_type {
            // In case of a new mode, we add 1 on top of the retries as the first
            // request is not from retries.
            RequestType::NewMode(_) => self.restart_config.max_retries + 1,
            // In case of restart, we set it to max_retries as it won't be restarted
            // if not retryable.
            RequestType::Crash => self.restart_config.max_retries,
        };

        let new_state = self.get_state_to_enforce(request)?;

        // This tracks the last error received within all the retries.
        let mut last_error: Option<Status> = None;
        while retries > 0 && !request.cancel.load(Ordering::Relaxed) {
            match Self::orchestrate_transition_internal(
                &self.fqin,
                self.lm_connection.clone(),
                new_state.clone(),
                request.cancel.clone(),
            ) {
                Ok(()) => {
                    *self.last_enforced_state.lock().unwrap() = LastState::STATE(new_state);
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
            return Self::to_error(format!(
                "Enforcement for {:?} to {:?} was cancelled",
                self.fqin, request.request_type
            ));
        } else if retries == 0 {
            return Self::to_error(format!(
                "No retries left to transition {:?} to {:?}",
                self.fqin, new_state
            ));
        }

        Ok(())
    }

    fn to_error<T>(error_msg: String) -> Result<T, Status> {
        Err(Status::new_service_specific_error(
            -1,
            Some(&std::ffi::CString::new(error_msg).unwrap()),
        ))
    }

    fn get_state_to_enforce(&self, request: &TransitionRequest) -> Result<InstanceState, Status> {
        match &request.request_type {
            RequestType::NewMode(state) => Ok(state.clone()),
            RequestType::Crash => {
                if let Some(last_state) = self.last_requested_state.lock().unwrap().clone() {
                    Ok(last_state)
                } else {
                    // TODO(b/419239856): if we do not know the last requested state, we should
                    // read the orch config and fetch it.
                    Self::to_error(format!("Bundle instance {:?} crashed but there's no state to restart to. Skipping transition.", self.fqin))
                }
            }
        }
    }
}

impl IInstanceManager for InstanceManager {
    fn orchestrate_transition_request(&self, request: TransitionRequest) {
        // Lock the enforce_queue to prevent other threads to pushing new tasks before
        // we finished processing this one.
        let mut enforce_queue_guard = self.enforce_queue.lock().unwrap();
        // We cancel any ongoing restart as we will enforce a new mode that will restart the bundle.
        if let Some(current_task) = self.current_enforcement_task.lock().unwrap().as_ref() {
            if current_task.request_type == RequestType::Crash {
                current_task.cancel.store(true, Ordering::Relaxed);
            }
        }
        // Any restart request in the queue is removed as it will be restarted in the new mode.
        // So we only keep the NewMode requests.
        enforce_queue_guard.retain(|req| matches!(req.request_type, RequestType::NewMode(_)));

        let last_state_guard = self.last_enforced_state.lock().unwrap();
        // If it's already destroyed, we skip the enforcement.
        // If last state was an error, we don't know if it's actually destroy,
        // so we don't skip the destroyment.
        if request.request_type == RequestType::NewMode(InstanceState::Destroyed)
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

        enforce_queue_guard.push_back(request);

        // Drop the guard to release the lock before notifying the other task.
        // This ensures the woken task can acquire the lock immediately.
        drop(enforce_queue_guard);

        self.notifier.notify_one();
    }

    fn is_restartable(&self) -> bool {
        self.restart_config.max_retries > 0
    }

    // The return value is used in tests only
    fn orchestrate_restart(&self) -> Option<oneshot::Receiver<Result<(), Status>>> {
        let mut enforce_queue = self.enforce_queue.lock().unwrap();
        // If there is a request in the queue to transition to a new state, we skip the restart as
        // that transition will handle the state of this instance.
        if enforce_queue.iter().any(|req| matches!(req.request_type, RequestType::NewMode(_))) {
            // A new mode will be processed, so we don't need to restart the bundle.
            return None;
        }
        // We use the receiver in tests to verify when a restart request finished its processing
        #[cfg(test)]
        let (responder, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (Some(sender), Some(receiver))
        };
        // In production code we do not care about return value
        #[cfg(not(test))]
        let (responder, receiver) = (None, None);

        enforce_queue.push_back(TransitionRequest {
            cancel: Arc::new(AtomicBool::new(false)),
            responder,
            request_type: RequestType::Crash,
        });
        self.notifier.notify_one();
        receiver
    }

    fn is_created_or_started(&self) -> bool {
        let last_state = self.last_enforced_state.lock().unwrap();
        *last_state == LastState::STATE(InstanceState::Created)
            || *last_state == LastState::STATE(InstanceState::Started)
    }
}

impl InstanceManager {
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
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Created),
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
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Started),
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
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Destroyed),
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
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Started),
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
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Started),
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
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Started),
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
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Started),
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
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Destroyed),
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
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Destroyed),
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
            cancel: Arc::new(AtomicBool::new(true)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Started),
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
            cancel: Arc::new(AtomicBool::new(false)),
            responder: None,
            request_type: RequestType::NewMode(InstanceState::Created),
        };
        manager.enforce_queue.lock().unwrap().push_back(create_request);

        // WHEN a "start" request is sent.
        let (responder, receiver) = oneshot::channel();
        let start_request = TransitionRequest {
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Started),
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

    #[tokio::test]
    async fn test_orchestrate_restart_no_retries_does_nothing() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        // Expect no calls to LM as retries are 0.
        mock_lm.expect_launchService().never();
        mock_lm.expect_startService().never();
        mock_lm.expect_shutdownService().never();

        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            RetryConfiguration::new(0), // No retries
        );

        // Set a last requested state to restart to.
        *manager.last_requested_state.lock().unwrap() = Some(InstanceState::Started);

        // WHEN orchestrate_restart is called.
        let receiver = manager.orchestrate_restart().unwrap();

        // THEN the mock expectations (no calls to LM) are verified and we receive an error
        // because there are no retries.
        assert!(receiver.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_orchestrate_restart_with_retries_exhaust_retries_on_error() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        // Expect 5 calls to startService for the restart.
        mock_lm.expect_startService().times(5).returning(|_| {
            Err(Status::new_service_specific_error(ResponseCode::INTERNAL_ERROR.0, Some(c"Failed")))
        });

        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            RetryConfiguration::new(5), // With retries
        );

        // Set a last requested state to restart to.
        *manager.last_requested_state.lock().unwrap() = Some(InstanceState::Started);

        // Use a channel to know when the restart attempts have finished.
        let receiver = manager.orchestrate_restart().unwrap();

        // THEN the restart fails after being called 5 times and returns an error.
        let result = receiver.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_orchestrate_restart_with_retries_restarts_instance() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        // Expect a call to startService for the restart.
        mock_lm.expect_startService().times(1).returning(|_| Ok(()));

        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            RetryConfiguration::new(1), // With retries
        );

        // Set a last requested state to restart to.
        *manager.last_requested_state.lock().unwrap() = Some(InstanceState::Started);

        // Use a channel to know when the restart attempt is complete.
        let receiver = manager.orchestrate_restart().unwrap();

        // THEN the restart is successful.
        let result = receiver.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_orchestrate_restart_is_skipped_if_new_mode_request_is_pending() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        // Expect no calls to LM because the restart is skipped.
        mock_lm.expect_startService().never();

        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            RetryConfiguration::new(1), // With retries, to make sure it's not the reason for skipping.
        );

        // Add a pending NewMode request to the queue.
        let (responder, _) = oneshot::channel();
        manager.orchestrate_transition_request(TransitionRequest {
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Started),
        });

        // WHEN orchestrate_restart is called.
        let result = manager.orchestrate_restart();

        // THEN the restart is skipped and None is returned.
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_orchestrate_restart_no_last_state() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let mut mock_lm = MockILifecycleManager::new();
        // Expect no calls to LM as there's no state to restart to.
        mock_lm.expect_startService().never();
        mock_lm.expect_launchService().never();
        mock_lm.expect_shutdownService().never();

        let fetcher = MockAllLmManagerWrapper::new(mock_lm);

        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            RetryConfiguration::new(1), // With retries, to show it's not the reason for skipping.
        );

        // WHEN orchestrate_restart is called with no last requested state.
        let receiver = manager.orchestrate_restart().unwrap();

        // THEN the mock expectations (no calls to LM) are verified and we receive an error
        // because there is no state to restart.
        assert!(receiver.await.unwrap().is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_request_cancels_ongoing_crash_recovery() {
        // GIVEN
        let fqin = Fqin::new("com.google.sdv.test", "ServiceName", "instance");
        let cancel_for_crash_task = Arc::new(AtomicBool::new(false));
        let cancel_clone = cancel_for_crash_task.clone();
        let mut mock_lm = MockILifecycleManager::new();

        // Use channels to synchronize the test.
        // `crash_recovery_started_tx` signals that the mock LM call for the crash has begun.
        // `unblock_crash_recovery_rx` is used to block the mock LM call, simulating a long-running task.
        let (crash_recovery_started_tx, mut crash_recovery_started_rx) =
            tokio::sync::mpsc::channel(1);

        // The start call to startService is for the crash recovery. It will block.
        mock_lm.expect_startService().times(1).returning(move |_| {
            // Signal that the crash recovery task has started.
            crash_recovery_started_tx.try_send(()).unwrap();
            // Block until cancelled.
            while !cancel_clone.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(Status::new_service_specific_error(-1, Some(c"Cancelled")))
        });
        // The create call is for the new request, which should succeed.
        mock_lm.expect_launchService().times(1).returning(|_| Ok(()));

        let fetcher = MockAllLmManagerWrapper::new(mock_lm);
        let manager = InstanceManager::new(
            fqin.clone(),
            PersistentBinderConnection::new(move || fetcher.get_lm()),
            RetryConfiguration::new(5),
        );

        // Start a crash recovery task.
        *manager.last_requested_state.lock().unwrap() = Some(InstanceState::Started);
        let (sender, crash_receiver) = oneshot::channel();
        // We cannot use the ochestrate_restart as we need a handle to the cancellation to ensure test finishes.
        // We need to add in the queue a Crash request and enforce it.
        manager.enforce_queue.lock().unwrap().push_back(TransitionRequest {
            cancel: cancel_for_crash_task,
            responder: Some(sender),
            request_type: RequestType::Crash,
        });
        manager.notifier.notify_one();

        // Wait until the crash recovery is inside the mock LM call.
        crash_recovery_started_rx.recv().await;

        // WHEN a new transition request is sent.
        let (responder, new_request_receiver) = oneshot::channel();
        manager.orchestrate_transition_request(TransitionRequest {
            cancel: Arc::new(AtomicBool::new(false)),
            responder: Some(responder),
            request_type: RequestType::NewMode(InstanceState::Created),
        });

        // THEN the crash recovery should be cancelled, and the new request should succeed.
        assert!(crash_receiver.await.unwrap().is_err(), "Crash recovery was not cancelled");
        assert!(new_request_receiver.await.unwrap().is_ok(), "New request did not succeed");
    }
}
