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
use tracing::instrument;

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

macro_rules! lm {
    ($owner:expr) => {{
        $owner.get_binder().lock().unwrap()
    }};
}

/// Handles the lifecycle of a single bundle instance
pub struct InstanceManager {
    fqin: Fqin,
    lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
    restart_config: RetryConfiguration,
}

impl InstanceManager {
    pub fn new(
        fqin: Fqin,
        lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
        restart_config: RetryConfiguration,
    ) -> Self {
        Self { fqin, lm_connection, restart_config }
    }

    #[instrument(skip(self), ret)]
    pub fn orchestrate_transition(
        &self,
        state: InstanceState,
        lm_cancel: Arc<AtomicBool>,
    ) -> Result<(), Status> {
        // We add 1 more than the max_retries for the initial enforcement that does not count as a retry.
        // In case the instance is not retryable, we would do this only once.
        let mut retries = self.restart_config.max_retries + 1;
        let mut last_error: Option<Status> = None;
        while retries > 0 && !lm_cancel.load(Ordering::Relaxed) {
            match Self::try_lifecycle_transition(
                self.lm_connection.clone(),
                &self.fqin,
                &state.clone(),
            ) {
                Ok(()) => {
                    // TODO(b/437891686): notify state to HM
                    return Ok(());
                }
                Err(message) => {
                    // TODO(b/437891686): notify state to HM
                    warn!(
                        "Failed to transition service instance {:?} to {:?}: {message}",
                        self.fqin, state
                    );
                    match Self::get_lm_error(&message) {
                        ErrorType::TRANSIENT => {
                            // Do nothing
                        }
                        ErrorType::PERSISTENT => {
                            let lm = lm!(self.lm_connection).clone();
                            // Should we retry if failed?
                            let res = lm.killService(&self.fqin.to_lm_fqin());
                            if let Err(err) = res {
                                warn!(
                                    "Failed to kill the bundle instance {:?} when retrying: {err:?}",
                                    self.fqin
                                );
                            }
                        }
                        ErrorType::PERMANENT => {
                            // Do not retry
                            return Err(message);
                        }
                    }
                    last_error = Some(message);
                    retries -= 1;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            let error_msg = format!("Failed to enforce state for {:?}", self.fqin);
            Status::new_service_specific_error(
                -1,
                Some(&std::ffi::CString::new(error_msg).unwrap()),
            )
        }))
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
        let res = manager
            .orchestrate_transition(InstanceState::Created, Arc::new(AtomicBool::new(false)));
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
            RetryConfiguration::new(0),
        );
        // WHEN
        let res = manager
            .orchestrate_transition(InstanceState::Started, Arc::new(AtomicBool::new(false)));
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
        // WHEN
        let res = manager
            .orchestrate_transition(InstanceState::Destroyed, Arc::new(AtomicBool::new(false)));
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
        let res = manager
            .orchestrate_transition(InstanceState::Started, Arc::new(AtomicBool::new(false)));
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
        let res = manager
            .orchestrate_transition(InstanceState::Started, Arc::new(AtomicBool::new(false)));
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
        let res = manager
            .orchestrate_transition(InstanceState::Started, Arc::new(AtomicBool::new(false)));
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
        let res = manager
            .orchestrate_transition(InstanceState::Started, Arc::new(AtomicBool::new(false)));
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
        let res =
            manager.orchestrate_transition(InstanceState::Started, Arc::new(AtomicBool::new(true)));
        // THEN
        assert!(res.is_err());
    }
}
