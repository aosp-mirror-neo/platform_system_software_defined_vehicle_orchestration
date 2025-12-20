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
//! Contains a Mock for LM that allows to intercept arguments used in the API calls and to inject return values for the calls.

use binder::BinderFeatures;
use binder::{SpIBinder, Status};
use google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::BnLifecycleManager;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::ILifecycleManager;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::IServiceBundleState::IServiceBundleState;
use google_sdv_lifecycle_manager::binder;
use google_sdv_lifecycle_manager::binder::Result as BinderResult;
use sdv_orchestration_common::fqin::Fqin;
use std::sync::Arc;
use std::sync::Mutex;

/// Intercepts all LM calls, allowing setting a specific return value and verifying with which params a LM API was called.
// TODO(b/313067871): replace with mockall
pub struct LMInterceptor {
    /// launch_service API was called with the given arguments.
    pub launch_service_called: Mutex<Result<Fqin, String>>,
    /// Sets a return value for the launch_service API call.
    pub on_launch_return_value: Result<(), String>,
    /// start_service API was called with the given arguments.
    pub start_service_called: Mutex<Result<Fqin, String>>,
    /// Sets a return value for the start_service API call.
    pub on_start_return_value: Result<(), String>,
    /// shutdown_service API was called with the given arguments.
    pub shutdown_service_called: Mutex<Result<Fqin, String>>,
    /// Sets a return value for the shutdown_service API call.
    pub on_shutdown_return_value: Result<(), String>,
    /// kill_service API was called with the given arguments.
    pub kill_service_called: Mutex<Result<Fqin, String>>,
}

impl Default for LMInterceptor {
    fn default() -> LMInterceptor {
        Self {
            launch_service_called: Mutex::new(Err("Launch service never called".to_string())),
            on_launch_return_value: Ok(()),
            start_service_called: Mutex::new(Err("Start service never called".to_string())),
            on_start_return_value: Ok(()),
            shutdown_service_called: Mutex::new(Err("Shutdown service never called".to_string())),
            on_shutdown_return_value: Ok(()),
            kill_service_called: Mutex::new(Err("Kill service never called".to_string())),
        }
    }
}

impl LMInterceptor {
    /// Resets the values for the LM API calls.
    pub fn reset_calls(&self) {
        *self.launch_service_called.lock().unwrap() =
            Err("Launch service never called".to_string());
        *self.start_service_called.lock().unwrap() = Err("Start service never called".to_string());
        *self.shutdown_service_called.lock().unwrap() =
            Err("Shutdown service never called".to_string());
        *self.kill_service_called.lock().unwrap() = Err("Kill service never called".to_string());
    }
}

/// Creates a mock of the LM API to use in tests
#[derive(Clone)]
pub struct MockLifecycleManager {
    /// The interceptor used in this mock to capture arguments in the API calls and set return values.
    pub lm_interceptor: Arc<LMInterceptor>,
}

impl binder::Interface for MockLifecycleManager {
    fn as_binder(&self) -> SpIBinder {
        BnLifecycleManager::new_binder(self.clone(), BinderFeatures::default()).as_binder()
    }
}

impl MockLifecycleManager {
    /// Creates a new Mock of the LM API using the given calls interceptor.
    pub fn create(lm_interceptor: Arc<LMInterceptor>) -> MockLifecycleManager {
        Self { lm_interceptor }
    }
}

impl ILifecycleManager for MockLifecycleManager {
    fn launchService(&self, service_fqin: &ServiceFqin) -> BinderResult<()> {
        let fqin = Fqin::from_lm_fqin(service_fqin);
        *self.lm_interceptor.launch_service_called.lock().unwrap() = Ok(fqin.clone());
        self.lm_interceptor
            .on_launch_return_value
            .clone()
            .map_err(|_e| Status::new_service_specific_error(0, None))
    }

    fn startService(&self, service_fqin: &ServiceFqin) -> BinderResult<()> {
        let fqin = Fqin::from_lm_fqin(service_fqin);
        *self.lm_interceptor.start_service_called.lock().unwrap() = Ok(fqin.clone());
        self.lm_interceptor
            .on_start_return_value
            .clone()
            .map_err(|_e| Status::new_service_specific_error(0, None))
    }

    fn shutdownService(&self, service_fqin: &ServiceFqin) -> BinderResult<()> {
        let fqin = Fqin::from_lm_fqin(service_fqin);
        *self.lm_interceptor.shutdown_service_called.lock().unwrap() = Ok(fqin.clone());
        self.lm_interceptor
            .on_shutdown_return_value
            .clone()
            .map_err(|_e| Status::new_service_specific_error(0, None))
    }

    fn stopService(&self, _service_fqin: &ServiceFqin) -> BinderResult<()> {
        // This API should not be used by Orchestrator.
        unreachable!()
    }

    fn killService(&self, service_fqin: &ServiceFqin) -> BinderResult<()> {
        let fqin = Fqin::from_lm_fqin(service_fqin);
        *self.lm_interceptor.kill_service_called.lock().unwrap() = Ok(fqin.clone());
        Ok(())
    }

    fn isServiceRunning(&self, _service_fqin: &ServiceFqin) -> BinderResult<bool> {
        // This API should not be used by Orchestrator.
        unreachable!()
    }

    fn getServiceBundleState(
        &self,
        _service_fqin: &ServiceFqin,
    ) -> BinderResult<IServiceBundleState> {
        // This API should not be used by Orchestrator.
        unreachable!()
    }
}
