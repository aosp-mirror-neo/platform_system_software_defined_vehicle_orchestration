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
//! Contains a mock for the Lifecycle Manager using mockall.

use binder::{BinderFeatures, SpIBinder};
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::{
    BnLifecycleManager, ILifecycleManager, MockILifecycleManager,
};
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::IServiceBundleState::IServiceBundleState;
use std::sync::{Arc, Mutex};

/// Creates a mock of the LM API to use in tests that supports mockall
#[derive(Clone)]
pub struct MockAllLmManagerWrapper {
    /// The MockILifecycleManager from mockall that can be used for assertions and returning values.
    pub mock_lm: Arc<Mutex<MockILifecycleManager>>,
}

impl MockAllLmManagerWrapper {
    /// Creates a new MockAllLmManagerWrapper wrapper
    pub fn new(mock_lm: MockILifecycleManager) -> Self {
        Self { mock_lm: Arc::new(Mutex::new(mock_lm)) }
    }
}

impl binder::Interface for MockAllLmManagerWrapper {
    fn as_binder(&self) -> SpIBinder {
        let mock_lm_clone = self.clone();
        BnLifecycleManager::new_binder(mock_lm_clone, BinderFeatures::default()).as_binder()
    }
}

impl ILifecycleManager for MockAllLmManagerWrapper {
    fn launchService(
        &self,
        service_fqin: &google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin,
    ) -> binder::Result<()> {
        self.mock_lm.lock().unwrap().launchService(service_fqin)
    }
    fn startService(
        &self,
        service_fqin: &google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin,
    ) -> binder::Result<()> {
        self.mock_lm.lock().unwrap().startService(service_fqin)
    }
    fn shutdownService(
        &self,
        service_fqin: &google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin,
    ) -> binder::Result<()> {
        self.mock_lm.lock().unwrap().shutdownService(service_fqin)
    }
    fn stopService(
        &self,
        _service_fqin: &google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin,
    ) -> binder::Result<()> {
        // This API should not be used by Orchestrator.
        unreachable!()
    }
    fn killService(
        &self,
        service_fqin: &google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin,
    ) -> binder::Result<()> {
        self.mock_lm.lock().unwrap().killService(service_fqin)
    }
    fn isServiceRunning(
        &self,
        _service_fqin: &google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin,
    ) -> binder::Result<bool> {
        // This API should not be used by Orchestrator.
        unreachable!()
    }
    fn getServiceBundleState(
        &self,
        _service_fqin: &google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin,
    ) -> binder::Result<IServiceBundleState> {
        // This API should not be used by Orchestrator.
        unreachable!()
    }
}
