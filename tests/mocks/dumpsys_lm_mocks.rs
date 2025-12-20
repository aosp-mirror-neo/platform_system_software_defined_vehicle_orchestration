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
//! Contains a LM mock that simulates long start service call by using tokio channels to block it until the test
//! finishes executing its code.

use binder::BinderFeatures;
use binder::SpIBinder;
use google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::BnLifecycleManager;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::ILifecycleManager;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::IServiceBundleState::IServiceBundleState;
use google_sdv_lifecycle_manager::binder;
use google_sdv_lifecycle_manager::binder::Result as BinderResult;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::broadcast::{Receiver, Sender};

/// Creates a mock for LM in which the start_service API call uses a sender/receiver channel to block the call.
/// This allows to verify behavior in tests simulating a long API call.
#[derive(Clone)]
pub struct DumpsysLmMock {
    /// Notify that the start_service was called
    pub start_service_started_notifier: Sender<()>,
    /// Block the start_service to assert values in test, simulating a long start_service call.
    pub block_start_service_recv: Arc<Receiver<()>>,
}

impl DumpsysLmMock {
    /// Creates a new instance of the Dumpsys LM mock.
    pub fn new(
        start_service_started_notifier: Sender<()>,
        block_start_service_recv: Arc<Receiver<()>>,
    ) -> Self {
        Self { start_service_started_notifier, block_start_service_recv }
    }
}

impl binder::Interface for DumpsysLmMock {
    fn as_binder(&self) -> SpIBinder {
        BnLifecycleManager::new_binder(self.clone(), BinderFeatures::default()).as_binder()
    }
}

impl ILifecycleManager for DumpsysLmMock {
    fn launchService(&self, _: &ServiceFqin) -> BinderResult<()> {
        // Not used by tests
        unreachable!()
    }

    fn startService(&self, _: &ServiceFqin) -> BinderResult<()> {
        // Notify that the start_service started
        let _ = self.start_service_started_notifier.send(());
        // As this fn is sync, we cannot call await and we want to block this fn simulating long startService call
        while self.block_start_service_recv.is_empty() {
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    fn shutdownService(&self, _: &ServiceFqin) -> BinderResult<()> {
        // Not used by tests
        unreachable!()
    }

    fn stopService(&self, _service_fqin: &ServiceFqin) -> BinderResult<()> {
        // This API should not be used by Orchestrator.
        unreachable!()
    }

    fn killService(&self, _service_fqin: &ServiceFqin) -> BinderResult<()> {
        // This API should not be used by Orchestrator.
        unreachable!()
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
