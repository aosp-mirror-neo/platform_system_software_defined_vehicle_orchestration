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
//! Contains a LM mock used for parallelization tests.

use binder::BinderFeatures;
use binder::SpIBinder;
use binder::Status;
use google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::BnLifecycleManager;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::ILifecycleManager;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::IServiceBundleState::IServiceBundleState;
use google_sdv_lifecycle_manager::binder;
use google_sdv_lifecycle_manager::binder::Result as BinderResult;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// A LM mock that's used in tests to verify that parallelization of bundles transitions works.
pub struct ParallelizationLmMock {
    /// Contains the pending transaction for the in progress transition.
    pub pending_transactions: AtomicU8,
    /// Maximum pending transactions.
    pub max_pending_transactions: AtomicU8,
}

impl Default for ParallelizationLmMock {
    /// Creates a new instance of the ParallelizationLmMock
    fn default() -> Self {
        Self { pending_transactions: AtomicU8::new(0), max_pending_transactions: AtomicU8::new(0) }
    }
}

/// Returns a mock of LM for testing parallelization use cases.
#[derive(Clone)]
pub struct ParallelizationLmFetcher {
    /// The ParallelizationLmMock that contains the pending transactions information.
    pub lm: Arc<ParallelizationLmMock>,
}

impl binder::Interface for ParallelizationLmFetcher {
    fn as_binder(&self) -> SpIBinder {
        BnLifecycleManager::new_binder(self.clone(), BinderFeatures::default()).as_binder()
    }
}

impl ILifecycleManager for ParallelizationLmFetcher {
    fn launchService(&self, _: &ServiceFqin) -> BinderResult<()> {
        let old_pending_transactions = self.lm.pending_transactions.fetch_add(1, Ordering::SeqCst);
        self.lm.max_pending_transactions.fetch_max(old_pending_transactions + 1, Ordering::SeqCst);

        std::thread::sleep(Duration::from_millis(100));

        self.lm.pending_transactions.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    fn startService(&self, _: &ServiceFqin) -> BinderResult<()> {
        let old_pending_transactions = self.lm.pending_transactions.fetch_add(1, Ordering::SeqCst);
        self.lm.max_pending_transactions.fetch_max(old_pending_transactions + 1, Ordering::SeqCst);

        std::thread::sleep(Duration::from_millis(100));

        self.lm.pending_transactions.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    fn shutdownService(&self, _: &ServiceFqin) -> BinderResult<()> {
        let old_pending_transactions = self.lm.pending_transactions.fetch_add(1, Ordering::SeqCst);
        self.lm.max_pending_transactions.fetch_max(old_pending_transactions + 1, Ordering::SeqCst);

        std::thread::sleep(Duration::from_millis(100));

        self.lm.pending_transactions.fetch_sub(1, Ordering::SeqCst);
        Ok(())
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

/// A LM mock that's used in tests to verify that parallelization of bundles transitions works when one of the transitions return error.
pub struct FailingParallelizationLmMock {
    /// Contains the pending transaction for the in progress transition.
    pub pending_transactions: AtomicU8,
    /// Maximum pending transactions.
    pub max_pending_transactions: AtomicU8,
}

impl Default for FailingParallelizationLmMock {
    /// Creates a new instance of the ParallelizationLmMock
    fn default() -> Self {
        Self { pending_transactions: AtomicU8::new(0), max_pending_transactions: AtomicU8::new(0) }
    }
}

/// Returns a mock of LM for testing parallelization use cases.
#[derive(Clone)]
pub struct FailingParallelizationLmFetcher {
    /// The ParallelizationLmMock that contains the pending transactions information.
    pub lm: Arc<FailingParallelizationLmMock>,
}

impl binder::Interface for FailingParallelizationLmFetcher {
    fn as_binder(&self) -> SpIBinder {
        BnLifecycleManager::new_binder(self.clone(), BinderFeatures::default()).as_binder()
    }
}

impl ILifecycleManager for FailingParallelizationLmFetcher {
    fn launchService(&self, _: &ServiceFqin) -> BinderResult<()> {
        let old_pending_transactions = self.lm.pending_transactions.fetch_add(1, Ordering::SeqCst);
        self.lm.max_pending_transactions.fetch_max(old_pending_transactions + 1, Ordering::SeqCst);

        std::thread::sleep(Duration::from_millis(100));

        self.lm.pending_transactions.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    fn startService(&self, _: &ServiceFqin) -> BinderResult<()> {
        // Simulate doing something before failing.
        let old_pending_transactions = self.lm.pending_transactions.fetch_add(1, Ordering::SeqCst);
        self.lm.max_pending_transactions.fetch_max(old_pending_transactions + 1, Ordering::SeqCst);
        self.lm.pending_transactions.fetch_sub(1, Ordering::SeqCst);

        Err(Status::new_service_specific_error_str(
            binder::StatusCode::FAILED_TRANSACTION as i32,
            Some("Failed to start service"),
        ))
    }

    fn shutdownService(&self, _: &ServiceFqin) -> BinderResult<()> {
        let old_pending_transactions = self.lm.pending_transactions.fetch_add(1, Ordering::SeqCst);
        self.lm.max_pending_transactions.fetch_max(old_pending_transactions + 1, Ordering::SeqCst);

        std::thread::sleep(Duration::from_millis(100));

        self.lm.pending_transactions.fetch_sub(1, Ordering::SeqCst);
        Ok(())
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
