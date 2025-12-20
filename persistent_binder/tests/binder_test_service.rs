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
//! Provides a Binder service for testing persistent binder connections.

use google_sdv_persistent_binder_test::aidl::google::sdv::persistent_binder::test::IPersistentBinderTest::{BnPersistentBinderTest, BpPersistentBinderTest, IPersistentBinderTest};
use log::{info, error};
use binder::{add_service, BinderFeatures, Interface, ProcessState, Result as BinderResult};

/// Binder interface that is called from the integration test suite
/// to verify that certain methods were called
#[derive(Default, Clone)]
pub struct PersistentBinderTestService {}

impl Interface for PersistentBinderTestService {}

impl IPersistentBinderTest for PersistentBinderTestService {
    fn ping(&self) -> BinderResult<bool> {
        info!("Pinged");
        Ok(true)
    }
}

fn main() {
    sdv_log::init_logger("persistent_binder_test_service")
        .expect("Failed to initialize persist binder logger");
    // Start the binder thread pool to handle incoming transactions.
    // This must be done before adding services to avoid race conditions.
    ProcessState::start_thread_pool();
    info!("Starting persistent binder test service");
    let service = PersistentBinderTestService::default();
    let service_name =
        format!("{}/default", <BpPersistentBinderTest as IPersistentBinderTest>::get_descriptor());
    info!("Service name is: {service_name:?}");
    let binder_service = BnPersistentBinderTest::new_binder(service, BinderFeatures::default());

    if let Err(err) = add_service(&service_name, binder_service.as_binder()) {
        error!("Failed to register service {service_name}: {err:?}");
        return;
    }
    info!("Successfully registered service {service_name}");
    ProcessState::join_thread_pool();
}
