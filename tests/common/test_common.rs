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
//! Contains common functionality for the orchestrator tests.
use binder::Status;
use google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::{
    BpLifecycleManager, ILifecycleManager,
};
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::IServiceBundleState::IServiceBundleState;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ResponseCode::ResponseCode;
use std::thread;
use std::time::Duration;

/// Returns the sample-main-start-stop-destroy service bundle fqin
pub fn get_service_bundle_fqin_start_stop_destroy() -> ServiceFqin {
    ServiceFqin {
        sdvVmName: "local-vm".to_owned(),
        sdvPackageName: "com.android.sdv.test.orchestrator".to_owned(),
        serviceBundleName: "OrchestratorSampleCppServiceBundle".to_owned(),
        serviceInstanceName: "sample-main-start-stop-destroy".to_owned(),
    }
}

/// Returns the sample-main service bundle fqin
pub fn get_service_bundle_fqin(instance_name: &str) -> ServiceFqin {
    ServiceFqin {
        sdvVmName: "local-vm".to_owned(),
        sdvPackageName: "com.android.sdv.test.orchestrator".to_owned(),
        serviceBundleName: "OrchestratorSampleCppServiceBundle".to_owned(),
        serviceInstanceName: instance_name.to_owned(),
    }
}

/// Returns the sample-main-no-conditions service bundle fqin
pub fn get_service_bundle_fqin_no_condition() -> ServiceFqin {
    ServiceFqin {
        sdvVmName: "local-vm".to_owned(),
        sdvPackageName: "com.android.sdv.test.orchestrator".to_owned(),
        serviceBundleName: "OrchestratorSampleRustServiceBundle".to_owned(),
        serviceInstanceName: "sample-main-no-conditions".to_owned(),
    }
}

/// Returns the sample-initial-power-state service bundle fqin
pub fn get_service_bundle_fqin_initial_power_state() -> ServiceFqin {
    ServiceFqin {
        sdvVmName: "local-vm".to_owned(),
        sdvPackageName: "com.android.sdv.test.orchestrator".to_owned(),
        serviceBundleName: "OrchSampleInitialPowerState".to_owned(),
        serviceInstanceName: "sample-initial-power-state".to_owned(),
    }
}

/// Gets lifecycle manager binder service handler
pub fn get_lifecycle_manager() -> binder::Strong<dyn ILifecycleManager> {
    let descriptor =
        <BpLifecycleManager as ILifecycleManager>::get_descriptor().to_owned() + "/default";
    let expect_str = "Unable to find {descriptor}";
    binder::wait_for_interface(&descriptor).expect(expect_str)
}

/// Asserts that a service bundle with the given service_fqin is in created state.
pub fn assert_service_is_created(service_fqin: &ServiceFqin) {
    let mut res = false;
    for _ in 1..20 {
        thread::sleep(Duration::from_millis(500));
        let result = get_lifecycle_manager().getServiceBundleState(service_fqin);
        if let Ok(state) = result {
            if state != IServiceBundleState::CREATED {
                continue;
            } else {
                res = true;
                break;
            }
        }
    }

    assert!(res, "Service bundle did not change state to created: {service_fqin:?}");
}

/// Asserts that a service bundle with the given service_fqin starts.
pub fn assert_service_is_started(service_fqin: &ServiceFqin) {
    let mut res = false;
    for _ in 1..20 {
        thread::sleep(Duration::from_millis(500));
        let result = get_lifecycle_manager().getServiceBundleState(service_fqin);
        if let Ok(state) = result {
            if state != IServiceBundleState::STARTED {
                continue;
            } else {
                res = true;
                break;
            }
        }
    }

    assert!(res, "Service bundle did not change state to started: {service_fqin:?}");
}

/// Asserts that a service bundle with the given fqin shutsdown (to destroy the service).
pub fn assert_service_is_destroyed(service_fqin: &ServiceFqin) {
    let mut res = false;
    for _ in 1..20 {
        thread::sleep(Duration::from_millis(500));
        let result = get_lifecycle_manager().getServiceBundleState(service_fqin);
        // In current LM logic, after service bundle stops, it deregisters it and removes it from internal states.
        // So we need to wait until the service returns SERVICE_NOT_FOUND error.
        if let Err(status) = result {
            res = status
                == Status::new_service_specific_error_str::<String>(
                    ResponseCode::SERVICE_NOT_FOUND.0,
                    None,
                );
            break;
        } else {
            continue;
        }
    }

    assert!(res, "Service bundle did not change state to destroyed: {service_fqin:?}");
}
