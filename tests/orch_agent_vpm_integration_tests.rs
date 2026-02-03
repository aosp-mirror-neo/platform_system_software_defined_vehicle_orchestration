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
//! Orchestrator agent device integration tests. Testing the integration with LM agent and VPM.
//! In each test we need to lock them using a NamedLock so they run sequentially.
//! Soong test runner runs each test in parallel in a separate process. This leads to errors
//! when running tests overlap due to binder registration and using same service name.

#[cfg(test)]
mod tests {
    use com_android_sdv_vpm_vpm_system_service_rpc::Client as VpmClient;
    use google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin;
    use named_lock::NamedLock;
    use sdv_comms::id::ServiceFqin as Fqin;
    use sdv_comms::ContextRef;
    use sdv_mw_rs_com_sdv_google_orchestrator_test_orch_test_service_bundle::OrchTestServiceBundle;
    use sdv_mw_rs_comms::ClientOptions;
    use sdv_mw_rs_comms::RpcClient;
    use sdv_mw_rs_comms::UnitName;
    use sdv_mw_rs_sdv_comms::SdvComms;
    use sdv_orch_tests_common::test_common::*;
    use sdv_vpm_api::vehicle::VpmVehicleState;
    use sdv_vpm_api::vpm::PowerStateChangeRequest;
    use sdv_vpm_api::vpm::PowerStateRequest;
    use sdv_vpm_api::vpm::SetVehicleStateRequest;
    use sdv_vpm_api::vpm::ShutdownType;
    use std::panic;
    use std::sync::Arc;

    struct TestContext {
        vpm_client: Arc<VpmClient>,
    }

    impl TestContext {
        async fn new(instance_name: &str) -> Self {
            let context_ref = Self::create_context(instance_name);
            let comms = SdvComms { context: context_ref };
            let bundle = OrchTestServiceBundle::new(Arc::new(comms)).await.unwrap();

            let vpm_client = bundle
                .create_rpc_client::<VpmClient>(
                    UnitName::builder()
                        .package_name("com.android.sdv.vpm")
                        .bundle_name("VpmSystemServiceBundle")
                        .service_unit_name(VpmClient::DEFAULT_UNIT_NAME)
                        .build()
                        .unwrap(),
                    ClientOptions::default(),
                )
                .await
                .unwrap();

            Self { vpm_client: Arc::new(vpm_client) }
        }

        async fn set_power_state(
            &self,
            power_state_request: PowerStateRequest,
            shutdown_type: Option<ShutdownType>,
        ) {
            let shutdown_type = shutdown_type.unwrap_or_default();
            let request = &PowerStateChangeRequest {
                power_request: power_state_request.into(),
                shutdown_type: shutdown_type.into(),
                timeout_ms: 10000,
                ..Default::default()
            };
            let res = self.vpm_client.SetPowerState(request).await;
            assert!(res.is_ok());
        }

        fn create_context(instance_name: &str) -> ContextRef {
            let fqin = Fqin::builder()
                .sdv_vm_name("vm")
                .sdv_package_name("com.sdv.google.orchestrator.test")
                .service_bundle_name("OrchTestServiceBundle")
                .service_instance_name(instance_name)
                .build()
                .unwrap();

            ContextRef::create(fqin)
        }

        async fn set_vehicle_state(&self, vehicle_state: VpmVehicleState) {
            let request = &SetVehicleStateRequest {
                vehicle_state: vehicle_state.into(),
                ..Default::default()
            };
            let res = self.vpm_client.SetVehicleState(request).await;
            assert!(res.is_ok());
        }
    }

    #[tokio::test]
    async fn test_service_is_created_stopped_and_destroyed_on_vehicle_updates() {
        // GIVEN
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // Used for setup / cleanup of the test
        let mut context = TestContext::new("vehicle-instance").await;
        // Move the service bundle to started state so we can stop it after
        assert_on_vpm_vehicle_update(
            VpmVehicleState::VEHICLE_ON,
            assert_service_is_started,
            &get_service_bundle_fqin_start_stop_destroy(),
            &mut context,
        )
        .await;
        // WHEN/THEN
        assert_on_vpm_vehicle_update(
            VpmVehicleState::PARK,
            // THEN
            assert_service_is_created,
            &get_service_bundle_fqin_start_stop_destroy(),
            &mut context,
        )
        .await;

        assert_on_vpm_vehicle_update(
            VpmVehicleState::LOW_POWER,
            // WHEN/THEN
            assert_service_is_destroyed,
            &get_service_bundle_fqin_start_stop_destroy(),
            &mut context,
        )
        .await;
    }

    #[tokio::test]
    async fn test_service_is_running_on_vpm_power_update() {
        // GIVEN
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // Used for setup / cleanup of the test
        let mut context = TestContext::new("power-instance").await;
        // WHEN/THEN
        assert_on_vpm_power_update(
            PowerStateRequest::TURN_ON,
            // THEN
            assert_service_is_started,
            &get_service_bundle_fqin("power-main"),
            &mut context,
        )
        .await;
    }

    async fn assert_on_vpm_power_update<F>(
        power_state_request: PowerStateRequest,
        test_assert: F,
        service_fqin: &ServiceFqin,
        context: &mut TestContext,
    ) where
        F: FnOnce(&ServiceFqin) + panic::UnwindSafe,
    {
        // When a new vehicle state is sent via RPC.
        context.set_power_state(power_state_request, None).await;
        // THEN
        test_assert(service_fqin);
    }

    async fn assert_on_vpm_vehicle_update<F>(
        new_vehicle_state: VpmVehicleState,
        test_assert: F,
        service_fqin: &ServiceFqin,
        context: &mut TestContext,
    ) where
        F: FnOnce(&ServiceFqin) + panic::UnwindSafe,
    {
        // When a new vehicle state is sent via RPC.
        context.set_vehicle_state(new_vehicle_state).await;
        // THEN
        test_assert(service_fqin);
    }
}
