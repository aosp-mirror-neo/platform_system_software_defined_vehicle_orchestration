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
    use com_android_sdv_orchestrator_custom_state_service_rpc::Client;
    use named_lock::NamedLock;
    use protobuf::well_known_types::timestamp::Timestamp;
    use protobuf::MessageField;
    use sdv_comms::id::ServiceFqin as Fqin;
    use sdv_comms::ContextRef;
    use sdv_mw_rs_com_android_sdv_orchestrator_test_orch_test_service_bundle::OrchTestServiceBundle;
    use sdv_mw_rs_comms::ClientOptions;
    use sdv_mw_rs_comms::RpcClient;
    use sdv_mw_rs_comms::UnitName;
    use sdv_mw_rs_sdv_comms::SdvComms;
    use sdv_orch_api_stable::local_custom_modes::{SetCustomStateRequest, SetCustomStateResponse};
    use sdv_orch_tests_common::test_common::*;
    use std::sync::Arc;

    struct TestContext {
        custom_modes_client: Arc<Client>,
    }

    impl TestContext {
        async fn new(instance_name: &str) -> Self {
            let fqin = Fqin::builder()
                .sdv_vm_name("local-vm")
                .sdv_package_name("com.android.sdv.test.orchestrator")
                .service_bundle_name("OrchestratorSampleCppServiceBundle")
                .service_instance_name(instance_name) // We need a different instance so there's no error when LM creates the Context for each bundle instance created
                .build()
                .unwrap();
            let context_ref = ContextRef::create(fqin);
            let comms = SdvComms { context: context_ref };
            let bundle = OrchTestServiceBundle::new(Arc::new(comms)).await.unwrap();

            let custom_modes_client = bundle
                .create_rpc_client::<Client>(
                    UnitName::builder()
                        .package_name("com.android.sdv.orchestrator")
                        .bundle_name("OrchestratorServiceBundle")
                        .service_unit_name(Client::DEFAULT_UNIT_NAME)
                        .build()
                        .unwrap(),
                    ClientOptions::default(),
                )
                .await
                .unwrap();

            Self { custom_modes_client: Arc::new(custom_modes_client) }
        }

        async fn set_custom_state(
            &self,
            request: SetCustomStateRequest,
        ) -> sdv_status_rs::SdvResult<SetCustomStateResponse> {
            self.custom_modes_client.SetCustomState(&request).await
        }

        async fn async_drop(&self) {
            let _ = self
                .set_custom_state(SetCustomStateRequest {
                    mode: "CHARGING".to_string(),
                    value: "UNDEFINED".to_string(),
                    timestamp: MessageField::some(Timestamp::now()),
                    ..Default::default()
                })
                .await;
        }
    }

    #[tokio::test]
    async fn test_service_is_running_on_orchestrator_boot() {
        // GIVEN
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // Used for setup / cleanup of the test
        let _context = TestContext::new("boot").await;
        // WHEN/THEN
        assert_service_is_started(&get_service_bundle_fqin_no_condition());
    }

    #[tokio::test]
    async fn test_set_custom_mode_should_fail_if_invalid_mode() {
        // GIVEN
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // Used for setup / cleanup of the test.
        let mut context = TestContext::new("invalid-mode").await;
        // WHEN
        let res = send_custom_update(
            "CHARGING&",
            "value",
            &mut context,
            MessageField::some(Timestamp::now()),
        )
        .await;
        // THEN
        assert!(res.is_err());
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_set_custom_mode_should_fail_if_invalid_mode_value() {
        // GIVEN
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // Used for setup / cleanup of the test.
        let mut context = TestContext::new("invalid-value").await;
        // WHEN
        let res = send_custom_update(
            "CHARGING",
            "&*^%$$%^&*",
            &mut context,
            MessageField::some(Timestamp::now()),
        )
        .await;
        // THEN
        assert!(res.is_err());
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_set_custom_mode_should_fail_if_bundle_not_allowed() {
        // GIVEN
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // Used for setup / cleanup of the test.
        let mut context = TestContext::new("not-allowed").await;
        // WHEN
        let res = send_custom_update(
            "NOT_ALLOWED_MODE",
            "value",
            &mut context,
            MessageField::some(Timestamp::now()),
        )
        .await;
        // THEN
        assert!(res.is_err());
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_service_start_and_stop_on_custom_update() {
        // GIVEN
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // Used for setup / cleanup of the test.
        let mut context = TestContext::new("custom-instance").await;
        let fqin = get_service_bundle_fqin("custom-mode");
        assert_service_is_destroyed(&fqin);
        // Start service bundle so we can stop in the next step
        let res = send_custom_update(
            "CHARGING",
            "ON",
            &mut context,
            MessageField::some(Timestamp::now()),
        )
        .await;
        assert!(res.is_ok());
        assert_service_is_started(&fqin);
        // WHEN stopping the service bundle
        let res = send_custom_update(
            "CHARGING",
            "OFF",
            &mut context,
            MessageField::some(Timestamp::now()),
        )
        .await;
        // THEN
        assert!(res.is_ok());
        assert_service_is_destroyed(&fqin);
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_custom_mode_should_not_apply_old_value() {
        // GIVEN
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // Used for setup / cleanup of the test.
        let mut context = TestContext::new("custom-timestamp").await;
        let fqin = get_service_bundle_fqin("custom-mode-timestamp");
        // Check initial state of the service bundle
        assert_service_is_destroyed(&fqin);
        let oldest_timestamp = MessageField::some(Timestamp::now());
        let middle_timestamp = MessageField::some(Timestamp::now());
        let newest_timestamp = MessageField::some(Timestamp::now());
        // Start service bundle
        let res = send_custom_update("CHARGING", "ON", &mut context, newest_timestamp).await;
        assert!(res.is_ok());
        assert_service_is_started(&fqin);
        // WHEN sending a new value but with oldest timestamp than the previously set
        let res = send_custom_update("CHARGING", "OFF", &mut context, oldest_timestamp).await;
        // THEN the mode should not have been applied, so the bundle should be still in started state
        assert!(res.is_ok());
        assert_service_is_started(&fqin);
        // WHEN sending a new value newest than the previous one but oldest than the first one set
        let res = send_custom_update("CHARGING", "OFF", &mut context, middle_timestamp).await;
        // THEN the mode should not have been applied, so the bundle should be still in started state
        assert!(res.is_ok());
        assert_service_is_started(&fqin);
        context.async_drop().await;
    }

    async fn send_custom_update(
        custom_mode: &str,
        custom_state_value: &str,
        context: &mut TestContext,
        timestamp: MessageField<Timestamp>,
    ) -> sdv_status_rs::SdvResult<SetCustomStateResponse> {
        let request = SetCustomStateRequest {
            mode: custom_mode.to_string(),
            value: custom_state_value.to_string(),
            timestamp,
            ..Default::default()
        };

        context.set_custom_state(request).await
    }
}
