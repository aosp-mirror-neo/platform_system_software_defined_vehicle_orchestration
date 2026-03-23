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
    use sdv_mw_rs_com_android_sdv_test_orchestrator_orch_test_service_bundle::OrchTestServiceBundle;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::ServiceBundleInstanceRecoveryState::ServiceBundleInstanceRecoveryState;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::ServiceBundleInstanceLifecycleState::ServiceBundleInstanceLifecycleState;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::BnServiceBundleInstanceStateChangeListener;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::IServiceBundleInstanceStateChangeListener;
    use google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin as AidlServiceFqin;
    use sdv_mw_rs_comms::ClientOptions;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::IOrchestrationAgent::BpOrchestrationAgent;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::IOrchestrationAgent::IOrchestrationAgent;
    use binder::{BinderFeatures, Interface, Strong};
    use sdv_mw_rs_comms::RpcClient;
    use sdv_mw_rs_comms::UnitName;
    use sdv_mw_rs_sdv_comms::SdvComms;
    use sdv_orch_api_stable::local_custom_modes::{SetCustomStateRequest, SetCustomStateResponse};
    use sdv_orch_tests_common::test_common::*;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::time::Duration;

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
            let _ = self
                .set_custom_state(SetCustomStateRequest {
                    mode: "LIFECYCLE".to_string(),
                    value: "reset".to_string(),
                    timestamp: MessageField::some(Timestamp::now()),
                    ..Default::default()
                })
                .await;
        }

        fn get_orchestrator_binder_service(&self) -> Strong<dyn IOrchestrationAgent> {
            let descriptor = <BpOrchestrationAgent as IOrchestrationAgent>::get_descriptor()
                .to_owned()
                + "/default";

            match binder::wait_for_interface(&descriptor) {
                Ok(orchestrator_agent) => orchestrator_agent,
                Err(e) => {
                    panic!("Failed to get IOrchestrationAgent interface, retrying: {e:?}")
                }
            }
        }

        fn new_bundle_state_change_listener(&self) -> mpsc::Receiver<UpdateState> {
            let (tx, rx) = mpsc::channel(1);
            let register_service = BnServiceBundleInstanceStateChangeListener::new_binder(
                BundleStateChangeClient { tx },
                BinderFeatures::default(),
            );
            self.get_orchestrator_binder_service()
                .registerServiceBundleInstanceStateChangeListener(&register_service)
                .expect("Registering should succeed");

            rx
        }
    }

    #[derive(Debug, Clone)]
    struct UpdateState {
        fqin: AidlServiceFqin,
        recovery_state: ServiceBundleInstanceRecoveryState,
        lifecycle_state: ServiceBundleInstanceLifecycleState,
    }

    struct BundleStateChangeClient {
        tx: mpsc::Sender<UpdateState>,
    }

    impl Interface for BundleStateChangeClient {}

    #[allow(non_snake_case)]
    impl IServiceBundleInstanceStateChangeListener for BundleStateChangeClient {
        fn onServiceBundleInstanceStateChanged(
            &self,
            aidl_fqin: &AidlServiceFqin,
            new_recovery_state: ServiceBundleInstanceRecoveryState,
            expected_lifecycle_state: ServiceBundleInstanceLifecycleState,
        ) -> binder::Result<()> {
            let event = UpdateState {
                fqin: aidl_fqin.clone(),
                recovery_state: new_recovery_state,
                lifecycle_state: expected_lifecycle_state,
            };
            self.tx.blocking_send(event).map_err(|err| {
                panic!("Failed to send new event {err:?}");
            })
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
    async fn test_bundle_created_sends_operational_created_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let (mut context, mut receiver) = setup_lifecycle_test("create-notification").await;

        send_lifecycle_update(&mut context, "create").await;

        verify_lifecycle_events(
            &mut receiver,
            "lifecycle",
            vec![(
                ServiceBundleInstanceRecoveryState::OPERATIONAL,
                ServiceBundleInstanceLifecycleState::CREATED,
            )],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_subscribe_after_create_gets_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // 1. Create context and trigger bundle creation
        let mut context = TestContext::new("create-then-subscribe").await;
        send_lifecycle_update(&mut context, "create").await;

        // 2. Wait for the bundle to be fully created
        let fqin = get_rust_service_bundle_fqin("lifecycle");
        assert_service_is_created(&fqin);

        // 3. Subscribe to state changes AFTER the bundle is created
        let mut receiver = context.new_bundle_state_change_listener();

        // 4. Verify that the new listener receives the current state
        verify_lifecycle_events(
            &mut receiver,
            "lifecycle",
            vec![(
                ServiceBundleInstanceRecoveryState::OPERATIONAL,
                ServiceBundleInstanceLifecycleState::CREATED,
            )],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_subscribe_after_non_restartable_crash_gets_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // 1. Create context and trigger a crash in a non-restartable bundle
        let mut context = TestContext::new("crash-then-subscribe-no-restarts").await;
        send_lifecycle_update(&mut context, "panic-create-no-restarts").await;

        // 2. Wait for the bundle to be fully destroyed
        let fqin = get_rust_service_bundle_fqin("panic-create-no-restarts");
        assert_service_is_destroyed(&fqin);

        // 3. Subscribe to state changes AFTER the crash
        let mut receiver = context.new_bundle_state_change_listener();

        // 4. Verify that the new listener receives the RETRY_FAILED state
        verify_lifecycle_events(
            &mut receiver,
            "panic-create-no-restarts",
            vec![(
                ServiceBundleInstanceRecoveryState::RETRY_FAILED,
                ServiceBundleInstanceLifecycleState::CREATED,
            )],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_crash_on_restartable_create_sends_retrying_and_failed_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let (mut context, mut receiver) = setup_lifecycle_test("crash-create-restarts").await;

        send_lifecycle_update(&mut context, "panic-create-restarts").await;

        verify_lifecycle_events(
            &mut receiver,
            "panic-create-restarts",
            vec![
                (
                    ServiceBundleInstanceRecoveryState::RETRYING,
                    ServiceBundleInstanceLifecycleState::CREATED,
                ),
                (
                    ServiceBundleInstanceRecoveryState::RETRY_FAILED,
                    ServiceBundleInstanceLifecycleState::CREATED,
                ),
            ],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_crash_on_non_restartable_create_sends_retrying_and_failed_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let (mut context, mut receiver) = setup_lifecycle_test("crash-create-no-restarts").await;

        send_lifecycle_update(&mut context, "panic-create-no-restarts").await;

        verify_lifecycle_events(
            &mut receiver,
            "panic-create-no-restarts",
            vec![(
                ServiceBundleInstanceRecoveryState::RETRY_FAILED,
                ServiceBundleInstanceLifecycleState::CREATED,
            )],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_crash_on_non_restartable_create_after_start_sends_failed_notifications() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();

        let (mut context, mut receiver) =
            setup_lifecycle_test("crash-stop-notification-no-restarts").await;

        send_lifecycle_update(&mut context, "panic-stop-no-restarts-start").await;
        let fqin = get_rust_service_bundle_fqin("panic-stop-no-restarts");
        assert_service_is_started(&fqin);

        send_lifecycle_update(&mut context, "panic-stop-no-restarts").await;

        let events = verify_lifecycle_events(
            &mut receiver,
            "panic-stop-no-restarts",
            vec![
                (
                    ServiceBundleInstanceRecoveryState::OPERATIONAL,
                    ServiceBundleInstanceLifecycleState::STARTED,
                ),
                (
                    ServiceBundleInstanceRecoveryState::RETRY_FAILED,
                    ServiceBundleInstanceLifecycleState::CREATED,
                ),
            ],
        )
        .await;
        // As the bundle is not restartable, if should not try to recover
        let not_found = (
            ServiceBundleInstanceRecoveryState::RETRYING,
            ServiceBundleInstanceLifecycleState::CREATED,
        );
        assert!(!events.iter().any(|e| (e.recovery_state, e.lifecycle_state) == not_found));
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_crash_on_restartable_create_after_start_sends_failed_notification_and_succeess_created(
    ) {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();

        let (mut context, mut receiver) =
            setup_lifecycle_test("crash-stop-notification-restarts").await;

        send_lifecycle_update(&mut context, "panic-stop-restarts-start").await;
        let fqin = get_rust_service_bundle_fqin("panic-stop-restarts");
        assert_service_is_started(&fqin);

        send_lifecycle_update(&mut context, "panic-stop-restarts").await;

        verify_lifecycle_events(
            &mut receiver,
            "panic-stop-restarts",
            vec![
                (
                    ServiceBundleInstanceRecoveryState::OPERATIONAL,
                    ServiceBundleInstanceLifecycleState::STARTED,
                ),
                (
                    ServiceBundleInstanceRecoveryState::RETRYING,
                    ServiceBundleInstanceLifecycleState::CREATED,
                ),
                // After the retry, it will call onCreate instead of onStop and will succeed.
                (
                    ServiceBundleInstanceRecoveryState::OPERATIONAL,
                    ServiceBundleInstanceLifecycleState::CREATED,
                ),
            ],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_bundle_started_sends_operational_started_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let (mut context, mut receiver) = setup_lifecycle_test("started-notification").await;

        send_lifecycle_update(&mut context, "start").await;

        verify_lifecycle_events(
            &mut receiver,
            "lifecycle",
            vec![(
                ServiceBundleInstanceRecoveryState::OPERATIONAL,
                ServiceBundleInstanceLifecycleState::STARTED,
            )],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_subscribe_after_start_gets_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // 1. Create context and trigger bundle start
        let mut context = TestContext::new("start-then-subscribe").await;
        send_lifecycle_update(&mut context, "start").await;

        // 2. Wait for the bundle to be fully started
        let fqin = get_rust_service_bundle_fqin("lifecycle");
        assert_service_is_started(&fqin);

        // 3. Subscribe to state changes AFTER the bundle is started
        let mut receiver = context.new_bundle_state_change_listener();

        // 4. Verify that the new listener receives the current state
        verify_lifecycle_events(
            &mut receiver,
            "lifecycle",
            vec![(
                ServiceBundleInstanceRecoveryState::OPERATIONAL,
                ServiceBundleInstanceLifecycleState::STARTED,
            )],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_crash_on_restartable_start_sends_retrying_success_and_failed_notifications() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let (mut context, mut receiver) =
            setup_lifecycle_test("crash-start-notification-restarts").await;

        send_lifecycle_update(&mut context, "panic-start-restarts").await;

        verify_lifecycle_events(
            &mut receiver,
            "panic-start-restarts",
            vec![
                (
                    ServiceBundleInstanceRecoveryState::OPERATIONAL,
                    ServiceBundleInstanceLifecycleState::CREATED,
                ),
                (
                    ServiceBundleInstanceRecoveryState::RETRYING,
                    ServiceBundleInstanceLifecycleState::STARTED,
                ),
                (
                    ServiceBundleInstanceRecoveryState::RETRY_FAILED,
                    ServiceBundleInstanceLifecycleState::STARTED,
                ),
            ],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_crash_on_restartable_start_sends_retrying_and_failed_notifications() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let (mut context, mut receiver) =
            setup_lifecycle_test("crash-start-notification-restarts-2").await;

        send_lifecycle_update(&mut context, "panic-create-on-start-restarts-start").await;

        verify_lifecycle_events(
            &mut receiver,
            "panic-create-on-start-restarts",
            vec![
                (
                    ServiceBundleInstanceRecoveryState::RETRYING,
                    ServiceBundleInstanceLifecycleState::STARTED,
                ),
                (
                    ServiceBundleInstanceRecoveryState::RETRY_FAILED,
                    ServiceBundleInstanceLifecycleState::STARTED,
                ),
            ],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_crash_on_non_restartable_start_sends_failed_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let (mut context, mut receiver) =
            setup_lifecycle_test("crash-start-notification-no-restarts").await;

        send_lifecycle_update(&mut context, "panic-start-no-restarts").await;

        let events = verify_lifecycle_events(
            &mut receiver,
            "panic-start-no-restarts",
            vec![
                (
                    ServiceBundleInstanceRecoveryState::OPERATIONAL,
                    ServiceBundleInstanceLifecycleState::CREATED,
                ),
                (
                    ServiceBundleInstanceRecoveryState::RETRY_FAILED,
                    ServiceBundleInstanceLifecycleState::STARTED,
                ),
            ],
        )
        .await;
        // As the bundle is not restartable, if should not try to recover
        let not_found = (
            ServiceBundleInstanceRecoveryState::RETRYING,
            ServiceBundleInstanceLifecycleState::STARTED,
        );
        assert!(!events.iter().any(|e| (e.recovery_state, e.lifecycle_state) == not_found));
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_bundle_destroyed_sends_operational_destroyed_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let (mut context, mut receiver) = setup_lifecycle_test("destroy-notification").await;

        send_lifecycle_update(&mut context, "destroy").await;

        verify_lifecycle_events(
            &mut receiver,
            "lifecycle-destroy",
            vec![(
                ServiceBundleInstanceRecoveryState::OPERATIONAL,
                ServiceBundleInstanceLifecycleState::DESTROYED,
            )],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_subscribe_after_destroy_gets_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // 1. Create context and trigger bundle destruction
        let mut context = TestContext::new("destroy-then-subscribe").await;
        send_lifecycle_update(&mut context, "destroy").await;

        // 2. Wait for the bundle to be fully destroyed
        let fqin = get_rust_service_bundle_fqin("lifecycle-destroy");
        assert_service_is_destroyed(&fqin);

        // 3. Subscribe to state changes AFTER the bundle is destroyed
        let mut receiver = context.new_bundle_state_change_listener();

        // 4. Verify that the new listener receives the current state
        verify_lifecycle_events(
            &mut receiver,
            "lifecycle-destroy",
            vec![(
                ServiceBundleInstanceRecoveryState::OPERATIONAL,
                ServiceBundleInstanceLifecycleState::DESTROYED,
            )],
        )
        .await;
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_crash_on_non_restartable_destroy_sends_failed_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let (mut context, mut receiver) =
            setup_lifecycle_test("crash-destroy-notification-no-restarts").await;

        let fqin = get_rust_service_bundle_fqin("panic-destroy-no-restarts");
        assert_service_is_started(&fqin);

        send_lifecycle_update(&mut context, "panic-destroy-no-restarts").await;

        let events = verify_lifecycle_events(
            &mut receiver,
            "panic-destroy-no-restarts",
            vec![
                (
                    ServiceBundleInstanceRecoveryState::OPERATIONAL,
                    ServiceBundleInstanceLifecycleState::STARTED,
                ),
                (
                    ServiceBundleInstanceRecoveryState::RETRY_FAILED,
                    ServiceBundleInstanceLifecycleState::DESTROYED,
                ),
            ],
        )
        .await;
        // As the bundle is not restartable, if should not try to recover
        let not_found = (
            ServiceBundleInstanceRecoveryState::RETRYING,
            ServiceBundleInstanceLifecycleState::DESTROYED,
        );
        assert!(!events.iter().any(|e| (e.recovery_state, e.lifecycle_state) == not_found));
        context.async_drop().await;
    }

    #[tokio::test]
    async fn test_crash_on_restartable_destroy_sends_failed_notification() {
        let lock = NamedLock::create("test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let (mut context, mut receiver) =
            setup_lifecycle_test("crash-destroy-notification-restarts").await;

        let fqin = get_rust_service_bundle_fqin("panic-destroy-restarts");
        assert_service_is_started(&fqin);

        send_lifecycle_update(&mut context, "panic-destroy-restarts").await;

        verify_lifecycle_events(
            &mut receiver,
            "panic-destroy-restarts",
            vec![
                (
                    ServiceBundleInstanceRecoveryState::OPERATIONAL,
                    ServiceBundleInstanceLifecycleState::STARTED,
                ),
                // First fail to destroy
                (
                    ServiceBundleInstanceRecoveryState::RETRYING,
                    ServiceBundleInstanceLifecycleState::DESTROYED,
                ),
                // As bundle is destroyed, second time will succeed.
                (
                    ServiceBundleInstanceRecoveryState::OPERATIONAL,
                    ServiceBundleInstanceLifecycleState::DESTROYED,
                ),
            ],
        )
        .await;
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

    async fn setup_lifecycle_test(test_name: &str) -> (TestContext, mpsc::Receiver<UpdateState>) {
        let context = TestContext::new(test_name).await;
        let receiver = context.new_bundle_state_change_listener();
        (context, receiver)
    }

    async fn send_lifecycle_update(context: &mut TestContext, custom_state_value: &str) {
        let res = send_custom_update(
            "LIFECYCLE",
            custom_state_value,
            context,
            MessageField::some(Timestamp::now()),
        )
        .await;
        assert!(res.is_ok());
    }

    async fn verify_lifecycle_events(
        receiver: &mut mpsc::Receiver<UpdateState>,
        expected_instance_name: &str,
        expected_states: Vec<(
            ServiceBundleInstanceRecoveryState,
            ServiceBundleInstanceLifecycleState,
        )>,
    ) -> Vec<UpdateState> {
        // We receive the state of multiple bundles as on registration we get previous states.
        // So we need to loop until we find the one we got. If not, the timeout will be reached.
        // We return the list of events to check in tests that no unexpected events happened in the middle.
        let found_events_result: Result<Vec<UpdateState>, (Vec<UpdateState>, &str)> = async {
            let mut expected_states_iter = expected_states.iter().peekable();
            let mut recorded_events: Vec<UpdateState> = Vec::new();
            let mut all_received_events: Vec<UpdateState> = Vec::new();
            let mut recording = false;

            let timeout_future = tokio::time::sleep(Duration::from_secs(5));
            tokio::pin!(timeout_future);

            loop {
                tokio::select! {
                    _ = &mut timeout_future => {
                        // Timeout elapsed
                        return Err((all_received_events, "Test timed out without receiving all expected events."));
                    }
                    maybe_event = receiver.recv() => {
                        if let Some(event) = maybe_event {
                            if event.fqin.serviceBundleName != "OrchestratorSampleRustServiceBundle"
                                || event.fqin.sdvPackageName != "com.android.sdv.test.orchestrator"
                                || event.fqin.serviceInstanceName != expected_instance_name
                            {
                                continue; // Ignore events from other instances
                            }

                            all_received_events.push(event.clone());

                            let is_expected = if let Some((recovery, lifecycle)) = expected_states_iter.peek() {
                                &event.recovery_state == recovery && &event.lifecycle_state == lifecycle
                            } else {
                                false
                            };

                            if !recording && is_expected {
                                recording = true;
                            }

                            if recording {
                                recorded_events.push(event);
                            }

                            if is_expected {
                                expected_states_iter.next();
                                if expected_states_iter.peek().is_none() {
                                     return Ok(recorded_events);
                                }
                            }
                        } else {
                            // Channel closed
                            return Err((all_received_events, "Did not find all expected events in order. The channel may have been closed prematurely."));
                        }
                    }
                }
            }
        }
        .await;

        match found_events_result {
            Ok(recorded_events) => recorded_events,
            Err((all_events, msg)) => {
                panic!("{} Found:\n{:#?}", msg, all_events);
            }
        }
    }
}
