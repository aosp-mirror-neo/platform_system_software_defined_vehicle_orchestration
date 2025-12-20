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

#[cfg(test)]
mod tests {
    use google_sdv_persistent_binder_test::aidl::google::sdv::persistent_binder::test::IPersistentBinderTest::{IPersistentBinderTest, BpPersistentBinderTest};
    use binder::Strong;
    use std::process::{Child, Command};
    use persistent_binder::{BinderConnectionManager, PersistentBinderConnection};

    /// Stores the lifecycle of the test service process to ble able to kill the process in the testes.
    struct TestServiceHandle {
        child: Child,
    }

    impl TestServiceHandle {}

    impl Drop for TestServiceHandle {
        fn drop(&mut self) {
            // Make sure we kill the process
            let _ = self.child.kill();
        }
    }

    fn start_test_service() -> TestServiceHandle {
        let child = Command::new("persistent_binder_test_service")
            .spawn()
            .expect("Failed to start persistent_binder_test_service");
        TestServiceHandle { child }
    }

    fn get_binder_service() -> Strong<dyn IPersistentBinderTest> {
        let descriptor =
            String::from(<BpPersistentBinderTest as IPersistentBinderTest>::get_descriptor())
                + "/default";
        google_sdv_persistent_binder_test::binder::wait_for_interface::<dyn IPersistentBinderTest>(
            &descriptor,
        )
        .expect("Failed to fetch IPersistentBinderTest interface")
    }

    #[tokio::test]
    async fn test_binder_service_is_available() {
        // GIVEN
        // Initialize the binder thread pool before any binder calls.
        binder::ProcessState::start_thread_pool();
        let mut service_handle = start_test_service();
        let (sender, mut recv) = tokio::sync::mpsc::channel::<()>(1);
        // Create the PersistentBinderConnection
        let binder_connection = PersistentBinderConnection::new(get_binder_service);
        binder_connection.set_reconnect_listener(Box::new(move |_| {
            let res = sender.try_send(());
            assert!(res.is_ok());
        }));
        // WHEN
        let kill_result = service_handle.child.kill();
        assert!(kill_result.is_ok());
        // Restart the service so that PersistentBinderConnection has something to reconnect to.
        // The guard will ensure that the new process is not immediately kill.
        let _second_service_handle = start_test_service();
        // THEN it should reconnect through the death callback
        let res = recv.recv().await;
        assert!(res.is_some());
        // We verify that the new binder connection was fetched
        assert!(binder_connection.get_binder().lock().unwrap().ping().unwrap());
    }
}
