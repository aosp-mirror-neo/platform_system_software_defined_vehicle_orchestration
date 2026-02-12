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
#![allow(non_snake_case)]

use binder::{BinderFeatures, Interface, Result as BinderResult, Strong};
use binder_tokio::TokioRuntime;
use google_sdv_orchestration::aidl::google::sdv::orchestration::IOrchestrationAgent::{
    BnOrchestrationAgent, BpOrchestrationAgent, IOrchestrationAgent, IOrchestrationAgentAsyncServer,
};
use binder::ThreadState;
use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::IServiceBundleInstanceStateChangeListener;
use google_sdv_orchestration::binder;
use log::{error, info, warn};
use sdv_status_rs::{SdvResult, SdvStatus, SdvStatusCode};
use std::collections::HashMap;
use std::sync::{Mutex, Arc};
use tokio::sync::mpsc::Sender;
use tokio::runtime::Handle;
use async_trait::async_trait;

pub(crate) struct ServiceBundleStateReporterService {
    bundle_updates_subscribers:
        Arc<Mutex<HashMap<u32, Strong<dyn IServiceBundleInstanceStateChangeListener>>>>,
    // Sender to notify about newly registered listeners.
    new_listener_sender: Sender<Strong<dyn IServiceBundleInstanceStateChangeListener>>,
}

impl ServiceBundleStateReporterService {
    pub fn new(
        bundle_updates_subscribers: Arc<
            Mutex<HashMap<u32, Strong<dyn IServiceBundleInstanceStateChangeListener>>>,
        >,
        new_listener_sender: Sender<Strong<dyn IServiceBundleInstanceStateChangeListener>>,
    ) -> Self {
        Self { bundle_updates_subscribers, new_listener_sender }
    }

    pub fn register_binder_service(
        new_listener_sender: Sender<Strong<dyn IServiceBundleInstanceStateChangeListener>>,
        rt: Handle,
    ) -> SdvResult<Arc<Mutex<HashMap<u32, Strong<dyn IServiceBundleInstanceStateChangeListener>>>>>
    {
        let descriptor =
            <BpOrchestrationAgent as IOrchestrationAgent>::get_descriptor().to_owned() + "/default";

        let sb_state_reporter_service = ServiceBundleStateReporterService::new(
            Arc::new(Mutex::new(HashMap::new())),
            new_listener_sender,
        );

        let subscribers = sb_state_reporter_service.bundle_updates_subscribers.clone();
        let sb_state_reporter_service_binder = BnOrchestrationAgent::new_async_binder(
            sb_state_reporter_service,
            TokioRuntime(rt),
            BinderFeatures::default(),
        );
        info!("Registering service bundle state reporter binder service {descriptor}");
        binder::add_service(&descriptor, sb_state_reporter_service_binder.as_binder()).map_err(
            |e| {
                SdvStatus::with_cause_and_message(
                    SdvStatusCode::Internal,
                    e,
                    "Failed to register service bundle state reporter binder service",
                )
            },
        )?;
        Ok(subscribers)
    }
}

impl Interface for ServiceBundleStateReporterService {}

#[async_trait]
impl IOrchestrationAgentAsyncServer for ServiceBundleStateReporterService {
    async fn registerServiceBundleInstanceStateChangeListener(
        &self,
        service_bundle_state_change_listener: &Strong<
            dyn IServiceBundleInstanceStateChangeListener,
        >,
    ) -> BinderResult<()> {
        let caller_uid = ThreadState::get_calling_uid();
        let previous_subscribers = self
            .bundle_updates_subscribers
            .lock()
            .unwrap()
            .insert(caller_uid, service_bundle_state_change_listener.clone());
        if previous_subscribers.is_some() {
            warn!(
                "Overwriting existing bundle updates subscribers for caller with UID {caller_uid}"
            );
        }

        // Notify the OrchestratorEngine about the new listener
        if let Err(e) =
            self.new_listener_sender.send(service_bundle_state_change_listener.clone()).await
        {
            error!("Failed to send new listener to OrchestratorEngine: {:?}", e);
        }
        Ok(())
    }

    async fn unregisterServiceBundleInstanceStateChangeListener(&self) -> BinderResult<()> {
        let caller_uid = ThreadState::get_calling_uid();
        let subscribers = self.bundle_updates_subscribers.lock().unwrap().remove(&caller_uid);
        if subscribers.is_none() {
            warn!("Attempting to unregister caller with UID {caller_uid} from bundle updates, from client that haven't subscribed before");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::{
        BnServiceBundleInstanceStateChangeListener, MockIServiceBundleInstanceStateChangeListener,
    };

    // This test is synchronous because it uses `binder::wait_for_interface`, which is a
    // blocking call. Using blocking calls inside an `async` test with `#[tokio::test]`
    // can lead to panics ("Cannot start a runtime from within a runtime").
    #[test]
    fn test_registration_and_unregistration_to_bundle_updates_binder_succeeds() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

        // GIVEN
        // Register the service so we can connect to it
        let registration_result = ServiceBundleStateReporterService::register_binder_service(
            tokio::sync::mpsc::channel(1).0,
            rt.handle().clone(),
        );
        assert!(registration_result.is_ok());
        let descriptor =
            <BpOrchestrationAgent as IOrchestrationAgent>::get_descriptor().to_owned() + "/default";

        // Get the binder client to subscribe to
        let client = binder::wait_for_interface::<dyn IOrchestrationAgent>(&descriptor)
            .expect("Failed to get IOrchestrationAgent binder client");

        let mock_listener = MockIServiceBundleInstanceStateChangeListener::new();
        let mock_listener_binder = BnServiceBundleInstanceStateChangeListener::new_binder(
            mock_listener,
            BinderFeatures::default(),
        );

        // WHEN/THEN subscribing for bundle updates, it should succeed.
        client
            .registerServiceBundleInstanceStateChangeListener(&mock_listener_binder)
            .expect("Registering should succeed");

        // WHEN/THEN unsubscribing for bundle updates, it should also succeed.
        client
            .unregisterServiceBundleInstanceStateChangeListener()
            .expect("Unregistering should succeed");
    }

    #[tokio::test]
    async fn test_registering_to_bundle_updates_multiple_times_succeeds() {
        // GIVEN
        let service = ServiceBundleStateReporterService::new(
            Arc::new(Mutex::new(HashMap::new())),
            tokio::sync::mpsc::channel(1).0,
        );
        let mock_listener = BnServiceBundleInstanceStateChangeListener::new_binder(
            MockIServiceBundleInstanceStateChangeListener::new(),
            BinderFeatures::default(),
        );
        // WHEN registering for the first time
        let result1 =
            service.registerServiceBundleInstanceStateChangeListener(&mock_listener).await;
        // THEN it should succeed and store subscriber
        assert!(result1.is_ok());
        assert_eq!(service.bundle_updates_subscribers.lock().unwrap().len(), 1);
        // WHEN registering a second time with the same listener (same UID)
        let result2 =
            service.registerServiceBundleInstanceStateChangeListener(&mock_listener).await;
        // THEN it should succeed and overwrite the previous entry
        assert!(result2.is_ok());
        assert_eq!(service.bundle_updates_subscribers.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_unregistering_removes_subscription() {
        // GIVEN
        let service = ServiceBundleStateReporterService::new(
            Arc::new(Mutex::new(HashMap::new())),
            tokio::sync::mpsc::channel(1).0,
        );
        let mock_listener = BnServiceBundleInstanceStateChangeListener::new_binder(
            MockIServiceBundleInstanceStateChangeListener::new(),
            BinderFeatures::default(),
        );
        service.registerServiceBundleInstanceStateChangeListener(&mock_listener).await.unwrap();
        // WHEN
        service.unregisterServiceBundleInstanceStateChangeListener().await.unwrap();
        // THEN
        assert!(service.bundle_updates_subscribers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_unregistering_non_registered_listener_succeeds() {
        // GIVEN
        let service = ServiceBundleStateReporterService::new(
            Arc::new(Mutex::new(HashMap::new())),
            tokio::sync::mpsc::channel(1).0,
        );
        // WHEN
        let result = service.unregisterServiceBundleInstanceStateChangeListener().await;
        // THEN
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_registering_sends_listener_to_channel() {
        // GIVEN
        let (new_listener_sender, mut receiver) =
            tokio::sync::mpsc::channel::<Strong<dyn IServiceBundleInstanceStateChangeListener>>(1);
        let service = ServiceBundleStateReporterService {
            bundle_updates_subscribers: Arc::new(Mutex::new(HashMap::new())),
            new_listener_sender,
        };
        let mock_listener = MockIServiceBundleInstanceStateChangeListener::new();
        let mock_listener_binder = BnServiceBundleInstanceStateChangeListener::new_binder(
            mock_listener,
            BinderFeatures::default(),
        );

        // WHEN registering the listener
        let result =
            service.registerServiceBundleInstanceStateChangeListener(&mock_listener_binder).await;
        assert!(result.is_ok(), "Registration failed");

        // THEN the listener should be received on the channel
        let received_listener = receiver.recv().await;
        assert!(received_listener.is_some());
    }
}
