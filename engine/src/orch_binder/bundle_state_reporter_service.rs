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
use google_sdv_orchestration::aidl::google::sdv::orchestration::IOrchestrationAgent::{
    BnOrchestrationAgent, BpOrchestrationAgent, IOrchestrationAgent,
};
use binder::ThreadState;
use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::IServiceBundleInstanceStateChangeListener;
use google_sdv_orchestration::binder;
use std::sync::{Mutex, Arc};
use log::{info, warn};
use std::collections::HashMap;
use sdv_status_rs::{SdvResult, SdvStatus, SdvStatusCode};

#[derive(Default)]
pub(crate) struct ServiceBundleStateReporterService {
    // List of subscribers to be notified of service bundle instances updates
    // TODO(b/437891686): Make sure the Arc<Mutex<>> is used in the updates or simplify structure.
    bundle_updates_subscribers:
        Arc<Mutex<HashMap<u32, Strong<dyn IServiceBundleInstanceStateChangeListener>>>>,
}

impl ServiceBundleStateReporterService {
    pub fn register_binder_service() -> SdvResult<()> {
        let descriptor =
            <BpOrchestrationAgent as IOrchestrationAgent>::get_descriptor().to_owned() + "/default";

        let sb_state_reporter_service = ServiceBundleStateReporterService::default();
        let sb_state_reporter_service_binder =
            BnOrchestrationAgent::new_binder(sb_state_reporter_service, BinderFeatures::default());
        info!("Registering service bundle state reporter binder service {descriptor}");
        binder::add_service(&descriptor, sb_state_reporter_service_binder.as_binder()).map_err(
            |e| {
                SdvStatus::with_cause_and_message(
                    SdvStatusCode::Internal,
                    e,
                    "Failed to register service bundle state reporter binder service",
                )
            },
        )
    }
}

impl Interface for ServiceBundleStateReporterService {}

impl IOrchestrationAgent for ServiceBundleStateReporterService {
    fn registerServiceBundleInstanceStateChangeListener(
        &self,
        listener: &Strong<dyn IServiceBundleInstanceStateChangeListener>,
    ) -> BinderResult<()> {
        // TODO(b/437890290): Implement state reporting logic

        let caller_uid = ThreadState::get_calling_uid();
        let previous_subscribers =
            self.bundle_updates_subscribers.lock().unwrap().insert(caller_uid, listener.clone());
        if previous_subscribers.is_some() {
            warn!(
                "Overwriting existing bundle updates subscribers for caller with UID {caller_uid}"
            );
        }

        Ok(())
    }

    fn unregisterServiceBundleInstanceStateChangeListener(&self) -> BinderResult<()> {
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

    #[test]
    fn test_registration_and_unregistration_to_bundle_updates_binder_succeeds() {
        // GIVEN
        // Register the service so we can connect to it
        ServiceBundleStateReporterService::register_binder_service()
            .expect("Failed to register ServiceBundleStateReporterService service");
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

        // WHEN subscribing for bundle updates
        let result = client.registerServiceBundleInstanceStateChangeListener(&mock_listener_binder);

        // THEN
        assert!(result.is_ok());

        // WHEN unsubscribing for bundle updates
        let result = client.unregisterServiceBundleInstanceStateChangeListener();

        // THEN
        assert!(result.is_ok());
    }

    #[test]
    fn test_registering_to_bundle_updates_multiple_times_succeeds() {
        // GIVEN
        let service = ServiceBundleStateReporterService::default();
        let mock_listener = BnServiceBundleInstanceStateChangeListener::new_binder(
            MockIServiceBundleInstanceStateChangeListener::new(),
            BinderFeatures::default(),
        );
        // WHEN registering for the first time
        let result1 = service.registerServiceBundleInstanceStateChangeListener(&mock_listener);
        // THEN it should succeed and store subscriber
        assert!(result1.is_ok());
        assert_eq!(service.bundle_updates_subscribers.lock().unwrap().len(), 1);
        // WHEN registering a second time with the same listener (same UID)
        let result2 = service.registerServiceBundleInstanceStateChangeListener(&mock_listener);
        // THEN it should succeed and overwrite the previous entry
        assert!(result2.is_ok());
        assert_eq!(service.bundle_updates_subscribers.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_unregistering_removes_subscription() {
        // GIVEN
        let service = ServiceBundleStateReporterService::default();
        let mock_listener = BnServiceBundleInstanceStateChangeListener::new_binder(
            MockIServiceBundleInstanceStateChangeListener::new(),
            BinderFeatures::default(),
        );
        service.registerServiceBundleInstanceStateChangeListener(&mock_listener).unwrap();
        // WHEN
        service.unregisterServiceBundleInstanceStateChangeListener().unwrap();
        // THEN
        assert!(service.bundle_updates_subscribers.lock().unwrap().is_empty());
    }

    #[test]
    fn test_unregistering_non_registered_listener_succeeds() {
        // GIVEN
        let service = ServiceBundleStateReporterService::default();
        // WHEN
        let result = service.unregisterServiceBundleInstanceStateChangeListener();
        // THEN
        assert!(result.is_ok());
    }
}
