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
//! Factory for creating `InstanceManager` instances.

use crate::instance_manager::{IInstanceManager, InstanceManager};
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::ILifecycleManager;
use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::IServiceBundleInstanceStateChangeListener;
use binder::Strong;
use mockall::automock;
use persistent_binder::BinderConnectionManager;
use sdv_orch_dist_config::evaluator::Evaluator;
use sdv_orchestration_common::fqin::Fqin;
use sdv_orchestration_common::retry_configuration::RetryConfiguration;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

/// A factory for creating InstanceManagers.
/// A trait for factories that create `InstanceManager` instances.
#[automock]
pub trait IInstanceManagerFactory: Send + Sync {
    /// Creates a new `InstanceManager` for the given FQIN.
    fn create(&self, fqin: &Fqin) -> Arc<dyn IInstanceManager>;
}

/// The concrete implementation of the `InstanceManager` factory.
pub struct InstanceManagerFactory {
    lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
    restart_config: Arc<HashMap<Fqin, RetryConfiguration>>,
    config: Arc<Mutex<Evaluator>>,
    bundle_updates_subscribers:
        Arc<Mutex<HashMap<u32, Strong<dyn IServiceBundleInstanceStateChangeListener>>>>,
    default_max_retries: u32,
}

impl InstanceManagerFactory {
    /// Creates a new InstanceManagerFactory.
    pub fn new(
        lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
        restart_config: Arc<HashMap<Fqin, RetryConfiguration>>,
        config: Arc<Mutex<Evaluator>>,
        bundle_updates_subscribers: Arc<
            Mutex<HashMap<u32, Strong<dyn IServiceBundleInstanceStateChangeListener>>>,
        >,
        default_max_retries: u32,
    ) -> Self {
        Self {
            lm_connection,
            restart_config,
            config,
            bundle_updates_subscribers,
            default_max_retries,
        }
    }

    /// Gets the retry configuration for a given FQIN, falling back to the default if not specified.
    fn get_retry_config(&self, fqin: &Fqin) -> RetryConfiguration {
        self.restart_config
            .get(fqin)
            .cloned()
            .unwrap_or_else(|| RetryConfiguration::new(self.default_max_retries))
    }
}

impl IInstanceManagerFactory for InstanceManagerFactory {
    fn create(&self, fqin: &Fqin) -> Arc<dyn IInstanceManager> {
        InstanceManager::new(
            fqin.to_lm_fqin(),
            self.lm_connection.clone(),
            self.get_retry_config(fqin),
            self.config.clone(),
            self.bundle_updates_subscribers.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::ILifecycleManager;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::IServiceBundleInstanceStateChangeListener;
    use persistent_binder::MockBinderConnectionManager;
    use sdv_orch_dist_config::evaluator::Evaluator;
    use sdv_orchestration_common::fqin::Fqin;
    use sdv_orchestration_common::retry_configuration::RetryConfiguration;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn setup_factory(
        restart_config: HashMap<Fqin, RetryConfiguration>,
        default_max_retries: u32,
    ) -> InstanceManagerFactory {
        let mock_lm_connection = MockBinderConnectionManager::<dyn ILifecycleManager>::new();
        let config = Arc::new(Mutex::new(Evaluator::new(None, &[])));
        let subscribers = Arc::new(Mutex::new(HashMap::<
            u32,
            Strong<dyn IServiceBundleInstanceStateChangeListener>,
        >::new()));

        InstanceManagerFactory::new(
            Arc::new(mock_lm_connection),
            Arc::new(restart_config),
            config,
            subscribers,
            default_max_retries,
        )
    }

    #[test]
    fn test_get_retry_config_with_specific_config() {
        // GIVEN
        let fqin = Fqin::new("com.android.sdv.test", "TestBundle", "instance1");
        let retry_config = RetryConfiguration::new(5);
        let factory = setup_factory(HashMap::from([(fqin.clone(), retry_config)]), 0);

        // WHEN
        let result_config = factory.get_retry_config(&fqin);

        // THEN
        assert_eq!(result_config.max_retries, 5);
    }

    #[test]
    fn test_get_retry_config_with_default_config() {
        // GIVEN
        let fqin = Fqin::new("com.android.sdv.test", "TestBundle", "instance2");
        let factory = setup_factory(HashMap::new(), 3);

        // WHEN
        let result_config = factory.get_retry_config(&fqin);

        // THEN
        assert_eq!(result_config.max_retries, 3);
    }
}
