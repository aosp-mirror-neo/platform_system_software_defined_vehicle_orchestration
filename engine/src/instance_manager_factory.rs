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
}

impl InstanceManagerFactory {
    /// Creates a new InstanceManagerFactory.
    pub fn new(
        lm_connection: Arc<dyn BinderConnectionManager<dyn ILifecycleManager>>,
        restart_config: Arc<HashMap<Fqin, RetryConfiguration>>,
        config: Arc<Mutex<Evaluator>>,
    ) -> Self {
        Self { lm_connection, restart_config, config }
    }
}

impl IInstanceManagerFactory for InstanceManagerFactory {
    fn create(&self, fqin: &Fqin) -> Arc<dyn IInstanceManager> {
        let retry_config =
            self.restart_config.get(fqin).cloned().unwrap_or_else(|| RetryConfiguration::new(0));
        InstanceManager::new(
            fqin.clone(),
            self.lm_connection.clone(),
            retry_config,
            self.config.clone(),
        )
    }
}
