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
//! Orchestrator agent
pub mod engine;
pub(crate) mod instance_manager;
pub(crate) mod instance_manager_factory;
pub(crate) mod orch_binder;
pub mod states;
use crate::engine::OrchestratorEngine;
use crate::orch_binder::bundle_state_reporter_service::ServiceBundleStateReporterService;
use crate::states::orch_states_updates_handler::OrchestratorStateUpdatesHandler;
use crate::states::states_subscriber_interface::StatesClientSubscriber;
use crate::instance_manager_factory::InstanceManagerFactory;
use crate::states::ORCH_POWER_READY_PROP;
use crate::states::ORCH_VEHICLE_READY_PROP;
use google_sdv_lifecycle_manager::aidl::google::sdv::lifecycle::ILifecycleManager::{
    BpLifecycleManager, ILifecycleManager,
};
use google_sdv_lifecycle_crash_notification_aidl::aidl::google::sdv::lifecycle::internal::crash_notification::IServiceBundleCrashNotificationService::{
    BpServiceBundleCrashNotificationService, IServiceBundleCrashNotificationService,
};
use google_sdv_lifecycle_manager::binder;
use google_sdv_service_bundles_registry::aidl::google::sdv::service_bundles_registry::IRegistry::{
    BpRegistry as BpServiceBundlesRegistry, IRegistry as IServiceBundlesRegistry,
};
use log::{debug, error, warn};
use mockall::automock;
use persistent_binder::{BinderConnectionManager, PersistentBinderConnection};
use protobuf::well_known_types::timestamp::Timestamp;
use rustutils::android::system_properties::{self, read};
use sdv_dump::DumpBinderConnection;
use sdv_orch_dist_config::evaluator::Evaluator;
use sdv_orchestration_common::mode::{Mode, UpdateEvent};
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use sdv_status_rs::SdvStatusCode;
use std::sync::atomic::AtomicBool;
use tokio::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;

const ORCH_CONFIG_ENV: &str = "persist.sdv.orchestrator_config_path";
/// Tag for doing dumpsys.
/// Invoke: dumpsys com.google.sdv.ISdvAgent/orch
static BINDER_TAG: &str = "orch"; // see agent's SELinux policies
static DUMP_REPORT_TITLE: &str = "SDV Agent dump - Orchestrator";

#[derive(Clone)]
/// Orchestrator agent
pub struct Agent {}

/// Orchestrator implementation that binds implementation of configuration and state updates with the orchestrator engine.
impl Agent {
    /// Creates a new orchestrator agent and subscribes for state updates.
    pub async fn new() -> anyhow::Result<Agent> {
        Self::reset_vpm_props();

        // We do not expect many subscribers, so channel size 1 should be enough
        let (new_listener_sender, new_listener_receiver) = mpsc::channel(1);
        let bundle_updates_subscribers =
            ServiceBundleStateReporterService::register_binder_service(
                new_listener_sender.clone(),
                tokio::runtime::Handle::current(),
            )?;
        let dm: Arc<sdv_dump::DumpManager> =
            DumpBinderConnection::initialize(DUMP_REPORT_TITLE, BINDER_TAG);
        dm.set_agent_state("See orchestrator state below.");
        let orch_config_property = read(ORCH_CONFIG_ENV);
        let config: Option<String>;
        if let Ok(orch_prop) = orch_config_property {
            if let Some(config_path) = orch_prop {
                debug!("Orchestrator global property '{ORCH_CONFIG_ENV}' is '{config_path}'");
                config = Some(config_path);
            } else {
                warn!("'{ORCH_CONFIG_ENV}' property is empty.");
                config = None;
            }
        } else {
            error!("Error reading orch config system property '{ORCH_CONFIG_ENV}'");
            config = None;
        }

        let registry = Self::get_service_bundles_registry()?;
        let available_service_bundles = registry.getAvailableServiceBundlesMetadata()?;
        let bundle_config_files = available_service_bundles
            .iter()
            .filter_map(|metadata| metadata.orchestrationConfigPath.as_ref())
            .inspect(|path| debug!("Orch bundle config path: {path:#?}"))
            .map(|path| path.as_str())
            .collect::<Vec<&str>>();

        let process_dump_info: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let mut evaluator = Evaluator::new(config, &bundle_config_files);
        evaluator.restore_modes();
        let modes = evaluator.get_custom_modes();
        let restart_config = Arc::new(evaluator.get_retry_configuration());
        let evaluator = Arc::new(Mutex::new(evaluator));
        let lm_fetcher = Box::new(LMBinder::default());
        let lm_connection = PersistentBinderConnection::new(move || lm_fetcher.get_lm());
        let crash_notifier: Arc<
            dyn BinderConnectionManager<dyn IServiceBundleCrashNotificationService>,
        > = PersistentBinderConnection::new(Self::get_crash_notification_binder);
        let instance_manager_factory = Arc::new(InstanceManagerFactory::new(
            lm_connection.clone(),
            restart_config,
            evaluator.clone(),
            bundle_updates_subscribers,
        ));
        let orch = Arc::new(OrchestratorEngine::new(
            evaluator,
            process_dump_info.clone(),
            instance_manager_factory,
            crash_notifier.clone(),
            new_listener_receiver,
        ));
        dm.add_reporter("Configuration state", &orch.clone());
        OrchestratorStateUpdatesHandler::new(
            orch,
            Arc::new(Mutex::new(modes)),
            dm.clone(),
            process_dump_info,
            lm_connection,
        )
        .await;
        Ok(Self {})
    }

    fn reset_vpm_props() {
        debug!("Resetting VPM ready properties in case of previous crash");
        if let Err(e) = system_properties::write(ORCH_VEHICLE_READY_PROP, "false") {
            error!("Couldn't write property {ORCH_VEHICLE_READY_PROP}: {e}");
        }
        if let Err(e) = system_properties::write(ORCH_POWER_READY_PROP, "false") {
            error!("Couldn't write property {ORCH_POWER_READY_PROP}: {e}");
        }
    }

    /// Gets service bundles registry handler.
    fn get_service_bundles_registry() -> SdvResult<binder::Strong<dyn IServiceBundlesRegistry>> {
        let descriptor = <BpServiceBundlesRegistry as IServiceBundlesRegistry>::get_descriptor()
            .to_owned()
            + "/default";
        binder::wait_for_interface::<dyn IServiceBundlesRegistry>(&descriptor).map_err(|err| {
            SdvStatus::with_message(
                SdvStatusCode::NotFound,
                format!("Failed to find interface ServiceBundlesRegistry with error: {err:?}"),
            )
        })
    }

    /// Gets LM crash notifications binder.
    fn get_crash_notification_binder() -> binder::Strong<dyn IServiceBundleCrashNotificationService>
    {
        debug!("Fetching LM crash notification binder");
        let descriptor = <BpServiceBundleCrashNotificationService as IServiceBundleCrashNotificationService>::get_descriptor()
            .to_owned()
            + "/default";
        let res =
            binder::wait_for_interface::<dyn IServiceBundleCrashNotificationService>(&descriptor)
                .unwrap_or_else(|error| {
                    warn!("Retrying LM crash notifier binder connection: {error}");
                    Self::get_crash_notification_binder()
                });
        debug!("Successfully fetched LM crash notification binder");

        res
    }
}

#[derive(Default)]
/// Returns the Binder implementation of LM.
pub struct LMBinder {}

impl LifecycleManagerFetcher for LMBinder {
    /// LM binder can crash so we want to fetch it each time we need to work with a new instance.
    fn get_lm(&self) -> binder::Strong<dyn ILifecycleManager> {
        debug!("Fetching LM lifecycle manager binder");
        let descriptor =
            <BpLifecycleManager as ILifecycleManager>::get_descriptor().to_owned() + "/default";
        let result = binder::wait_for_interface(&descriptor).unwrap_or_else(|error| {
            warn!("Retrying LM lifecycle manager binder connection: {error}");
            self.get_lm()
        });
        debug!("Successfully fetched LM lifecycle manager binder");

        result
    }
}

/// Trait used to fetch a new instance of LM.
pub trait LifecycleManagerFetcher: Send + Sync {
    /// Returns an object implementing the ILifecycleManager interface.
    fn get_lm(&self) -> binder::Strong<dyn ILifecycleManager>;
}

#[automock]
#[async_trait::async_trait]
/// Handler for receiving updates from the implementer of state updates to the orchestrator.
/// Used by the engine to reacts on a new state update.
pub trait StatesUpdateHandler: Send + Sync {
    /// Request an update to the current configuration based on the given mode and state.
    /// After the update, the configuration is evaluated and service bundles are transitioned.
    async fn on_state_update(
        &self,
        mode: &Mode,
        state: &str,
        cancellation_boolean: Arc<AtomicBool>,
        timestamp: &Option<Timestamp>,
    );

    /// Triggers an event update to evaluate the current configuration and transitioning bundles
    /// without going through a new mode update.
    async fn on_event_update(&self, mode: &UpdateEvent, cancellation_boolean: Arc<AtomicBool>);
}
