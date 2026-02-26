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
//! Abstracts functionality from the Service Discovery Manager so it can
//! be mocked for unit tests without using the real SD implementation.

use futures::StreamExt;
use log::{debug, warn};
use mockall::automock;
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::ServiceDiscoveryManager;
use sdv_comms::sd::ServiceUnitChangeEvent as CommsServiceUnitChangeEvent;
use sdv_comms::sd::ServiceUnitDefinition;
use sdv_comms::sd::UnitChangeEventStream;
use sdv_comms::sd::UnitNameDiscoveryArgs;
use sdv_comms::sd::UnitType;
use sdv_mw_rs_com_android_sdv_orchestrator_orchestrator_service_bundle::OrchestratorServiceBundle;
use sdv_mw_rs_comms::Communicate;
use sdv_mw_rs_comms::Publication;
use sdv_mw_rs_comms::UnitQuery;
use sdv_mw_rs_sdv_comms::SdvComms;
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use sdv_status_rs::SdvStatusCode;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

#[automock]
#[async_trait::async_trait]
/// Wrapper used to mock the Comm stack in tests.
pub(crate) trait OrchestrationComms: Send + Sync {
    async fn get_orch_service_bundle(
        &self,
        servers: (
            std::sync::Arc<dyn com_android_sdv_orchestrator_custom_state_service_rpc::Interface>,
        ),
    ) -> SdvResult<OrchestratorServiceBundle>;

    async fn get_publication(&self, query: &UnitQuery) -> SdvResult<Arc<dyn Publication>>;

    async fn subscribe_service_unit_change_by_type(
        &self,
        sender: Sender<ServiceUnitChangeEvent>,
        unit_type: UnitType,
    ) -> SdvResult<()>;

    async fn subscribe_service_unit_change_by_name(
        &self,
        sender: Sender<ServiceUnitChangeEvent>,
        unit_discovery_args: UnitNameDiscoveryArgs,
    ) -> SdvResult<()>;
}

#[derive(Debug)]
/// Represents the ServiceUnitChangeEvent needed by orchestrator.
/// Captures the service unit name and the Fqin of every registered/unregistered bundles.
pub(crate) enum ServiceUnitChangeEvent {
    /// A new Service Unit was registered with the given unit name and Fqin.
    Registered(ServiceUnitIdentity),

    /// A Service Unit with the given unit name and Fqin was unregistered.
    Unregistered(ServiceUnitIdentity),
}

#[derive(Debug)]
/// Captures the service unit name and the Fqin of every registered/unregistered bundles.
pub(crate) struct ServiceUnitIdentity {
    pub(crate) name: String,
    pub(crate) fqin: ServiceFqin,
}

impl From<ServiceUnitDefinition> for ServiceUnitIdentity {
    fn from(sud: ServiceUnitDefinition) -> Self {
        ServiceUnitIdentity {
            name: sud.get_unit_identifier().get_unit_name().to_string(),
            fqin: sud.get_service_bundle_identity().get_fqin().clone(),
        }
    }
}

async fn handle_unit_change_stream(
    sender: Sender<ServiceUnitChangeEvent>,
    unit_name: &str,
    unit_type_stream: SdvResult<UnitChangeEventStream>,
) -> SdvResult<()> {
    if let Ok(mut unit_type_stream) = unit_type_stream {
        while let Some(unit_change_event) = unit_type_stream.next().await {
            match unit_change_event {
                CommsServiceUnitChangeEvent::Registered(sud) => {
                    debug!(
                        "Service Unit Definition registered for Unit Type {:?} in VM {:?}",
                        unit_name,
                        sud.get_service_bundle_identity().get_fqin().get_sdv_vm_name()
                    );
                    let res = sender
                        .send(ServiceUnitChangeEvent::Registered(ServiceUnitIdentity::from(sud)))
                        .await;
                    if let Err(err) = res {
                        warn!(
                            "Error subscribing to new publisher of Unit Type {unit_name:?} with error {err:?}"
                        );
                    }
                }
                CommsServiceUnitChangeEvent::Unregistered(sud) => {
                    debug!(
                        "Service Unit Definition unregistered for Unit Type {:?} in VM {:?}",
                        unit_name,
                        sud.get_service_bundle_identity().get_fqin().get_sdv_vm_name()
                    );
                    let res = sender
                        .send(ServiceUnitChangeEvent::Unregistered(ServiceUnitIdentity::from(sud)))
                        .await;
                    if let Err(err) = res {
                        warn!(
                            "Error unregistering new publisher of Unit Type {unit_name:?} with error: {err:?}"
                        );
                    }
                }
                CommsServiceUnitChangeEvent::Changed(sud) => {
                    debug!(
                        "Service Unit Definition change for Unit Type {:?} in VM {:?}",
                        unit_name,
                        sud.get_service_bundle_identity().get_fqin().get_sdv_vm_name()
                    );
                }
            }
        }
        let error_msg = format!(
            "Stream for listening to Unit Type {unit_name:?} from Service Discovery Manager was closed."
        );
        Err(SdvStatus::with_message(SdvStatusCode::Unknown, error_msg))
    } else {
        let error_msg =
            format!("Error subscribing to Unit Type {unit_name:?} from Service Discovery Manager.");
        Err(SdvStatus::with_message(SdvStatusCode::Unknown, error_msg))
    }
}

#[async_trait::async_trait]
impl OrchestrationComms for SdvComms {
    async fn get_orch_service_bundle(
        &self,
        servers: (
            std::sync::Arc<dyn com_android_sdv_orchestrator_custom_state_service_rpc::Interface>,
        ),
    ) -> SdvResult<OrchestratorServiceBundle> {
        OrchestratorServiceBundle::new(Arc::new(self.clone()), servers).await
    }

    async fn get_publication(&self, query: &UnitQuery) -> SdvResult<Arc<dyn Publication>> {
        self.find_publication(query).await
    }

    async fn subscribe_service_unit_change_by_type(
        &self,
        sender: Sender<ServiceUnitChangeEvent>,
        unit_type: UnitType,
    ) -> SdvResult<()> {
        let service_discovery_manager = ServiceDiscoveryManager::new(self.context);
        let stream =
            service_discovery_manager.subscribe_service_unit_change_by_type(&unit_type).await;
        handle_unit_change_stream(sender, unit_type.get_unit_type_name(), stream).await
    }

    async fn subscribe_service_unit_change_by_name(
        &self,
        sender: Sender<ServiceUnitChangeEvent>,
        unit_discovery_args: UnitNameDiscoveryArgs,
    ) -> SdvResult<()> {
        debug!("Subscribe to service unit change by name");
        let service_discovery_manager = ServiceDiscoveryManager::new(self.context);
        let stream = service_discovery_manager
            .subscribe_service_unit_change_by_name(&unit_discovery_args)
            .await;
        match unit_discovery_args.get_unit_name() {
            Some(unit_name) => handle_unit_change_stream(sender, unit_name, stream).await,
            None => Err(SdvStatus::with_message(
                SdvStatusCode::Internal,
                format!("Failed to get unit name for {unit_discovery_args:?}"),
            )),
        }
    }
}
