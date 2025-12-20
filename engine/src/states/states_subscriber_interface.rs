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
//! Contains different traits for hidding the VPM implementation details.
//! This allows the orchestrator vpm client to provide the logic without depending on the
//! underlaying VPM implementation, allowing it to change without changing the internal logic.

use ::protobuf::well_known_types::timestamp::Timestamp;
use async_trait::async_trait;
use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;
use mockall::automock;
use sdv_orchestration_common::fqin::Fqin;
use sdv_orchestration_common::mode::UpdateEvent;
use sdv_status_rs::SdvResult;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio_util::sync::CancellationToken;

#[automock]
#[async_trait]
/// VPM calls. This is an abstraction layer that hides the VPM and custom modes implementation details.
/// Implementers need to make sure to handle new power, vehicle and custom modes properly.
pub trait StatesClientSubscriber: Send + Sync {
    /// Registers a new listener for vehicle updates
    async fn subscribe_to_vehicle_updates(&self) -> SdvResult<()>;

    /// Registers a new listener for power updates
    async fn subscribe_to_power_updates(&self) -> SdvResult<PowerStateReport>;

    /// Registers a new listener for custom mode updates
    async fn subscribe_to_custom_updates(
        &self,
        custom_modes_allowed: Arc<Mutex<HashMap<Fqin, HashSet<String>>>>,
    ) -> SdvResult<()>;
}

/// New mode update request
#[derive(Debug)]
pub struct StateUpdateRequest {
    /// The requested update event.
    pub update_event: UpdateEvent,
    /// Notifies that the state update finished. Only used once in power transitions to notify VPM.
    pub state_notifier: Mutex<Option<OneShotSender<bool>>>,
    /// Used to cancel the current task to avoid waiting for the state_notifier to be notified of completion.
    pub cancel_token: CancellationToken,
    /// Timestamp for custom modes types only, for other types it is None
    pub timestamp: Option<Timestamp>,
}
