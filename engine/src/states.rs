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

//! Exposes orchestrator code that has the integration with vpm to consume vehicle, power and custom modes.
mod custom_modes;
pub mod orch_states_subscriber;
pub mod orch_states_updates_handler;
mod orchestration_comms;
pub mod states_subscriber_interface;

/// Boolean property set to true the first time that orch establishes the power connection to VPM
/// If VPM crash, the property won't be reset. Only when orchestrator starts it will be reset to false
/// in case it crashed. The value of the property being true does not guarantee that the connection to VPM is valid
/// and messages can still be lost.
pub(crate) const ORCH_POWER_READY_PROP: &str = "sdv.orchestrator.bootup.power_mode.ready";

/// Boolean property set to true the first time that orch establishes the vehicle connection to VPM
/// If VPM crash, the property won't be reset. Only when orchestrator starts it will be reset to false
/// in case it crashed. The value of the property being true does not guarantee that the connection to VPM is valid
/// and messages can still be lost.
pub(crate) const ORCH_VEHICLE_READY_PROP: &str = "sdv.orchestrator.bootup.vehicle_mode.ready";
