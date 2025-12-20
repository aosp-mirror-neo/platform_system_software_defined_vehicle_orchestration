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

//! Orchestration configuration representation.

use crate::condition::Condition;
use crate::configuration_parser::parse_configurations_and_log_errors;
use crate::configuration_parser::parse_configurations_or_fail;
use crate::orchestration_state::OrchestrationState;
use sdv_orchestration_common::fqin::Fqin;
use sdv_orchestration_common::instance_state::InstanceState;
use sdv_orchestration_common::retry_configuration::RetryConfiguration;
use sdv_status_rs::SdvResult;
use std::collections::HashMap;
use std::collections::HashSet;

pub(crate) struct Configuration {
    /// List of conditions and corresponding instances states.
    ///
    /// All conditions satisfied by the current orchestration state are contributing to the evaluation of the service instances states.
    state_conditions: Vec<(Condition, HashMap<Fqin, InstanceState>)>,

    /// Map that contains for each service bundle, which custom modes they are allowed to publish.
    /// We use FQIN as key but we don't care about the instance name.
    custom_modes: HashMap<Fqin, HashSet<String>>,

    /// Map that contains for each service bundle, its retry configuration (if one exists).
    retry_mapping: HashMap<Fqin, RetryConfiguration>,
}

impl Configuration {
    pub fn new(vm_config_path: Option<String>, service_bundle_config_paths: &[&str]) -> Self {
        let (state_conditions, custom_modes, retry_mapping) =
            parse_configurations_and_log_errors(vm_config_path, service_bundle_config_paths);
        Self { state_conditions, custom_modes, retry_mapping }
    }

    pub fn get_custom_modes(&self) -> HashMap<Fqin, HashSet<String>> {
        self.custom_modes.clone()
    }

    pub fn get_retry_mapping(&self) -> HashMap<Fqin, RetryConfiguration> {
        self.retry_mapping.clone()
    }

    #[allow(dead_code)] // TODO not used at runtime
    pub fn new_or_fail_on_bad_vm_config(
        vm_config_path: Option<String>,
        service_bundle_config_paths: &[&str],
    ) -> SdvResult<Self> {
        let (state_conditions, custom_modes, retry_mapping) =
            parse_configurations_or_fail(vm_config_path, service_bundle_config_paths)?;
        Ok(Self { state_conditions, custom_modes, retry_mapping })
    }

    pub fn get_instances_states(
        &self,
        orchestration_state: &OrchestrationState,
    ) -> HashMap<Fqin, InstanceState> {
        self.state_conditions
            .iter()
            .filter(|(condition, _)| condition.evaluate(orchestration_state))
            .map(|(_, instances_states)| instances_states)
            .fold(HashMap::new(), |mut acc, states| {
                for (fqin, state) in states.iter() {
                    acc.entry(fqin.clone())
                        .and_modify(|previous_state| {
                            *previous_state = previous_state.merge_with(state)
                        })
                        .or_insert(state.clone());
                }
                acc
            })
    }
}
