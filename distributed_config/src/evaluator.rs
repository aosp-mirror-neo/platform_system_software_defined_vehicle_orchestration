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

//! Implementation for Orchestrator configuration evaluation.

use crate::configuration::Configuration;
use crate::orchestration_state::OrchestrationState;
use chrono::{DateTime, Utc};
use log::{info, warn};
use protobuf::well_known_types::timestamp::Timestamp;
use sdv_dump::DumpStateReporter;
use sdv_orchestration_common::fqin::Fqin;
use sdv_orchestration_common::instance_state::InstanceState;
use sdv_orchestration_common::mode::Mode;
use sdv_orchestration_common::retry_configuration::RetryConfiguration;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::io::Error;
use tracing::instrument;

const MODES_STATE_BACKUP_FILE_PATH: &str = "/data/orchestrator/modes_backup.json";
const MODES_STATE_BACKUP_TMP_FILE_PATH: &str = "/data/orchestrator/modes_backup.json.tmp";

/// Struct for orchestrator configuration parsing and evaluation.
pub struct Evaluator {
    orchestration_state: OrchestrationState,
    configuration: Configuration,
}

fn update_timestamp(
    prev_timestamp: Option<&DateTime<Utc>>,
    curr_timestamp: Option<&DateTime<Utc>>,
) -> bool {
    match (prev_timestamp, curr_timestamp) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(prev), Some(curr)) => match prev.timestamp().cmp(&curr.timestamp()) {
            Less => true,
            Greater => false,
            Equal => prev.timestamp_subsec_nanos() < curr.timestamp_subsec_nanos(),
        },
    }
}

impl Evaluator {
    /// Create a new evaluator.
    pub fn new(vm_config_path: Option<String>, service_bundle_config_paths: &[&str]) -> Self {
        Self {
            configuration: Configuration::new(vm_config_path, service_bundle_config_paths),
            orchestration_state: OrchestrationState::new(),
        }
    }

    /// Update the current orchestration state given the timestamps.
    /// If there is no entry or there is no timestamp for the entry the states are updated
    /// Else if the incoming timestamp is none or smaller than the present entry, the states are NOT updated
    /// Else the states are updated
    ///
    /// Returns states of service bundle instances corresponding to the updated orchestration state.
    /// Returns None if there is no update due to an outdated timestamp.
    #[instrument(skip(self))]
    pub fn update_orchestration_state(
        &mut self,
        mode: &Mode,
        state: &str,
        proto_timestamp: &Option<Timestamp>,
    ) -> Option<HashMap<Fqin, InstanceState>> {
        // Timestamp protobuf documentation says it stored date in UTC time, so we transform to UTC DateTime for using serde.
        let timestamp: Option<DateTime<Utc>> = proto_timestamp
            .as_ref()
            .and_then(|t| DateTime::<Utc>::from_timestamp(t.seconds, t.nanos as u32));
        let prev_timestamp =
            self.orchestration_state.get(mode).map(|(_, timestamp)| timestamp).unwrap_or(&None);
        if update_timestamp(prev_timestamp.as_ref(), timestamp.as_ref()) {
            self.orchestration_state.insert(mode.clone(), (state.to_string(), timestamp));
            self.back_up_orch_state();
            Some(self.get_instances_states())
        } else {
            None
        }
    }

    fn back_up_orch_state(&mut self) {
        match serde_json::to_string(&self.orchestration_state) {
            Ok(serialized_states) => {
                match fs::write(MODES_STATE_BACKUP_TMP_FILE_PATH, serialized_states) {
                    Ok(_) => {
                        if let Err(err) = fs::rename(
                            MODES_STATE_BACKUP_TMP_FILE_PATH,
                            MODES_STATE_BACKUP_FILE_PATH,
                        ) {
                            warn!("Failed to store back up of modes to file: {err:?}")
                        }
                    }
                    Err(err) => {
                        warn!("Failed to store back up of modes to tmp file: {err:?}");
                    }
                }
            }
            Err(err) => {
                warn!("Failed to serialize backup for modes: {err:?}");
            }
        }
    }

    #[instrument(skip(self))]
    /// Get the state of service bundle instances corresponding to the current orchestration state.
    pub fn restore_modes(&mut self) {
        let tmp_exists = fs::exists(MODES_STATE_BACKUP_TMP_FILE_PATH);
        if tmp_exists.unwrap_or(false) {
            // This means there was a problem when renaming the tmp file to the persistent file.
            warn!("Tmp back up of custom modes exists, renaming to persistent file and restoring modes.");
            match fs::rename(MODES_STATE_BACKUP_TMP_FILE_PATH, MODES_STATE_BACKUP_FILE_PATH) {
                Ok(_) => {
                    self.try_restore_modes_from_path(MODES_STATE_BACKUP_FILE_PATH);
                }
                Err(err) => {
                    warn!(
                        "Failed to store back up of modes to persistent file: {err:?}. Restoring modes from tmp file."
                        );
                    self.try_restore_modes_from_path(MODES_STATE_BACKUP_TMP_FILE_PATH);
                }
            }
        } else {
            // Tmp file does not exist or we cannot verify its existence. So we check the persistent file.
            match fs::exists(MODES_STATE_BACKUP_FILE_PATH) {
                Ok(exists) => {
                    if exists {
                        self.try_restore_modes_from_path(MODES_STATE_BACKUP_FILE_PATH);
                    } else {
                        info!("No back up exists to restore modes, nothing to do.");
                    }
                }
                Err(err) => {
                    warn!("Error trying to read back up file: {err}")
                }
            }
        }
    }

    fn try_restore_modes_from_path(&mut self, path: &str) {
        info!("Restoring orch modes from {path}");
        match std::fs::read_to_string(path) {
            Ok(file_content) => match serde_json::from_str::<OrchestrationState>(&file_content) {
                Ok(modes) => {
                    for (mode, value) in modes {
                        self.orchestration_state.insert(mode.clone(), value.clone());
                    }
                }
                Err(err) => {
                    warn!("Failed to restore previous orch state from file {path}: {err}");
                }
            },
            Err(err) => {
                warn!("Failed to read file {path}: {err}");
            }
        }
    }

    #[instrument(skip(self))]
    /// Get the state of service bundle instances corresponding to the current orchestration state.
    pub fn get_instances_states(&self) -> HashMap<Fqin, InstanceState> {
        self.configuration.get_instances_states(&self.orchestration_state)
    }

    /// Returns the custom modes from the orchestrator configuration
    pub fn get_custom_modes(&self) -> HashMap<Fqin, HashSet<String>> {
        self.configuration.get_custom_modes()
    }

    /// Returns the retry configuration from the orchestrator configuration
    pub fn get_retry_configuration(&self) -> HashMap<Fqin, RetryConfiguration> {
        self.configuration.get_retry_mapping()
    }

    fn dump_active_modes(&self, writer: &mut dyn std::io::Write) -> Result<(), Error> {
        writeln!(writer, "Active modes:")?;
        // We set a bit of padding so the dump is easier to read
        let padding = 5;
        // We set a minimum length between the columns so the dump is easier to read
        let min_length = 25;
        let max_mode_size = self
            .orchestration_state
            .keys()
            .fold(min_length, |current_max, mode| current_max.max(format!("{mode:?}").len()))
            + padding;
        let max_value_size = self
            .orchestration_state
            .values()
            .fold(min_length, |current_max, value| current_max.max(value.0.len()))
            + padding;
        writeln!(
            writer,
            "{:<max_mode_size$}{:<max_value_size$}TIMESTAMP (scs, ns)",
            "MODE", "VALUE"
        )?;
        let mut state_vec: Vec<(&Mode, &(String, Option<DateTime<Utc>>))> =
            self.orchestration_state.iter().collect();
        // We sort by mode
        state_vec.sort_by(|inst_1, inst_2| inst_1.0.cmp(inst_2.0));
        state_vec.iter().try_for_each(|(mode, value)| -> Result<(), Error> {
            writeln!(
                writer,
                "{:<max_mode_size$}{:<max_value_size$}{}",
                format!("{:?}", mode),
                value.0,
                value
                    .1
                    .map(|tmp| format!(
                        "{} (scs) {} (ns)",
                        tmp.timestamp(),
                        tmp.timestamp_subsec_nanos()
                    ))
                    .unwrap_or(String::from("-"))
            )?;
            Result::Ok(())
        })?;
        Ok(())
    }

    fn dump_allowed_custom_modes(&self, writer: &mut dyn std::io::Write) -> Result<(), Error> {
        writeln!(writer, "\nModes allowed to publish by bundle (FQIN: modes):")?;
        let mut modes_per_fqin: Vec<(Fqin, HashSet<String>)> =
            self.get_custom_modes().into_iter().collect();
        modes_per_fqin.sort_by(|inst_1, inst_2| inst_1.0.cmp(&inst_2.0));
        modes_per_fqin.iter().try_for_each(|(fqin, modes_list)| -> Result<(), Error> {
            let mut modes_vec: Vec<String> = modes_list.iter().cloned().collect();
            modes_vec.sort();
            writeln!(
                writer,
                "{}/{}: {}",
                fqin.package_name,
                fqin.service_bundle_name,
                modes_vec.join(", ")
            )?;
            Result::Ok(())
        })?;
        Ok(())
    }

    fn dump_instances_per_fqin(&self, writer: &mut dyn std::io::Write) -> Result<(), Error> {
        writeln!(writer, "\nRequested state for instances:")?;
        writeln!(writer, "{:<15}FQIN", "STATE")?;
        let mut hash_vec: Vec<(Fqin, InstanceState)> =
            self.get_instances_states().into_iter().collect();
        hash_vec.sort_by(|inst_1, inst_2| inst_1.0.cmp(&inst_2.0));
        hash_vec.iter().try_for_each(|(fqin, instance)| -> Result<(), Error> {
            writeln!(writer, "{:<15}{}", format!("{:?}", instance), fqin)?;
            Result::Ok(())
        })?;
        Ok(())
    }
}

/// Dumps information with the following format:
///
/// Active modes:
/// MODE                              VALUE                         TIMESTAMP (scs, ns)
/// Power                             POWER_OFF_EXIT                -
/// Vehicle                           VEHICLE_ON                    -
/// Custom("performance_control")     create                        1743519391 (scs) 999378263 (ns)
///
///
/// Modes allowed to publish by bundle (FQIN: modes):
/// sdv.test.embedded/MergeConfig: charging, tire_pressure
///
/// Requested state for instances:
/// STATE       FQIN
/// Started     com.sdv.google.perf.apex0/ServiceBundle0/instance0
/// Started     com.sdv.google.perf.apex0/ServiceBundle1/instance0
/// Started     com.sdv.google.perf.apex1/ServiceBundle0/instance0
/// Started     com.sdv.google.perf.apex1/ServiceBundle1/instance0
///
impl DumpStateReporter for Evaluator {
    fn dump(
        &self,
        writer: &mut dyn std::io::Write,
        _args: &[&std::ffi::CStr],
    ) -> Result<(), Error> {
        self.dump_active_modes(writer)?;
        self.dump_allowed_custom_modes(writer)?;
        self.dump_instances_per_fqin(writer)?;
        Result::Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use googletest::{assert_that, matchers::eq, matchers::none};
    use named_lock::NamedLock;
    use protobuf::text_format;
    use regex::Regex;
    use sdv_orch_dist_config_proto::service_bundle_config::ServiceBundleConfig;
    use sdv_orch_dist_config_proto::vm_config::VmConfig;
    use sdv_orchestration_common::mode::Mode;
    use std::panic;

    #[test]
    fn test_possible_vm_configs_correspond_to_schema() {
        // GIVEN paths to valid VM config files.
        let config_paths = &[
            "test/data/empty.textproto",
            "test/data/valid/vm1.textproto",
            "test/data/groups/vm_config.textproto",
        ];

        // THEN parsing should succeed.
        let _ = config_paths
            .iter()
            .map(|path| std::fs::read_to_string(path).unwrap())
            .map(|path_content| text_format::parse_from_str::<VmConfig>(&path_content).unwrap())
            .collect::<Vec<VmConfig>>();
    }

    #[test]
    fn test_possible_bundle_configs_correspond_to_schema() {
        // GIVEN paths to valid Bundle config files.
        let config_paths = &[
            "test/data/empty.textproto",
            "test/data/valid/sb1.textproto",
            "test/data/valid/sb2.textproto",
            "test/data/groups/sb_with_group_and_instance_config.textproto",
            "test/data/groups/sb_with_group_config_only.textproto",
        ];

        // THEN parsing should succeed.
        let _ = config_paths
            .iter()
            .map(|path| std::fs::read_to_string(path).unwrap())
            .map(|path_content| {
                text_format::parse_from_str::<ServiceBundleConfig>(&path_content).unwrap()
            })
            .collect::<Vec<ServiceBundleConfig>>();
    }

    #[test]
    fn test_evaluator_usage_blank_vm_config() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN the configured evaluator.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/empty.textproto")),
            &["test/data/valid/sb1.textproto", "test/data/valid/sb2.textproto"],
        );

        test_evaluator_usage_common(&mut evaluator);
    }

    #[test]
    fn test_empty_custom_modes() {
        // GIVEN the configured evaluator.
        let evaluator = Evaluator::new(
            Some(String::from("test/data/empty.textproto")),
            &["test/data/valid/sb1.textproto"],
        );

        // THEN custom modes should be empty
        let custom_modes = evaluator.get_custom_modes();
        assert_that!(custom_modes.len(), eq(0));
    }

    #[test]
    fn test_evaluator_usage_no_vm_config_provided() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN the configured evaluator.
        let mut evaluator = Evaluator::new(
            None,
            &["test/data/valid/sb1.textproto", "test/data/valid/sb2.textproto"],
        );

        test_evaluator_usage_common(&mut evaluator);
    }

    #[test]
    fn test_evaluator_usage_not_existent_vm_config() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN the configured evaluator.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/not-existent.textproto")),
            &["test/data/valid/sb1.textproto", "test/data/valid/sb2.textproto"],
        );

        // THEN if the VM file is not existent, it should still parse the bundles configs.
        test_evaluator_usage_common(&mut evaluator);
    }

    #[test]
    fn test_evaluator_usage_not_existent_bundle_config() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN the configured evaluator.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/embedded/vm_evaluator_usage.textproto")),
            &["test/data/not-existent.textproto"],
        );

        // THEN if the bundle configs does not exist, it should still parse the VM config.
        test_evaluator_usage_common(&mut evaluator);
    }

    #[test]
    fn test_evaluator_usage_not_existent_vm_and_bundle_configs() {
        // GIVEN the configured evaluator with not existent protos.
        let evaluator = Evaluator::new(
            Some(String::from("test/data/not-existent.textproto")),
            &["test/data/not-existent.textproto"],
        );

        // THEN no instances were created.
        let instances_states = evaluator.get_instances_states();
        assert_that!(instances_states.len(), eq(0));
    }

    #[test]
    fn test_evaluator_usage_embedded() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN the configured evaluator.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/embedded/vm_evaluator_usage.textproto")),
            &[],
        );

        test_evaluator_usage_common(&mut evaluator);
    }

    fn test_evaluator_usage_common(evaluator: &mut Evaluator) {
        // THEN evaluator correctly decides that all instances are created.
        let instances_states = evaluator.get_instances_states();
        assert_that!(instances_states.len(), eq(4));
        instances_states.iter().for_each(|(_, state)| {
            assert_that!(state, eq(&InstanceState::Created));
        });

        // WHEN the evaluator is updated to a new state.
        let mode = Mode::Power;
        let state = String::from("ON");
        let instances_states = evaluator.update_orchestration_state(&mode, &state, &None).unwrap();

        // THEN the conditions are correctly evaluated.
        // Two of the instances moved to the started state.
        assert_that!(instances_states.len(), eq(4));
        assert_that!(
            instances_states[&Fqin {
                package_name: "sdv.test".to_string(),
                service_bundle_name: "TestBundle1".to_string(),
                instance_name: "left".to_string(),
            }],
            eq(&InstanceState::Created)
        );
        assert_that!(
            instances_states[&Fqin {
                package_name: "sdv.test".to_string(),
                service_bundle_name: "TestBundle1".to_string(),
                instance_name: "right".to_string(),
            }],
            eq(&InstanceState::Created)
        );
        assert_that!(
            instances_states[&Fqin {
                package_name: "sdv.test".to_string(),
                service_bundle_name: "TestBundle2".to_string(),
                instance_name: "left".to_string(),
            }],
            eq(&InstanceState::Started)
        );
        assert_that!(
            instances_states[&Fqin {
                package_name: "sdv.test".to_string(),
                service_bundle_name: "TestBundle2".to_string(),
                instance_name: "right".to_string(),
            }],
            eq(&InstanceState::Destroyed)
        );
    }

    #[test]
    fn test_bundle_with_group_config_only() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN evaluator is configured with bundle with group config only.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/groups/vm_config.textproto")),
            &["test/data/groups/sb_with_group_config_only.textproto"],
        );
        let left_instance = Fqin::new("sdv.test.group", "GroupConfigOnly", "left");
        let right_instance = Fqin::new("sdv.test.group", "GroupConfigOnly", "right");

        // WHEN the initial states are queried.
        let instances_states = evaluator.get_instances_states();

        // THEN the initial states are empty, because the groups are not configured to run in this state.
        assert_that!(instances_states.len(), eq(0));

        // WHEN we enter system state configured for the group
        // THEN instances are in the correct state.
        let instances_states = evaluator
            .update_orchestration_state(&Mode::Custom("RightTest".to_string()), "Create", &None)
            .unwrap();
        assert_that!(instances_states.len(), eq(1));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Created));

        let instances_states = evaluator
            .update_orchestration_state(&Mode::Custom("LeftTest".to_string()), "Start", &None)
            .unwrap();
        assert_that!(instances_states.len(), eq(2));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Started));

        // WHEN multiple group configs are valid
        // THEN precedence rules apply
        let instances_states = evaluator
            .update_orchestration_state(
                &Mode::Custom("DestroyTest".to_string()),
                "AllInstances",
                &None,
            )
            .unwrap();
        assert_that!(instances_states.len(), eq(2));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Destroyed));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Destroyed));

        let instances_states = evaluator
            .update_orchestration_state(
                &Mode::Custom("DestroyTest".to_string()),
                "AllBundleInstances",
                &None,
            )
            .unwrap();
        assert_that!(instances_states.len(), eq(2));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Destroyed));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Destroyed));
    }

    #[test]
    fn test_bundle_with_group_and_instance_config() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN evaluator is configured with bundle with group config only.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/groups/vm_config.textproto")),
            &["test/data/groups/sb_with_group_and_instance_config.textproto"],
        );
        let left_instance = Fqin::new("sdv.test.group", "MixedConfig", "left");
        let right_instance = Fqin::new("sdv.test.group", "MixedConfig", "right");

        // WHEN the initial states are queried.
        let instances_states = evaluator.get_instances_states();
        // THEN the initial states are corresponding to the bundle instances configs.
        assert_that!(instances_states.len(), eq(2));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Started));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Created));

        // WHEN group config(s) and instance config(s) are valid
        // THEN precedence rules apply
        let instances_states = evaluator
            .update_orchestration_state(&Mode::Custom("RightTest".to_string()), "Create", &None)
            .unwrap();
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Started));

        let instances_states = evaluator
            .update_orchestration_state(&Mode::Custom("LeftTest".to_string()), "Start", &None)
            .unwrap();
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Started));

        let instances_states = evaluator
            .update_orchestration_state(
                &Mode::Custom("DestroyTest".to_string()),
                "AllInstances",
                &None,
            )
            .unwrap();
        assert_that!(instances_states.len(), eq(2));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Destroyed));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Destroyed));
    }

    #[test]
    fn test_cross_bundle_groups_config() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN evaluator is configured with bundle with group config only.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/groups/vm_config.textproto")),
            &[
                "test/data/groups/sb_with_group_and_instance_config.textproto",
                "test/data/groups/sb_with_group_config_only.textproto",
            ],
        );

        test_cross_bundle_groups_config_common(&mut evaluator);
    }

    #[test]
    fn test_cross_bundle_groups_config_embedded() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN evaluator is configured with bundle with group config only.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/embedded/vm_cross_bundle_groups.textproto")),
            &[],
        );

        test_cross_bundle_groups_config_common(&mut evaluator);
    }

    fn test_cross_bundle_groups_config_common(evaluator: &mut Evaluator) {
        let left_instance = Fqin::new("sdv.test.group", "GroupConfigOnly", "left");
        let right_instance = Fqin::new("sdv.test.group", "GroupConfigOnly", "right");
        let left_mixed_instance = Fqin::new("sdv.test.group", "MixedConfig", "left");
        let right_mixed_instance = Fqin::new("sdv.test.group", "MixedConfig", "right");

        // WHEN the initial states are queried.
        let instances_states = evaluator.get_instances_states();
        // THEN the initial states are corresponding to the bundle instances configs.
        assert_that!(instances_states.len(), eq(2));
        assert_that!(instances_states[&right_mixed_instance], eq(&InstanceState::Started));
        assert_that!(instances_states[&left_mixed_instance], eq(&InstanceState::Created));

        // WHEN universal group config is valid
        // THEN it applies to all instances
        let instances_states = evaluator
            .update_orchestration_state(
                &Mode::Custom("DestroyTest".to_string()),
                "AllInstances",
                &None,
            )
            .unwrap();
        assert_that!(instances_states.len(), eq(4));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Destroyed));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Destroyed));
        assert_that!(instances_states[&right_mixed_instance], eq(&InstanceState::Destroyed));
        assert_that!(instances_states[&left_mixed_instance], eq(&InstanceState::Destroyed));

        let instances_states = evaluator
            .update_orchestration_state(
                &Mode::Custom("DestroyTest".to_string()),
                "AllBundleInstances",
                &None,
            )
            .unwrap();
        assert_that!(instances_states.len(), eq(4));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Destroyed));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Destroyed));
        assert_that!(instances_states[&right_mixed_instance], eq(&InstanceState::Started));
        assert_that!(instances_states[&left_mixed_instance], eq(&InstanceState::Created));
    }

    #[test]
    fn test_circular_dependencies() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN evaluator is configured with VM config with circular dependencies.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/groups/vm_circular_dependencies_config.textproto")),
            &["test/data/groups/sb_for_circular_dependencies.textproto"],
        );

        // THEN does not crash

        // WHEN the custom state is changed, groups with circular dependencies are empty.
        let instances_states = evaluator
            .update_orchestration_state(&Mode::Custom("Test".to_string()), "StartA", &None)
            .unwrap();
        assert_that!(instances_states.len(), eq(0));

        let instances_states = evaluator
            .update_orchestration_state(&Mode::Custom("Test".to_string()), "StartB", &None)
            .unwrap();
        assert_that!(instances_states.len(), eq(0));

        let instances_states = evaluator
            .update_orchestration_state(&Mode::Custom("Test".to_string()), "StartC", &None)
            .unwrap();
        assert_that!(instances_states.len(), eq(0));
    }

    #[test]
    fn test_embedded_and_standalone_sb_config() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // GIVEN evaluator is configured with Service Bundle configured both in separate and vm config file.
        // THEN two Service Bundle configurations are effectively merged.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/embedded/vm_merge.textproto")),
            &["test/data/embedded/sb_merge.textproto"],
        );

        let left_instance = Fqin::new("sdv.test.embedded", "MergeConfig", "left");
        let right_instance = Fqin::new("sdv.test.embedded", "MergeConfig", "right");

        // WHEN the initial states are queried.
        let instances_states = evaluator.get_instances_states();
        // THEN the initial states are corresponding to the bundle instances configs.
        assert_that!(instances_states.len(), eq(2));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Started));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Created));

        let bundle_identity = Fqin::new("sdv.test.embedded", "MergeConfig", "");

        // WHEN the allowed custom modes are queried.
        let custom_modes = evaluator.get_custom_modes();
        assert_that!(custom_modes.len(), eq(1));
        let modes_allowed_for_bundle =
            custom_modes.get(&bundle_identity).expect("Custom modes were set for service bundle.");
        // THEN the custom modes are corresponding to the bundle custom mode configs.
        assert_that!(modes_allowed_for_bundle.len(), eq(2));
        assert!(modes_allowed_for_bundle.contains("CHARGING"));
        assert!(modes_allowed_for_bundle.contains("TIRE_PRESSURE"));

        // WHEN restart config is queried.
        let retry_config = evaluator.get_retry_configuration();
        // THEN the corresponding restart configs per bundle are retrieved.
        assert_that!(retry_config.len(), eq(2));
        assert_that!(retry_config[&right_instance].max_retries, eq(3));
        assert_that!(retry_config[&left_instance].max_retries, eq(5));

        // WHEN group config(s) and instance config(s) are valid
        // THEN precedence rules apply
        let instances_states = evaluator
            .update_orchestration_state(&Mode::Custom("RightTest".to_string()), "Create", &None)
            .unwrap();
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Started));

        let instances_states = evaluator
            .update_orchestration_state(&Mode::Custom("LeftTest".to_string()), "Start", &None)
            .unwrap();
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Started));

        let instances_states = evaluator
            .update_orchestration_state(
                &Mode::Custom("DestroyTest".to_string()),
                "AllInstances",
                &None,
            )
            .unwrap();
        assert_that!(instances_states.len(), eq(2));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Destroyed));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Destroyed));
    }

    #[test]
    fn test_config_parser_no_vm_config_provided() {
        let config = Configuration::new_or_fail_on_bad_vm_config(
            None,
            &["test/data/valid/sb1.textproto", "test/data/valid/sb2.textproto"],
        );
        assert_that!(config.is_err(), eq(true));
    }

    #[test]
    fn test_config_parser_partial_retry_config_provided_should_return_default_values() {
        // GIVEN
        let evaluator = Evaluator::new(None, &["test/data/valid/sb1.textproto"]);
        let left_instance = Fqin::new("sdv.test", "TestBundle1", "left");
        let right_instance = Fqin::new("sdv.test", "TestBundle1", "right");
        // WHEN
        let retry_config = evaluator.get_retry_configuration();
        // THEN default values were filled by proto
        assert_that!(retry_config.len(), eq(2));
        assert_that!(retry_config[&right_instance].max_retries, eq(0));
        assert_that!(retry_config[&left_instance].max_retries, eq(0));
    }

    #[test]
    fn test_config_parser_multiple_retry_config_provided_should_return_config_with_highest_retries()
    {
        // GIVEN
        let evaluator = Evaluator::new(None, &["test/data/valid/sb2.textproto"]);
        let left_instance = Fqin::new("sdv.test", "TestBundle2", "left");
        // WHEN
        let retry_config = evaluator.get_retry_configuration();
        // THEN the one with highest value was stored
        assert_that!(retry_config.len(), eq(1));
        assert_that!(retry_config[&left_instance].max_retries, eq(5));
    }

    #[test]
    fn test_invalid_vm_configuration() {
        let config = Configuration::new_or_fail_on_bad_vm_config(
            Some(String::from("test/data/invalid-vm-config.textproto")),
            &["test/data/empty.textproto"],
        );

        assert_that!(config.is_err(), eq(true));
    }

    #[test]
    fn test_config_parser_circular_dependencies() {
        // GIVEN VM config with circular dependencies.
        let config = Configuration::new_or_fail_on_bad_vm_config(
            Some(String::from("test/data/groups/vm_circular_dependencies_config.textproto")),
            &["test/data/groups/sb_for_circular_dependencies.textproto"],
        );

        assert_that!(config.is_err(), eq(true));
    }

    #[test]
    fn test_config_parser_bundle_groups_and_vm_subgroups_config() {
        // GIVEN bundle that contains subgroups defined for same group in different group mappings.
        let config = Configuration::new_or_fail_on_bad_vm_config(
            Some(String::from("test/data/groups/vm_with_groups_subgroups.textproto")),
            &[
                "test/data/groups/sb_with_group_and_instance_config.textproto",
                "test/data/groups/sb_with_group_config_only.textproto",
            ],
        );
        assert_that!(config.is_ok(), eq(true));
    }

    #[test]
    fn test_config_parser_bundle_groups_config() {
        // GIVEN bundle with group config only.
        let config = Configuration::new_or_fail_on_bad_vm_config(
            Some(String::from("test/data/groups/vm_config.textproto")),
            &[
                "test/data/groups/sb_with_group_and_instance_config.textproto",
                "test/data/groups/sb_with_group_config_only.textproto",
            ],
        );
        assert_that!(config.is_ok(), eq(true));
    }

    #[test]
    fn test_bundle_with_group_config_only_check_timestamps() {
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        // same as test_bundle_with_group_config_only test, but with checking for valid/invalid timestamps
        // GIVEN a current, a past and a future timestamp
        let timestamp_init = Some(Timestamp { seconds: 10, nanos: 0, ..Timestamp::new() });
        let timestamp_now = Some(Timestamp { seconds: 10, nanos: 1000, ..Timestamp::new() });
        let timestamp_past = Some(Timestamp { seconds: 8, nanos: 0, ..Timestamp::new() });
        let timestamp_future = Some(Timestamp { seconds: 11, nanos: 0, ..Timestamp::new() });

        // GIVEN evaluator is configured with bundle with group config only.
        let mut evaluator = Evaluator::new(
            Some(String::from("test/data/groups/vm_config.textproto")),
            &["test/data/groups/sb_with_group_config_only.textproto"],
        );
        let left_instance = Fqin::new("sdv.test.group", "GroupConfigOnly", "left");
        let right_instance = Fqin::new("sdv.test.group", "GroupConfigOnly", "right");

        // WHEN the initial states are queried.
        let instances_states = evaluator.get_instances_states();

        // THEN the initial states are empty, because the groups are not configured to run in this state.
        assert_that!(instances_states.len(), eq(0));

        // WHEN we enter system state configured for the group
        // THEN instances are in the correct state, if the timestamps are in incrementing order.
        let instances_states = evaluator
            .update_orchestration_state(
                &Mode::Custom("RightTest".to_string()),
                "Create",
                &timestamp_init,
            )
            .unwrap();
        assert_that!(instances_states.len(), eq(1));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Created));

        // WHEN the timestamp for an update is Some, but is equal than the timestamp of the previous update
        // THEN the update is skipped and the result is None.
        let instances_states = evaluator.update_orchestration_state(
            &Mode::Custom("RightTest".to_string()),
            "Create",
            &timestamp_init,
        );
        assert_that!(instances_states, none());

        // WHEN the timestamp for an update is None, while the previous timestamp is a Some
        // THEN the update is skipped and the result is None.
        let instances_states = evaluator.update_orchestration_state(
            &Mode::Custom("RightTest".to_string()),
            "Create",
            &None,
        );
        assert_that!(instances_states, none());

        // WHEN the timestamp for an update is Some, but is smaller than the timestamp of the previous update
        // THEN the update is skipped and the result is None.
        let instances_states = evaluator.update_orchestration_state(
            &Mode::Custom("RightTest".to_string()),
            "Create",
            &timestamp_past,
        );
        assert_that!(instances_states, none());

        // WHEN the timestamp for an update is Some, and it is larger than the timestamp of the previous same-mode update
        // THEN the update is performed and the result is Some(_)
        let instances_states = evaluator
            .update_orchestration_state(
                &Mode::Custom("RightTest".to_string()),
                "Start",
                &timestamp_now,
            )
            .unwrap();
        assert_that!(instances_states.len(), eq(1));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Started));

        // WHEN the timestamp for an update is Some, and it is larger than the timestamp of the previous update with different mode
        // THEN the update is performed and the result is as Some(_)
        let instances_states = evaluator
            .update_orchestration_state(
                &Mode::Custom("LeftTest".to_string()),
                "Start",
                &timestamp_now,
            )
            .unwrap();
        assert_that!(instances_states.len(), eq(2));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Started));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Started));

        // WHEN multiple group configs are valid
        // THEN precedence rules apply
        let instances_states = evaluator
            .update_orchestration_state(
                &Mode::Custom("DestroyTest".to_string()),
                "AllInstances",
                &timestamp_future,
            )
            .unwrap();
        assert_that!(instances_states.len(), eq(2));
        assert_that!(instances_states[&right_instance], eq(&InstanceState::Destroyed));
        assert_that!(instances_states[&left_instance], eq(&InstanceState::Destroyed));

        let instances_states = evaluator.update_orchestration_state(
            &Mode::Custom("DestroyTest".to_string()),
            "AllBundleInstances",
            &None,
        );
        assert_that!(instances_states, none());
    }

    #[test]
    fn test_dump_modes_are_sorted() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/data/valid/sb2.textproto"]);
        evaluator.update_orchestration_state(&Mode::Custom(String::from("Mode2")), "C", &None);
        evaluator.update_orchestration_state(&Mode::Vehicle, "ON", &None);
        evaluator.update_orchestration_state(&Mode::Custom(String::from("Mode1")), "B", &None);
        evaluator.update_orchestration_state(&Mode::Power, "ON", &None);
        evaluator.update_orchestration_state(&Mode::Custom(String::from("Mode3")), "A", &None);
        let mut buffer = Vec::new();
        // WHEN
        let res = evaluator.dump(&mut buffer, &[]);
        // THEN
        assert!(res.is_ok());
        assert!(Regex::new(
            r#"Power(.|\s)*Vehicle(.|\s)*Custom\("Mode1"\)(.|\s)*Custom\("Mode2"\)(.|\s)*Custom\("Mode3"\)"#
        )
        .unwrap()
        .is_match(&String::from_utf8(buffer).unwrap()));
    }

    #[test]
    fn test_dump_power_mode_state() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/data/valid/sb2.textproto"]);
        evaluator.update_orchestration_state(&Mode::Power, "ON", &None);
        let mut buffer = Vec::new();
        // WHEN
        let res = evaluator.dump(&mut buffer, &[]);
        // THEN
        assert!(res.is_ok());
        assert!(Regex::new(r#"Power(\s)+ON(\s)+-"#)
            .unwrap()
            .is_match(&String::from_utf8(buffer).unwrap()));
    }

    #[test]
    fn test_dump_vehicle_mode_state() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/data/valid/sb2.textproto"]);
        evaluator.update_orchestration_state(&Mode::Vehicle, "PARK", &None);
        let mut buffer = Vec::new();
        // WHEN
        let res = evaluator.dump(&mut buffer, &[]);
        // THEN
        assert!(res.is_ok());
        assert!(Regex::new(r#"Vehicle(\s)+PARK(\s)+-"#)
            .unwrap()
            .is_match(&String::from_utf8(buffer).unwrap()));
    }

    #[test]
    fn test_dump_custom_mode_state() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/data/valid/sb2.textproto"]);
        let timestamp = Some(Timestamp { seconds: 10, nanos: 15, ..Timestamp::new() });
        evaluator.update_orchestration_state(
            &Mode::Custom(String::from("Test")),
            "Val",
            &timestamp,
        );
        let mut buffer = Vec::new();
        // WHEN
        let res = evaluator.dump(&mut buffer, &[]);
        // THEN
        assert!(res.is_ok());
        assert!(Regex::new(r#"Custom\("Test"\)(\s)+Val(\s)+10 \(scs\) 15 \(ns\)"#)
            .unwrap()
            .is_match(&String::from_utf8(buffer).unwrap()));
    }

    #[test]
    fn test_dump_allowed_bundles_for_custom_mode_sorted() {
        // GIVEN
        let evaluator = Evaluator::new(
            None,
            &["test/data/embedded/sb_merge.textproto", "test/data/valid/sb2.textproto"],
        );
        let mut buffer = Vec::new();
        // WHEN
        let res = evaluator.dump(&mut buffer, &[]);
        // THEN
        assert!(res.is_ok());
        assert!(Regex::new(r#"sdv.test/TestBundle2: CHARGING\nsdv.test.embedded.MergeConfig: CHARGING, TIRE_PRESSURE"#)
            .unwrap()
            .is_match(&String::from_utf8(buffer).unwrap()));
    }

    #[test]
    fn test_dump_bundles_instances_in_default_mode() {
        // GIVEN
        let evaluator = Evaluator::new(None, &["test/data/valid/sb2.textproto"]);
        let mut buffer = Vec::new();
        // WHEN
        let res = evaluator.dump(&mut buffer, &[]);
        let buf_str = String::from_utf8(buffer.clone()).unwrap();
        // THEN
        assert!(res.is_ok());
        assert!(Regex::new(r#"Created(\s)+sdv.test/TestBundle2/left"#).unwrap().is_match(&buf_str));
        assert!(Regex::new(r#"Created(\s)+sdv.test/TestBundle2/right"#)
            .unwrap()
            .is_match(&buf_str));
        // We also want to verify that the bundles were sorted
        assert!(Regex::new(r#"left(.|\s)*right"#).unwrap().captures(&buf_str).is_some());
    }

    #[test]
    fn test_dump_bundles_instances_after_mode_set() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/data/valid/sb2.textproto"]);
        evaluator.update_orchestration_state(&Mode::Power, "ON", &None);
        let mut buffer = Vec::new();
        // WHEN
        let res = evaluator.dump(&mut buffer, &[]);
        let buf_str = String::from_utf8(buffer.clone()).unwrap();
        // THEN
        assert!(res.is_ok());
        assert!(Regex::new(r#"Started(\s)+sdv.test/TestBundle2/left"#).unwrap().is_match(&buf_str));
        assert!(Regex::new(r#"Destroyed(\s)+sdv.test/TestBundle2/right"#)
            .unwrap()
            .is_match(&buf_str));
        // We also want to verify that the bundles were sorted
        assert!(Regex::new(r#"left(.|\s)*right"#).unwrap().captures(&buf_str).is_some());
    }

    // Empty dump looks like:
    //
    // Active modes:
    // MODE                          VALUE                         TIMESTAMP (scs, ns)
    //
    // Modes allowed to publish by bundle (FQIN: modes):
    //
    // Requested state for instances:
    // STATE          FQIN
    #[test]
    fn test_dump_empty_state() {
        // GIVEN
        let evaluator = Evaluator::new(None, &["test/data/valid/sb1.textproto"]);
        let mut buffer = Vec::new();
        // WHEN
        let res = evaluator.dump(&mut buffer, &[]);
        let buf_str = String::from_utf8(buffer.clone()).unwrap();
        // THEN
        assert!(res.is_ok());
        assert!(Regex::new(r#"Active modes:\nMODE(\s)+VALUE(\s)+TIMESTAMP \(scs, ns\)\n\nModes allowed to publish by bundle \(FQIN: modes\):\n\nRequested state for instances:\nSTATE(\s)+FQIN"#).unwrap().is_match(&buf_str));
    }

    #[test]
    fn test_should_persist_only_new_custom_modes() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/valid/sb1.textproto"]);
        let timestamp_old = Timestamp { seconds: 100, nanos: 0, ..Default::default() };
        let timestamp_new = Timestamp { seconds: 100, nanos: 100, ..Default::default() };
        // We set some modes in the evaluator to check that they were stored into the back up file
        let _ = evaluator.update_orchestration_state(
            &Mode::Custom("CHARGING".to_string()),
            "NEW",
            &Some(timestamp_new.clone()),
        );
        let _ = evaluator.update_orchestration_state(
            &Mode::Custom("CHARGING".to_string()),
            "OLD",
            &Some(timestamp_old.clone()),
        );
        // Create hashmap we should expect as the result of parsing the file
        let dt = DateTime::<Utc>::from_timestamp(timestamp_new.seconds, timestamp_new.nanos as u32)
            .unwrap();
        let mut expected: OrchestrationState = HashMap::new();
        expected.insert(Mode::Custom("CHARGING".to_string()), ("NEW".to_string(), Some(dt)));
        // WHEN parsing the data from the file
        let modes = std::fs::read_to_string(MODES_STATE_BACKUP_FILE_PATH).unwrap();
        let parse_res = serde_json::from_str(&modes).unwrap();
        // THEN
        assert_eq!(expected, parse_res);
    }

    #[test]
    fn test_should_save_current_state_into_file() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/valid/sb1.textproto"]);
        let timestamp = Timestamp { seconds: 100, nanos: 0, ..Default::default() };
        // We set some modes in the evaluator to check that they were stored into the back up file
        let _ = evaluator.update_orchestration_state(
            &Mode::Custom("CHARGING".to_string()),
            "ON",
            &Some(timestamp.clone()),
        );
        let _ = evaluator.update_orchestration_state(&Mode::Power, "POWER_OFF_EXIT", &None);
        let _ = evaluator.update_orchestration_state(&Mode::Vehicle, "PARK", &None);

        // Create hashmap we should expect as the result of parsing the file
        let dt =
            DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos as u32).unwrap();
        let mut expected: OrchestrationState = HashMap::new();
        expected.insert(Mode::Power, ("POWER_OFF_EXIT".to_string(), None));
        expected.insert(Mode::Custom("CHARGING".to_string()), ("ON".to_string(), Some(dt)));
        expected.insert(Mode::Vehicle, ("PARK".to_string(), None));
        // WHEN parsing the data from the file
        let modes = std::fs::read_to_string(MODES_STATE_BACKUP_FILE_PATH).unwrap();
        let parse_res = serde_json::from_str(&modes).unwrap();
        // THEN
        assert_eq!(expected, parse_res);
        // We verify that the tmp file was removed
        assert!(!fs::exists(MODES_STATE_BACKUP_TMP_FILE_PATH).unwrap());
    }

    #[test]
    fn test_should_restore_modes_from_file() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/valid/sb1.textproto"]);
        let timestamp = Timestamp { seconds: 100, nanos: 0, ..Default::default() };

        // Create hashmap we should expect as the result of parsing the file
        let dt =
            DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos as u32).unwrap();
        let mut expected: OrchestrationState = HashMap::new();
        expected.insert(Mode::Power, ("POWER_OFF_EXIT".to_string(), None));
        expected.insert(Mode::Custom("CHARGING".to_string()), ("ON".to_string(), Some(dt)));
        expected.insert(Mode::Vehicle, ("PARK".to_string(), None));

        let res = fs::write(
            MODES_STATE_BACKUP_FILE_PATH,
            r#"
            {
                "Custom: CHARGING":["ON","1970-01-01T00:01:40Z"],
                "Power":["POWER_OFF_EXIT",null],
                "Vehicle":["PARK",null]
            }
            "#,
        );
        assert!(res.is_ok());
        // WHEN
        evaluator.restore_modes();
        // THEN
        assert_eq!(expected, evaluator.orchestration_state);
    }

    #[test]
    fn test_should_restore_modes_from_tmp_file() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/valid/sb1.textproto"]);
        let timestamp = Timestamp { seconds: 100, nanos: 0, ..Default::default() };

        // Create hashmap we should expect as the result of parsing the file
        let dt =
            DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos as u32).unwrap();
        let mut expected: OrchestrationState = HashMap::new();
        expected.insert(Mode::Power, ("POWER_OFF_EXIT".to_string(), None));
        expected.insert(Mode::Custom("CHARGING".to_string()), ("ON".to_string(), Some(dt)));
        expected.insert(Mode::Vehicle, ("PARK".to_string(), None));

        let res = fs::write(
            MODES_STATE_BACKUP_TMP_FILE_PATH,
            r#"
            {
                "Custom: CHARGING":["ON","1970-01-01T00:01:40Z"],
                "Power":["POWER_OFF_EXIT",null],
                "Vehicle":["PARK",null]
            }
            "#,
        );
        assert!(res.is_ok());
        // WHEN
        evaluator.restore_modes();
        // THEN
        assert_eq!(expected, evaluator.orchestration_state);
        // We verify that the data was written to the persist file and the tmp one was deleted
        assert!(!fs::exists(MODES_STATE_BACKUP_TMP_FILE_PATH).unwrap());
    }

    #[test]
    fn test_should_not_restore_modes_if_default_mode_back_up() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/valid/sb1.textproto"]);

        let res = fs::write(
            MODES_STATE_BACKUP_FILE_PATH,
            r#"
            {
                "Custom: CHARGING":["ON","1970-01-01T00:01:40Z"],
                "Power":["POWER_OFF_EXIT",null],
                "Default":["val",null]
            }
            "#,
        );
        assert!(res.is_ok());
        // WHEN
        evaluator.restore_modes();
        // THEN
        assert!(evaluator.orchestration_state.is_empty());
    }

    #[test]
    fn test_should_not_restore_modes_if_invalid_back_up() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/valid/sb1.textproto"]);

        let res = fs::write(
            MODES_STATE_BACKUP_FILE_PATH,
            r#"
            {
                "Custom: INVALID":["ON","1970-01-01T00:01:
            }
            "#,
        );
        assert!(res.is_ok());
        // WHEN
        evaluator.restore_modes();
        // THEN
        assert!(evaluator.orchestration_state.is_empty());
    }

    #[test]
    fn test_should_not_restore_modes_if_no_back_up() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/valid/sb1.textproto"]);
        let _ = fs::remove_file(MODES_STATE_BACKUP_FILE_PATH);
        // WHEN
        evaluator.restore_modes();
        // THEN
        assert!(evaluator.orchestration_state.is_empty());
    }

    #[test]
    fn test_should_not_restore_modes_if_empty_back_up() {
        // GIVEN
        let lock = NamedLock::create("persist_modes_test_lock").unwrap();
        let _guard = lock.lock().unwrap();
        let mut evaluator = Evaluator::new(None, &["test/valid/sb1.textproto"]);

        let res = fs::write(MODES_STATE_BACKUP_FILE_PATH, "{}");
        assert!(res.is_ok());
        // WHEN
        evaluator.restore_modes();
        // THEN
        assert!(evaluator.orchestration_state.is_empty());
    }

    #[test]
    fn test_should_not_restore_modes_if_invalid_path() {
        // GIVEN
        let mut evaluator = Evaluator::new(None, &["test/valid/sb1.textproto"]);
        // WHEN
        evaluator.try_restore_modes_from_path("invalid/path");
        // THEN
        assert!(evaluator.orchestration_state.is_empty());
    }
}
