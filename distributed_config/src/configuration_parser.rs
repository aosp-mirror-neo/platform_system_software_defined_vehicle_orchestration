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

//! File loader for Orchestration configuration representation.

use crate::condition::Condition;
use log::{info, warn};
use protobuf::text_format;
use protobuf::Enum;
use protobuf::MessageFull;
use sdv_orch_dist_config_proto::service_bundle_config::{
    InstancesStateConfiguration, ServiceBundleConfig,
};
use sdv_orch_dist_config_proto::vm_config::{GroupsStateConfiguration, VmConfig};
use sdv_orchestration_common::fqin::Fqin;
use sdv_orchestration_common::instance_state::InstanceState;
use sdv_orchestration_common::mode::Mode;
use sdv_orchestration_common::retry_configuration::RetryConfiguration;
use std::collections::{HashMap, HashSet};

use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;
use sdv_status_rs::{SdvResult, SdvStatus, SdvStatusCode};
use sdv_vpm_api::vehicle::VpmVehicleState;
type Result<T> = anyhow::Result<T, ()>;

/// Returns:
///  * a tuple containing as first item the list of conditions and corresponding instances states,
///  * a map that contains for each service bundle, which custom modes they are allowed to publish, and
///  * a mapping between instances to retry configuration
pub(crate) fn parse_configurations_and_log_errors(
    vm_config_path: Option<String>,
    service_bundle_config_paths: &[&str],
) -> (
    Vec<(Condition, HashMap<Fqin, InstanceState>)>,
    HashMap<Fqin, HashSet<String>>,
    HashMap<Fqin, RetryConfiguration>,
) {
    parse_vm_and_service_bundle_configurations(
        vm_config_path,
        service_bundle_config_paths,
        &OperationType::LogOnError,
    )
    .unwrap_or_default()
}

pub(crate) fn parse_configurations_or_fail(
    vm_config_path: Option<String>,
    service_bundle_config_paths: &[&str],
) -> SdvResult<(
    Vec<(Condition, HashMap<Fqin, InstanceState>)>,
    HashMap<Fqin, HashSet<String>>,
    HashMap<Fqin, RetryConfiguration>,
)> {
    let result = parse_vm_and_service_bundle_configurations(
        vm_config_path,
        service_bundle_config_paths,
        &OperationType::FailOnError,
    );
    match result {
        Ok(config) => Ok(config),
        Err(()) => Err(SdvStatus::new(SdvStatusCode::Internal)),
    }
}

#[derive(PartialEq)]
enum OperationType {
    LogOnError,
    FailOnError,
}

fn parse_vm_and_service_bundle_configurations(
    vm_config_path: Option<String>,
    service_bundle_config_paths: &[&str],
    operation_type: &OperationType,
) -> Result<(
    Vec<(Condition, HashMap<Fqin, InstanceState>)>,
    HashMap<Fqin, HashSet<String>>,
    HashMap<Fqin, RetryConfiguration>,
)> {
    let mut state_conditions = vec![];
    let mut groups_instances = HashMap::<String, HashSet<Fqin>>::new();
    let mut custom_modes = HashMap::<Fqin, HashSet<String>>::new();
    let mut retry_configuration = HashMap::<Fqin, RetryConfiguration>::new();

    // Parse service bundle configs:
    // - contributes groups_instances mappings
    // - contributes to state_conditions, when service instance states are configured directly on the service bundle config level.
    parse_bundle_configs(
        service_bundle_config_paths,
        &mut groups_instances,
        &mut state_conditions,
        &mut custom_modes,
        &mut retry_configuration,
    );

    // Parse VM config:
    // - parse embedded bundle configs
    // - based on group_instances and parsed group->subgroups mappings evaluates the flat group->instance mapping
    // - contributes to state_conditions, for the configured group states
    if let Some(vm_path) = vm_config_path {
        parse_vm_config(
            &vm_path,
            &mut groups_instances,
            &mut state_conditions,
            operation_type,
            &mut custom_modes,
            &mut retry_configuration,
        )?;
    }

    let result = state_conditions
        .iter()
        .try_for_each(|(condition, _hashmap)| -> Result<()> { validate_condition_mode(condition) });

    if result.is_err() && *operation_type == OperationType::FailOnError {
        Err(())
    } else {
        Ok((state_conditions, custom_modes, retry_configuration))
    }
}

fn string_matches_enum_vehicle_state(state: &String) -> Result<()> {
    for enum_variant in VpmVehicleState::VALUES {
        if *state == format!("{enum_variant:?}") {
            return Ok(());
        }
    }
    warn!("Unsupported vehicle state: {state}");
    Err(())
}

fn string_matches_enum_power_state(state: &String) -> Result<()> {
    for enum_variant in PowerStateReport::enum_values() {
        // note, the [2..] is needed to skip the "#r" prefix
        if *state == format!("{enum_variant:?}")[2..] {
            return Ok(());
        }
    }
    warn!("Unsupported power state: {state}");
    Err(())
}

fn validate_condition_mode(condition: &Condition) -> Result<()> {
    match condition {
        Condition::State { mode, state } => match mode {
            Mode::Power => string_matches_enum_power_state(state),
            Mode::Vehicle => string_matches_enum_vehicle_state(state),
            Mode::Custom(_) => Ok(()),
        },
        Condition::And { arguments } => validate_condition_modes(arguments),
        Condition::Or { arguments } => validate_condition_modes(arguments),
        Condition::Not { condition } => validate_condition_mode(condition),
        Condition::Empty => Ok(()),
    }
}

fn validate_condition_modes(conditions: &[Condition]) -> Result<()> {
    conditions.iter().try_for_each(|condition| -> Result<()> { validate_condition_mode(condition) })
}

fn parse_vm_config(
    vm_config_path: &str,
    groups_instances: &mut HashMap<String, HashSet<Fqin>>,
    state_conditions: &mut Vec<(Condition, HashMap<Fqin, InstanceState>)>,
    operation_type: &OperationType,
    custom_modes: &mut HashMap<Fqin, HashSet<String>>,
    retry_configuration: &mut HashMap<Fqin, RetryConfiguration>,
) -> Result<()> {
    // This log is read for performance tests. Please do not change or inform Test team if so.
    info!("Started loading vm_config");

    let vm_config = proto_from_file::<VmConfig>(vm_config_path);

    let Some(vm_config) = vm_config else {
        warn!("Failed to parse vm_config from {vm_config_path}.");
        return match operation_type {
            OperationType::FailOnError => Err(()),
            OperationType::LogOnError => Ok(()),
        };
    };

    parse_embedded_bundle_configs(
        &vm_config,
        groups_instances,
        state_conditions,
        custom_modes,
        retry_configuration,
    );
    parse_groups_config(&vm_config, groups_instances, state_conditions, operation_type)?;

    // This log is read for performance tests. Please do not change or inform Test team if so.
    info!("Finished loading vm_config");

    Ok(())
}

fn parse_embedded_bundle_configs(
    vm_config: &VmConfig,
    groups_instances: &mut HashMap<String, HashSet<Fqin>>,
    state_conditions: &mut Vec<(Condition, HashMap<Fqin, InstanceState>)>,
    custom_modes: &mut HashMap<Fqin, HashSet<String>>,
    retry_configuration: &mut HashMap<Fqin, RetryConfiguration>,
) {
    vm_config.service_bundle_config.iter().for_each(|bundle_config| {
        parse_bundle_config(
            bundle_config,
            groups_instances,
            state_conditions,
            custom_modes,
            retry_configuration,
        )
    });
}

fn parse_groups_config(
    vm_config: &VmConfig,
    groups_instances: &HashMap<String, HashSet<Fqin>>,
    state_conditions: &mut Vec<(Condition, HashMap<Fqin, InstanceState>)>,
    operation_type: &OperationType,
) -> Result<()> {
    let mut groups_subgroups = HashMap::<String, HashSet<String>>::new();
    let mut groups_cache = HashMap::<String, GroupCacheEntry>::new();

    // parse group->subgroups mappings
    vm_config.group_mapping.iter().for_each(|group_mapping| {
        for group in &group_mapping.group {
            let subgroups = group_mapping.subgroup.iter().cloned().collect::<HashSet<_>>();

            groups_subgroups
                .entry(group.clone())
                .and_modify(|subgroups| subgroups.extend(subgroups.clone()))
                .or_insert(subgroups);
        }
    });

    // parse group configurations:
    // - parse conditions
    // - flatten involved groups to group->instances mapping
    // - add (condition, instance->state map) to state_conditions
    vm_config.state.iter().try_for_each(|group_state_config| -> Result<()> {
        let condition = Condition::new(&group_state_config.condition);
        let instances_states_map = create_instances_state_map_from_groups(
            group_state_config,
            &groups_subgroups,
            groups_instances,
            &mut groups_cache,
            operation_type,
        )?;

        state_conditions.push((condition, instances_states_map));
        Ok(())
    })?;

    Ok(())
}

fn parse_bundle_configs(
    service_bundle_config_paths: &[&str],
    groups_instances: &mut HashMap<String, HashSet<Fqin>>,
    state_conditions: &mut Vec<(Condition, HashMap<Fqin, InstanceState>)>,
    custom_modes: &mut HashMap<Fqin, HashSet<String>>,
    retry_configuration: &mut HashMap<Fqin, RetryConfiguration>,
) {
    // This log is read for performance tests. Please do not change or inform Test team if so.
    info!("Started loading all service bundles orch configs.");

    // parse individual bundle configs.
    // these can be parsed sequentially, since no cross-references between bundle configs are allowed.
    service_bundle_config_paths
        .iter()
        .map(|path| (path, proto_from_file::<ServiceBundleConfig>(path)))
        .inspect(|(path, config)| {
            if config.is_none() {
                warn!("Failed to parse service bundle config from {path}");
            }
        })
        .filter_map(|(_, config)| config)
        .for_each(|bundle_config| {
            parse_bundle_config(
                &bundle_config,
                groups_instances,
                state_conditions,
                custom_modes,
                retry_configuration,
            )
        });

    // This log is read for performance tests. Please do not change or inform Test team if so.
    info!("Finished loading all service bundles orch configs.");
}

fn parse_bundle_config(
    bundle_config: &ServiceBundleConfig,
    groups_instances: &mut HashMap<String, HashSet<Fqin>>,
    state_conditions: &mut Vec<(Condition, HashMap<Fqin, InstanceState>)>,
    custom_modes: &mut HashMap<Fqin, HashSet<String>>,
    retry_configuration: &mut HashMap<Fqin, RetryConfiguration>,
) {
    // parse declared instances.
    let instances = bundle_config.instance.iter().collect::<HashSet<_>>();

    // parse custom modes
    bundle_config.custom_mode.iter().for_each(|mode| {
        custom_modes
            .entry(Fqin::new(
                &bundle_config.package_name,
                &bundle_config.service_bundle_name,
                // We don't care about instances for custom modes
                "",
            ))
            .and_modify(|entry| {
                entry.insert(mode.to_string());
            })
            .or_insert({
                let mut set = HashSet::new();
                set.insert(mode.to_string());
                set
            });
    });

    // parse retry mapping
    bundle_config.retry_mapping.iter().for_each(|retry_mapping| {
        retry_mapping
            .instance
            .iter()
            .filter_map(|instance| {
                if instances.contains(instance) {
                    Some(create_fqin(instance, bundle_config))
                } else {
                    warn!(
                        "Instance '{}' in retry_mapping for service bundle '{}' is not a declared instance",
                        instance,
                        bundle_config.service_bundle_name
                    );
                    None
                }
            })
            .for_each(|fqin| {
                let new_config =
                    RetryConfiguration::new(retry_mapping.retry_config.max_retries);
                retry_configuration
                    .entry(fqin)
                    .and_modify(|existing_config| {
                        if new_config.max_retries > existing_config.max_retries {
                            *existing_config = new_config;
                        }
                    })
                    .or_insert(new_config);
            });
    });

    // parse group->instance mappings.
    bundle_config.group_mapping.iter().for_each(|group_mapping| {
        let member_instances = group_mapping
            .instance
            .iter()
            .filter_map(|instance| {
                if instances.contains(instance) {
                    Some(create_fqin(instance, bundle_config))
                } else {
                    warn!(
                        "Instance '{}' in group_mapping for service bundle '{}' is not a declared instance.",
                        instance,
                        bundle_config.service_bundle_name
                    );
                    None
                }
            })
            .collect::<HashSet<Fqin>>();

        for group in &group_mapping.group {
            groups_instances
                .entry(group.clone())
                .and_modify(|members| members.extend(member_instances.clone()))
                .or_insert(member_instances.clone());
        }
    });

    // parse instances states configurations and add to state_conditions.
    bundle_config.state.iter().for_each(|state_config| {
        let condition = Condition::new(&state_config.condition);
        let instances_states_map =
            create_instances_state_map(state_config, &instances, bundle_config);

        state_conditions.push((condition, instances_states_map));
    });
}

/// Creates instances states map (FQIN -> InstanceState) from groups states configuration.
fn create_instances_state_map_from_groups(
    groups_state_config: &GroupsStateConfiguration,
    groups_subgroups: &HashMap<String, HashSet<String>>,
    groups_instances: &HashMap<String, HashSet<Fqin>>,
    groups_cache: &mut HashMap<String, GroupCacheEntry>,
    operation_type: &OperationType,
) -> Result<HashMap<Fqin, InstanceState>> {
    let mut instances_states_map = HashMap::new();
    let groups_states = &groups_state_config.groups_states;

    for (groups, state) in [
        (&groups_states.created, InstanceState::Created),
        (&groups_states.started, InstanceState::Started),
        (&groups_states.destroyed, InstanceState::Destroyed),
    ] {
        let fqins = groups
            .iter()
            .map(|group| {
                get_group_instance_members(group, groups_subgroups, groups_instances, groups_cache)
            })
            .try_fold(HashSet::new(), |all_instances, group_instances_result| {
                group_instances_result
                    .ok()
                    .map(|group_instances| all_instances.union(&group_instances).cloned().collect())
            });

        if fqins.is_none() {
            warn!("We've detected a circular dependency");
            if *operation_type == OperationType::FailOnError {
                return Err(());
            }
        }

        let fqins = fqins.unwrap_or(HashSet::new());

        add_state_for_fqins(state, &mut instances_states_map, fqins);
    }

    Ok(instances_states_map)
}

/// Flattens group from group->instances & group->subgroups mappings to a flat set of FQINs.
fn compute_group_instance_members(
    group: &str,
    groups_subgroups: &HashMap<String, HashSet<String>>,
    groups_instances: &HashMap<String, HashSet<Fqin>>,
    groups_cache: &mut HashMap<String, GroupCacheEntry>,
) -> Result<HashSet<Fqin>> {
    let result: Result<HashSet<Fqin>> = groups_subgroups
        .get(group)
        .cloned()
        .into_iter()
        .flatten()
        .map(|subgroup| {
            get_group_instance_members(&subgroup, groups_subgroups, groups_instances, groups_cache)
        })
        .try_fold(HashSet::new(), |all_members, subgroup_members_result| {
            subgroup_members_result
                .ok()
                .map(|subgroup_members| all_members.union(&subgroup_members).cloned().collect())
        })
        .ok_or(());

    result.map(|all_members| {
        all_members.union(groups_instances.get(group).unwrap_or(&HashSet::new())).cloned().collect()
    })
}

fn get_group_instance_members(
    group: &str,
    group_subgroups: &HashMap<String, HashSet<String>>,
    group_instances: &HashMap<String, HashSet<Fqin>>,
    groups_cache: &mut HashMap<String, GroupCacheEntry>,
) -> Result<HashSet<Fqin>> {
    let result = match groups_cache.get(group) {
        Some(GroupCacheEntry::Computing) => {
            // We've detected a circular dependency.
            Err(())
        }
        Some(GroupCacheEntry::Present(members)) => members.clone(),
        _ => {
            groups_cache.insert(group.to_string(), GroupCacheEntry::Computing);
            let members = compute_group_instance_members(
                group,
                group_subgroups,
                group_instances,
                groups_cache,
            );
            groups_cache.insert(group.to_string(), GroupCacheEntry::Present(members.clone()));
            members
        }
    };

    result
}

/// Converts [InstancesStateConfiguration] to [HashMap<Fqin, InstanceState>] and:
/// - removes duplicates
/// - applies instance state precedence rules
fn create_instances_state_map(
    state_config: &InstancesStateConfiguration,
    valid_instances: &HashSet<&String>,
    bundle_config: &ServiceBundleConfig,
) -> HashMap<Fqin, InstanceState> {
    let mut instances_states_map = HashMap::new();

    for (instances, state) in [
        (&state_config.instances_states.created, InstanceState::Created),
        (&state_config.instances_states.started, InstanceState::Started),
        (&state_config.instances_states.destroyed, InstanceState::Destroyed),
    ] {
        add_state_for_instances(
            state,
            &mut instances_states_map,
            instances,
            bundle_config,
            valid_instances,
        );
    }

    instances_states_map
}

fn add_state_for_instances(
    state: InstanceState,
    instances_states_map: &mut HashMap<Fqin, InstanceState>,
    instances: &[String],
    bundle_config: &ServiceBundleConfig,
    valid_instances: &HashSet<&String>,
) {
    add_state_for_fqins(
        state,
        instances_states_map,
        instances
            .iter()
            .filter(|instance| valid_instances.contains(instance))
            .map(|instance| create_fqin(instance, bundle_config))
            .collect::<HashSet<Fqin>>(),
    );
}

fn add_state_for_fqins(
    state: InstanceState,
    instances_states_map: &mut HashMap<Fqin, InstanceState>,
    fqins: HashSet<Fqin>,
) {
    fqins.iter().for_each(|fqin| {
        instances_states_map
            .entry(fqin.clone())
            .and_modify(|previous_state| *previous_state = previous_state.merge_with(&state))
            .or_insert(state.clone());
    });
}

fn create_fqin(instance: &str, bundle_config: &ServiceBundleConfig) -> Fqin {
    Fqin::new(&bundle_config.package_name, &bundle_config.service_bundle_name, instance)
}

fn proto_from_file<T: MessageFull>(path: &str) -> Option<T> {
    let path_contents = Option::Some(path).and_then(|path| std::fs::read_to_string(path).ok());

    if path_contents.is_none() {
        warn!("Failed to read orchestration configuration file: {path}");
    }

    let proto = path_contents.and_then(|content| text_format::parse_from_str::<T>(&content).ok());

    if proto.is_none() {
        warn!("Failed to parse orchestration configuration file: {path}");
    } else {
        info!("Successfully parsed orchestration configuration file: {path}");
    }

    proto
}
enum GroupCacheEntry {
    /// Group is being computed.
    ///
    /// Used to detect circular group dependencies.
    Computing,
    /// Group is in the cache.
    Present(Result<HashSet<Fqin>>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_matches_power_mode_to_string() {
        assert!(string_matches_enum_power_state(&"POWER_OFF_EXIT".to_string()).is_ok());
    }

    #[test]
    fn test_undefined_power_mode_to_string_should_return_error() {
        assert!(string_matches_enum_power_state(&"Undefined".to_string()).is_err());
    }

    #[test]
    fn test_string_matches_vehicle_mode() {
        assert!(string_matches_enum_vehicle_state(&"PARK".to_string()).is_ok());
    }

    #[test]
    fn test_undefined_vehicle_mode_to_string_should_return_error() {
        assert!(string_matches_enum_vehicle_state(&"Undefined".to_string()).is_err());
    }

    #[test]
    fn test_validate_power_mode_should_return_ok() {
        assert!(validate_condition_mode(&Condition::State {
            mode: Mode::Power,
            state: "POWER_OFF_EXIT".to_string()
        })
        .is_ok());
    }

    #[test]
    fn test_validate_vehicle_mode_should_return_ok() {
        assert!(validate_condition_mode(&Condition::State {
            mode: Mode::Vehicle,
            state: "PARK".to_string()
        })
        .is_ok());
    }

    #[test]
    fn test_validate_custom_mode_should_return_ok() {
        assert!(validate_condition_mode(&Condition::State {
            mode: Mode::Custom("mode".to_string()),
            state: "val".to_string()
        })
        .is_ok());
    }

    #[test]
    fn test_validate_modes_with_and_condition_should_return_ok() {
        assert!(validate_condition_mode(&Condition::And {
            arguments: vec![
                Condition::State { mode: Mode::Power, state: "ON".to_string() },
                Condition::State { mode: Mode::Vehicle, state: "LIFE_ON_BOARD".to_string() }
            ]
        })
        .is_ok());
    }

    #[test]
    fn test_validate_modes_with_or_condition_should_return_ok() {
        assert!(validate_condition_mode(&Condition::Or {
            arguments: vec![
                Condition::State { mode: Mode::Power, state: "ON".to_string() },
                Condition::State { mode: Mode::Vehicle, state: "LIFE_ON_BOARD".to_string() }
            ]
        })
        .is_ok());
    }

    #[test]
    fn test_validate_modes_with_not_condition_should_return_ok() {
        assert!(validate_condition_mode(&Condition::Not {
            condition: Box::new(Condition::State {
                mode: Mode::Custom("occupancy".to_string()),
                state: "OCCUPANCY_EMPTY".to_string()
            })
        })
        .is_ok());
    }

    #[test]
    fn test_validate_modes_with_empty_condition_should_return_ok() {
        assert!(validate_condition_mode(&Condition::Empty).is_ok());
    }
}
