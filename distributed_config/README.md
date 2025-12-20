<!--
  Copyright (C) 2026 The Android Open Source Project

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Orchestration Configuration

The SDV orchestration configuration provides a mechanism for controlling service bundles startup and shutdown behavior. The SDV orchestration configuration defines a set of rules that determine when and how actions are performed on service bundle instances. These rules are based on vehicle, power and custom modes.

For further information about the orchestrator, please visit the [Orchestrator at Partner's Site Documentation](https://docs.partner.android.com/automotive/sdv/core-areas/orchestrator).

## File Format

Orchestration Configuration should be defined in `.textproto` (i.e. structured text)
format, so that new configurations can be loaded at runtime.

The orchestration configuration can be defined in a single VM-level `.textproto` file, which is parsed at the start of the Orchestration Agent.

Note: The configuration syntax for the VM-level file is defined in [vm_config.proto](src/protos/vm_config.proto).

Each service bundle can also provide its own orchestrator configuration by including it inside its apex (which is loaded via Service Bundle Registry). Please refer to [Orchestration metadata in SBR at the Partner's Site Documentation](https://docs.partner.android.com/automotive/sdv/workstreams/core/service-bundle-metadata#orchestration-metadata) for further information.

## VM Configuration Syntax

### Service Bundles

Orchestrator configurations can be defined as part of the VM config or inside the corresponding service bundle's APEXes.

The configuration syntax for the Service Bundle config is defined in [service_bundle_config.proto](src/protos/service_bundle_config.proto).

#### Required Bundle Configuration

At a minimum, bundle configuration provides the following attributes:

```textproto
service_bundle_config {
    package_name: "package.name"
    service_bundle_name: "ServiceBundleName"

    instance: "instance #1"
    #...
    instance: "instance #N"
}
```

This declaration defines `N` *service bundle instances* with the following FQINs: `[current_vm_name].[package_name].[service_bundle_name].[instance]`.

Note that VM name is not declared in the configuration explicitly.
Since configuration is defined per VM, VM name is always the name of the VM to which the configuration file is deployed and it is already known to the Orchestration Agent.

Glossary: *service bundle instance* - a runtime instance of a service bundle library, which maps to a Linux
process in the Lifecycle Manager agent.

#### Instances Configuration

Just declaring service bundle instances has no effect.

To be executed by the Orchestrator, instances must be configured, i.e. Orchestrator should know under which conditions / state of the VM/vehicle an instance should be running.

To configure instances, configuration states (`states`) should be defined which consist of two parts:

- `condition`: an expression on the VM/vehicle state that should be evaluated,
- `instances_states`: a set of states per instances that should be applied, if `condition` evaluates to `true`.

```textproto
service_bundle_config {
    package_name: "package.name"
    service_bundle_name: "ServiceBundleName"

    instance: "instance #1"
    #...
    instance: "instance #N"

    state {
        condition {
            power_state: "ON"
        }

        instances_states {
            started: "instance #1"
            destroyed: "instance #2"
        }
    }
}
```

Syntax for configuration states is described in detail in one of the following sections.

#### Instance to Group Mapping

Another way of configuring service instances is to add them to service groups.

At a service bundle config level, instances can be added to groups. The groups are then can be configured at a VM config level.

Syntax for configuration of groups and a VM level is described in detail in one of the following sections.

```textproto
service_bundle_config {
    package_name: "package.name"
    service_bundle_name: "ServiceBundleName"

    instance: "instance #1"
    #...
    instance: "instance #N"

    # Declare that group #1 contains instances #1 and #2, and group #2 contains instances #1 and #2.
    group_mapping {
        group: "group #1"
        group: "group #2"
        instance: "instance #1"
        instance: "instance #2"
    }
}
```

### VM-level config

Apart from individual service bundles configurations, service instances groups can be configured at a VM level.

Note: groups are declared implicitly, by simply using the group name in any of the configuration parts (e.g. instance to group mapping, group to group mapping, group configuration states).

#### Group to Group Mapping

Groups can contain other groups. By declaring the group #1 contains group #2, we effectively add all service instances from group #2 to group #1.

```textproto
# Declare that group #1 contains subgroups #1 and #2, and group #2 contains subgroups #1 and #2.
group_mapping {
    group: "group #1"
    group: "group #2"
    subgroup: "subgroup #1"
    subgroup: "subgroup #2"
}
```

#### Group Configuration

Just declaring a group has no effect.

To be executed by the Orchestrator, groups must be configured - i.e. Orchestrator should know under which conditions / state of the VVM/vehicle, groups should be running.

Groups can be configured in a similar way to service instances: by using Configuration States.
The only difference is a use of `groups_states` instead of `instances_states` in the syntax.

Syntax for configuration states is described in detail in one of the following sections.

```textproto
state {
    condition {
        power_state: "ON"
    }

    groups_states {
        started: "group #1"
        destroyed: "group #2"
    }
}
```

### Configuration States

Configuration States (`states`) define when a Service Instance/ Group is started or
stopped. Configuration States consist of `condition` and `instances_states` (bundle config) / `groups_states` (VM config).

#### Conditions

- `condition`
  - Is an arbitrarily complex boolean expression (`and`, `or`, `not`), based
        on supported vehicle signals.
  - Is optional. A Config State *without a `condition` is always active*.

#### Instances states / Group states

- `instances_states` / `groups_states`
  - Dictate the states that the Orchestration Agent needs to apply, given the Config State
        is *active*.
  - While applying state to the group apply the state to each instance in the group.
Currently supported states include
  - `started`: after `Service::on_start` is called
  - `created`
    - after `Service::new`, but before `Service::on_start` is called, OR
    - after `Service::on_stop` is called, but before `Service::drop` is called
  - `destroyed` (after `Service::drop` is called)

#### Ruleset

- A Configuration State can be either *active* or *inactive*, depending on the
    `condition`.
- Multiple states can be active at any time.
- When Orchestration Agent receives a vehicle signal update, **all**
    Configuration States are evaluated before starting or stopping Service
    Instances.
- A Service Instance states are evaluated with the following rules:
  - If *none of the active* states applies to the service instance, it will be `destroyed`.
  - If *one or more active* states apply, the following precedence apply:
    - `destroyed` has absolute precedence.
    - `started` has precedence before `created`.

#### Samples

***Please Note***: The following custom state signals are fictional.

Test state 0:

- Has no `condition`, hence it's **always** *active*.
- *Starts* a single service instance.

```textproto
# Test state 0: Base Services
states {
  # Note: This state has no condition, hence its actions are valid throughout the lifetime of the program
  instances_states { started: "SomeBaseService" }
}
```

Test state 1:

- Is active if `custom_state != occupancy.OCCUPANCY_EMPTY || custom_state ==
    preheat.PREHEAT_ON`.
- Declares multiple HVAC-related Service Instances as *started*.

```textproto
# Test state 1: HVAC Application
states {
  condition {
    or {
      # I.e. when the vehicle is occupied (e.g. by _DRIVER / _NON_DRIVER / _PET)
      not {
        custom_state {
          mode: "occupancy"
          state: "OCCUPANCY_EMPTY"
        }
      }
      custom_state {
        mode: "preheat"
        state: "PREHEAT_ON"
      }
    }
  }

  # HVAC-related services
  instances_states {
    started: "HvacTemperatureCommand"
    started: "TempSensorDriverZone"
    started: "TempSensorPassengerZone"
    started: "RefrigerantLoop"
  }
}
```

Test state 2:

- Is active if `custom_state == system_power.SYSTEM_POWER_LOW &&
    custom_state == range_ext.RANGE_EXT_ON`. I.e. this state could be viewed as
    a 'Power Savings' state.
- Declares one of the HVAC-related Service Instances as *destroyed*.
- In this example, if both 'Test State 1' and 'Test State 2' are active, HVAC
    application would run without the `RefrigerantLoop` Service Instance.

```textproto
# Test state 2: Power Saving
states {
  condition {
    and {
      custom_state {
        mode: "system_power"
        state: "SYSTEM_POWER_LOW"
      }
      custom_state {
        mode: "range_ext"
        state: "RANGE_EXT_ON"
      }
    }
  }

  # Disable services with high power consumption
  instances_states { destroyed: "RefrigerantLoop" }
}

```

Test state 3:

- Is active if `power_state == ON && vehicle_state == LIFE_ON_BOARD`. I.e.
    this state could be viewed as a 'Someone is in the car and the car is
    powered on`
- Declares temperature sensors as running.
- Whenever someone is in the car, we monitor the temperature, e.g. for safety
    reasons.

```textproto
# Test state 3: Someone in the car
states {
  condition {
    and {
      power_state: "ON"
      vehicle_state: "LIFE_ON_BOARD"
    }
  }

  # Temperature monitoring services
  instances_states {
    started: "TempSensorDriverZone"
    started: "TempSensorPassengerZone"
  }
}

```
