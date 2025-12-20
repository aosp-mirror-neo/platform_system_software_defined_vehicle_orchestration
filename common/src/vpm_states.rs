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

//! Utility functions related to VPM states.

use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;
use log::error;
use sdv_vpm_api::vehicle::VpmVehicleState;

/// Converts a PowerStateReport to a string.
pub fn power_state_to_string(power_state_report: PowerStateReport) -> Option<String> {
    match power_state_report {
        PowerStateReport::POWER_OFF_EXIT => Some("POWER_OFF_EXIT".to_string()),
        PowerStateReport::SUSPEND_TO_RAM_EXIT => Some("SUSPEND_TO_RAM_EXIT".to_string()),
        PowerStateReport::SUSPEND_TO_DISK_EXIT => Some("SUSPEND_TO_DISK_EXIT".to_string()),
        PowerStateReport::ON => Some("ON".to_string()),
        PowerStateReport::SHUTDOWN_CANCELLED => Some("SHUTDOWN_CANCELLED".to_string()),
        PowerStateReport::POWER_OFF_ENTER => Some("POWER_OFF_ENTER".to_string()),
        PowerStateReport::SUSPEND_TO_RAM_ENTER => Some("SUSPEND_TO_RAM_ENTER".to_string()),
        PowerStateReport::SUSPEND_TO_DISK_ENTER => Some("SUSPEND_TO_DISK_ENTER".to_string()),
        PowerStateReport::POWER_OFF_POST_FINISH => Some("POWER_OFF_POST_FINISH".to_string()),
        PowerStateReport::SUSPEND_TO_RAM_POST_FINISH => {
            Some("SUSPEND_TO_RAM_POST_FINISH".to_string())
        }
        PowerStateReport::SUSPEND_TO_DISK_POST_FINISH => {
            Some("SUSPEND_TO_DISK_POST_FINISH".to_string())
        }
        PowerStateReport::WAIT_FOR_FINISH => Some("WAIT_FOR_FINISH".to_string()),
        _ => {
            error!("This conversion should never have been called!");
            None
        }
    }
}

/// Converts a VpmVehicleState to a string.
pub fn vehicle_state_to_string(value: VpmVehicleState) -> Option<String> {
    match value {
        VpmVehicleState::LOW_POWER => Some(String::from("LOW_POWER")),
        VpmVehicleState::SOFTWARE_UPDATE => Some(String::from("SOFTWARE_UPDATE")),
        VpmVehicleState::PARK => Some(String::from("PARK")),
        VpmVehicleState::LIFE_ON_BOARD => Some(String::from("LIFE_ON_BOARD")),
        VpmVehicleState::VEHICLE_ON => Some(String::from("VEHICLE_ON")),
        VpmVehicleState::TRACTION_ON => Some(String::from("TRACTION_ON")),
        VpmVehicleState::VEHICLE_STATE_UNSPECIFIED => {
            error!("This conversion should never have been called!");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_power_mode_to_string_conversion() {
        assert!(
            power_state_to_string(PowerStateReport::POWER_OFF_EXIT).unwrap() == "POWER_OFF_EXIT"
        );
        assert!(
            power_state_to_string(PowerStateReport::SUSPEND_TO_RAM_EXIT).unwrap()
                == "SUSPEND_TO_RAM_EXIT"
        );
        assert!(
            power_state_to_string(PowerStateReport::SUSPEND_TO_DISK_EXIT).unwrap()
                == "SUSPEND_TO_DISK_EXIT"
        );
        assert!(power_state_to_string(PowerStateReport::ON).unwrap() == "ON");
        assert!(
            power_state_to_string(PowerStateReport::SHUTDOWN_CANCELLED).unwrap()
                == "SHUTDOWN_CANCELLED"
        );
        assert!(
            power_state_to_string(PowerStateReport::POWER_OFF_ENTER).unwrap() == "POWER_OFF_ENTER"
        );
        assert!(
            power_state_to_string(PowerStateReport::SUSPEND_TO_RAM_ENTER).unwrap()
                == "SUSPEND_TO_RAM_ENTER"
        );
        assert!(
            power_state_to_string(PowerStateReport::SUSPEND_TO_DISK_ENTER).unwrap()
                == "SUSPEND_TO_DISK_ENTER"
        );
        assert!(
            power_state_to_string(PowerStateReport::POWER_OFF_POST_FINISH).unwrap()
                == "POWER_OFF_POST_FINISH"
        );
        assert!(
            power_state_to_string(PowerStateReport::SUSPEND_TO_RAM_POST_FINISH).unwrap()
                == "SUSPEND_TO_RAM_POST_FINISH"
        );
        assert!(
            power_state_to_string(PowerStateReport::SUSPEND_TO_DISK_POST_FINISH).unwrap()
                == "SUSPEND_TO_DISK_POST_FINISH"
        );
        assert!(
            power_state_to_string(PowerStateReport::WAIT_FOR_FINISH).unwrap() == "WAIT_FOR_FINISH"
        );
        assert!(power_state_to_string(PowerStateReport::REPORT_UNSPECIFIED).is_none());
    }

    #[test]
    fn test_vehicle_mode_to_string_conversion() {
        assert!(vehicle_state_to_string(VpmVehicleState::LOW_POWER).unwrap() == "LOW_POWER");
        assert!(
            vehicle_state_to_string(VpmVehicleState::SOFTWARE_UPDATE).unwrap() == "SOFTWARE_UPDATE"
        );
        assert!(vehicle_state_to_string(VpmVehicleState::PARK).unwrap() == "PARK");
        assert!(
            vehicle_state_to_string(VpmVehicleState::LIFE_ON_BOARD).unwrap() == "LIFE_ON_BOARD"
        );
        assert!(vehicle_state_to_string(VpmVehicleState::VEHICLE_ON).unwrap() == "VEHICLE_ON");
        assert!(vehicle_state_to_string(VpmVehicleState::TRACTION_ON).unwrap() == "TRACTION_ON");
        assert!(vehicle_state_to_string(VpmVehicleState::VEHICLE_STATE_UNSPECIFIED).is_none());
    }
}
