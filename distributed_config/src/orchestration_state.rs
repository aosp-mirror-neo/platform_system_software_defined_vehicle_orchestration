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

//! Orchestration state representation.

use chrono::{DateTime, Utc};
use sdv_orchestration_common::mode::Mode;
use std::collections::HashMap;

/// Current orchestration state of the system.
///
/// Maps from mode to the current state and optional timestamp of that mode.
pub type OrchestrationState = HashMap<Mode, (String, Option<DateTime<Utc>>)>;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_hashmap_serialization() {
        // GIVEN
        let mut map: HashMap<Mode, (String, Option<DateTime<Utc>>)> = HashMap::new();
        let dt1 = Utc.with_ymd_and_hms(2025, 7, 20, 8, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2024, 5, 10, 15, 30, 0).unwrap();

        map.insert(
            Mode::Custom("TIRE_PRESSURE".to_string()),
            ("FRONT_LEFT".to_string(), Some(dt1)),
        );
        map.insert(Mode::Power, ("POWER_OFF_EXIT".to_string(), None));
        map.insert(Mode::Custom("CHARGING".to_string()), ("ON".to_string(), Some(dt2)));
        map.insert(Mode::Vehicle, ("PARK".to_string(), None));

        // WHEN
        let serialized_json = serde_json::to_string(&map).unwrap();

        // THEN
        // Due to HashMap iteration order, we cannot directly compare the full JSON string.
        // Instead, we check if the serialized string contains the expected parts for each key-value pair.
        assert!(serialized_json.contains(r#""Power":["POWER_OFF_EXIT",null]"#));
        assert!(serialized_json.contains(r#""Custom: CHARGING":["ON","2024-05-10T15:30:00Z"]"#));
        assert!(serialized_json.contains(r#""Vehicle":["PARK",null]"#));
        assert!(serialized_json
            .contains(r#""Custom: TIRE_PRESSURE":["FRONT_LEFT","2025-07-20T08:00:00Z"]"#));
        assert!(serialized_json.starts_with('{'));
        assert!(serialized_json.ends_with('}'));
    }

    #[test]
    fn test_empty_hashmap_serialization() {
        // GIVEN
        let empty_map: HashMap<Mode, (String, Option<DateTime<Utc>>)> = HashMap::new();
        // WHEN
        let serialized_json = serde_json::to_string(&empty_map).unwrap();
        // THEN
        assert_eq!(serialized_json, "{}");
    }

    #[test]
    fn test_hashmap_deserialization() {
        // GIVEN
        let mut original_map: HashMap<Mode, (String, Option<DateTime<Utc>>)> = HashMap::new();
        let dt1 = Utc.with_ymd_and_hms(2025, 7, 20, 8, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2024, 5, 10, 15, 30, 0).unwrap();
        original_map.insert(
            Mode::Custom("TIRE_PRESSURE".to_string()),
            ("FRONT_LEFT".to_string(), Some(dt1)),
        );
        original_map.insert(Mode::Power, ("POWER_OFF_EXIT".to_string(), None));
        original_map.insert(Mode::Vehicle, ("PARK".to_string(), None));
        original_map.insert(Mode::Custom("CHARGING".to_string()), ("ON".to_string(), Some(dt2)));

        // WHEN
        let deserialized_map: HashMap<Mode, (String, Option<DateTime<Utc>>)> =
            serde_json::from_str(
                r#"
                {
                    "Custom: CHARGING":["ON","2024-05-10T15:30:00Z"],
                    "Power":["POWER_OFF_EXIT",null],
                    "Vehicle":["PARK",null],
                    "Custom: TIRE_PRESSURE":["FRONT_LEFT","2025-07-20T08:00:00Z"]
                }
                "#,
            )
            .unwrap();

        // THEN the deserialized map should be equal to the original map
        assert_eq!(original_map, deserialized_map);
    }

    #[test]
    fn test_empty_hashmap_deserialization() {
        // GIVEN
        let empty_map: HashMap<Mode, (String, Option<DateTime<Utc>>)> = HashMap::new();
        // WHEN
        let deserialized_map: HashMap<Mode, (String, Option<DateTime<Utc>>)> =
            serde_json::from_str("{}").unwrap();
        // THEN
        assert_eq!(empty_map, deserialized_map);
    }
}
