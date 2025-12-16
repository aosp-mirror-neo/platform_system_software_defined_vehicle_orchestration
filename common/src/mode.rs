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

//! Orchestrator modes definition.

use serde::de::{Deserializer, Error, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use std::fmt;

/// Message for the default update event.
pub const NO_CONDITIONS_BUNDLES: &str = "Requesting state for bundles with no conditions";
/// Message for the recovery update event.
pub const RECOVER_BUNDLES_AFTER_LM_CRASH: &str = "Recovering bundles state after LM crash";

// Ord is used for sorting when doing a dumpsys.
#[derive(Eq, Hash, PartialEq, Debug, Clone, PartialOrd, Ord)]
/// Represents supported state modes that require an update to the current configuration and evaluation.
pub enum Mode {
    /// VPM power mode.
    Power,
    /// VPM vehicle mode.
    Vehicle,
    /// Custom modes.
    Custom(String),
}

#[derive(Eq, Hash, PartialEq, Debug, Clone)]
/// Represents different events that do not update the current configuration, but only request the evaluation.
pub enum UpdateEvent {
    /// Triggers the update of the default state when orch starts (transitioning modes with no conditions attached).
    Default,
    /// Triggers a recovery process in which current configuration is evaluated and modes are enforced
    /// without having received a new mode update.
    Recovery,
    /// Triggers a Mode update with the value of the new mode defined by the given string.
    StateChange(Mode, String),
}

impl fmt::Display for UpdateEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // {} prints NO_CONDITIONS_BUNDLES, whereas {:?} prints "Default"
            UpdateEvent::Default => write!(f, "{}", NO_CONDITIONS_BUNDLES),
            // {} prints RECOVER_BUNDLES_AFTER_LM_CRASH, whereas {:?} prints "Recovery"
            UpdateEvent::Recovery => write!(f, "{}", RECOVER_BUNDLES_AFTER_LM_CRASH),
            UpdateEvent::StateChange(mode, state) => {
                write!(f, "{mode:?}: {state:?}")
            }
        }
    }
}

// We create custom serializer/deserializer for the Mode enum as serde does not support non-string keys for hashmap.
// This means that we cannot derive the Serialize, Deserialize from serde for the Mode as the Custom(String) is
// serialized by serde as a JSON object and not a string. This makes the serialization of the HashMap of the orchestrator state
// to fail at runtime with the error saying that the key of the map is not a string.
impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Mode::Power => write!(f, "Power"),
            Mode::Vehicle => write!(f, "Vehicle"),
            // Example: "Custom: my_custom_value"
            Mode::Custom(s) => write!(f, "Custom: {s}"),
        }
    }
}

impl Serialize for Mode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Mode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ModeVisitor)
    }
}

struct ModeVisitor;

impl Visitor<'_> for ModeVisitor {
    type Value = Mode;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string representing a Mode enum variant")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match value {
            "Power" => Ok(Mode::Power),
            "Vehicle" => Ok(Mode::Vehicle),
            s if s.starts_with("Custom: ") => {
                // Extract "some_value" from "Custom: some_value"
                let inner_str = &s[8..s.len()];
                Ok(Mode::Custom(inner_str.to_string()))
            }
            _ => Err(E::custom(format!("unrecognized Mode variant: {value}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_test::{assert_de_tokens, assert_de_tokens_error, assert_ser_tokens, Token};

    #[test]
    fn test_valid_modes_serialization() {
        assert_ser_tokens(&Mode::Power, &[Token::Str("Power")]);
        assert_ser_tokens(&Mode::Vehicle, &[Token::Str("Vehicle")]);
        assert_ser_tokens(
            &Mode::Custom("custom_value".to_string()),
            &[Token::Str("Custom: custom_value")],
        );
    }

    #[test]
    fn test_valid_modes_deserialization() {
        assert_de_tokens(&Mode::Power, &[Token::Str("Power")]);
        assert_de_tokens(&Mode::Vehicle, &[Token::Str("Vehicle")]);
        assert_de_tokens(&Mode::Custom("my_value".to_string()), &[Token::Str("Custom: my_value")]);
    }

    #[test]
    fn test_unknown_mode_deserialization_should_fail() {
        assert_de_tokens_error::<Mode>(
            &[Token::Str("UnknownMode")],
            "unrecognized Mode variant: UnknownMode",
        );
    }

    #[test]
    fn test_empty_deserialization_should_fail() {
        assert_de_tokens_error::<Mode>(
            &[Token::Unit],
            "invalid type: unit value, expected a string representing a Mode enum variant",
        );
    }

    #[test]
    fn test_invalid_type_deserialization_should_fail() {
        assert_de_tokens_error::<Mode>(
            &[Token::U32(123)],
            "invalid type: integer `123`, expected a string representing a Mode enum variant",
        );
    }

    #[test]
    fn test_invalid_custom_modes_deserialization_should_fail() {
        assert_de_tokens_error::<Mode>(
            &[Token::Str("Custom")],
            "unrecognized Mode variant: Custom",
        );
        assert_de_tokens_error::<Mode>(
            &[Token::Str("Custom:missing_space")],
            "unrecognized Mode variant: Custom:missing_space",
        );
    }

    // We have this test to avoid changing these formats as they are used in unit and e2e testes.
    #[test]
    fn test_update_event_display() {
        assert_eq!(format!("{}", UpdateEvent::Default), NO_CONDITIONS_BUNDLES);
        assert_eq!(format!("{}", UpdateEvent::Recovery), RECOVER_BUNDLES_AFTER_LM_CRASH);
        assert_eq!(format!("{:?}", UpdateEvent::Default), "Default");
        assert_eq!(format!("{:?}", UpdateEvent::Recovery), "Recovery");
        assert_eq!(
            format!("{}", UpdateEvent::StateChange(Mode::Power, "ON".to_string())),
            "Power: \"ON\""
        );
        assert_eq!(
            format!("{}", UpdateEvent::StateChange(Mode::Vehicle, "PARK".to_string())),
            "Vehicle: \"PARK\""
        );
        assert_eq!(
            format!(
                "{}",
                UpdateEvent::StateChange(Mode::Custom("CHARGING".to_string()), "IDLE".to_string())
            ),
            "Custom(\"CHARGING\"): \"IDLE\""
        );
    }
}
