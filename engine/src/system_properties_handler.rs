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

//! Handles the interaction with system properties.

use log::{error, info};
use rustutils::android::system_properties;

pub struct SystemProperties;

impl SystemProperties {
    /// Reads a system property with the given name and parses it to the specified type `T`.
    /// If reading or parsing fails, it returns the provided `fallback_value`.
    pub fn read_property<T: std::str::FromStr + Clone>(
        property_name: &str,
        fallback_value: T,
    ) -> T {
        Self::read_property_impl(system_properties::read, property_name, fallback_value)
    }

    fn read_property_impl<T: std::str::FromStr + Clone>(
        read: impl Fn(&str) -> system_properties::Result<Option<String>>,
        property_name: &str,
        fallback_value: T,
    ) -> T {
        match read(property_name) {
            Ok(Some(prop_str)) => match prop_str.parse::<T>() {
                Ok(value) => value,
                Err(_) => {
                    error!(
                        "Could not parse property '{property_name}' with value '{prop_str}', using fallback value."
                    );
                    fallback_value
                }
            },
            Ok(None) => {
                info!("System property '{property_name}' value not set, using fallback value.");
                fallback_value
            }
            Err(e) => {
                error!(
                    "Failed to read system property '{property_name}': {e}. Using fallback value."
                );
                fallback_value
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustutils::android::system_properties::PropertyWatcherError as SystemPropertyError;

    const TEST_PROP: &str = "test.prop";
    const FALLBACK_VALUE: u32 = 42;

    #[test]
    fn test_read_property_returns_set_sys_prop() {
        const EXPECTED_VALUE: u32 = 99;
        let read_value: u32 = SystemProperties::read_property_impl(
            |sys_prop_name| {
                assert_eq!(sys_prop_name, TEST_PROP);
                Ok(Some(EXPECTED_VALUE.to_string()))
            },
            TEST_PROP,
            FALLBACK_VALUE,
        );
        assert_eq!(read_value, EXPECTED_VALUE);
    }

    #[test]
    fn test_read_property_returns_fallback_if_missing_sys_prop() {
        let read_value: u32 =
            SystemProperties::read_property_impl(|_| Ok(None), TEST_PROP, FALLBACK_VALUE);
        assert_eq!(read_value, FALLBACK_VALUE);
    }

    #[test]
    fn test_read_property_returns_fallback_if_parsing_fails() {
        let read_value: u32 = SystemProperties::read_property_impl(
            |_| Ok(Some("not a number".to_string())),
            TEST_PROP,
            FALLBACK_VALUE,
        );
        assert_eq!(read_value, FALLBACK_VALUE);
    }

    #[test]
    fn test_read_property_returns_fallback_if_read_fails() {
        let read_value: u32 = SystemProperties::read_property_impl(
            |_| Err(SystemPropertyError::WaitFailed), // Example error
            TEST_PROP,
            FALLBACK_VALUE,
        );
        assert_eq!(read_value, FALLBACK_VALUE);
    }
}
