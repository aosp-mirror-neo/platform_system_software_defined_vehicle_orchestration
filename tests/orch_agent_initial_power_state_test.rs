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
//! Orchestrator integration test that tests that services are started when fetching the initial
//! power mode state from VPM. This test is isolated from the rest of the integration tests as
//! we do not want any logic that will change the power mode (to avoid having to configure a
//! power mode initially and then starting the test). On CI, each rust_test runs on a fresh VM,
//! in which the initial power mode is always POWER_OFF_EXIT.

#[cfg(test)]
mod tests {
    use sdv_orch_tests_common::test_common::*;

    #[test]
    fn test_service_is_running_on_initial_power_state() {
        // GIVEN the bundle configured to run on initial power state
        // WHEN/THEN
        assert_service_is_started(&get_service_bundle_fqin_initial_power_state());
    }
}
