// Copyright (C) 2026 The Android Open Source Project

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package google.sdv.orchestration;

/**
 * Represents health of a service bundle instance from the orchestrator's perspective.
 *
 * This state is used by the orchestrator's restart and retry strategies and contributes to the
 * overall health assessment of the virtual machine.
 */
enum ServiceBundleInstanceState {
    /**
     * The default value if no state is specified.
     */
    SERVICE_BUNDLE_INSTANCE_STATE_UNSPECIFIED = 0,

    /**
     * The service bundle instance has successfully reached its required state (created or started),
     * based on the orchestrator configuration and active modes.
     */
    OPERATIONAL = 1,

    /**
     * The service bundle instance is recovering from a failure, such as a crash or a failed
     * lifecycle manager transition. After retrying, it should transition to either
     * `OPERATIONAL` or `RETRY_FAILED`.
     */
    RETRYING = 2,

    /**
     * The orchestrator was unable to recover the service bundle instance from a failure.
     * No more retries attempts will be made for this instance.
     */
    RETRY_FAILED = 3,

    /**
     * The service bundle instance was cleanly destroyed and is no longer being tracked.
     */
    TERMINATED = 4,
}
