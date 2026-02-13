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
 * Represents different lifecycle states of a service bundle instance.
 *
 * This state is determined by the orchestrator based on the current system modes and configuration.
 */
enum ServiceBundleInstanceLifecycleState {
    /**
     * The default value if no state is specified.
     */
    SERVICE_BUNDLE_INSTANCE_LIFECYCLE_STATE_UNSPECIFIED = 0,

    /**
     * The service bundle instance is started.
     */
    STARTED = 1,

    /**
     * The service bundle instance is created.
     */
    CREATED = 2,

    /**
     * The service bundle instance is destroyed.
     */
    DESTROYED = 3,
}
