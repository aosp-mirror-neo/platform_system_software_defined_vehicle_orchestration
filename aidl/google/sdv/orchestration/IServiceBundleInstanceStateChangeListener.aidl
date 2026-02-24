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

import google.sdv.identity.ServiceFqin;
import google.sdv.orchestration.ServiceBundleInstanceLifecycleState;
import google.sdv.orchestration.ServiceBundleInstanceRecoveryState;

/**
 * A callback used to get notified of service bundle instance state changes from the
 * orchestrator.
 */
oneway interface IServiceBundleInstanceStateChangeListener {
    /**
     * Informs subscribers that a service bundle instance has changed its state.
     *
     * The `expectedLifecycleState` represents the lifecycle state that the orchestrator intends
     * for the service bundle instance to be in, based on the current configuration and active
     * modes.
     *
     * The `newRecoveryState` indicates the status of reaching that intended lifecycle state:
     *  - `OPERATIONAL`: The instance has successfully reached the `expectedLifecycleState`.
     *  - `RETRYING`: The instance failed to reach the `expectedLifecycleState` (e.g., due to a
     *    crash or a transition failure), and the orchestrator is attempting to recover it.
     *  - `RETRY_FAILED`: The orchestrator has stopped trying to recover the instance after
     *    repeated failures. The instance should be at `expectedLifecycleState`.
     *
     * @param fqin The FQIN of the service bundle instance that changed state.
     * @param newRecoveryState The new recovery state of the service bundle instance.
     * @param expectedLifecycleState The intended lifecycle state for the service bundle instance.
     */
    void onServiceBundleInstanceStateChanged(
            in ServiceFqin fqin,
            ServiceBundleInstanceRecoveryState newRecoveryState,
            ServiceBundleInstanceLifecycleState expectedLifecycleState);
}
