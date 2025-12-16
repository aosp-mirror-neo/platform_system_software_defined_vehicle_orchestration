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
import google.sdv.orchestration.ServiceBundleInstanceState;

/**
 * A callback used to get notified of updates of service bundle instance state changes from the
 * orchestrator.
 */
oneway interface IServiceBundleInstanceStateChangeListener {
    /**
     * Inform to subscribers that a service bundle with the given FQIN has change its state.
     *
     * @param fqin The FQIN of the service bundle that reports a new state.
     * @param newState The new state of the service bundle instance.
     */
    void onServiceBundleInstanceStateChanged(
            in ServiceFqin fqin, ServiceBundleInstanceState newState);
}
