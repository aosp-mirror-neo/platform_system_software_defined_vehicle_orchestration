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

import google.sdv.orchestration.IServiceBundleInstanceStateChangeListener;

/**
 * Interface for handling requests to the orchestrator agent.
 * This API is internal and should not be exposed outside SDV.
 */
interface IOrchestrationAgent {
    /**
     * Register to IServiceBundleInstanceStateChangeListener updates which informs whenever a
     * service bundle instance changes its state. The supported states are defined in the
     * IServiceBundleInstanceState enum. If the same caller registered twice, the new subscription
     * will override the previous one, generating no error.
     *
     * If no mode transition is taking place when registration happened, the state from the last
     * completed transition will be informed by invoking the onServiceBundleInstanceStateChanged
     * callback immediately. If a mode transition is ongoing, the
     * onServiceBundleInstanceStateChanged will be invoked with the partial state of the transition
     * at the moment the registration took place. Any crash or state change that happened outside
     * these two scenarios will not be reported at registration time. Subsequent state changes after
     * the registration will trigger the listener.
     *
     * @param listener implementation of IServiceBundleInstanceStateChangeListener, which will be
     *         informed when the instance state of a bundle has changed.
     */
    void registerServiceBundleInstanceStateChangeListener(
            IServiceBundleInstanceStateChangeListener listener);

    /**
     * Unregister from IServiceBundleInstanceStateChangeListener updates.
     *
     * This method does nothing if the caller has not registered before, and can be called
     * multiple times safely.
     */
    void unregisterServiceBundleInstanceStateChangeListener();
}
