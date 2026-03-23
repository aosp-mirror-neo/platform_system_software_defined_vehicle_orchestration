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

use log::debug;
use sdv_comms::ContextRef;
use sdv_lifecycle_client::service_bundle::ServiceBundle;

/// Sample implementation of the [sdv_lifecycle_client::service_bundle::ServiceBundle] interface.
pub struct OrchServiceBundle {
    context: ContextRef,
}

impl ServiceBundle for OrchServiceBundle {
    /// Creates new instance of the service bundle.
    fn new(context: ContextRef) -> OrchServiceBundle {
        debug!("OrchServiceBundleRust::new() called for {:?}", context.get_self_fqin());
        if [
            "panic-create-no-restarts",
            "panic-create-restarts",
            "panic-create-on-start-restarts",
            "crash-on-create-no-restarts",
            "crash-on-create-restarts",
        ]
        .contains(&context.get_self_fqin().get_service_instance_name())
        {
            panic!("Bundle crashed");
        }
        OrchServiceBundle { context }
    }

    fn on_start(&mut self) {
        debug!("OrchServiceBundleRust::on_start() called for {:?}", self.context.get_self_fqin());
        if [
            "panic-start-no-restarts",
            "panic-start-restarts",
            "crash-on-start-no-restarts",
            "crash-on-start-restarts",
            "crash-on-start-default-retries",
        ]
        .contains(&self.context.get_self_fqin().get_service_instance_name())
        {
            panic!("Bundle crashed");
        }
    }

    fn on_stop(&mut self) {
        debug!("OrchServiceBundleRust::on_stop() called");
        if ["panic-stop-no-restarts", "panic-stop-restarts"]
            .contains(&self.context.get_self_fqin().get_service_instance_name())
        {
            panic!("Bundle crashed");
        }
    }
}

impl Drop for OrchServiceBundle {
    fn drop(&mut self) {
        debug!("OrchServiceBundleRust::drop() called");
        if ["panic-destroy-no-restarts", "panic-destroy-restarts"]
            .contains(&self.context.get_self_fqin().get_service_instance_name())
        {
            panic!("Bundle crashed");
        }
    }
}
