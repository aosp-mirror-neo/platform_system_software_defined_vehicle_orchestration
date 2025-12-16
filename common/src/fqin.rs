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

//! Defines unique identifier for service bundle instances on the current VM.

use google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin;
use std::fmt::Display;

/// Unique identifier for a service bundle instance on the current VM.
// Ord is used for sorting by FQIN when doing a dumpsys.
#[derive(Clone, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct Fqin {
    /// The package name of the service bundle instance.
    pub package_name: String,
    /// The bundle name of the service bundle instance.
    pub service_bundle_name: String,
    /// The instance name of the service bundle instance.
    pub instance_name: String,
}

impl Display for Fqin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.package_name, self.service_bundle_name, self.instance_name)
    }
}

impl Fqin {
    /// Creates a new `Fqin` instance.
    pub fn new(package_name: &str, service_bundle_name: &str, instance_name: &str) -> Self {
        Self {
            package_name: package_name.to_string(),
            service_bundle_name: service_bundle_name.to_string(),
            instance_name: instance_name.to_string(),
        }
    }

    /// Creates a new `Fqin` instance from a representation used by LifecycleManager.
    pub fn from_lm_fqin(name: &ServiceFqin) -> Self {
        Fqin::new(&name.sdvPackageName, &name.serviceBundleName, &name.serviceInstanceName)
    }

    /// Convert Orchestrator service instance representation to the representation used by LifecycleManager.
    pub fn to_lm_fqin(&self) -> ServiceFqin {
        ServiceFqin {
            sdvVmName: "local-vm".to_owned(),
            sdvPackageName: self.package_name.clone(),
            serviceBundleName: self.service_bundle_name.clone(),
            serviceInstanceName: self.instance_name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use googletest::{assert_that, prelude::eq};

    const PACKAGE: &str = "com.sdv.google.test";
    const BUNDLE: &str = "ServiceBundle";
    const INSTANCE: &str = "default";

    #[test]
    fn test_fqin_to_service_fqin_conversion() {
        // GIVEN
        let fqin = Fqin::new(PACKAGE, BUNDLE, INSTANCE);
        // WHEN
        let service_fqin = fqin.to_lm_fqin();
        // THEN
        assert_that!(service_fqin.sdvPackageName, eq(PACKAGE));
        assert_that!(service_fqin.serviceBundleName, eq(BUNDLE));
        assert_that!(service_fqin.serviceInstanceName, eq(INSTANCE));
    }

    #[test]
    fn test_service_fqin_to_fqin_conversion() {
        // GIVEN
        let service_fqin = ServiceFqin {
            sdvVmName: "vm".to_owned(),
            sdvPackageName: PACKAGE.to_owned(),
            serviceBundleName: BUNDLE.to_owned(),
            serviceInstanceName: INSTANCE.to_owned(),
        };
        // WHEN
        let fqin = Fqin::from_lm_fqin(&service_fqin);
        // THEN
        assert_that!(fqin.package_name, eq(PACKAGE));
        assert_that!(fqin.service_bundle_name, eq(BUNDLE));
        assert_that!(fqin.instance_name, eq(INSTANCE));
    }
}
