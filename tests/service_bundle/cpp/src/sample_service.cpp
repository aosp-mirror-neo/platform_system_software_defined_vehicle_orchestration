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

#include "sample_service.hpp"

#include <sdv/logging.h>
#include <sdv/sb_macro.h>

#include <iostream>

using android::sdv::samples::service_bundle::OrchServiceBundle;

REGISTER_SERVICE_BUNDLE(OrchServiceBundle);

/// Sample implementation of the service bundle interface.
namespace android::sdv::samples::service_bundle {
OrchServiceBundle::OrchServiceBundle(
    [[maybe_unused]] sdv_comms::ctx::Context context)
    : ServiceBundle(context) {
  LOG(INFO) << "OrchServiceBundleCpp::constructor";
}

void OrchServiceBundle::onStart() {
  LOG(INFO) << "OrchServiceBundleCpp::onStart()";
}

void OrchServiceBundle::onStop() {
  LOG(INFO) << "OrchServiceBundleCpp::onStop()";
}

OrchServiceBundle::~OrchServiceBundle() {
  LOG(INFO) << "OrchServiceBundleCpp::destructor";
}
}  // namespace android::sdv::samples::service_bundle