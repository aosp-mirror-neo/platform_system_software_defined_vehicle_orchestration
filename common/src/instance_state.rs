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

//! Defines possible runtime states of service bundle instances.

use crate::instance_state::InstanceState::*;

/// The runtime state of an instance.
#[derive(Debug, Clone, PartialEq)]
pub enum InstanceState {
    /// Instance should be in the "created" state.
    ///
    /// Corresponds to the state of the instance:
    /// - after the `on_create`, but before the `on_start` callback.
    /// - after the `on_stop`, but before the `on_destroy` callback.
    Created,
    /// Instance should be in the "started" state.
    ///
    /// Takes precedence over the "created" state.
    ///
    /// Corresponds to the state of the instance after the `on_start` callback.
    Started,
    /// Instance should be in the "destroyed" state.
    ///
    /// Takes precedence over the "started" and "created" states.
    ///
    /// Corresponds to the state of the instance:
    /// - before the `on_create` callback.
    /// - after the `on_destroy` callback.
    /// - when the instance process is killed / not yet started.
    Destroyed,
}

impl InstanceState {
    /// "Merge" current instance state with the other instance state.
    ///
    /// Returns the new instance state, which is equal to one of two states, according to the precedence of the states:
    /// Created > Started > Destroyed.
    pub fn merge_with(&self, other: &InstanceState) -> InstanceState {
        match (self, other) {
            (Destroyed, _) => Destroyed,
            (_, Destroyed) => Destroyed,
            (Started, _) => Started,
            (_, Started) => Started,
            _ => Created,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use googletest::{assert_that, prelude::eq};

    #[test]
    fn test_instanace_state_merge_scenarios() {
        assert_that!(
            InstanceState::Created.merge_with(&InstanceState::Started),
            eq(&InstanceState::Started)
        );
        assert_that!(
            InstanceState::Created.merge_with(&InstanceState::Destroyed),
            eq(&InstanceState::Destroyed)
        );
        assert_that!(
            InstanceState::Started.merge_with(&InstanceState::Created),
            eq(&InstanceState::Started)
        );
        assert_that!(
            InstanceState::Started.merge_with(&InstanceState::Destroyed),
            eq(&InstanceState::Destroyed)
        );
        assert_that!(
            InstanceState::Destroyed.merge_with(&InstanceState::Created),
            eq(&InstanceState::Destroyed)
        );
        assert_that!(
            InstanceState::Destroyed.merge_with(&InstanceState::Started),
            eq(&InstanceState::Destroyed)
        );
        assert_that!(
            InstanceState::Destroyed.merge_with(&InstanceState::Destroyed),
            eq(&InstanceState::Destroyed)
        );
        assert_that!(
            InstanceState::Started.merge_with(&InstanceState::Started),
            eq(&InstanceState::Started)
        );
        assert_that!(
            InstanceState::Created.merge_with(&InstanceState::Created),
            eq(&InstanceState::Created)
        );
    }
}
