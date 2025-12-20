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

//! Rust representation of Orchestrator configuration conditions and implementation for their evaluation

use crate::orchestration_state::OrchestrationState;
use log::warn;
use sdv_orch_dist_config_proto::common::CustomState;
use sdv_orch_dist_config_proto::common::{
    condition::Root, Condition as ConditionProto, Expression,
};
use sdv_orchestration_common::mode::Mode;
use std::ops::{BitAnd, BitOr, Not};

#[derive(Debug, PartialEq, Clone)]
/// Rust representation of state configuration condition.
pub enum Condition {
    /// Condition that checks the current state of the mode
    State {
        /// Mode to be checked
        mode: Mode,
        /// Expected state of the mode
        state: String,
    },
    /// Logical AND condition
    And {
        /// Subconditions
        arguments: Vec<Condition>,
    },
    /// Logical OR condition
    Or {
        /// Subconditions
        arguments: Vec<Condition>,
    },
    /// Logical NOT condition
    Not {
        /// Negated condition
        condition: Box<Condition>,
    },
    /// Empty condition - always true
    Empty,
}

impl Condition {
    pub fn new(condition_proto: &ConditionProto) -> Self {
        Self::from_proto(condition_proto).unwrap_or(Self::Empty)
    }

    /// Create from proto message
    fn from_proto(condition_proto: &ConditionProto) -> Option<Self> {
        match &condition_proto.root {
            Some(Root::PowerState(state)) => {
                Some(Condition::State { mode: Mode::Power, state: state.to_string() })
            }
            Some(Root::VehicleState(state)) => {
                Some(Condition::State { mode: Mode::Vehicle, state: state.to_string() })
            }
            Some(Root::CustomState(CustomState { mode, state, .. })) => Some(Condition::State {
                mode: Mode::Custom(mode.to_string()),
                state: state.to_string(),
            }),
            Some(Root::Not(not)) => Self::parse_not_condition(not),
            Some(Root::Or(or)) => Some(Condition::Or { arguments: Self::parse_expression(or) }),
            Some(Root::And(and)) => Some(Condition::And { arguments: Self::parse_expression(and) }),
            Some(_) => {
                warn!("Unmatched orchestration state expression: {condition_proto:#?}");
                None
            }
            None => {
                warn!("Undefined orchestration state expression: {condition_proto:#?}",);
                None
            }
        }
    }

    fn parse_not_condition(condition_proto: &ConditionProto) -> Option<Self> {
        Self::from_proto(condition_proto)
            .map(Box::new)
            .map(|not_condition| Condition::Not { condition: not_condition })
    }

    fn parse_expression(expression_proto: &Expression) -> Vec<Condition> {
        let mut expressions = vec![];

        for state in expression_proto.power_state.iter().cloned() {
            expressions.push(Condition::State { mode: Mode::Power, state })
        }

        for state in expression_proto.vehicle_state.iter().cloned() {
            expressions.push(Condition::State { mode: Mode::Vehicle, state })
        }

        for CustomState { mode, state, .. } in expression_proto.custom_state.iter().cloned() {
            expressions.push(Condition::State { mode: Mode::Custom(mode), state })
        }

        for and in &expression_proto.and {
            expressions.push(Condition::And { arguments: Self::parse_expression(and) });
        }

        for or in &expression_proto.or {
            expressions.push(Condition::Or { arguments: Self::parse_expression(or) });
        }

        for not in &expression_proto.not {
            if let Some(not) = Self::parse_not_condition(not) {
                expressions.push(not)
            };
        }

        expressions
    }

    /// Evaluate the condition, given the current vpm states
    pub fn evaluate(&self, orchestration_state: &OrchestrationState) -> bool {
        self.evaluate_internal(orchestration_state).into()
    }

    fn evaluate_internal(&self, orchestration_state: &OrchestrationState) -> EvaluationResult {
        match self {
            Condition::State { mode, state } => match orchestration_state.get(mode) {
                None => EvaluationResult::Undefined,
                Some((current_state, _)) => {
                    if state == current_state {
                        EvaluationResult::True
                    } else {
                        EvaluationResult::False
                    }
                }
            },
            Condition::And { arguments } => {
                let mut result = EvaluationResult::True;

                for argument in arguments {
                    result = result & argument.evaluate_internal(orchestration_state);
                    if result == EvaluationResult::Undefined {
                        break;
                    }
                }

                result
            }
            Condition::Or { arguments } => {
                let mut result = EvaluationResult::Undefined;

                for argument in arguments {
                    result = result | argument.evaluate_internal(orchestration_state);
                    if result == EvaluationResult::True {
                        break;
                    }
                }

                result
            }
            Condition::Not { condition } => !condition.evaluate_internal(orchestration_state),
            Condition::Empty => EvaluationResult::True,
        }
    }
}

#[derive(PartialEq)]
enum EvaluationResult {
    True,
    False,
    Undefined,
}

impl From<EvaluationResult> for bool {
    fn from(val: EvaluationResult) -> Self {
        val == EvaluationResult::True
    }
}

impl Not for EvaluationResult {
    type Output = EvaluationResult;

    fn not(self) -> Self::Output {
        match self {
            EvaluationResult::True => EvaluationResult::False,
            EvaluationResult::False => EvaluationResult::True,
            EvaluationResult::Undefined => EvaluationResult::Undefined,
        }
    }
}

impl BitAnd for EvaluationResult {
    type Output = EvaluationResult;

    fn bitand(self, rhs: Self) -> Self::Output {
        match self {
            EvaluationResult::True => rhs,
            EvaluationResult::False => {
                if rhs == EvaluationResult::Undefined {
                    EvaluationResult::Undefined
                } else {
                    EvaluationResult::False
                }
            }
            EvaluationResult::Undefined => EvaluationResult::Undefined,
        }
    }
}

impl BitOr for EvaluationResult {
    type Output = EvaluationResult;

    fn bitor(self, rhs: Self) -> Self::Output {
        match self {
            EvaluationResult::True => EvaluationResult::True,
            EvaluationResult::False => {
                if rhs == EvaluationResult::True {
                    EvaluationResult::True
                } else {
                    EvaluationResult::False
                }
            }
            EvaluationResult::Undefined => rhs,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::condition::Condition;
    use crate::condition::EvaluationResult;
    use crate::orchestration_state::OrchestrationState;
    use googletest::assert_that;
    use googletest::prelude::eq;
    use protobuf::text_format;
    use sdv_orch_dist_config_proto::common::Condition as ConditionProto;
    use sdv_orchestration_common::mode::Mode;
    use std::collections::HashMap;
    use std::ops::{BitAnd, BitOr};

    fn orchestration_state() -> OrchestrationState {
        let mut result = HashMap::new();
        result.insert(Mode::Custom("true".to_string()), ("state".to_string(), None));
        result.insert(Mode::Custom("false".to_string()), ("not_state".to_string(), None));
        // "undefined" state is not present in the map
        result
    }

    fn vpm_state_condition(mode: &'static str) -> Condition {
        Condition::State { mode: Mode::Custom(mode.to_string()), state: "state".to_string() }
    }

    fn true_condition() -> Condition {
        vpm_state_condition("true")
    }

    fn false_condition() -> Condition {
        vpm_state_condition("false")
    }

    fn undefined_condition() -> Condition {
        vpm_state_condition("undefined")
    }

    fn all_conditions() -> Vec<Condition> {
        vec![true_condition(), false_condition(), undefined_condition()]
    }

    #[test]
    fn test_true_state() {
        assert!(true_condition().evaluate(&orchestration_state()))
    }

    #[test]
    fn test_false_state() {
        assert!(!false_condition().evaluate(&orchestration_state()))
    }

    #[test]
    fn test_undefined_state() {
        assert!(!undefined_condition().evaluate(&orchestration_state()))
    }

    #[test]
    fn test_from_empty_proto() {
        let condition =
            Condition::from_proto(&text_format::parse_from_str::<ConditionProto>("").unwrap());
        assert_eq!(condition, None);

        let condition = Condition::new(&text_format::parse_from_str::<ConditionProto>("").unwrap());
        assert_eq!(condition, Condition::Empty);
    }

    #[test]
    fn test_from_power_state_root_proto() {
        let condition = Condition::from_proto(
            &text_format::parse_from_str::<ConditionProto>(
                r#"
                power_state: "ON"
                "#,
            )
            .unwrap(),
        );

        assert_eq!(
            condition,
            Some(Condition::State { mode: Mode::Power, state: "ON".to_string() })
        );
    }

    #[test]
    fn test_from_vehicle_state_root_proto() {
        let condition = Condition::from_proto(
            &text_format::parse_from_str::<ConditionProto>(
                r#"
                vehicle_state: "LIFE_ON_BOARD"
                "#,
            )
            .unwrap(),
        );

        assert_eq!(
            condition,
            Some(Condition::State { mode: Mode::Vehicle, state: "LIFE_ON_BOARD".to_string() })
        );
    }

    #[test]
    fn test_from_custom_state_root_proto() {
        let condition = Condition::from_proto(
            &text_format::parse_from_str::<ConditionProto>(
                r#"
                custom_state {
                    mode: "occupancy"
                    state: "OCCUPANCY_EMPTY"
                }
                "#,
            )
            .unwrap(),
        );

        assert_eq!(
            condition,
            Some(Condition::State {
                mode: Mode::Custom("occupancy".to_string()),
                state: "OCCUPANCY_EMPTY".to_string()
            })
        );
    }

    #[test]
    fn test_from_or_root_proto() {
        let condition = Condition::from_proto(
            &text_format::parse_from_str::<ConditionProto>(
                r#"
                or {
                    power_state: "ON"
                    vehicle_state: "LIFE_ON_BOARD"
                }
                "#,
            )
            .unwrap(),
        );

        assert_eq!(
            condition,
            Some(Condition::Or {
                arguments: vec![
                    Condition::State { mode: Mode::Power, state: "ON".to_string() },
                    Condition::State { mode: Mode::Vehicle, state: "LIFE_ON_BOARD".to_string() },
                ]
            })
        );
    }

    #[test]
    fn test_from_not_root_proto() {
        let condition = Condition::from_proto(
            &text_format::parse_from_str::<ConditionProto>(
                r#"
                not {
                    power_state: "ON"
                }
                "#,
            )
            .unwrap(),
        );

        assert_eq!(
            condition,
            Some(Condition::Not {
                condition: Box::new(Condition::State {
                    mode: Mode::Power,
                    state: "ON".to_string()
                },)
            })
        );
    }
    #[test]
    fn test_from_nested_and_proto() {
        let condition = Condition::from_proto(
            &text_format::parse_from_str::<ConditionProto>(
                r#"
                and {
                    and {
                        power_state: "ON"
                        vehicle_state: "LIFE_ON_BOARD"
                    }
                    not {
                        custom_state {
                            mode: "occupancy"
                            state: "OCCUPANCY_EMPTY"
                        }
                    }
                }
                "#,
            )
            .unwrap(),
        );

        assert_eq!(
            condition,
            Some(Condition::And {
                arguments: vec![
                    Condition::And {
                        arguments: vec![
                            Condition::State { mode: Mode::Power, state: "ON".to_string() },
                            Condition::State {
                                mode: Mode::Vehicle,
                                state: "LIFE_ON_BOARD".to_string()
                            }
                        ]
                    },
                    Condition::Not {
                        condition: Box::new(Condition::State {
                            mode: Mode::Custom("occupancy".to_string()),
                            state: "OCCUPANCY_EMPTY".to_string()
                        })
                    }
                ]
            })
        );
    }

    #[test]
    fn test_from_and_root_proto() {
        let condition = Condition::from_proto(
            &text_format::parse_from_str::<ConditionProto>(
                r#"
                and {
                    or {
                        power_state: "ON"
                        vehicle_state: "LIFE_ON_BOARD"
                    }
                    not {
                        custom_state {
                            mode: "occupancy"
                            state: "OCCUPANCY_EMPTY"
                        }
                    }
                }
                "#,
            )
            .unwrap(),
        );

        assert_eq!(
            condition,
            Some(Condition::And {
                arguments: vec![
                    Condition::Or {
                        arguments: vec![
                            Condition::State { mode: Mode::Power, state: "ON".to_string() },
                            Condition::State {
                                mode: Mode::Vehicle,
                                state: "LIFE_ON_BOARD".to_string()
                            }
                        ]
                    },
                    Condition::Not {
                        condition: Box::new(Condition::State {
                            mode: Mode::Custom("occupancy".to_string()),
                            state: "OCCUPANCY_EMPTY".to_string()
                        })
                    }
                ]
            })
        );
    }

    #[test]
    fn test_and() {
        for lhs in all_conditions() {
            for rhs in all_conditions() {
                let and_condition = Condition::And { arguments: vec![lhs.clone(), rhs.clone()] };

                assert_that!(
                    and_condition.evaluate(&orchestration_state()),
                    eq(lhs.evaluate(&orchestration_state())
                        && rhs.evaluate(&orchestration_state()))
                )
            }
        }
    }

    /// The BitAnd with initial Undefined case needs to be tested like this because of the existent logic of `and``
    /// inside the [evaluate_internal()] with the following code:
    ///
    /// ```
    /// if result == EvaluationResult::Undefined {
    ///      break;
    /// }
    /// ```
    ///
    #[test]
    fn test_undefined_and() {
        let res: bool = EvaluationResult::Undefined.bitand(EvaluationResult::True).into();
        assert!(!res);
    }

    /// The BitOr with initial True case needs to be tested like this because of the existent logic of `or`
    /// inside the [evaluate_internal()] with the following code:
    ///
    /// ```
    /// if result == EvaluationResult::True { {
    ///      break;
    /// }
    /// ```
    ///
    #[test]
    fn test_true_or() {
        let res: bool = EvaluationResult::True.bitor(EvaluationResult::Undefined).into();
        assert!(res);
    }

    #[test]
    fn test_or() {
        for lhs in all_conditions() {
            for rhs in all_conditions() {
                let or_condition = Condition::Or { arguments: vec![lhs.clone(), rhs.clone()] };

                assert_that!(
                    or_condition.evaluate(&orchestration_state()),
                    eq(lhs.evaluate(&orchestration_state())
                        || rhs.evaluate(&orchestration_state()))
                )
            }
        }
    }

    #[test]
    fn test_not() {
        let not_true = Condition::Not { condition: Box::new(vpm_state_condition("true")) };
        let not_false = Condition::Not { condition: Box::new(vpm_state_condition("false")) };
        let not_undefined =
            Condition::Not { condition: Box::new(vpm_state_condition("undefined")) };

        assert!(!not_true.evaluate(&orchestration_state()));
        assert!(not_false.evaluate(&orchestration_state()));
        assert!(!not_undefined.evaluate(&orchestration_state()));
    }
}
