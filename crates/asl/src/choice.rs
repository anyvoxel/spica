use serde::{Deserialize, Serialize};

use crate::{utils::JsonataExpr, AssignObject};

/// The value of a `Choice` rule's `Condition` field in the JSONata-only subset.
///
/// Per the spec, a JSONata Choice rule MAY specify either:
/// - a boolean literal, or
/// - a JSONata string whose evaluated value must be a boolean.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum ChoiceCondition {
    Bool(bool),
    Expr(JsonataExpr),
}

fn parse_choice_condition<E>(value: serde_json::Value) -> Result<ChoiceCondition, E>
where
    E: serde::de::Error,
{
    match value {
        serde_json::Value::Bool(b) => Ok(ChoiceCondition::Bool(b)),
        serde_json::Value::String(s) => JsonataExpr::parse(s, "Condition").map(ChoiceCondition::Expr),
        _ => Err(serde::de::Error::custom(
            "Condition must be either a boolean or a JSONata string",
        )),
    }
}

impl<'de> Deserialize<'de> for ChoiceCondition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        parse_choice_condition(value)
    }
}

fn deserialize_choice_condition_option<'de, D>(deserializer: D) -> Result<Option<ChoiceCondition>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Err(serde::de::Error::custom(
            "Condition must be either a boolean or a JSONata string",
        )),
        value => parse_choice_condition(value).map(Some),
    }
}

/// A `Choice` state (`"Type": "Choice"`) adds branching logic to a state machine. It is not a
/// terminal state: it has no `Next`/`End` of its own, and instead transitions based on the
/// `choices` rules (or to the `default` state if no rule matches).
///
/// Each rule carries a `Condition` that accepts either a boolean value or a JSONata string.
///
/// See:
/// - https://docs.aws.amazon.com/step-functions/latest/dg/state-choice.html
/// - https://states-language.net/spec.html#choice-state
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct ChoiceState {
    /// Optional. A human-readable description of the state.
    #[serde(rename = "Comment", skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Optional. Used to specify and transform output from the state. When specified, the value
    /// overrides the state output default. Accepts any JSON value; strings surrounded by
    /// `{% %}` are evaluated as JSONata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,

    /// Optional. A collection of key-value pairs to assign data to variables. Any string value
    /// surrounded by `{% %}` is evaluated as JSONata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assign: Option<AssignObject>,

    /// Optional. A string that must match a state name. The state to transition to when none
    /// of the `choices` rules match. If no rule matches and `default` is not specified, the
    /// interpreter throws a `States.NoChoiceMatched` error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,

    /// Required. A non-empty array of choice rules. Each top-level rule must have a `Next`
    /// field naming the state to transition to when the rule matches.
    pub choices: Vec<ChoiceRule>,
}

/// A choice rule is an element of a `Choice` state's `choices` array. A rule carries a
/// `Condition` evaluated to decide whether the rule matches.
///
/// A top-level rule (a direct member of `choices`) must have a `next` field.
///
/// See: https://states-language.net/spec.html#choice-state
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct ChoiceRule {
    /// Optional. A boolean literal or JSONata string used to decide whether the rule matches.
    /// JSONata strings must evaluate to a boolean value.
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_choice_condition_option",
        default
    )]
    pub condition: Option<ChoiceCondition>,

    /// Required for top-level rules. A string that must match a state name; the state machine
    /// transitions to this state when the rule matches.
    pub next: String,

    /// Optional. A collection of key-value pairs to assign data to variables. If a rule is
    /// chosen, its `assign` is evaluated instead of the state's top-level `assign`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assign: Option<AssignObject>,

    /// Optional. Used to specify and transform output from the matched rule. Accepts any JSON
    /// value; strings surrounded by `{% %}` are evaluated as JSONata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::{AssignObject, JsonataExpr, State};

    use super::{ChoiceCondition, ChoiceRule};

    fn assign(v: serde_json::Value) -> Option<AssignObject> {
        Some(AssignObject(v.as_object().expect("assign must be object").clone()))
    }

    #[test]
    fn test_choice_state_jsonata_condition() -> Result<(), Box<dyn Error>> {
        // Uses `Condition` expressions to match rules instead of JSONPath comparisons. The
        // rules use JSONata to query `$states.input`, and each may carry `Assign`/`Output`. The
        // state also has a top-level `Assign` and a `Default`.
        let content = r#"{
          "Type": "Choice",
          "Choices": [
            {
              "Condition": "{% $states.input.type != 'Private' %}",
              "Next": "Public"
            },
            {
              "Condition": "{% $exists($value) and $type($value)='number' and $value>=20 and $value<30 %}",
              "Assign": {
                "range": "twenties"
              },
              "Next": "ValueInTwenties"
            },
            {
              "Condition": "{% $states.input.rating >= $states.input.auditThreshold %}",
              "Output": {
                "excess": "{% $states.input.rating - $states.input.auditThreshold %}"
              },
              "Next": "StartAudit"
            }
          ],
          "Default": "RecordEvent",
          "Assign": {
            "range": "default"
          }
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Choice(ref choice) = value else {
            panic!("expected Choice state, got {:?}", value);
        };

        assert_eq!(choice.default.as_deref(), Some("RecordEvent"));
        assert_eq!(
            choice.assign,
            assign(serde_json::json!({ "range": "default" }))
        );
        assert_eq!(choice.choices.len(), 3);

        // Rule 1: bare condition.
        let r0 = &choice.choices[0];
        assert_eq!(r0.next, "Public");
        assert_eq!(
            r0.condition,
            Some(ChoiceCondition::Expr(
                JsonataExpr::new("{% $states.input.type != 'Private' %}").unwrap()
            ))
        );
        assert_eq!(r0.assign, None);
        assert_eq!(r0.output, None);

        // Rule 2: condition with a per-rule Assign.
        let r1 = &choice.choices[1];
        assert_eq!(r1.next, "ValueInTwenties");
        assert_eq!(
            r1.condition,
            Some(ChoiceCondition::Expr(
                JsonataExpr::new(
                    "{% $exists($value) and $type($value)='number' and $value>=20 and $value<30 %}"
                )
                .unwrap()
            ))
        );
        assert_eq!(
            r1.assign,
            assign(serde_json::json!({ "range": "twenties" }))
        );

        // Rule 3: condition with an Output object.
        let r2 = &choice.choices[2];
        assert_eq!(r2.next, "StartAudit");
        assert_eq!(
            r2.condition,
            Some(ChoiceCondition::Expr(
                JsonataExpr::new("{% $states.input.rating >= $states.input.auditThreshold %}")
                    .unwrap()
            ))
        );
        assert_eq!(
            r2.output,
            Some(serde_json::json!({
                "excess": "{% $states.input.rating - $states.input.auditThreshold %}"
            }))
        );

        // Round-trip: the JSONata Condition expressions, per-rule Assign/Output, top-level
        // Assign, and Default must all be preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_choice_state_boolean_condition() -> Result<(), Box<dyn Error>> {
        // A Choice rule may use a boolean literal for `Condition`.
        let content = r#"{
          "Type": "Choice",
          "Choices": [
            {
              "Condition": true,
              "Next": "AlwaysMatch"
            },
            {
              "Condition": false,
              "Next": "NeverMatch"
            }
          ],
          "Default": "Fallback"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Choice(ref choice) = value else {
            panic!("expected Choice state, got {:?}", value);
        };

        assert_eq!(choice.default.as_deref(), Some("Fallback"));
        assert_eq!(
            choice.choices[0].condition,
            Some(ChoiceCondition::Bool(true))
        );
        assert_eq!(
            choice.choices[1].condition,
            Some(ChoiceCondition::Bool(false))
        );

        let reserialized = serde_json::to_string(&value)?;
        assert!(reserialized.contains(r#""Condition":true"#));
        assert!(reserialized.contains(r#""Condition":false"#));
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_choice_state_jsonata_category_routing() -> Result<(), Box<dyn Error>> {
        // Routes on a category field using `Condition`, assigning a numeric discount per rule and
        // a default discount of 0 at the state level.
        let content = r#"{
          "Type": "Choice",
          "Choices": [
            {
              "Condition": "{% $states.input.category = 'premium' %}",
              "Next": "PremiumPath",
              "Assign": {
                "discount": 20
              }
            },
            {
              "Condition": "{% $states.input.category = 'standard' %}",
              "Next": "StandardPath",
              "Assign": {
                "discount": 5
              }
            }
          ],
          "Default": "DefaultPath",
          "Assign": {
            "discount": 0
          }
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Choice(ref choice) = value else {
            panic!("expected Choice state, got {:?}", value);
        };

        assert_eq!(choice.default.as_deref(), Some("DefaultPath"));
        assert_eq!(
            choice.assign,
            assign(serde_json::json!({ "discount": 0 }))
        );
        assert_eq!(choice.choices.len(), 2);

        let r0 = &choice.choices[0];
        assert_eq!(r0.next, "PremiumPath");
        assert_eq!(
            r0.condition,
            Some(ChoiceCondition::Expr(
                JsonataExpr::new("{% $states.input.category = 'premium' %}").unwrap()
            ))
        );
        assert_eq!(
            r0.assign,
            assign(serde_json::json!({ "discount": 20 }))
        );

        let r1 = &choice.choices[1];
        assert_eq!(r1.next, "StandardPath");
        assert_eq!(
            r1.condition,
            Some(ChoiceCondition::Expr(
                JsonataExpr::new("{% $states.input.category = 'standard' %}").unwrap()
            ))
        );
        assert_eq!(
            r1.assign,
            assign(serde_json::json!({ "discount": 5 }))
        );

        // Round-trip.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_choice_rule_construction() -> Result<(), Box<dyn Error>> {
        // Construct ChoiceRule values directly to verify the public API builds correctly.
        let expr_rule = ChoiceRule {
            next: "NextState".to_string(),
            condition: Some(ChoiceCondition::Expr(
                JsonataExpr::new("{% $score > 90 %}").unwrap()
            )),
            assign: None,
            output: None,
        };

        let expr_serialized = serde_json::to_string(&expr_rule)?;
        assert!(expr_serialized.contains(r#""Next":"NextState""#));
        assert!(expr_serialized.contains(r#""Condition":"{% $score > 90 %}""#));
        let expr_reparsed: ChoiceRule = serde_json::from_str(&expr_serialized)?;
        assert_eq!(expr_rule, expr_reparsed);

        let bool_rule = ChoiceRule {
            next: "AlwaysMatch".to_string(),
            condition: Some(ChoiceCondition::Bool(true)),
            assign: None,
            output: None,
        };

        let bool_serialized = serde_json::to_string(&bool_rule)?;
        assert!(bool_serialized.contains(r#""Next":"AlwaysMatch""#));
        assert!(bool_serialized.contains(r#""Condition":true"#));
        let bool_reparsed: ChoiceRule = serde_json::from_str(&bool_serialized)?;
        assert_eq!(bool_rule, bool_reparsed);

        Ok(())
    }

    #[test]
    fn test_choice_rule_rejects_non_jsonata_string_condition() {
        let content = r#"{
          "Condition": "true",
          "Next": "NextState"
        }"#;

        let err = serde_json::from_str::<ChoiceRule>(content).expect_err("expected parse failure");
        assert!(
            err.to_string()
                .contains("Condition string must be a JSONata expression wrapped in {% %}"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_choice_rule_rejects_invalid_condition_types() {
        for condition in ["123", "null", "[]", "{}"] {
            let content = format!(
                r#"{{
                  "Condition": {condition},
                  "Next": "NextState"
                }}"#
            );

            let err =
                serde_json::from_str::<ChoiceRule>(&content).expect_err("expected parse failure");
            assert!(
                err.to_string()
                    .contains("Condition must be either a boolean or a JSONata string"),
                "unexpected error for {condition}: {err}"
            );
        }
    }
}
