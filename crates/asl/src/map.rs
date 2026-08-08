use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{
    AssignObject, Catcher, ItemProcessor, Retrier,
    utils::{IntOrExpr, JsonataExpr, parse_non_negative_int_or_expr},
};

/// The value of a JSONata `Map` state's `Items` field.
///
/// Per the spec, a JSONata Map state MAY specify either:
/// - a JSON array literal, or
/// - a JSONata string whose evaluated value must be a JSON array.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum MapItems {
    Array(Vec<serde_json::Value>),
    Expr(JsonataExpr),
}

fn parse_map_items<E>(value: serde_json::Value) -> Result<MapItems, E>
where
    E: serde::de::Error,
{
    match value {
        serde_json::Value::Array(items) => Ok(MapItems::Array(items)),
        serde_json::Value::String(s) => JsonataExpr::parse(s, "Items").map(MapItems::Expr),
        _ => Err(serde::de::Error::custom(
            "Items must be either a JSON array or a JSONata string",
        )),
    }
}

impl<'de> Deserialize<'de> for MapItems {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        parse_map_items(value)
    }
}

fn deserialize_map_items_option<'de, D>(deserializer: D) -> Result<Option<MapItems>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Err(serde::de::Error::custom(
            "Items must be either a JSON array or a JSONata string",
        )),
        value => parse_map_items(value).map(Some),
    }
}

/// The value of a JSONata `Map` state's `MaxConcurrency` field.
///
/// Per the spec, a JSONata Map state MAY specify either:
/// - a non-negative integer literal, or
/// - a JSONata string whose evaluated value must be a non-negative integer.
pub type MapMaxConcurrency = IntOrExpr;

fn parse_map_max_concurrency<E>(value: serde_json::Value) -> Result<MapMaxConcurrency, E>
where
    E: serde::de::Error,
{
    parse_non_negative_int_or_expr(value, "MaxConcurrency")
}

fn deserialize_map_max_concurrency_option<'de, D>(
    deserializer: D,
) -> Result<Option<MapMaxConcurrency>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Err(serde::de::Error::custom(
            "MaxConcurrency must be either a non-negative integer or a JSONata string",
        )),
        value => parse_map_max_concurrency(value).map(Some),
    }
}

/// The value of a JSONata `Map` state's `ToleratedFailureCount` field.
///
/// Per the spec, a JSONata Map state MAY specify either:
/// - a non-negative integer literal, or
/// - a JSONata string whose evaluated value must be a non-negative integer.
pub type MapToleratedFailureCount = IntOrExpr;

fn parse_map_tolerated_failure_count<E>(
    value: serde_json::Value,
) -> Result<MapToleratedFailureCount, E>
where
    E: serde::de::Error,
{
    parse_non_negative_int_or_expr(value, "ToleratedFailureCount")
}

fn deserialize_map_tolerated_failure_count_option<'de, D>(
    deserializer: D,
) -> Result<Option<MapToleratedFailureCount>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Err(serde::de::Error::custom(
            "ToleratedFailureCount must be either a non-negative integer or a JSONata string",
        )),
        value => parse_map_tolerated_failure_count(value).map(Some),
    }
}

/// The value of a JSONata `Map` state's `ToleratedFailurePercentage` field.
///
/// Per the spec, a JSONata Map state MAY specify either:
/// - a number between 0 and 100, or
/// - a JSONata string whose evaluated value must be a number between 0 and 100.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum MapToleratedFailurePercentage {
    Number(serde_json::Number),
    Expr(JsonataExpr),
}

fn parse_map_tolerated_failure_percentage<E>(
    value: serde_json::Value,
) -> Result<MapToleratedFailurePercentage, E>
where
    E: serde::de::Error,
{
    match value {
        serde_json::Value::Number(n) => match n.as_f64() {
            Some(v) if (0.0..=100.0).contains(&v) => Ok(MapToleratedFailurePercentage::Number(n)),
            _ => Err(serde::de::Error::custom(
                "ToleratedFailurePercentage must be a number between 0 and 100",
            )),
        },
        serde_json::Value::String(s) => JsonataExpr::parse(s, "ToleratedFailurePercentage")
            .map(MapToleratedFailurePercentage::Expr),
        _ => Err(serde::de::Error::custom(
            "ToleratedFailurePercentage must be either a number between 0 and 100 or a JSONata string",
        )),
    }
}

impl<'de> Deserialize<'de> for MapToleratedFailurePercentage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        parse_map_tolerated_failure_percentage(value)
    }
}

fn deserialize_map_tolerated_failure_percentage_option<'de, D>(
    deserializer: D,
) -> Result<Option<MapToleratedFailurePercentage>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Err(serde::de::Error::custom(
            "ToleratedFailurePercentage must be either a number between 0 and 100 or a JSONata string",
        )),
        value => parse_map_tolerated_failure_percentage(value).map(Some),
    }
}

/// A `Map` state (`"Type": "Map"`) runs a set of workflow steps for each item in a dataset
/// (by default a JSON array in the input). Iterations run in parallel up to `max_concurrency`,
/// and each iteration applies the same item processor to a different input element. The state's
/// output is an array with one element per processed item.
///
/// The item processor is defined by `item_processor`. `item_selector` overrides each array
/// element before processing; `items` may be a JSON array literal or a JSONata string that
/// identifies the items array to process.
///
/// This model is the **Inline-only** learning subset: AWS Distributed-Map fields
/// (`ItemReader`/`ItemBatcher`/`ResultWriter`/`Label`) and the legacy `Iterator`/`Parameters`
/// aliases are not modeled.
///
/// See:
/// - https://docs.aws.amazon.com/step-functions/latest/dg/state-map.html
/// - https://states-language.net/spec.html#map-state
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct MapState {
    /// Optional. A human-readable description of the state.
    pub comment: Option<String>,

    /// Optional. Used to specify and transform output from the state. When specified, the value
    /// overrides the state output default. Accepts any JSON value; strings surrounded by
    /// `{% %}` are evaluated as JSONata.
    pub output: Option<serde_json::Value>,

    /// Optional. A collection of key-value pairs to assign data to variables. Any string value
    /// surrounded by `{% %}` is evaluated as JSONata.
    pub assign: Option<AssignObject>,

    /// Optional. The name of the next state that is run when all iterations terminate. One of
    /// `next` or `end` must be set.
    pub next: Option<String>,

    /// Optional. Designates this state as a terminal state (ends the execution) when `true`.
    /// One of `next` or `end` must be set.
    pub end: Option<bool>,

    /// Optional. The item processor defining the state machine that processes each item.
    pub item_processor: Option<ItemProcessor>,

    /// Optional. A JSON array literal, or a JSONata string that must evaluate to a JSON array.
    #[serde(
        default,
        deserialize_with = "deserialize_map_items_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub items: Option<MapItems>,

    /// Optional. If present, the interpreter uses this field to override each single element of
    /// the items array to produce an array of selected items. By default, the input to each
    /// invocation is a single element of the items array. Within this field, the context object
    /// exposes `$states.context.Map.Item.Index` (the zero-based array index being processed) and
    /// `$states.context.Map.Item.Value` (the array element being processed).
    pub item_selector: Option<serde_json::Value>,

    /// Optional. A non-negative integer upper bound on the number of concurrent iterations
    /// (Inline mode: up to 40). A JSONata string is also accepted and must evaluate to a
    /// non-negative integer. A value of 0 means unlimited.
    #[serde(
        default,
        deserialize_with = "deserialize_map_max_concurrency_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_concurrency: Option<MapMaxConcurrency>,

    /// Optional. The maximum percentage of failed items the Map state tolerates before failing.
    /// The literal value must be a number between 0 and 100, inclusive; when specified as a
    /// JSONata string, its evaluated value must also be a number in that range. The default is 0.
    /// If both `tolerated_failure_percentage` and `tolerated_failure_count` are specified, the
    /// Map state fails when either threshold is breached.
    #[serde(
        default,
        deserialize_with = "deserialize_map_tolerated_failure_percentage_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub tolerated_failure_percentage: Option<MapToleratedFailurePercentage>,

    /// Optional. The maximum number of failed items the Map state tolerates before failing.
    /// The literal value must be a non-negative integer; when specified as a JSONata string, its
    /// evaluated value must also be a non-negative integer. The default is 0. If both
    /// `tolerated_failure_percentage` and `tolerated_failure_count` are specified, the Map state
    /// fails when either threshold is breached.
    #[serde(
        default,
        deserialize_with = "deserialize_map_tolerated_failure_count_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub tolerated_failure_count: Option<MapToleratedFailureCount>,

    /// Optional. An array of retrier objects that define a retry policy if the state encounters
    /// runtime errors.
    pub retry: Option<Vec<Retrier>>,

    /// Optional. An array of catcher objects that define a fallback state, executed if the
    /// state encounters runtime errors and its retry policy is exhausted or undefined.
    pub catch: Option<Vec<Catcher>>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::{AssignObject, JsonataExpr, State};

    use super::{
        MapItems, MapMaxConcurrency, MapToleratedFailureCount, MapToleratedFailurePercentage,
    };

    fn assign(v: serde_json::Value) -> Option<AssignObject> {
        Some(AssignObject(
            v.as_object().expect("assign must be object").clone(),
        ))
    }

    #[test]
    fn test_map_state_inline_item_processor() -> Result<(), Box<dyn Error>> {
        // Inline Map state: processes a JSON array with an item processor that invokes a Lambda
        // function for each item.
        let content = r#"{
          "Type": "Map",
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:123456789012:function:ProcessItem",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Map(ref map) = value else {
            panic!("expected Map state, got {:?}", value);
        };

        assert_eq!(map.end, Some(true));
        assert_eq!(map.next, None);

        let processor = map
            .item_processor
            .as_ref()
            .expect("item_processor should be present");
        assert_eq!(processor.start_at, "ProcessItem");
        assert!(processor.states.contains_key("ProcessItem"));

        // Round-trip: item processor preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_map_state_array_items() -> Result<(), Box<dyn Error>> {
        let content = r#"{
          "Type": "Map",
          "Items": [
            { "sku": "A-1" },
            { "sku": "B-2" }
          ],
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Map(ref map) = value else {
            panic!("expected Map state, got {:?}", value);
        };

        assert_eq!(
            map.items,
            Some(MapItems::Array(vec![
                serde_json::json!({ "sku": "A-1" }),
                serde_json::json!({ "sku": "B-2" }),
            ]))
        );

        let reserialized = serde_json::to_string(&value)?;
        assert!(reserialized.contains(r#""Items":[{"sku":"A-1"},{"sku":"B-2"}]"#));
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_map_state_jsonata_items() -> Result<(), Box<dyn Error>> {
        // JSONata Map state: identifies the items to process with a JSONata `Items` expression,
        // uses `MaxConcurrency: 0` (unlimited), and transforms the output via top-level
        // `Assign`/`Output`.
        let content = r#"{
          "Type": "Map",
          "Items": "{% $states.input.detail.shipped %}",
          "MaxConcurrency": 0,
          "ItemProcessor": {
            "StartAt": "Validate",
            "States": {
              "Validate": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:123456789012:function:ship-val",
                "End": true
              }
            }
          },
          "Assign": {
            "shipped": "{% $states.result %}"
          },
          "Output": {
            "numItemsProcessed": "{% $count($states.input.detail.shipped) %}"
          },
          "End": true
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Map(ref map) = value else {
            panic!("expected Map state, got {:?}", value);
        };

        assert_eq!(map.end, Some(true));
        assert_eq!(map.next, None);
        assert_eq!(map.max_concurrency, Some(MapMaxConcurrency::Int(0)));
        assert_eq!(
            map.items,
            Some(MapItems::Expr(
                JsonataExpr::new("{% $states.input.detail.shipped %}").unwrap()
            ))
        );
        assert_eq!(
            map.assign,
            assign(serde_json::json!({ "shipped": "{% $states.result %}" }))
        );
        assert_eq!(
            map.output,
            Some(serde_json::json!({
                "numItemsProcessed": "{% $count($states.input.detail.shipped) %}"
            }))
        );

        let processor = map
            .item_processor
            .as_ref()
            .expect("item_processor should be present");
        assert_eq!(processor.start_at, "Validate");

        // Round-trip: the JSONata Items expression, Assign, and Output must be preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_map_state_jsonata_max_concurrency() -> Result<(), Box<dyn Error>> {
        let content = r#"{
          "Type": "Map",
          "Items": [1, 2, 3],
          "MaxConcurrency": "{% $states.input.maxConcurrency %}",
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Map(ref map) = value else {
            panic!("expected Map state, got {:?}", value);
        };

        assert_eq!(
            map.max_concurrency,
            Some(MapMaxConcurrency::Expr(
                JsonataExpr::new("{% $states.input.maxConcurrency %}").unwrap()
            ))
        );

        let reserialized = serde_json::to_string(&value)?;
        assert!(reserialized.contains(r#""MaxConcurrency":"{% $states.input.maxConcurrency %}""#));
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_map_state_jsonata_item_selector() -> Result<(), Box<dyn Error>> {
        // Adds `ItemSelector` to override each array element before passing it to an iteration,
        // using JSONata expressions that reference the context object and input.
        let content = r#"{
          "Type": "Map",
          "Items": "{% $states.input.detail.shipped %}",
          "MaxConcurrency": 0,
          "ItemSelector": {
            "parcel": "{% $states.context.Map.Item.Value %}",
            "courier": "{% $states.input.delivery-partner %}"
          },
          "ItemProcessor": {
            "StartAt": "Validate",
            "States": {
              "Validate": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:123456789012:function:ship-val",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Map(ref map) = value else {
            panic!("expected Map state, got {:?}", value);
        };

        assert_eq!(map.end, Some(true));
        assert_eq!(map.max_concurrency, Some(MapMaxConcurrency::Int(0)));
        assert_eq!(
            map.item_selector,
            Some(serde_json::json!({
                "parcel": "{% $states.context.Map.Item.Value %}",
                "courier": "{% $states.input.delivery-partner %}"
            }))
        );

        let processor = map
            .item_processor
            .as_ref()
            .expect("item_processor should be present");
        assert_eq!(processor.start_at, "Validate");

        // Round-trip: the ItemSelector (with context object references) and all JSONata
        // expressions must be preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_map_state_tolerated_failure_thresholds_roundtrip() -> Result<(), Box<dyn Error>> {
        let content = r#"{
          "Type": "Map",
          "Items": [1, 2, 3],
          "MaxConcurrency": 2,
          "ToleratedFailurePercentage": 12.5,
          "ToleratedFailureCount": 3,
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Map(ref map) = value else {
            panic!("expected Map state, got {:?}", value);
        };

        assert_eq!(
            map.tolerated_failure_percentage,
            Some(MapToleratedFailurePercentage::Number(
                serde_json::Number::from_f64(12.5).unwrap()
            ))
        );
        assert_eq!(
            map.tolerated_failure_count,
            Some(MapToleratedFailureCount::Int(3))
        );

        let reserialized = serde_json::to_string(&value)?;
        assert!(reserialized.contains(r#""ToleratedFailurePercentage":12.5"#));
        assert!(reserialized.contains(r#""ToleratedFailureCount":3"#));
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_map_state_jsonata_tolerated_failure_thresholds() -> Result<(), Box<dyn Error>> {
        let content = r#"{
          "Type": "Map",
          "Items": [1, 2, 3],
          "ToleratedFailurePercentage": "{% $states.input.failureTolerancePercent %}",
          "ToleratedFailureCount": "{% $states.input.failureToleranceCount %}",
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Map(ref map) = value else {
            panic!("expected Map state, got {:?}", value);
        };

        assert_eq!(
            map.tolerated_failure_percentage,
            Some(MapToleratedFailurePercentage::Expr(
                JsonataExpr::new("{% $states.input.failureTolerancePercent %}").unwrap()
            ))
        );
        assert_eq!(
            map.tolerated_failure_count,
            Some(MapToleratedFailureCount::Expr(
                JsonataExpr::new("{% $states.input.failureToleranceCount %}").unwrap()
            ))
        );

        let reserialized = serde_json::to_string(&value)?;
        assert!(reserialized.contains(
            r#""ToleratedFailurePercentage":"{% $states.input.failureTolerancePercent %}""#
        ));
        assert!(
            reserialized
                .contains(r#""ToleratedFailureCount":"{% $states.input.failureToleranceCount %}""#)
        );
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_map_state_rejects_non_jsonata_string_items() {
        let content = r#"{
          "Type": "Map",
          "Items": "items",
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let err = serde_json::from_str::<State>(content).expect_err("expected parse failure");
        assert!(
            err.to_string()
                .contains("Items string must be a JSONata expression wrapped in {% %}"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_map_state_rejects_invalid_items_types() {
        for items in ["{}", "123", "true", "null"] {
            let content = format!(
                r#"{{
                  "Type": "Map",
                  "Items": {items},
                  "ItemProcessor": {{
                    "StartAt": "ProcessItem",
                    "States": {{
                      "ProcessItem": {{
                        "Type": "Pass",
                        "End": true
                      }}
                    }}
                  }},
                  "End": true
                }}"#
            );

            let err = serde_json::from_str::<State>(&content).expect_err("expected parse failure");
            assert!(
                err.to_string()
                    .contains("Items must be either a JSON array or a JSONata string"),
                "unexpected error for {items}: {err}"
            );
        }
    }

    #[test]
    fn test_map_state_rejects_non_jsonata_string_max_concurrency() {
        let content = r#"{
          "Type": "Map",
          "Items": [1, 2, 3],
          "MaxConcurrency": "ten",
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let err = serde_json::from_str::<State>(content).expect_err("expected parse failure");
        assert!(
            err.to_string()
                .contains("MaxConcurrency string must be a JSONata expression wrapped in {% %}"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_map_state_rejects_invalid_max_concurrency_types() {
        for max_concurrency in ["{}", "true", "null"] {
            let content = format!(
                r#"{{
                  "Type": "Map",
                  "Items": [1, 2, 3],
                  "MaxConcurrency": {max_concurrency},
                  "ItemProcessor": {{
                    "StartAt": "ProcessItem",
                    "States": {{
                      "ProcessItem": {{
                        "Type": "Pass",
                        "End": true
                      }}
                    }}
                  }},
                  "End": true
                }}"#
            );

            let err = serde_json::from_str::<State>(&content).expect_err("expected parse failure");
            assert!(
                err.to_string().contains(
                    "MaxConcurrency must be either a non-negative integer or a JSONata string"
                ),
                "unexpected error for {max_concurrency}: {err}"
            );
        }
    }

    #[test]
    fn test_map_state_rejects_negative_max_concurrency() {
        let content = r#"{
          "Type": "Map",
          "Items": [1, 2, 3],
          "MaxConcurrency": -1,
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let err = serde_json::from_str::<State>(content).expect_err("expected parse failure");
        assert!(
            err.to_string()
                .contains("MaxConcurrency must be a non-negative integer"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_map_state_rejects_non_jsonata_string_tolerated_failure_count() {
        let content = r#"{
          "Type": "Map",
          "Items": [1, 2, 3],
          "ToleratedFailureCount": "three",
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let err = serde_json::from_str::<State>(content).expect_err("expected parse failure");
        assert!(
            err.to_string().contains(
                "ToleratedFailureCount string must be a JSONata expression wrapped in {% %}"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_map_state_rejects_invalid_tolerated_failure_count_types() {
        for tolerated_failure_count in ["{}", "[]", "true", "null"] {
            let content = format!(
                r#"{{
                  "Type": "Map",
                  "Items": [1, 2, 3],
                  "ToleratedFailureCount": {tolerated_failure_count},
                  "ItemProcessor": {{
                    "StartAt": "ProcessItem",
                    "States": {{
                      "ProcessItem": {{
                        "Type": "Pass",
                        "End": true
                      }}
                    }}
                  }},
                  "End": true
                }}"#
            );

            let err = serde_json::from_str::<State>(&content).expect_err("expected parse failure");
            assert!(
                err.to_string().contains(
                    "ToleratedFailureCount must be either a non-negative integer or a JSONata string"
                ),
                "unexpected error for {tolerated_failure_count}: {err}"
            );
        }
    }

    #[test]
    fn test_map_state_rejects_negative_tolerated_failure_count() {
        let content = r#"{
          "Type": "Map",
          "Items": [1, 2, 3],
          "ToleratedFailureCount": -1,
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let err = serde_json::from_str::<State>(content).expect_err("expected parse failure");
        assert!(
            err.to_string()
                .contains("ToleratedFailureCount must be a non-negative integer"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_map_state_rejects_fractional_tolerated_failure_count() {
        let content = r#"{
          "Type": "Map",
          "Items": [1, 2, 3],
          "ToleratedFailureCount": 1.5,
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let err = serde_json::from_str::<State>(content).expect_err("expected parse failure");
        assert!(
            err.to_string()
                .contains("ToleratedFailureCount must be a non-negative integer"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_map_state_rejects_non_jsonata_string_tolerated_failure_percentage() {
        let content = r#"{
          "Type": "Map",
          "Items": [1, 2, 3],
          "ToleratedFailurePercentage": "ten",
          "ItemProcessor": {
            "StartAt": "ProcessItem",
            "States": {
              "ProcessItem": {
                "Type": "Pass",
                "End": true
              }
            }
          },
          "End": true
        }"#;

        let err = serde_json::from_str::<State>(content).expect_err("expected parse failure");
        assert!(
            err.to_string().contains(
                "ToleratedFailurePercentage string must be a JSONata expression wrapped in {% %}"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_map_state_rejects_invalid_tolerated_failure_percentage_types() {
        for tolerated_failure_percentage in ["{}", "[]", "true", "null"] {
            let content = format!(
                r#"{{
                  "Type": "Map",
                  "Items": [1, 2, 3],
                  "ToleratedFailurePercentage": {tolerated_failure_percentage},
                  "ItemProcessor": {{
                    "StartAt": "ProcessItem",
                    "States": {{
                      "ProcessItem": {{
                        "Type": "Pass",
                        "End": true
                      }}
                    }}
                  }},
                  "End": true
                }}"#
            );

            let err = serde_json::from_str::<State>(&content).expect_err("expected parse failure");
            assert!(
                err.to_string().contains(
                    "ToleratedFailurePercentage must be either a number between 0 and 100 or a JSONata string"
                ),
                "unexpected error for {tolerated_failure_percentage}: {err}"
            );
        }
    }

    #[test]
    fn test_map_state_rejects_out_of_range_tolerated_failure_percentage() {
        for tolerated_failure_percentage in ["-0.1", "100.1", "101"] {
            let content = format!(
                r#"{{
                  "Type": "Map",
                  "Items": [1, 2, 3],
                  "ToleratedFailurePercentage": {tolerated_failure_percentage},
                  "ItemProcessor": {{
                    "StartAt": "ProcessItem",
                    "States": {{
                      "ProcessItem": {{
                        "Type": "Pass",
                        "End": true
                      }}
                    }}
                  }},
                  "End": true
                }}"#
            );

            let err = serde_json::from_str::<State>(&content).expect_err("expected parse failure");
            assert!(
                err.to_string()
                    .contains("ToleratedFailurePercentage must be a number between 0 and 100"),
                "unexpected error for {tolerated_failure_percentage}: {err}"
            );
        }
    }
}
