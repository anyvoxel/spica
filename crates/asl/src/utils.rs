use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct JsonataExpr(String);

impl JsonataExpr {
    pub fn new(value: impl Into<String>) -> Result<Self, &'static str> {
        let value = value.into();
        if is_jsonata_expr(&value) {
            Ok(Self(value))
        } else {
            Err("value must be a JSONata expression wrapped in {% %}")
        }
    }

    pub(crate) fn parse<E>(value: String, field_name: &str) -> Result<Self, E>
    where
        E: serde::de::Error,
    {
        if is_jsonata_expr(&value) {
            Ok(Self(value))
        } else {
            Err(serde::de::Error::custom(format!(
                "{field_name} string must be a JSONata expression wrapped in {{% %}}"
            )))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum IntOrExpr {
    Int(i64),
    Expr(JsonataExpr),
}

pub(crate) fn parse_int_or_expr<E>(
    value: serde_json::Value,
    field_name: &str,
) -> Result<IntOrExpr, E>
where
    E: serde::de::Error,
{
    match value {
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(IntOrExpr::Int)
            .ok_or_else(|| serde::de::Error::custom(format!("{field_name} must be an integer"))),
        serde_json::Value::String(s) => JsonataExpr::parse(s, field_name).map(IntOrExpr::Expr),
        _ => Err(serde::de::Error::custom(format!(
            "{field_name} must be either an integer or a JSONata string"
        ))),
    }
}

pub(crate) fn parse_non_negative_int_or_expr<E>(
    value: serde_json::Value,
    field_name: &str,
) -> Result<IntOrExpr, E>
where
    E: serde::de::Error,
{
    match value {
        serde_json::Value::Number(n) => n
            .as_i64()
            .filter(|v| *v >= 0)
            .map(IntOrExpr::Int)
            .ok_or_else(|| {
                serde::de::Error::custom(format!("{field_name} must be a non-negative integer"))
            }),
        serde_json::Value::String(s) => JsonataExpr::parse(s, field_name).map(IntOrExpr::Expr),
        _ => Err(serde::de::Error::custom(format!(
            "{field_name} must be either a non-negative integer or a JSONata string"
        ))),
    }
}

fn is_jsonata_expr(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.starts_with("{%") && trimmed.ends_with("%}")
}

impl<'de> Deserialize<'de> for JsonataExpr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        JsonataExpr::parse(value, "value")
    }
}

impl<'de> Deserialize<'de> for IntOrExpr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        parse_int_or_expr(value, "value")
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_non_negative_int_or_expr, IntOrExpr, JsonataExpr};

    #[test]
    fn test_jsonata_expr_deserializes_wrapped_expression() {
        let value = serde_json::from_str::<JsonataExpr>(r#""{% $states.input.delay %}""#)
            .expect("expected JSONata expression to parse");
        assert_eq!(value, JsonataExpr::new("{% $states.input.delay %}").unwrap());
    }

    #[test]
    fn test_jsonata_expr_rejects_missing_closing_marker() {
        let err = serde_json::from_str::<JsonataExpr>(r#""{% $states.input.delay""#)
            .expect_err("expected incomplete JSONata expression to fail");
        assert!(
            err.to_string()
                .contains("value string must be a JSONata expression wrapped in {% %}"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_int_or_expr_deserializes_integer() {
        let value = serde_json::from_str::<IntOrExpr>("10").expect("expected integer to parse");
        assert_eq!(value, IntOrExpr::Int(10));
    }

    #[test]
    fn test_int_or_expr_deserializes_jsonata_expr() {
        let value = serde_json::from_str::<IntOrExpr>(r#""{% $states.input.delay %}""#)
            .expect("expected JSONata expression to parse");
        assert_eq!(
            value,
            IntOrExpr::Expr(JsonataExpr::new("{% $states.input.delay %}").unwrap())
        );
    }

    #[test]
    fn test_int_or_expr_rejects_bare_string() {
        let err = serde_json::from_str::<IntOrExpr>(r#""delay""#)
            .expect_err("expected bare string to fail");
        assert!(
            err.to_string()
                .contains("value string must be a JSONata expression wrapped in {% %}"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_non_negative_int_or_expr_rejects_negative_integer() {
        let err = parse_non_negative_int_or_expr::<serde::de::value::Error>(
            serde_json::json!(-1),
            "value",
        )
        .expect_err("expected negative integer to fail");
        assert!(
            err.to_string()
                .contains("value must be a non-negative integer"),
            "unexpected error: {err}"
        );
    }
}
