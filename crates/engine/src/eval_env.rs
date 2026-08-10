use std::collections::HashMap;

use jsonata_core::ast::AstNode;
use jsonata_core::evaluator::{Context, Evaluator};
use jsonata_core::parser;
use jsonata_core::value::JValue;
use serde_json::Value;

use crate::error::ExecutionError;
use crate::scope::Scope;

/// The JSONata evaluation environment for a single execution.
///
/// Each call to [`eval_expr`](Self::eval_expr) builds a fresh [`Context`] (binding `$states` and
/// the current user variables) so no state leaks between evaluations. Parsed expression ASTs are
/// cached for the lifetime of the execution to avoid re-parsing repeated expressions (notably
/// retried states in a later milestone).
pub struct EvalEnv {
    ast_cache: HashMap<String, AstNode>,
}

impl EvalEnv {
    pub fn new() -> Self {
        EvalEnv {
            ast_cache: HashMap::new(),
        }
    }

    /// Evaluates a JSONata `expr` (the inner text, without the `{% %}` delimiters) against the
    /// given `$states` object and variable `scope`.
    ///
    /// `$states` and every variable in `scope` are bound by their bare name (so `$states.input`
    /// and `$outer` resolve). The JSONata input data (`.` / `$`) is set to `$states.input`.
    pub fn eval_expr(
        &mut self,
        expr: &str,
        states: &Value,
        scope: &Scope,
    ) -> Result<Value, ExecutionError> {
        let ast = self.ast_for(expr)?;
        let data = JValue::from(states.get("input").unwrap_or(&Value::Null).clone());

        let mut ctx = Context::new();
        ctx.bind("states".to_string(), JValue::from(states.clone()));
        for (name, value) in scope {
            ctx.bind(name.clone(), JValue::from(value.clone()));
        }

        let mut evaluator = Evaluator::with_context(ctx);
        let result = evaluator
            .evaluate(ast, &data)
            .map_err(|e| ExecutionError::Jsonata {
                field: expr.to_string(),
                message: e.to_string(),
            })?;

        Ok(Value::from(&result))
    }

    /// Recursively walks `value`, replacing every `{% ... %}` string with its evaluated JSONata
    /// result. Non-expression strings, numbers, booleans and null are left untouched; objects and
    /// arrays are rebuilt with their values processed (object keys stay literal).
    pub fn eval_json(
        &mut self,
        value: &Value,
        states: &Value,
        scope: &Scope,
    ) -> Result<Value, ExecutionError> {
        match value {
            Value::String(s) => match extract_jsonata(s) {
                Some(inner) => self.eval_expr(inner, states, scope),
                None => Ok(value.clone()),
            },
            Value::Object(map) => {
                let mut out = serde_json::Map::with_capacity(map.len());
                for (key, val) in map {
                    out.insert(key.clone(), self.eval_json(val, states, scope)?);
                }
                Ok(Value::Object(out))
            }
            Value::Array(arr) => {
                let mut out = Vec::with_capacity(arr.len());
                for val in arr {
                    out.push(self.eval_json(val, states, scope)?);
                }
                Ok(Value::Array(out))
            }
            _ => Ok(value.clone()),
        }
    }

    /// Returns the parsed AST for `expr`, parsing and caching it on first use.
    fn ast_for(&mut self, expr: &str) -> Result<&AstNode, ExecutionError> {
        if !self.ast_cache.contains_key(expr) {
            let parsed = parser::parse(expr).map_err(|e| ExecutionError::Jsonata {
                field: expr.to_string(),
                message: e.to_string(),
            })?;
            self.ast_cache.insert(expr.to_string(), parsed);
        }
        // SAFETY-equivalent: established `contains_key` above, so unwrap is sound.
        Ok(self.ast_cache.get(expr).expect("ast just inserted"))
    }
}

impl Default for EvalEnv {
    fn default() -> Self {
        Self::new()
    }
}

/// If `s` is a `{% ... %}` JSONata expression string, returns the trimmed inner expression;
/// otherwise returns `None`.
///
/// Mirrors the rule in `spica_asl::utils::is_jsonata_expr`: the trimmed string must both start
/// with `{%` and end with `%}`. A string with only a leading `{%` (and no closing `%}`) is a
/// literal.
pub(crate) fn extract_jsonata(s: &str) -> Option<&str> {
    let trimmed = s.trim();
    if trimmed.starts_with("{%") && trimmed.ends_with("%}") {
        // `{%` and `%}` are ASCII, so byte slicing lands on char boundaries.
        let inner = &trimmed[2..trimmed.len() - 2];
        Some(inner.trim())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn states(input: Value) -> Value {
        serde_json::json!({
            "input": input,
            "result": null,
            "context": { "State": { "Name": "Test" } }
        })
    }

    fn env() -> EvalEnv {
        EvalEnv::new()
    }

    #[test]
    fn extract_jsonata_detects_wrapped_expression() {
        assert_eq!(
            extract_jsonata("{% $states.input %}"),
            Some("$states.input")
        );
        assert_eq!(
            extract_jsonata("  {% $states.input %}  "),
            Some("$states.input")
        );
    }

    #[test]
    fn extract_jsonata_rejects_partial_markers() {
        // Only a leading `{%` (no closing `%}`) is a literal string.
        assert_eq!(extract_jsonata("{% not closed"), None);
        assert_eq!(extract_jsonata("plain string"), None);
        assert_eq!(extract_jsonata("trailing %}"), None);
    }

    #[test]
    fn eval_json_passes_through_literal_string() {
        let mut env = env();
        let value = serde_json::json!("hello");
        let out = env
            .eval_json(&value, &states(Value::Null), &Scope::new())
            .unwrap();
        assert_eq!(out, serde_json::json!("hello"));
    }

    #[test]
    fn eval_json_substitutes_expression_string() {
        let mut env = env();
        let value = serde_json::json!("{% $states.input.total %}");
        let st = states(serde_json::json!({ "total": 42 }));
        let out = env.eval_json(&value, &st, &Scope::new()).unwrap();
        // jsonata-core numbers are f64, so 42 round-trips as 42.0.
        assert_eq!(out, serde_json::json!(42.0));
    }

    #[test]
    fn eval_json_recurses_into_objects_and_arrays() {
        let mut env = env();
        let value = serde_json::json!({
            "total": "{% $states.input.total %}",
            "items": ["{% $states.input.name %}", "literal"]
        });
        let st = states(serde_json::json!({ "total": 7, "name": "widget" }));
        let out = env.eval_json(&value, &st, &Scope::new()).unwrap();
        assert_eq!(
            out,
            serde_json::json!({
                "total": 7.0,
                "items": ["widget", "literal"]
            })
        );
    }

    #[test]
    fn eval_json_expression_returning_object() {
        let mut env = env();
        // A JSONata expression that constructs an object.
        let value = serde_json::json!("{% { 'a': $states.input.x, 'b': 2 } %}");
        let st = states(serde_json::json!({ "x": 9 }));
        let out = env.eval_json(&value, &st, &Scope::new()).unwrap();
        assert_eq!(out, serde_json::json!({ "a": 9.0, "b": 2.0 }));
    }

    #[test]
    fn eval_json_undefined_becomes_null() {
        let mut env = env();
        // Referencing a missing field yields JSONata Undefined, which maps to null.
        let value = serde_json::json!("{% $states.input.missing %}");
        let st = states(serde_json::json!({}));
        let out = env.eval_json(&value, &st, &Scope::new()).unwrap();
        assert_eq!(out, Value::Null);
    }

    #[test]
    fn eval_expr_binds_user_variables() {
        let mut env = env();
        let mut scope = Scope::new();
        scope.insert("greeting".to_string(), serde_json::json!("hi"));
        let out = env
            .eval_expr("$greeting", &states(Value::Null), &scope)
            .unwrap();
        assert_eq!(out, serde_json::json!("hi"));
    }

    #[test]
    fn eval_expr_reports_jsonata_errors() {
        let mut env = env();
        // A syntactically invalid expression.
        let err = env
            .eval_expr("$states.input..", &states(Value::Null), &Scope::new())
            .unwrap_err();
        assert!(matches!(err, ExecutionError::Jsonata { .. }));
    }
}
