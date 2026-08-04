use serde::{Deserialize, Serialize};

/// The value of an `Assign` field.
///
/// Per the Amazon States Language spec, the value of an `Assign` field **must** be a JSON
/// object. Each top-level field name is the variable to assign, and the field's value is the
/// value assigned to that variable.
///
/// This wrapper makes that constraint part of the type system: `Assign` can deserialize only
/// from a JSON object, not from a string/array/number/boolean/null.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct AssignObject(pub serde_json::Map<String, serde_json::Value>);

impl AssignObject {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::assign::AssignObject;

    #[test]
    fn test_assign_object_roundtrip() -> Result<(), Box<dyn Error>> {
        // Using Assign: the value of an Assign field must be a JSON object whose top-level keys
        // name variables to assign.
        let content = r#"{
          "outer": "hello",
          "discount": 5,
          "payload": "{% $states.input %}",
          "meta": {
            "source": "workflow"
          }
        }"#;

        let assign: AssignObject = serde_json::from_str(content)?;
        assert_eq!(assign.0.len(), 4);
        assert!(assign.0.contains_key("outer"));
        assert!(assign.0.contains_key("discount"));
        assert!(assign.0.contains_key("payload"));
        assert!(assign.0.contains_key("meta"));

        let reserialized = serde_json::to_string(&assign)?;
        let reparsed: AssignObject = serde_json::from_str(&reserialized)?;
        assert_eq!(assign, reparsed);

        Ok(())
    }

    #[test]
    fn test_assign_object_rejects_string() {
        let err = serde_json::from_str::<AssignObject>(r#""hello""#).unwrap_err();
        assert!(
            err.to_string().contains("map") || err.to_string().contains("object"),
            "expected object/map mismatch, got: {err}"
        );
    }

    #[test]
    fn test_assign_object_rejects_array() {
        let err = serde_json::from_str::<AssignObject>(r#"[1,2,3]"#).unwrap_err();
        assert!(
            err.to_string().contains("map") || err.to_string().contains("object"),
            "expected object/map mismatch, got: {err}"
        );
    }

    #[test]
    fn test_assign_object_rejects_number() {
        let err = serde_json::from_str::<AssignObject>(r#"123"#).unwrap_err();
        assert!(
            err.to_string().contains("map") || err.to_string().contains("object"),
            "expected object/map mismatch, got: {err}"
        );
    }

    #[test]
    fn test_assign_object_rejects_boolean() {
        let err = serde_json::from_str::<AssignObject>(r#"true"#).unwrap_err();
        assert!(
            err.to_string().contains("map") || err.to_string().contains("object"),
            "expected object/map mismatch, got: {err}"
        );
    }

    #[test]
    fn test_assign_object_rejects_null() {
        let err = serde_json::from_str::<AssignObject>(r#"null"#).unwrap_err();
        assert!(
            err.to_string().contains("map") || err.to_string().contains("object"),
            "expected object/map mismatch, got: {err}"
        );
    }
}
