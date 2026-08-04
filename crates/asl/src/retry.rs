use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// A retrier is an entry in a state's `Retry` array (available to `Task`, `Parallel`, and
/// `Map` states). It defines a retry policy applied when the state reports an error whose
/// name appears in `error_equals`.
///
/// See:
/// - https://states-language.net/spec.html#error-handling
/// - https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Retrier {
    /// Required. A non-empty array of strings matching error names. When the state reports
    /// an error whose name is in this array, the retry policy described by this retrier is
    /// applied. The reserved name `States.ALL` is a wildcard matching any error name and
    /// must appear alone in the array; `States.TaskFailed` matches any error except
    /// `States.Timeout`.
    pub error_equals: Vec<String>,

    /// Optional. A positive integer representing the number of seconds before the first
    /// retry attempt (default 1).
    pub interval_seconds: Option<i64>,

    /// Optional. A non-negative integer representing the maximum number of retry attempts
    /// (default 3). A value of 0 specifies that the error is never retried.
    pub max_attempts: Option<i64>,

    /// Optional. A number (≥ 1.0) that is the multiplier by which the retry interval
    /// increases after each attempt (default 2.0).
    pub backoff_rate: Option<serde_json::Number>,

    /// Optional. A positive integer setting the maximum value, in seconds, up to which a
    /// retry interval can increase. Limits the exponential wait times resulting from
    /// `backoff_rate`.
    pub max_delay_seconds: Option<i64>,

    /// Optional. A string representing the jitter strategy to use in the retry interval
    /// calculation, spreading retry attempts over a randomized delay interval (default
    /// `None`). The spec does not constrain the set of accepted values.
    pub jitter_strategy: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::Retrier;

    #[test]
    fn test_retrier_minimal() -> Result<(), Box<dyn Error>> {
        // Minimal retrier: only the required `ErrorEquals` array.
        let content = r#"{
          "ErrorEquals": ["States.ALL"]
        }"#;

        let retrier: Retrier = serde_json::from_str(content)?;
        assert_eq!(retrier.error_equals, ["States.ALL"]);
        assert_eq!(retrier.interval_seconds, None);
        assert_eq!(retrier.max_attempts, None);
        assert_eq!(retrier.backoff_rate, None);
        assert_eq!(retrier.max_delay_seconds, None);
        assert_eq!(retrier.jitter_strategy, None);

        // Round-trip: omitted optional fields must not reappear on serialization.
        let reserialized = serde_json::to_string(&retrier)?;
        assert!(
            !reserialized.contains("IntervalSeconds"),
            "IntervalSeconds should be absent, got: {reserialized}"
        );
        let reparsed: Retrier = serde_json::from_str(&reserialized)?;
        assert_eq!(retrier, reparsed);

        Ok(())
    }

    #[test]
    fn test_retrier_full() -> Result<(), Box<dyn Error>> {
        // Retrier with every field set, including `MaxDelaySeconds` and `JitterStrategy`.
        let content = r#"{
          "ErrorEquals": ["States.Timeout"],
          "IntervalSeconds": 3,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "MaxDelaySeconds": 5,
          "JitterStrategy": "FULL"
        }"#;

        let retrier: Retrier = serde_json::from_str(content)?;
        assert_eq!(retrier.error_equals, ["States.Timeout"]);
        assert_eq!(retrier.interval_seconds, Some(3));
        assert_eq!(retrier.max_attempts, Some(3));
        assert_eq!(retrier.backoff_rate, Some(serde_json::Number::from(2)));
        assert_eq!(retrier.max_delay_seconds, Some(5));
        assert_eq!(retrier.jitter_strategy.as_deref(), Some("FULL"));

        // Round-trip: all fields must be preserved.
        let reserialized = serde_json::to_string(&retrier)?;
        let reparsed: Retrier = serde_json::from_str(&reserialized)?;
        assert_eq!(retrier, reparsed);
        assert!(reserialized.contains(r#""JitterStrategy":"FULL""#));

        Ok(())
    }
}
