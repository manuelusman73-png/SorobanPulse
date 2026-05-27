//! Issue #267: XDR validation for Soroban event data using the `stellar-xdr` crate.
//! Issue #370: Contract ID Strkey format validation.
//!
//! Validates `event_data.value` and each element of `event_data.topic` as `ScVal`
//! (Soroban Contract Value). Also validates contract_id conforms to Stellar Strkey format.
//! Events that fail validation are logged at WARN and counted in metrics.

use serde_json::Value;
use stellar_xdr::curr::ScVal;
use tracing::warn;

use crate::metrics;

/// Validate that a JSON value can be deserialized as a `ScVal`.
/// Returns `true` if valid, `false` otherwise.
fn is_valid_sc_val(v: &Value) -> bool {
    serde_json::from_value::<ScVal>(v.clone()).is_ok()
}

/// Validate that a contract_id is a valid Stellar Strkey (C-type, 56 chars, base32).
/// Returns `true` if valid, `false` otherwise.
pub fn validate_contract_id(contract_id: &str) -> bool {
    // C-type Strkey: starts with 'C', 56 characters total, base32 encoded
    if contract_id.len() != 56 {
        return false;
    }
    if !contract_id.starts_with('C') {
        return false;
    }
    // Verify all characters are valid base32 (A-Z, 2-7)
    contract_id.chars().all(|c| {
        (c >= 'A' && c <= 'Z') || (c >= '2' && c <= '7')
    })
}

/// Validate the contract_id, `event_data.value` and `event_data.topic` fields of a Soroban event.
///
/// Returns `true` if the event passes all validations, `false` if it should be skipped.
/// On failure, logs a WARN and increments appropriate metrics.
pub fn validate_xdr(
    tx_hash: &str,
    contract_id: &str,
    ledger: u64,
    value: &Value,
    topic: Option<&Vec<Value>>,
) -> bool {
    // Validate contract_id format first
    if !validate_contract_id(contract_id) {
        warn!(
            tx_hash = %tx_hash,
            contract_id = %contract_id,
            ledger = ledger,
            "contract_id failed Strkey validation, skipping event",
        );
        metrics::record_invalid_contract_id();
        return false;
    }

    // Null value is acceptable (no XDR to validate)
    if !value.is_null() && !is_valid_sc_val(value) {
        warn!(
            tx_hash = %tx_hash,
            contract_id = %contract_id,
            ledger = ledger,
            raw_value = %value,
            "event_data.value failed XDR/ScVal validation, skipping event",
        );
        metrics::record_xdr_invalid();
        return false;
    }

    if let Some(topics) = topic {
        for (i, t) in topics.iter().enumerate() {
            if !is_valid_sc_val(t) {
                warn!(
                    tx_hash = %tx_hash,
                    contract_id = %contract_id,
                    ledger = ledger,
                    topic_index = i,
                    raw_topic = %t,
                    "event_data.topic[{}] failed XDR/ScVal validation, skipping event",
                    i,
                );
                metrics::record_xdr_invalid();
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn call(value: Value, topic: Option<Vec<Value>>) -> bool {
        validate_xdr("txhash", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF", 100, &value, topic.as_ref())
    }

    #[test]
    fn null_value_is_valid() {
        assert!(call(Value::Null, None));
    }

    #[test]
    fn valid_sc_val_void_passes() {
        // ScVal::Void serializes as {"void": null} in stellar-xdr serde
        let v = json!({"void": null});
        assert!(call(v, None));
    }

    #[test]
    fn valid_sc_val_bool_passes() {
        let v = json!({"bool": true});
        assert!(call(v, None));
    }

    #[test]
    fn invalid_value_fails() {
        // A plain string is not a valid ScVal
        let v = json!("not_a_scval");
        assert!(!call(v, None));
    }

    #[test]
    fn invalid_number_value_fails() {
        let v = json!(42);
        assert!(!call(v, None));
    }

    #[test]
    fn valid_topic_passes() {
        let v = Value::Null;
        let topic = vec![json!({"void": null}), json!({"bool": false})];
        assert!(call(v, Some(topic)));
    }

    #[test]
    fn invalid_topic_element_fails() {
        let v = Value::Null;
        let topic = vec![json!({"void": null}), json!("bad_topic")];
        assert!(!call(v, Some(topic)));
    }

    #[test]
    fn empty_topic_passes() {
        assert!(call(Value::Null, Some(vec![])));
    }

    #[test]
    fn valid_c_type_strkey() {
        assert!(validate_contract_id("CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"));
    }

    #[test]
    fn invalid_strkey_wrong_type() {
        // G-type (account) instead of C-type (contract)
        assert!(!validate_contract_id("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"));
    }

    #[test]
    fn invalid_strkey_wrong_length() {
        assert!(!validate_contract_id("CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"));
    }

    #[test]
    fn invalid_strkey_invalid_chars() {
        assert!(!validate_contract_id("CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA@WHF"));
    }

    #[test]
    fn invalid_strkey_lowercase() {
        assert!(!validate_contract_id("caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaawhf"));
    }

    #[test]
    fn contract_id_validation_rejects_invalid_format() {
        let v = Value::Null;
        // Invalid contract ID should fail validation
        assert!(!validate_xdr("txhash", "INVALID", 100, &v, None));
    }
}
