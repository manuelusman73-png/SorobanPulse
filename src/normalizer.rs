use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

/// A single normalization rule loaded from the DB.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct NormalizationRule {
    pub pointer: String,
    pub transform: String,
    pub params: Value,
}

/// Built-in transform names.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transform {
    DivideByDecimals,
    HexToDecimal,
    Base64Decode,
}

impl std::str::FromStr for Transform {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "divide_by_decimals" => Ok(Transform::DivideByDecimals),
            "hex_to_decimal" => Ok(Transform::HexToDecimal),
            "base64_decode" => Ok(Transform::Base64Decode),
            other => Err(format!("unknown transform: {other}")),
        }
    }
}

/// Apply a single transform to a JSON value, returning the transformed value.
pub fn apply_transform(
    transform: &Transform,
    params: &Value,
    value: &Value,
) -> Result<Value, String> {
    match transform {
        Transform::DivideByDecimals => {
            let decimals = params
                .get("decimals")
                .and_then(|v| v.as_u64())
                .ok_or("divide_by_decimals requires params.decimals (u64)")?;
            let raw = value
                .as_i64()
                .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
                .ok_or_else(|| format!("divide_by_decimals: expected integer, got {value}"))?;
            let divisor = 10_i64.pow(decimals as u32);
            // Return as a JSON number (f64) — sufficient for display purposes
            Ok(Value::from(raw as f64 / divisor as f64))
        }
        Transform::HexToDecimal => {
            let hex = value
                .as_str()
                .ok_or_else(|| format!("hex_to_decimal: expected string, got {value}"))?
                .trim_start_matches("0x");
            let n = u128::from_str_radix(hex, 16).map_err(|e| format!("hex_to_decimal: {e}"))?;
            // u128 may exceed f64 precision; store as string to preserve accuracy
            Ok(Value::String(n.to_string()))
        }
        Transform::Base64Decode => {
            let encoded = value
                .as_str()
                .ok_or_else(|| format!("base64_decode: expected string, got {value}"))?;
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|e| format!("base64_decode: {e}"))?;
            match std::str::from_utf8(&bytes) {
                Ok(s) => Ok(Value::String(s.to_owned())),
                Err(_) => {
                    // Not valid UTF-8 — return hex representation
                    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
                    Ok(Value::String(hex))
                }
            }
        }
    }
}

/// Resolve a JSON Pointer (RFC 6901) to a mutable reference within a Value.
fn pointer_get(value: &Value, pointer: &str) -> Option<Value> {
    if pointer.is_empty() || pointer == "/" {
        return Some(value.clone());
    }
    let mut current = value;
    for token in pointer.trim_start_matches('/').split('/') {
        let token = token.replace("~1", "/").replace("~0", "~");
        current = match current {
            Value::Object(map) => map.get(&token)?,
            Value::Array(arr) => {
                let idx: usize = token.parse().ok()?;
                arr.get(idx)?
            }
            _ => return None,
        };
    }
    Some(current.clone())
}

/// Set a value at a JSON Pointer path (mutates `root`).
fn pointer_set(root: &mut Value, pointer: &str, new_val: Value) {
    if pointer.is_empty() || pointer == "/" {
        *root = new_val;
        return;
    }
    let tokens: Vec<String> = pointer
        .trim_start_matches('/')
        .split('/')
        .map(|t| t.replace("~1", "/").replace("~0", "~"))
        .collect();
    let mut current = root;
    for (i, token) in tokens.iter().enumerate() {
        let is_last = i == tokens.len() - 1;
        current = match current {
            Value::Object(map) => {
                if is_last {
                    map.insert(token.clone(), new_val);
                    return;
                }
                map.entry(token.clone())
                    .or_insert(Value::Object(Default::default()))
            }
            Value::Array(arr) => {
                if let Ok(idx) = token.parse::<usize>() {
                    if is_last {
                        if idx < arr.len() {
                            arr[idx] = new_val;
                        }
                        return;
                    }
                    if idx < arr.len() {
                        &mut arr[idx]
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
            _ => return,
        };
    }
}

/// Run the normalization pipeline for a given contract and event_data.
/// Returns `None` if there are no rules for this contract or if event_data is null/empty.
pub fn normalize(rules: &[NormalizationRule], event_data: &Value) -> Option<Value> {
    if rules.is_empty() {
        return None;
    }
    
    // Handle null or empty event_data
    if event_data.is_null() || (event_data.is_object() && event_data.as_object().map_or(false, |o| o.is_empty())) {
        tracing::warn!("event_data is null or empty, skipping normalization");
        return None;
    }
    
    let mut normalized = event_data.clone();
    for rule in rules {
        let transform: Transform = match rule.transform.parse() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(transform = %rule.transform, error = %e, "Unknown transform, skipping");
                continue;
            }
        };
        let current = match pointer_get(&normalized, &rule.pointer) {
            Some(v) => v,
            None => {
                tracing::debug!(pointer = %rule.pointer, "JSON Pointer not found in event_data, skipping");
                continue;
            }
        };
        match apply_transform(&transform, &rule.params, &current) {
            Ok(new_val) => pointer_set(&mut normalized, &rule.pointer, new_val),
            Err(e) => {
                tracing::warn!(pointer = %rule.pointer, error = %e, "Transform failed, skipping")
            }
        }
    }
    Some(normalized)
}

/// Load normalization rules for a contract from the DB.
pub async fn load_rules(pool: &PgPool, contract_id: &str) -> Vec<NormalizationRule> {
    sqlx::query_as::<_, NormalizationRule>(
        "SELECT pointer, transform, params FROM normalization_rules WHERE contract_id = $1 ORDER BY created_at",
    )
    .bind(contract_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule(pointer: &str, transform: &str, params: Value) -> NormalizationRule {
        NormalizationRule {
            pointer: pointer.to_string(),
            transform: transform.to_string(),
            params,
        }
    }

    // --- divide_by_decimals ---

    #[test]
    fn divide_by_decimals_integer() {
        let t = Transform::DivideByDecimals;
        let result = apply_transform(&t, &json!({"decimals": 7}), &json!(10_000_000)).unwrap();
        assert_eq!(result, json!(1.0));
    }

    #[test]
    fn divide_by_decimals_string_input() {
        let t = Transform::DivideByDecimals;
        let result = apply_transform(&t, &json!({"decimals": 2}), &json!("500")).unwrap();
        assert_eq!(result, json!(5.0));
    }

    #[test]
    fn divide_by_decimals_missing_params() {
        let t = Transform::DivideByDecimals;
        assert!(apply_transform(&t, &json!({}), &json!(100)).is_err());
    }

    // --- hex_to_decimal ---

    #[test]
    fn hex_to_decimal_plain() {
        let t = Transform::HexToDecimal;
        let result = apply_transform(&t, &json!({}), &json!("ff")).unwrap();
        assert_eq!(result, json!("255"));
    }

    #[test]
    fn hex_to_decimal_with_prefix() {
        let t = Transform::HexToDecimal;
        let result = apply_transform(&t, &json!({}), &json!("0x1a")).unwrap();
        assert_eq!(result, json!("26"));
    }

    #[test]
    fn hex_to_decimal_invalid() {
        let t = Transform::HexToDecimal;
        assert!(apply_transform(&t, &json!({}), &json!("xyz")).is_err());
    }

    // --- base64_decode ---

    #[test]
    fn base64_decode_utf8() {
        let t = Transform::Base64Decode;
        // "hello" in standard base64
        let result = apply_transform(&t, &json!({}), &json!("aGVsbG8=")).unwrap();
        assert_eq!(result, json!("hello"));
    }

    #[test]
    fn base64_decode_binary_returns_hex() {
        let t = Transform::Base64Decode;
        // bytes [0xff, 0xfe] — not valid UTF-8
        let result = apply_transform(&t, &json!({}), &json!("//4=")).unwrap();
        assert_eq!(result, json!("fffe"));
    }

    #[test]
    fn base64_decode_invalid() {
        let t = Transform::Base64Decode;
        assert!(apply_transform(&t, &json!({}), &json!("!!!")).is_err());
    }

    // --- pipeline ---

    #[test]
    fn normalize_applies_rules_in_order() {
        let rules = vec![rule(
            "/value/amount",
            "divide_by_decimals",
            json!({"decimals": 2}),
        )];
        let data = json!({"value": {"amount": 1000}, "topic": []});
        let result = normalize(&rules, &data).unwrap();
        assert_eq!(result["value"]["amount"], json!(10.0));
        // original untouched fields preserved
        assert_eq!(result["topic"], json!([]));
    }

    #[test]
    fn normalize_no_rules_returns_none() {
        let data = json!({"value": {}, "topic": []});
        assert!(normalize(&[], &data).is_none());
    }

    #[test]
    fn normalize_missing_pointer_skips() {
        let rules = vec![rule("/value/nonexistent", "hex_to_decimal", json!({}))];
        let data = json!({"value": {}, "topic": []});
        // Should not panic, just skip
        let result = normalize(&rules, &data).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn normalize_null_event_data_returns_none() {
        let rules = vec![rule("/value/amount", "divide_by_decimals", json!({"decimals": 2}))];
        let data = Value::Null;
        assert!(normalize(&rules, &data).is_none());
    }

    #[test]
    fn normalize_empty_object_returns_none() {
        let rules = vec![rule("/value/amount", "divide_by_decimals", json!({"decimals": 2}))];
        let data = json!({});
        assert!(normalize(&rules, &data).is_none());
    }

    #[test]
    fn normalize_diagnostic_event_with_missing_keys() {
        let rules = vec![rule("/value", "hex_to_decimal", json!({}))];
        let data = json!({"topic": []});
        // Should not panic, just return the data as-is
        let result = normalize(&rules, &data);
        assert!(result.is_some());
    }
}
