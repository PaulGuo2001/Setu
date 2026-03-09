//! Storage key format constants and helpers.
//!
//! All state entries in SMT use structured key prefixes:
//! - `"oid:{64hex}"` — object data (Coin, Profile, etc.)
//! - `"user:{hex}:subnet:{id}"` — user registration metadata
//! - `"solver:{id}"` — solver registration metadata
//! - `"validator:{id}"` — validator registration metadata
//!
//! ## Key Categories
//!
//! | Prefix | Category | Key Format |
//! |--------|----------|-----------|
//! | `oid:` | Object data | `oid:{64 hex chars}` → direct decode to ObjectId |
//! | `user:` | User metadata | `user:{addr}:subnet:{id}` → BLAKE3(key) as SMT key |
//! | `solver:` | Solver metadata | `solver:{id}` → BLAKE3(key) as SMT key |
//! | `validator:` | Validator metadata | `validator:{id}` → BLAKE3(key) as SMT key |

use crate::object::ObjectId;

/// Key prefix for object data (Coin, Profile, etc.)
pub const KEY_PREFIX_OBJECT: &str = "oid:";

/// Key prefix for user registration metadata
pub const KEY_PREFIX_USER: &str = "user:";

/// Key prefix for solver registration metadata
pub const KEY_PREFIX_SOLVER: &str = "solver:";

/// Key prefix for validator registration metadata
pub const KEY_PREFIX_VALIDATOR: &str = "validator:";

/// Format an ObjectId as a canonical storage key: `"oid:{64hex}"`.
///
/// This is the only correct way to generate a storage key from an ObjectId.
/// All components (runtime, validator, storage) must use this function.
pub fn object_key(id: &ObjectId) -> String {
    format!("oid:{}", hex::encode(id.as_bytes()))
}

/// Format raw 32-byte ObjectId as a canonical storage key: `"oid:{64hex}"`.
pub fn object_key_from_bytes(id: &[u8; 32]) -> String {
    format!("oid:{}", hex::encode(id))
}

/// Parse an `"oid:{hex}"` key back to ObjectId.
///
/// Returns `None` if the key is not in the correct format.
pub fn parse_object_key(key: &str) -> Option<ObjectId> {
    let hex_str = key.strip_prefix(KEY_PREFIX_OBJECT)?;
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Some(ObjectId::new(arr))
}

/// Check if a key uses a known non-object prefix.
///
/// These keys are legitimate metadata entries stored in the SMT
/// using BLAKE3(key) as the SMT key (since they don't have a native 32-byte ID).
pub fn is_known_metadata_key(key: &str) -> bool {
    key.starts_with(KEY_PREFIX_USER)
        || key.starts_with(KEY_PREFIX_SOLVER)
        || key.starts_with(KEY_PREFIX_VALIDATOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_key_roundtrip() {
        let id = ObjectId::new([0xab; 32]);
        let key = object_key(&id);
        assert!(key.starts_with("oid:"));
        assert_eq!(key.len(), 4 + 64); // "oid:" + 64 hex chars

        let parsed = parse_object_key(&key).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_parse_invalid_key() {
        assert!(parse_object_key("object:abc").is_none());
        assert!(parse_object_key("oid:not_hex").is_none());
        assert!(parse_object_key("oid:abcd").is_none()); // too short
    }

    #[test]
    fn test_known_metadata_keys() {
        assert!(is_known_metadata_key("user:0xabc:subnet:ROOT"));
        assert!(is_known_metadata_key("solver:solver-1"));
        assert!(is_known_metadata_key("validator:val-1"));
        assert!(!is_known_metadata_key("oid:abcd"));
        assert!(!is_known_metadata_key("unknown:key"));
    }
}
