//! Unified hash utilities for Setu.
//!
//! This module provides the single source of truth for all hash operations in Setu.
//!
//! # Hash Function Policy
//!
//! - **BLAKE3** (`setu_hash`, `setu_hash_with_domain`): Default for all internal hashing.
//!   3-5x faster than SHA256 with equivalent 128-bit security.
//! - **SHA256** (`sha256_hash`): ONLY for external-standard-constrained contexts:
//!   - TEE attestation (AWS Nitro, Intel SGX)
//!   - Address derivation (Ethereum, Nostr)

use sha2::{Sha256, Digest as ShaDigest};

/// Standard 32-byte hash output.
pub type Hash = [u8; 32];

/// Zero hash constant.
pub const ZERO_HASH: Hash = [0u8; 32];

// =============================================================================
// Primary hash functions
// =============================================================================

/// Primary hash function for internal use (BLAKE3).
///
/// This is the default hash function for all new code.
/// 3-5x faster than SHA256 with equivalent 128-bit security.
#[inline]
pub fn setu_hash(data: &[u8]) -> Hash {
    let hash = blake3::hash(data);
    *hash.as_bytes()
}

/// Domain-separated hash (BLAKE3).
///
/// Adds a domain prefix to prevent cross-context hash collisions.
/// Each caller should use a unique domain string (e.g., `b"SETU_EVENT_ID:"`).
#[inline]
pub fn setu_hash_with_domain(domain: &[u8], data: &[u8]) -> Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(data);
    *hasher.finalize().as_bytes()
}

/// SHA256 hash — ONLY for external-standard-constrained contexts.
///
/// Use ONLY when required by external standards:
/// - TEE attestation (AWS Nitro, Intel SGX)
/// - Address derivation (Ethereum, Nostr)
///
/// For all other uses, prefer `setu_hash()` (BLAKE3).
#[inline]
pub fn sha256_hash(data: &[u8]) -> Hash {
    let result = Sha256::digest(data);
    let mut output = [0u8; 32];
    output.copy_from_slice(&result);
    output
}

// =============================================================================
// Canonical composite hash functions (single source of truth)
// =============================================================================

/// Chain hash: H(domain || prev_root || new_hash)
///
/// Single canonical implementation for anchor chain append operations.
/// Replaces the 3 duplicated implementations in anchor_builder, rocks/anchor_store,
/// and memory/anchor_store.
#[inline]
pub fn chain_hash(prev_root: &Hash, new_hash: &Hash) -> Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"SETU_ANCHOR_CHAIN:");
    hasher.update(prev_root);
    hasher.update(new_hash);
    *hasher.finalize().as_bytes()
}

/// Compute write-set commitment from state changes.
///
/// Single canonical implementation used by both Validator (tee_verifier) and
/// Enclave (stf) sides. Uses presence-byte serialization for completeness.
///
/// Each change is serialized as: key_bytes || presence(old) || old_value || presence(new) || new_value
/// where presence is `[1u8]` if Some, `[0u8]` if None.
pub fn compute_write_set_commitment(
    changes: &[(String, Option<Vec<u8>>, Option<Vec<u8>>)],
) -> Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"SETU_WRITE_SET_COMMITMENT:");
    for (key, old_value, new_value) in changes {
        hasher.update(key.as_bytes());
        match old_value {
            Some(v) => {
                hasher.update(&[1u8]);
                hasher.update(v);
            }
            None => {
                hasher.update(&[0u8]);
            }
        }
        match new_value {
            Some(v) => {
                hasher.update(&[1u8]);
                hasher.update(v);
            }
            None => {
                hasher.update(&[0u8]);
            }
        }
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setu_hash_deterministic() {
        let data = b"hello world";
        let h1 = setu_hash(data);
        let h2 = setu_hash(data);
        assert_eq!(h1, h2);
        assert_ne!(h1, ZERO_HASH);
    }

    #[test]
    fn test_setu_hash_different_inputs() {
        let h1 = setu_hash(b"hello");
        let h2 = setu_hash(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_domain_separation() {
        let data = b"same data";
        let h1 = setu_hash_with_domain(b"DOMAIN_A:", data);
        let h2 = setu_hash_with_domain(b"DOMAIN_B:", data);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_chain_hash_deterministic() {
        let prev = [1u8; 32];
        let new = [2u8; 32];
        let h1 = chain_hash(&prev, &new);
        let h2 = chain_hash(&prev, &new);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_chain_hash_order_matters() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        assert_ne!(chain_hash(&a, &b), chain_hash(&b, &a));
    }

    #[test]
    fn test_sha256_hash_deterministic() {
        let data = b"test";
        let h1 = sha256_hash(data);
        let h2 = sha256_hash(data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_write_set_commitment_deterministic() {
        let changes = vec![
            ("key1".to_string(), None, Some(b"value1".to_vec())),
            ("key2".to_string(), Some(b"old".to_vec()), Some(b"new".to_vec())),
        ];
        let h1 = compute_write_set_commitment(&changes);
        let h2 = compute_write_set_commitment(&changes);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_write_set_commitment_order_matters() {
        let changes_a = vec![
            ("key1".to_string(), None, Some(b"v1".to_vec())),
            ("key2".to_string(), None, Some(b"v2".to_vec())),
        ];
        let changes_b = vec![
            ("key2".to_string(), None, Some(b"v2".to_vec())),
            ("key1".to_string(), None, Some(b"v1".to_vec())),
        ];
        assert_ne!(
            compute_write_set_commitment(&changes_a),
            compute_write_set_commitment(&changes_b)
        );
    }
}
