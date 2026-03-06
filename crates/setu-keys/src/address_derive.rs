// Copyright (c) Hetu Project
// SPDX-License-Identifier: Apache-2.0

//! Address derivation utilities for Setu.
//!
//! Setu uses 32-byte addresses derived from public keys:
//! - From secp256k1 public keys: full Keccak256 hash (32 bytes)
//! - From Nostr public keys: SHA256 hash (32 bytes)
//!
//! Unlike Ethereum (which truncates to 20 bytes), Setu keeps the full
//! 32-byte hash for stronger collision resistance and alignment with
//! the internal `Address([u8; 32])` type used throughout the system.

use crate::error::KeyError;
use sha2::{Sha256, Digest as Sha2Digest};
use sha3::Keccak256;

/// Derive a 32-byte Setu address from an uncompressed secp256k1 public key.
///
/// Algorithm:
/// 1. Take the uncompressed public key (65 bytes: 0x04 || x || y)
/// 2. Remove the 0x04 prefix (64 bytes)
/// 3. Compute Keccak256 hash (32 bytes)
/// 4. Return the full 32-byte hash as the address
///
/// This is similar to Ethereum's derivation but keeps all 32 bytes
/// instead of truncating to the last 20.
pub fn derive_address_from_secp256k1(public_key: &[u8]) -> Result<[u8; 32], KeyError> {
    // Validate public key format
    if public_key.len() != 65 {
        return Err(KeyError::InvalidKeyFormat(format!(
            "Expected 65-byte uncompressed secp256k1 public key, got {} bytes",
            public_key.len()
        )));
    }

    if public_key[0] != 0x04 {
        return Err(KeyError::InvalidKeyFormat(
            "Public key must start with 0x04 (uncompressed format)".to_string()
        ));
    }

    // Remove the 0x04 prefix
    let key_without_prefix = &public_key[1..];

    // Compute Keccak256 hash — full 32 bytes
    let hash = Keccak256::digest(key_without_prefix);

    let mut address = [0u8; 32];
    address.copy_from_slice(&hash);

    Ok(address)
}

/// Derive a 32-byte Setu address from a Nostr public key (32-byte Schnorr x-only).
///
/// Algorithm:
/// 1. Take the Nostr public key (32 bytes, x-only Schnorr)
/// 2. Compute SHA256 hash (32 bytes)
/// 3. Return the full 32-byte hash as the address
pub fn derive_address_from_nostr_pubkey(nostr_pubkey: &[u8]) -> Result<[u8; 32], KeyError> {
    // Validate Nostr public key format
    if nostr_pubkey.len() != 32 {
        return Err(KeyError::InvalidKeyFormat(format!(
            "Expected 32-byte Nostr public key, got {} bytes",
            nostr_pubkey.len()
        )));
    }

    // Compute SHA256 hash — full 32 bytes
    let hash = Sha256::digest(nostr_pubkey);

    let mut address = [0u8; 32];
    address.copy_from_slice(&hash);

    Ok(address)
}

/// Format a 32-byte address as a hex string with 0x prefix.
pub fn address_to_hex(address: &[u8; 32]) -> String {
    format!("0x{}", hex::encode(address))
}

/// Parse a hex string (with or without 0x prefix) into a 32-byte address.
pub fn address_from_hex(s: &str) -> Result<[u8; 32], KeyError> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|e| KeyError::Decoding(e.to_string()))?;
    if bytes.len() != 32 {
        return Err(KeyError::Decoding(format!(
            "Invalid Setu address length: expected 32, got {}",
            bytes.len()
        )));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

/// Verify that an address was correctly derived from a Nostr public key.
pub fn verify_nostr_address_derivation(
    address: &str,
    nostr_pubkey: &[u8],
) -> Result<bool, KeyError> {
    let expected = derive_address_from_nostr_pubkey(nostr_pubkey)?;
    let provided = address_from_hex(address)?;
    Ok(expected == provided)
}

/// Verify that an address was correctly derived from a secp256k1 public key.
pub fn verify_secp256k1_address_derivation(
    address: &str,
    public_key: &[u8],
) -> Result<bool, KeyError> {
    let expected = derive_address_from_secp256k1(public_key)?;
    let provided = address_from_hex(address)?;
    Ok(expected == provided)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k256::ecdsa::{SigningKey, VerifyingKey};
    use k256::elliptic_curve::sec1::ToEncodedPoint;

    #[test]
    fn test_secp256k1_address_derivation() {
        // Generate a secp256k1 keypair
        let signing_key = SigningKey::random(&mut rand::thread_rng());
        let verifying_key = VerifyingKey::from(&signing_key);

        // Get uncompressed public key (65 bytes)
        let public_key = verifying_key.to_encoded_point(false);
        let public_key_bytes = public_key.as_bytes();

        // Derive 32-byte address
        let address = derive_address_from_secp256k1(public_key_bytes).unwrap();

        // Verify format: 32 bytes → "0x" + 64 hex chars
        assert_eq!(address.len(), 32);
        let hex = address_to_hex(&address);
        assert!(hex.starts_with("0x"));
        assert_eq!(hex.len(), 66); // 0x + 64 hex chars

        // Verify derivation round-trip
        assert!(verify_secp256k1_address_derivation(&hex, public_key_bytes).unwrap());
    }

    #[test]
    fn test_nostr_address_derivation() {
        let nostr_pubkey = [0x42u8; 32];

        let address = derive_address_from_nostr_pubkey(&nostr_pubkey).unwrap();

        assert_eq!(address.len(), 32);
        let hex = address_to_hex(&address);
        assert!(hex.starts_with("0x"));
        assert_eq!(hex.len(), 66);

        assert!(verify_nostr_address_derivation(&hex, &nostr_pubkey).unwrap());
    }

    #[test]
    fn test_deterministic_derivation() {
        let nostr_pubkey = [0x42u8; 32];
        let addr1 = derive_address_from_nostr_pubkey(&nostr_pubkey).unwrap();
        let addr2 = derive_address_from_nostr_pubkey(&nostr_pubkey).unwrap();
        assert_eq!(addr1, addr2);
    }

    #[test]
    fn test_different_keys_different_addresses() {
        let nostr_pubkey1 = [0x42u8; 32];
        let nostr_pubkey2 = [0x43u8; 32];

        let addr1 = derive_address_from_nostr_pubkey(&nostr_pubkey1).unwrap();
        let addr2 = derive_address_from_nostr_pubkey(&nostr_pubkey2).unwrap();

        assert_ne!(addr1, addr2);
    }

    #[test]
    fn test_invalid_nostr_pubkey_length() {
        let invalid_pubkey = [0x42u8; 31];
        let result = derive_address_from_nostr_pubkey(&invalid_pubkey);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_secp256k1_pubkey_length() {
        let invalid_pubkey = [0x04u8; 64];
        let result = derive_address_from_secp256k1(&invalid_pubkey);
        assert!(result.is_err());
    }

    #[test]
    fn test_address_hex_round_trip() {
        let addr = [0x42u8; 32];
        let hex = address_to_hex(&addr);
        let parsed = address_from_hex(&hex).unwrap();
        assert_eq!(addr, parsed);

        // Test without 0x prefix
        let hex_no_prefix = &hex[2..];
        let parsed2 = address_from_hex(hex_no_prefix).unwrap();
        assert_eq!(addr, parsed2);
    }

    #[test]
    fn test_secp256k1_deterministic() {
        let signing_key = SigningKey::random(&mut rand::thread_rng());
        let verifying_key = VerifyingKey::from(&signing_key);
        let pk_bytes = verifying_key.to_encoded_point(false);

        let addr1 = derive_address_from_secp256k1(pk_bytes.as_bytes()).unwrap();
        let addr2 = derive_address_from_secp256k1(pk_bytes.as_bytes()).unwrap();
        assert_eq!(addr1, addr2);
    }
}

