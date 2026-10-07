//! Shared human-input validation; this does not change IPC serialization.
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error(
    "Enter a SHA-256 fingerprint: 32 hexadecimal byte pairs separated by colons, or 64 hexadecimal digits"
)]
pub struct FingerprintError;

/// Normalize pasted SHA-256 bytes without accepting omitted bytes or separators.
pub fn normalize_fingerprint(value: &str) -> Result<String, FingerprintError> {
    let bytes = value.trim().as_bytes();
    let separated = match bytes.len() {
        64 => false,
        95 => true,
        _ => return Err(FingerprintError),
    };
    let mut normalized = String::with_capacity(95);
    for index in 0..32 {
        let offset = index * if separated { 3 } else { 2 };
        let pair = &bytes[offset..offset + 2];
        if !pair.iter().all(u8::is_ascii_hexdigit)
            || (separated && index < 31 && bytes[offset + 2] != b':')
        {
            return Err(FingerprintError);
        }
        if index != 0 {
            normalized.push(':');
        }
        normalized.push(pair[0].to_ascii_lowercase() as char);
        normalized.push(pair[1].to_ascii_lowercase() as char);
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fingerprint_normalization_preserves_exact_digest_bytes() {
        let canonical = (0u8..32)
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join(":");
        for value in [
            canonical.clone(),
            canonical.to_uppercase(),
            canonical.replace(':', ""),
            format!(" \n{}\t", canonical.to_uppercase()),
        ] {
            assert_eq!(normalize_fingerprint(&value).unwrap(), canonical);
        }
    }
    #[test]
    fn invalid_digests_and_lossy_separator_repair_are_rejected() {
        for value in [
            String::new(),
            "aa".into(),
            "a".repeat(63),
            "a".repeat(65),
            "gg".repeat(32),
            "é".repeat(32),
            "aa:".repeat(32),
            "aa-".repeat(31) + "aa",
            "aa::".repeat(31) + "aa",
            "aa: ".repeat(31) + "aa",
            "SHA256:".to_owned() + &"a".repeat(64),
            "a".repeat(1_000_000),
        ] {
            assert!(
                normalize_fingerprint(&value).is_err(),
                "accepted malformed fingerprint"
            );
        }
    }
}
