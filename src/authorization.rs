//! Normalize trust independently from the original persisted configuration.
use lan_mouse_ipc::normalize_fingerprint;
use std::collections::{HashMap, hash_map::Entry};

pub(crate) struct AuthorizationConfig {
    pub trusted: HashMap<String, String>,
    pub invalid: usize,
    pub conflicts: usize,
}

impl AuthorizationConfig {
    pub fn new(raw: HashMap<String, String>) -> Self {
        let mut invalid = 0;
        let mut valid = Vec::with_capacity(raw.len());
        for (key, description) in raw {
            match normalize_fingerprint(&key) {
                Ok(canonical) => valid.push((canonical, key, description)),
                Err(_) => invalid += 1,
            }
        }
        // Group by digest, prefer its exact canonical key, then original spelling.
        // Description selection must not depend on HashMap iteration order.
        valid.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| (b.0 == b.1).cmp(&(a.0 == a.1)))
                .then_with(|| a.1.cmp(&b.1))
        });
        let mut trusted = HashMap::new();
        let mut conflicts = 0;
        for (key, _, description) in valid {
            match trusted.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(description);
                }
                Entry::Occupied(entry) => {
                    if entry.get() != &description {
                        conflicts += 1;
                    }
                }
            }
        }
        Self {
            trusted,
            invalid,
            conflicts,
        }
    }

    pub fn warning(&self) -> Option<String> {
        (self.invalid != 0 || self.conflicts != 0).then(|| format!(
            "Check [authorized_fingerprints] in the configuration: {} malformed key(s) are not trusted; {} alias description conflict(s). Exact canonical spelling wins, otherwise original keys are ordered lexicographically. The file is preserved until you explicitly edit these entries.",
            self.invalid, self.conflicts
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn aliases_are_trusted_once_and_description_selection_is_deterministic() {
        let canonical = vec!["ab"; 32].join(":");
        let entries = [
            (canonical.clone(), "canonical"),
            (canonical.to_uppercase(), "uppercase"),
            ("ab".repeat(32), "compact"),
            ("bad".into(), "invalid"),
        ];
        for reverse in [false, true] {
            for _ in 0..100 {
                let mut raw = HashMap::new();
                for index in 0..entries.len() {
                    let (key, description) = &entries[if reverse {
                        entries.len() - 1 - index
                    } else {
                        index
                    }];
                    raw.insert(key.clone(), description.to_string());
                }
                let parsed = AuthorizationConfig::new(raw);
                assert_eq!(
                    parsed.trusted,
                    HashMap::from([(canonical.clone(), "canonical".into())])
                );
                assert_eq!((parsed.invalid, parsed.conflicts), (1, 2));
                assert!(parsed.warning().is_some());
            }
        }
        let parsed = AuthorizationConfig::new(HashMap::from([
            (canonical.to_uppercase(), "uppercase".into()),
            ("ab".repeat(32), "compact".into()),
        ]));
        assert_eq!(parsed.trusted[&canonical], "uppercase");
    }
    #[test]
    fn invalid_values_never_enter_trust_and_identical_aliases_do_not_warn() {
        let canonical = vec!["01"; 32].join(":");
        let parsed = AuthorizationConfig::new(HashMap::from([
            (canonical.clone(), "peer".into()),
            ("01".repeat(32), "peer".into()),
        ]));
        assert_eq!(parsed.trusted.len(), 1);
        assert!(parsed.warning().is_none());
        let parsed = AuthorizationConfig::new(HashMap::from([
            ("".into(), "blank".into()),
            ("z".repeat(64), "bad".into()),
        ]));
        assert!(parsed.trusted.is_empty());
        assert_eq!(parsed.invalid, 2);
        assert!(AuthorizationConfig::new(HashMap::new()).warning().is_none());
    }
}
