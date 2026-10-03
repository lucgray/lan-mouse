//! Keep unsubmitted text and the latest outstanding submission separate from
//! the daemon's confirmed value, so stale status updates cannot erase edits.
#[derive(Default)]
pub(super) struct Draft<T> {
    pending: Option<T>,
    awaiting: Option<T>,
}

impl<T: Clone + PartialEq> Draft<T> {
    pub(super) fn stage(&mut self, value: T) {
        self.pending = Some(value);
    }

    pub(super) fn submit(&mut self, confirmed: &T) -> Option<T> {
        let value = self.pending.take()?;
        // If another value is still awaiting acknowledgement, returning to the
        // confirmed value is a new submission that must supersede it.
        if self.awaiting.is_none() && &value == confirmed {
            return None;
        }
        if self.awaiting.as_ref() == Some(&value) {
            return None;
        }
        self.awaiting = Some(value.clone());
        Some(value)
    }

    pub(super) fn accept(&mut self, value: &T) -> bool {
        if self.pending.is_some()
            || self
                .awaiting
                .as_ref()
                .is_some_and(|expected| expected != value)
        {
            return false;
        }
        self.awaiting = None;
        true
    }

    pub(super) fn clear(&mut self) {
        self.pending = None;
        self.awaiting = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_submits_latest_value_and_ignores_old_state_until_acknowledged() {
        let mut draft = Draft::default();
        for value in ["h", "ho", "host.local"] {
            draft.stage(value.to_string());
        }
        assert!(!draft.accept(&"old.local".into()));
        assert_eq!(draft.submit(&"old.local".into()), Some("host.local".into()));
        assert!(!draft.accept(&"old.local".into()));
        assert!(draft.accept(&"host.local".into()));
        assert!(draft.accept(&"external.local".into()));
    }

    #[test]
    fn returning_to_confirmed_value_supersedes_unacknowledged_edit() {
        let mut draft = Draft::default();
        draft.stage(2u16);
        assert_eq!(draft.submit(&1), Some(2));
        draft.stage(1);
        assert_eq!(draft.submit(&1), Some(1));
        assert!(!draft.accept(&2));
        assert!(draft.accept(&1));
        draft.stage(1);
        assert_eq!(draft.submit(&1), None);
    }

    #[test]
    fn deletion_discards_draft_and_duplicate_submissions_are_suppressed() {
        let mut draft = Draft::default();
        draft.stage(5u16);
        assert_eq!(draft.submit(&1), Some(5));
        draft.stage(5);
        assert_eq!(draft.submit(&1), None);
        draft.stage(7);
        draft.clear();
        assert_eq!(draft.submit(&1), None);
        assert!(draft.accept(&1));
    }
}
