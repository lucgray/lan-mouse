//! Keep the active authorization interaction stable while remote retries arrive.
use std::{
    collections::{HashSet, VecDeque},
    time::{Duration, Instant},
};

const MAX_PENDING: usize = 64;
const MAX_DISMISSED: usize = 128;
const DISMISS_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct AuthorizationQueue {
    active: Option<String>,
    pending: VecDeque<String>,
    dismissed: VecDeque<(String, Instant)>,
    known: HashSet<String>,
}

impl AuthorizationQueue {
    fn expire(&mut self, now: Instant) {
        while self
            .dismissed
            .front()
            .is_some_and(|(_, at)| now.duration_since(*at) >= DISMISS_INTERVAL)
        {
            self.dismissed.pop_front();
        }
    }

    pub(super) fn enqueue(&mut self, fingerprint: &str, now: Instant) {
        self.expire(now);
        if self.known.contains(fingerprint)
            || self.active.as_deref() == Some(fingerprint)
            || self.pending.iter().any(|key| key == fingerprint)
            || self.dismissed.iter().any(|(key, _)| key == fingerprint)
        {
            return;
        }
        if self.pending.len() == MAX_PENDING {
            self.pending.pop_front();
        }
        self.pending.push_back(fingerprint.into());
    }

    pub(super) fn next(&mut self) -> Option<String> {
        if self.active.is_some() {
            return None;
        }
        let next = self.pending.pop_front()?;
        self.active = Some(next.clone());
        Some(next)
    }

    pub(super) fn complete(&mut self, now: Instant) {
        self.expire(now);
        if let Some(active) = self.active.take() {
            if self.dismissed.len() == MAX_DISMISSED {
                self.dismissed.pop_front();
            }
            self.dismissed.push_back((active, now));
        }
    }

    pub(super) fn set_authorized(&mut self, known: HashSet<String>) -> bool {
        self.pending.retain(|key| !known.contains(key));
        let active_authorized = self.active.as_ref().is_some_and(|key| known.contains(key));
        self.known = known;
        active_authorized
    }

    pub(super) fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_flood_keeps_active_and_bounds_newest_pending_requests() {
        let mut queue = AuthorizationQueue::default();
        let now = Instant::now();
        queue.enqueue("active", now);
        assert_eq!(queue.next().as_deref(), Some("active"));
        for index in 0..10_000 {
            queue.enqueue("active", now);
            queue.enqueue(&format!("peer-{index}"), now);
        }
        assert_eq!(queue.active.as_deref(), Some("active"));
        assert_eq!(queue.pending.len(), MAX_PENDING);
        assert!(queue.next().is_none());
        queue.enqueue("peer-9999", now);
        assert_eq!(queue.pending.len(), MAX_PENDING);
        queue.complete(now);
        assert_eq!(queue.next().as_deref(), Some("peer-9936"));
        queue.complete(now);
        queue.enqueue("active", now + Duration::from_secs(29));
        assert!(!queue.pending.iter().any(|key| key == "active"));
        queue.enqueue("active", now + DISMISS_INTERVAL);
        assert_eq!(queue.pending.back().unwrap(), "active");
    }

    #[test]
    fn authorization_prunes_waiting_and_clear_removes_previous_session() {
        let mut queue = AuthorizationQueue::default();
        let now = Instant::now();
        for key in ["active", "already-authorized", "next"] {
            queue.enqueue(key, now);
        }
        queue.next();
        assert!(queue.set_authorized(HashSet::from([
            "active".into(),
            "already-authorized".into()
        ])));
        queue.complete(now);
        assert_eq!(queue.next().as_deref(), Some("next"));
        queue.complete(now);
        queue.clear();
        assert!(queue.pending.is_empty());
        assert!(queue.active.is_none());
        assert!(queue.dismissed.is_empty());
        assert!(queue.known.is_empty());
        queue.enqueue("active", now);
        assert_eq!(queue.next().as_deref(), Some("active"));
    }

    #[test]
    fn dismissed_history_is_bounded_even_during_fast_user_completions() {
        let mut queue = AuthorizationQueue::default();
        let now = Instant::now();
        for index in 0..10_000 {
            queue.enqueue(&format!("peer-{index}"), now);
            queue.next().unwrap();
            queue.complete(now);
        }
        assert_eq!(queue.dismissed.len(), MAX_DISMISSED);
        queue.enqueue("new", now + DISMISS_INTERVAL);
        assert!(queue.dismissed.is_empty());
    }
}
