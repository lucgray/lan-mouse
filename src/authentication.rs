//! Bounded authorization prompts sourced from the certificate verifier itself.
//! Independent of accept errors and the input event channels.
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Notify;

const MAX_PENDING: usize = 64;
const MAX_RECENT: usize = 128;
const REPEAT_INTERVAL: Duration = Duration::from_secs(2);
const DELIVERY_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Default)]
struct State {
    pending: VecDeque<String>,
    recent: VecDeque<(String, Instant)>,
    last_delivery: Option<Instant>,
}

impl State {
    fn expire(&mut self, now: Instant) {
        while self
            .recent
            .front()
            .is_some_and(|(_, at)| now.duration_since(*at) >= REPEAT_INTERVAL)
        {
            self.recent.pop_front();
        }
    }

    fn record(&mut self, fingerprint: String, now: Instant) -> bool {
        self.expire(now);
        if self.pending.contains(&fingerprint)
            || self.recent.iter().any(|(key, _)| key == &fingerprint)
        {
            return false;
        }
        if self.pending.len() == MAX_PENDING {
            // Keep recent requests usable when a frontend has been absent/busy.
            self.pending.pop_front();
        }
        self.pending.push_back(fingerprint);
        true
    }

    fn take(&mut self, now: Instant) -> Result<String, Option<Duration>> {
        self.expire(now);
        if self.pending.is_empty() {
            return Err(None);
        }
        if let Some(last) = self.last_delivery {
            if let Some(wait) = DELIVERY_INTERVAL.checked_sub(now.duration_since(last)) {
                if !wait.is_zero() {
                    return Err(Some(wait));
                }
            }
        }
        let fingerprint = self.pending.pop_front().expect("pending prompt");
        if self.recent.len() == MAX_RECENT {
            self.recent.pop_front();
        }
        self.recent.push_back((fingerprint.clone(), now));
        self.last_delivery = Some(now);
        Ok(fingerprint)
    }
}

#[derive(Clone, Default)]
pub(crate) struct AuthenticationNotices {
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
}

impl AuthenticationNotices {
    pub(crate) fn record(&self, fingerprint: String) {
        if self
            .state
            .lock()
            .expect("authentication notices")
            .record(fingerprint, Instant::now())
        {
            self.changed.notify_one();
        }
    }

    pub(crate) async fn next(&self) -> String {
        loop {
            // Register the wake before inspecting state. Only one service consumes.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let result = self
                .state
                .lock()
                .expect("authentication notices")
                .take(Instant::now());
            match result {
                Ok(fingerprint) => return fingerprint,
                Err(Some(wait)) => {
                    tokio::select! { _ = &mut changed => {}, _ = tokio::time::sleep(wait) => {} }
                }
                Err(None) => changed.await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_flood_is_bounded_and_preserves_newest_pending_requests() {
        let mut state = State::default();
        let now = Instant::now();
        for index in 0..10_000 {
            assert!(state.record(format!("fingerprint-{index}"), now));
        }
        assert_eq!(state.pending.len(), MAX_PENDING);
        assert_eq!(state.pending.front().unwrap(), "fingerprint-9936");
        assert_eq!(state.pending.back().unwrap(), "fingerprint-9999");
        for index in 0..10_000 {
            let at = now + DELIVERY_INTERVAL * index;
            state.record(format!("delivered-{index}"), at);
            state.take(at).unwrap();
            assert!(state.pending.len() <= MAX_PENDING);
            assert!(state.recent.len() <= MAX_RECENT);
        }
    }

    #[test]
    fn retries_do_not_slide_the_prompt_deadline_and_delivery_is_rate_limited() {
        let mut state = State::default();
        let now = Instant::now();
        assert!(state.record("peer".into(), now));
        assert!(!state.record("peer".into(), now));
        assert_eq!(state.take(now).unwrap(), "peer");
        for millis in [100, 500, 1000, 1999] {
            assert!(!state.record("peer".into(), now + Duration::from_millis(millis)));
        }
        assert!(state.record("peer".into(), now + REPEAT_INTERVAL));
        assert_eq!(state.take(now + REPEAT_INTERVAL).unwrap(), "peer");
        assert!(state.record("other".into(), now + REPEAT_INTERVAL));
        assert_eq!(
            state.take(now + REPEAT_INTERVAL),
            Err(Some(DELIVERY_INTERVAL))
        );
        assert_eq!(
            state
                .take(now + REPEAT_INTERVAL + DELIVERY_INTERVAL)
                .unwrap(),
            "other"
        );
        state.expire(now + REPEAT_INTERVAL * 3);
        assert!(state.recent.is_empty());
    }

    #[tokio::test]
    async fn canceled_wait_does_not_lose_queued_prompts_or_wakes() {
        let notices = AuthenticationNotices::default();
        assert!(
            tokio::time::timeout(Duration::from_millis(5), notices.next())
                .await
                .is_err()
        );
        notices.record("first".into());
        notices.record("second".into());
        assert_eq!(notices.next().await, "first");
        assert!(
            tokio::time::timeout(Duration::from_millis(5), notices.next())
                .await
                .is_err()
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), notices.next())
                .await
                .unwrap(),
            "second"
        );
    }
}
