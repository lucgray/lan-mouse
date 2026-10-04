use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

const GLOBAL_INPUT_LIMIT: usize = 256;
const PEER_INPUT_LIMIT: usize = 64;
const MAX_INPUT_AGE: Duration = Duration::from_millis(50);

/// Admission follows the frame through both listener and proxy queues, and is
/// released only after delivery (or rejection), not merely after forwarding.
pub(crate) struct InputLease {
    deadline: Instant,
    cancellation: CancellationToken,
    identity: Arc<()>,
    _global: OwnedSemaphorePermit,
    _peer: OwnedSemaphorePermit,
}

impl InputLease {
    pub(crate) fn identity(&self) -> std::sync::Weak<()> {
        Arc::downgrade(&self.identity)
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
    pub(crate) fn cancel(&self) -> bool {
        if self.cancellation.is_cancelled() {
            false
        } else {
            self.cancellation.cancel();
            true
        }
    }
}

#[derive(Clone)]
pub(crate) struct InputBudget {
    global: Arc<Semaphore>,
    peer: Arc<Semaphore>,
    identity: Arc<()>,
    timeout: Duration,
}

impl Default for InputBudget {
    fn default() -> Self {
        Self {
            global: Arc::new(Semaphore::new(GLOBAL_INPUT_LIMIT)),
            peer: Arc::new(Semaphore::new(PEER_INPUT_LIMIT)),
            identity: Arc::new(()),
            timeout: Duration::from_millis(250),
        }
    }
}

impl InputBudget {
    pub(crate) fn for_peer(&self) -> Self {
        Self {
            peer: Arc::new(Semaphore::new(PEER_INPUT_LIMIT)),
            identity: Arc::new(()),
            ..self.clone()
        }
    }

    pub(crate) async fn acquire(&self, cancellation: &CancellationToken) -> Option<InputLease> {
        let deadline = Instant::now() + MAX_INPUT_AGE;
        let admission_deadline = deadline.min(Instant::now() + self.timeout);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => None,
            result = tokio::time::timeout_at(admission_deadline, async {
                let peer = self.peer.clone().acquire_owned().await.ok()?;
                let global = self.global.clone().acquire_owned().await.ok()?;
                Some(InputLease { deadline, cancellation: cancellation.clone(), identity: self.identity.clone(), _global: global, _peer: peer })
            }) => result.ok().flatten(),
        }
    }

    #[cfg(test)]
    pub(crate) fn available(&self) -> (usize, usize) {
        (
            self.global.available_permits(),
            self.peer.available_permits(),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_limits(global: usize, peer: usize, timeout: Duration) -> Self {
        Self {
            global: Arc::new(Semaphore::new(global)),
            peer: Arc::new(Semaphore::new(peer)),
            identity: Arc::new(()),
            timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cloned_peer_keeps_identity_and_replacement_gets_a_fresh_identity() {
        let peer = InputBudget::default();
        let clone = peer.clone();
        let replacement = peer.for_peer();
        let token = CancellationToken::new();
        let a = peer.acquire(&token).await.unwrap();
        let b = clone.acquire(&token).await.unwrap();
        let c = replacement.acquire(&token).await.unwrap();
        assert!(a.identity().ptr_eq(&b.identity()));
        assert!(!a.identity().ptr_eq(&c.identity()));
        assert_eq!(peer.available(), (253, 62));
        assert_eq!(replacement.available(), (253, 63));
        let old = a.identity();
        drop((a, b, peer, clone));
        assert_eq!(old.strong_count(), 0);
        assert!(
            !old.ptr_eq(&c.identity()),
            "dead old allocation must not alias live replacement"
        );
    }

    #[tokio::test]
    async fn shared_global_limit_peer_isolation_and_cancellation_release_capacity() {
        let budget = InputBudget::with_limits(3, 2, Duration::from_millis(10));
        let other = InputBudget {
            peer: Arc::new(Semaphore::new(2)),
            identity: Arc::new(()),
            ..budget.clone()
        };
        let token = CancellationToken::new();
        let a = budget.acquire(&token).await.unwrap();
        let b = budget.acquire(&token).await.unwrap();
        assert!(budget.acquire(&token).await.is_none());
        let c = other.acquire(&token).await.unwrap();
        assert_eq!(budget.global.available_permits(), 0);
        assert!(other.acquire(&token).await.is_none());
        assert_eq!(
            other.peer.available_permits(),
            1,
            "failed global wait releases its peer slot"
        );
        drop(a);
        let d = other.acquire(&token).await.unwrap();
        token.cancel();
        assert!(budget.acquire(&token).await.is_none());
        drop((b, c, d));
        assert_eq!(budget.global.available_permits(), 3);
        assert_eq!(budget.peer.available_permits(), 2);
        assert_eq!(other.peer.available_permits(), 2);
    }
}
