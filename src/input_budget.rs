use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

const GLOBAL_INPUT_LIMIT: usize = 256;
const PEER_INPUT_LIMIT: usize = 64;

/// Admission follows the frame through both listener and proxy queues, and is
/// released only after delivery (or rejection), not merely after forwarding.
pub(crate) struct InputLease {
    _global: OwnedSemaphorePermit,
    _peer: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub(crate) struct InputBudget {
    global: Arc<Semaphore>,
    peer: Arc<Semaphore>,
    timeout: Duration,
}

impl Default for InputBudget {
    fn default() -> Self {
        Self {
            global: Arc::new(Semaphore::new(GLOBAL_INPUT_LIMIT)),
            peer: Arc::new(Semaphore::new(PEER_INPUT_LIMIT)),
            timeout: Duration::from_millis(250),
        }
    }
}

impl InputBudget {
    pub(crate) fn for_peer(&self) -> Self {
        Self {
            peer: Arc::new(Semaphore::new(PEER_INPUT_LIMIT)),
            ..self.clone()
        }
    }

    pub(crate) async fn acquire(&self, cancellation: &CancellationToken) -> Option<InputLease> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => None,
            result = tokio::time::timeout(self.timeout, async {
                let peer = self.peer.clone().acquire_owned().await.ok()?;
                let global = self.global.clone().acquire_owned().await.ok()?;
                Some(InputLease { _global: global, _peer: peer })
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
            timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_global_limit_peer_isolation_and_cancellation_release_capacity() {
        let budget = InputBudget::with_limits(3, 2, Duration::from_millis(10));
        let other = InputBudget {
            peer: Arc::new(Semaphore::new(2)),
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
