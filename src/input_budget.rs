use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

const GLOBAL_INPUT_LIMIT: usize = 256;
const PEER_INPUT_LIMIT: usize = 64;
const GLOBAL_CONTROL_LIMIT: usize = 128;
const PEER_CONTROL_LIMIT: usize = 32;
const CONTROL_ADMISSION_TIMEOUT: Duration = Duration::from_millis(250);

const MAX_INPUT_AGE: Duration = Duration::from_millis(50);

/// Bounds decoded control/clipboard frames through listener and Service queues.
/// It carries no input freshness deadline; control messages are never silently
/// discarded as stale keyboard/motion input.
pub(crate) struct ControlLease {
    _global: OwnedSemaphorePermit,
    _peer: OwnedSemaphorePermit,
}

struct InputReservation {
    _global: OwnedSemaphorePermit,
    _peer: OwnedSemaphorePermit,
}

/// Additional owner for Service notices derived from an input frame.
#[derive(Clone)]
pub(crate) struct InputAdmission {
    _reservation: Arc<InputReservation>,
}

/// Admission follows the frame through listener, proxy and derived Service
/// notices. It is released after the final delivery/rejection owner drops.
pub(crate) struct InputLease {
    deadline: Instant,
    cancellation: CancellationToken,
    identity: Arc<()>,
    owned: Option<InputReservation>,
    shared: Option<InputAdmission>,
}

impl InputLease {
    // Promote only frames that derive additional queued notices. Ordinary
    // input keeps its directly owned permits without another Arc allocation.
    pub(crate) fn share_admission(&mut self) -> InputAdmission {
        let owned = &mut self.owned;
        self.shared
            .get_or_insert_with(|| InputAdmission {
                _reservation: Arc::new(
                    owned
                        .take()
                        .expect("input reservation has one owner before sharing"),
                ),
            })
            .clone()
    }

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
    control_global: Arc<Semaphore>,
    control_peer: Arc<Semaphore>,
}

impl Default for InputBudget {
    fn default() -> Self {
        Self {
            global: Arc::new(Semaphore::new(GLOBAL_INPUT_LIMIT)),
            peer: Arc::new(Semaphore::new(PEER_INPUT_LIMIT)),
            identity: Arc::new(()),
            timeout: Duration::from_millis(250),
            control_global: Arc::new(Semaphore::new(GLOBAL_CONTROL_LIMIT)),
            control_peer: Arc::new(Semaphore::new(PEER_CONTROL_LIMIT)),
        }
    }
}

impl InputBudget {
    pub(crate) fn for_peer(&self) -> Self {
        Self {
            control_peer: Arc::new(Semaphore::new(PEER_CONTROL_LIMIT)),
            peer: Arc::new(Semaphore::new(PEER_INPUT_LIMIT)),
            identity: Arc::new(()),
            ..self.clone()
        }
    }

    pub(crate) async fn acquire_control(
        &self,
        cancellation: &CancellationToken,
    ) -> Option<ControlLease> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => None,
            result = tokio::time::timeout(CONTROL_ADMISSION_TIMEOUT, async {
                let peer = self.control_peer.clone().acquire_owned().await.ok()?;
                let global = self.control_global.clone().acquire_owned().await.ok()?;
                Some(ControlLease { _global: global, _peer: peer })
            }) => result.ok().flatten(),
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
                Some(InputLease { deadline, cancellation: cancellation.clone(), identity: self.identity.clone(), owned: Some(InputReservation { _global: global, _peer: peer }), shared: None })
            }) => result.ok().flatten(),
        }
    }

    #[cfg(test)]
    pub(crate) fn control_available(&self) -> (usize, usize) {
        (
            self.control_global.available_permits(),
            self.control_peer.available_permits(),
        )
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
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn derived_input_notices_share_one_reservation_without_renewing_deadline() {
        let budget = InputBudget::with_limits(1, 1, Duration::from_millis(50));
        let token = CancellationToken::new();
        let mut lease = budget.acquire(&token).await.unwrap();
        let deadline = lease.deadline();
        let first = lease.share_admission();
        let second = lease.share_admission();
        assert_eq!(lease.deadline(), deadline);
        assert_eq!(budget.available(), (0, 0));
        drop(first);
        assert_eq!(budget.available(), (0, 0));
        token.cancel();
        assert!(lease.cancellation().is_cancelled());
        drop(lease);
        assert_eq!(
            budget.available(),
            (0, 0),
            "cancellation cannot free still-queued notice capacity"
        );
        drop(second);
        assert_eq!(budget.available(), (1, 1));
    }

    #[tokio::test]
    async fn control_capacity_is_global_with_peer_isolation_and_cancellation() {
        let budget = InputBudget::default();
        let token = CancellationToken::new();
        let first = budget.for_peer();
        let mut held = Vec::new();
        for _ in 0..32 {
            held.push(first.acquire_control(&token).await.unwrap());
        }
        assert_eq!(first.control_peer.available_permits(), 0);
        assert_eq!(budget.control_global.available_permits(), 96);
        let canceled = CancellationToken::new();
        let waiting = first.acquire_control(&canceled);
        let cancel = async {
            tokio::task::yield_now().await;
            canceled.cancel();
        };
        let (lease, ()) = tokio::join!(waiting, cancel);
        assert!(lease.is_none());
        for _ in 0..3 {
            let peer = budget.for_peer();
            for _ in 0..32 {
                held.push(peer.acquire_control(&token).await.unwrap());
            }
        }
        assert_eq!(budget.control_global.available_permits(), 0);
        assert!(budget.for_peer().acquire_control(&token).await.is_none());
        // Saturating control traffic cannot take input queue capacity.
        assert_eq!(budget.available(), (256, 64));
        let input = budget.acquire(&token).await.unwrap();
        drop(held);
        assert_eq!(budget.control_global.available_permits(), 128);
        assert_eq!(first.control_peer.available_permits(), 32);
        let lease = first.acquire_control(&token).await.unwrap();
        drop((lease, input));
    }

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
