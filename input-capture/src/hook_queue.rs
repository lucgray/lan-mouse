//! Bounded hook-to-async handoff. Overflow is a session failure, never a
//! silently dropped key/button release. Only adjacent motion is coalesced.
use crate::{CaptureError, CaptureEvent, Position};
use futures::task::AtomicWaker;
use input_event::{Event, PointerEvent};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

#[cfg(any(windows, test))]
const CAPACITY: usize = 256;
const COALESCE_AFTER: usize = 32;
type Item = (Position, CaptureEvent);

struct Shared {
    capacity: usize,
    failure: fn() -> CaptureError,
    events: Mutex<VecDeque<Item>>,
    failed: AtomicBool,
    closed: AtomicBool,
    waker: AtomicWaker,
}

pub(crate) struct HookSender(Arc<Shared>);
pub(crate) struct HookReceiver {
    shared: Arc<Shared>,
    failure_reported: bool,
}

#[cfg(any(windows, test))]
pub(crate) fn channel() -> (HookSender, HookReceiver) {
    channel_with(CAPACITY, || CaptureError::HookQueueOverloaded)
}

#[cfg(x11)]
pub(crate) fn x11_channel() -> (HookSender, HookReceiver) {
    channel_with(64, || CaptureError::X11QueueOverloaded)
}

fn channel_with(capacity: usize, failure: fn() -> CaptureError) -> (HookSender, HookReceiver) {
    let shared = Arc::new(Shared {
        capacity,
        failure,
        events: Mutex::new(VecDeque::with_capacity(capacity)),
        failed: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        waker: AtomicWaker::new(),
    });
    (
        HookSender(shared.clone()),
        HookReceiver {
            shared,
            failure_reported: false,
        },
    )
}

impl HookSender {
    pub(crate) fn available(&self) -> bool {
        !self.0.failed.load(Ordering::Acquire) && !self.0.closed.load(Ordering::Acquire)
    }

    #[cfg(x11)]
    pub(crate) fn discard_pending(&self) {
        self.0.events.lock().unwrap().clear();
        // An overload latch survives release: discarding input is not recovery.
    }

    /// Never waits for consumer capacity. The mutex protects only a bounded
    /// push/pop or tail merge, with no I/O, callback, or await under the lock.
    pub(crate) fn send(&self, pos: Position, event: CaptureEvent) -> Result<(), ()> {
        if !self.available() {
            return Err(());
        }
        let mut events = self.0.events.lock().unwrap();
        let merged = if events.len() >= COALESCE_AFTER {
            match (events.back_mut(), &event) {
                (
                    Some((
                        old_pos,
                        CaptureEvent::Input(Event::Pointer(PointerEvent::Motion { time, dx, dy })),
                    )),
                    CaptureEvent::Input(Event::Pointer(PointerEvent::Motion {
                        time: new_time,
                        dx: new_dx,
                        dy: new_dy,
                    })),
                ) if *old_pos == pos => {
                    *time = *new_time;
                    *dx += new_dx;
                    *dy += new_dy;
                    true
                }
                _ => false,
            }
        } else {
            false
        };
        if !merged {
            if events.len() == self.0.capacity {
                self.0.failed.store(true, Ordering::Release);
                drop(events);
                self.0.waker.wake();
                return Err(());
            }
            events.push_back((pos, event));
        }
        drop(events);
        self.0.waker.wake();
        Ok(())
    }
}

impl Drop for HookSender {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
        self.0.waker.wake();
    }
}

impl HookReceiver {
    #[cfg(any(windows, x11))]
    pub(crate) fn failed(&self) -> bool {
        self.shared.failed.load(Ordering::Acquire)
    }

    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Item, CaptureError>>> {
        self.shared.waker.register(cx.waker());
        let mut events = self.shared.events.lock().unwrap();
        if self.shared.failed.load(Ordering::Acquire) {
            events.clear();
            if std::mem::replace(&mut self.failure_reported, true) {
                return Poll::Ready(None);
            }
            return Poll::Ready(Some(Err((self.shared.failure)())));
        }
        if let Some(event) = events.pop_front() {
            return Poll::Ready(Some(Ok(event)));
        }
        if self.shared.closed.load(Ordering::Acquire) {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for HookReceiver {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::poll_fn;
    use input_event::{BTN_LEFT, KeyboardEvent};

    fn motion(dx: f64) -> CaptureEvent {
        CaptureEvent::Input(Event::Pointer(PointerEvent::Motion {
            time: 1,
            dx,
            dy: -dx,
        }))
    }
    fn key(state: u8) -> CaptureEvent {
        CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key {
            time: 2,
            key: 29,
            state,
        }))
    }
    fn button(state: u32) -> CaptureEvent {
        CaptureEvent::Input(Event::Pointer(PointerEvent::Button {
            time: 3,
            button: BTN_LEFT,
            state,
        }))
    }

    #[tokio::test]
    async fn stalled_motion_burst_preserves_discrete_order_and_total_movement() {
        let (tx, mut rx) = channel();
        let mut expected_discrete = vec![CaptureEvent::Begin(0.5), key(1), button(1)];
        for event in &expected_discrete {
            tx.send(Position::Left, event.clone()).unwrap();
        }
        for _ in 0..8000 {
            tx.send(Position::Left, motion(0.25)).unwrap();
        }
        tx.send(Position::Left, button(0)).unwrap();
        tx.send(Position::Left, key(0)).unwrap();
        expected_discrete.extend([button(0), key(0)]);
        assert!(tx.0.events.lock().unwrap().len() <= COALESCE_AFTER + 2);
        drop(tx);
        let mut discrete = vec![];
        let mut total = (0., 0.);
        while let Some(event) = poll_fn(|cx| rx.poll_recv(cx)).await {
            match event.unwrap().1 {
                CaptureEvent::Input(Event::Pointer(PointerEvent::Motion { dx, dy, .. })) => {
                    total.0 += dx;
                    total.1 += dy;
                }
                event => discrete.push(event),
            }
        }
        assert_eq!(total, (2000., -2000.));
        assert_eq!(discrete, expected_discrete);
    }

    #[tokio::test]
    async fn motion_never_merges_across_button_or_target_boundary() {
        let (tx, mut rx) = channel();
        for _ in 0..COALESCE_AFTER {
            tx.send(Position::Left, key(1)).unwrap();
        }
        for (pos, event) in [
            (Position::Left, motion(1.)),
            (Position::Left, button(1)),
            (Position::Left, motion(2.)),
            (Position::Right, motion(3.)),
        ] {
            tx.send(pos, event).unwrap();
        }
        drop(tx);
        for _ in 0..COALESCE_AFTER {
            poll_fn(|cx| rx.poll_recv(cx)).await.unwrap().unwrap();
        }
        for expected in [
            (Position::Left, motion(1.)),
            (Position::Left, button(1)),
            (Position::Left, motion(2.)),
            (Position::Right, motion(3.)),
        ] {
            assert_eq!(
                poll_fn(|cx| rx.poll_recv(cx)).await.unwrap().unwrap(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn overflow_reports_failure_before_any_stale_event_and_latches_sender() {
        let (tx, mut rx) = channel();
        for _ in 0..CAPACITY {
            tx.send(Position::Left, key(1)).unwrap();
        }
        assert!(tx.send(Position::Left, key(0)).is_err());
        assert!(!tx.available());
        assert!(matches!(
            poll_fn(|cx| rx.poll_recv(cx)).await,
            Some(Err(CaptureError::HookQueueOverloaded))
        ));
        assert!(tx.send(Position::Right, CaptureEvent::Begin(0.5)).is_err());
        assert!(poll_fn(|cx| rx.poll_recv(cx)).await.is_none());
        assert!(tx.0.events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn receiver_drop_closes_sender() {
        let (tx, rx) = channel();
        drop(rx);
        assert!(!tx.available());
        assert!(tx.send(Position::Left, key(0)).is_err());
    }

    #[tokio::test]
    async fn concurrent_motion_and_producer_close_do_not_lose_final_sample() {
        let (tx, mut rx) = channel();
        let producer = std::thread::spawn(move || {
            for _ in 0..100_000 {
                tx.send(Position::Left, motion(0.25)).unwrap();
            }
        });
        let mut total = 0.;
        while let Some(event) = poll_fn(|cx| rx.poll_recv(cx)).await {
            let (_, CaptureEvent::Input(Event::Pointer(PointerEvent::Motion { dx, .. }))) =
                event.unwrap()
            else {
                panic!("unexpected event");
            };
            total += dx;
        }
        producer.join().unwrap();
        assert_eq!(total, 25_000.);
    }

    #[tokio::test]
    async fn consumer_is_woken_by_final_motion_without_another_hook_event() {
        let (tx, mut rx) = channel();
        let read = tokio::spawn(async move { poll_fn(|cx| rx.poll_recv(cx)).await });
        tokio::task::yield_now().await;
        tx.send(Position::Left, motion(0.5)).unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), read)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .unwrap(),
            (Position::Left, motion(0.5))
        );
    }
}
