//! Bounded input queue with adjacent relative-motion coalescing.
//!
//! The consumer owns delivery: the final movement must remain readable even
//! when the mouse stops. Buttons, keys and capture boundaries keep their order.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::task::AtomicWaker;
use input_event::{Event, PointerEvent};
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};

use crate::{CaptureEvent, Position};

type Item = (Position, CaptureEvent);

struct State {
    events: VecDeque<Item>,
    capacity: usize,
    sender_open: bool,
    receiver_open: bool,
}

struct Shared {
    state: Mutex<State>,
    waker: AtomicWaker,
}

pub(super) struct Sender(Arc<Shared>);
pub(super) struct Receiver(Arc<Shared>);

pub(super) fn channel(capacity: usize) -> (Sender, Receiver) {
    assert!(capacity > 0);
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            events: VecDeque::with_capacity(capacity),
            capacity,
            sender_open: true,
            receiver_open: true,
        }),
        waker: AtomicWaker::new(),
    });
    (Sender(shared.clone()), Receiver(shared))
}

impl Sender {
    pub(super) fn try_send(&self, item: Item) -> Result<(), TrySendError<Item>> {
        let mut state = self.0.state.lock().expect("input queue poisoned");
        if !state.receiver_open {
            return Err(TrySendError::Closed(item));
        }
        if let Some(last) = state.events.back_mut() {
            if merge_motion(last, &item) {
                drop(state);
                self.0.waker.wake();
                return Ok(());
            }
        }
        if state.events.len() == state.capacity {
            return Err(TrySendError::Full(item));
        }
        state.events.push_back(item);
        drop(state);
        self.0.waker.wake();
        Ok(())
    }
}

fn merge_motion(last: &mut Item, next: &Item) -> bool {
    if last.0 != next.0 {
        return false;
    }
    match (&mut last.1, &next.1) {
        (
            CaptureEvent::Input(Event::Pointer(PointerEvent::Motion { time, dx, dy })),
            CaptureEvent::Input(Event::Pointer(PointerEvent::Motion {
                time: next_time,
                dx: next_dx,
                dy: next_dy,
            })),
        ) => {
            *time = *next_time;
            *dx += next_dx;
            *dy += next_dy;
            true
        }
        _ => false,
    }
}

impl Receiver {
    pub(super) fn try_recv(&mut self) -> Result<Item, TryRecvError> {
        let mut state = self.0.state.lock().expect("input queue poisoned");
        state.events.pop_front().ok_or(if state.sender_open {
            TryRecvError::Empty
        } else {
            TryRecvError::Disconnected
        })
    }

    pub(super) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Item>> {
        // Register before checking the queue so an enqueue cannot lose a wakeup.
        self.0.waker.register(cx.waker());
        match self.try_recv() {
            Ok(item) => Poll::Ready(Some(item)),
            Err(TryRecvError::Empty) => Poll::Pending,
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
        }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("input queue poisoned")
            .sender_open = false;
        self.0.waker.wake();
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("input queue poisoned")
            .receiver_open = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::{ArcWake, waker};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn motion(dx: f64, dy: f64) -> Item {
        (
            Position::Right,
            CaptureEvent::Input(Event::Pointer(PointerEvent::Motion { time: 0, dx, dy })),
        )
    }

    fn delta(item: Item) -> (f64, f64) {
        match item.1 {
            CaptureEvent::Input(Event::Pointer(PointerEvent::Motion { dx, dy, .. })) => (dx, dy),
            _ => panic!("expected motion"),
        }
    }

    #[test]
    fn stalled_consumer_retains_final_motion_without_another_input() {
        let (tx, mut rx) = channel(10);
        tx.try_send((Position::Right, CaptureEvent::Begin(0.25)))
            .unwrap();
        for _ in 0..100_000 {
            tx.try_send(motion(1., -0.5)).unwrap();
        }
        assert_eq!(tx.0.state.lock().unwrap().events.len(), 2);
        assert!(matches!(
            rx.try_recv().unwrap().1,
            CaptureEvent::Begin(0.25)
        ));
        assert_eq!(delta(rx.try_recv().unwrap()), (100_000., -50_000.));
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn motion_does_not_cross_buttons_or_capture_boundaries() {
        let (tx, mut rx) = channel(10);
        tx.try_send(motion(3., 4.)).unwrap();
        tx.try_send((
            Position::Right,
            CaptureEvent::Input(Event::Pointer(PointerEvent::Button {
                time: 0,
                button: input_event::BTN_LEFT,
                state: 1,
            })),
        ))
        .unwrap();
        tx.try_send(motion(-1., 2.)).unwrap();
        tx.try_send((Position::Right, CaptureEvent::End)).unwrap();
        tx.try_send(motion(8., 9.)).unwrap();
        assert_eq!(delta(rx.try_recv().unwrap()), (3., 4.));
        assert!(matches!(
            rx.try_recv().unwrap().1,
            CaptureEvent::Input(Event::Pointer(PointerEvent::Button { .. }))
        ));
        assert_eq!(delta(rx.try_recv().unwrap()), (-1., 2.));
        assert!(matches!(rx.try_recv().unwrap().1, CaptureEvent::End));
        assert_eq!(delta(rx.try_recv().unwrap()), (8., 9.));
    }

    #[test]
    fn motion_for_different_edges_is_not_merged() {
        let (tx, mut rx) = channel(2);
        tx.try_send(motion(1., 0.)).unwrap();
        let mut other = motion(2., 0.);
        other.0 = Position::Left;
        tx.try_send(other).unwrap();
        assert_eq!(delta(rx.try_recv().unwrap()), (1., 0.));
        assert_eq!(delta(rx.try_recv().unwrap()), (2., 0.));
    }

    #[test]
    fn critical_overload_and_closed_receiver_are_reported() {
        let (tx, rx) = channel(1);
        tx.try_send((Position::Right, CaptureEvent::Begin(0.5)))
            .unwrap();
        assert!(matches!(
            tx.try_send(motion(1., 0.)),
            Err(TrySendError::Full(_))
        ));
        drop(rx);
        assert!(matches!(
            tx.try_send(motion(1., 0.)),
            Err(TrySendError::Closed(_))
        ));
    }

    #[derive(Default)]
    struct WakeCount(AtomicUsize);
    impl ArcWake for WakeCount {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn enqueue_and_sender_close_wake_idle_consumer() {
        let (tx, mut rx) = channel(1);
        let count = Arc::new(WakeCount::default());
        let w = waker(count.clone());
        let mut cx = Context::from_waker(&w);
        assert!(rx.poll_recv(&mut cx).is_pending());
        tx.try_send(motion(1., 2.)).unwrap();
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
        assert!(rx.poll_recv(&mut cx).is_ready());
        assert!(rx.poll_recv(&mut cx).is_pending());
        drop(tx);
        assert_eq!(count.0.load(Ordering::Relaxed), 2);
        assert!(matches!(rx.poll_recv(&mut cx), Poll::Ready(None)));
    }
}
