use std::collections::HashSet;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use crate::hook_queue::{HookReceiver, HookSender, x11_channel};
use async_trait::async_trait;
use futures_core::Stream;

use x11::xlib::{
    ButtonMotionMask, ButtonPress, ButtonPressMask, ButtonRelease, ButtonReleaseMask, CurrentTime,
    Display, False, GrabModeAsync, GrabSuccess, KeyPress, KeyRelease, MotionNotify,
    PointerMotionMask, Window, XButtonEvent, XCloseDisplay, XDefaultRootWindow, XDefaultScreen,
    XDisplayHeight, XDisplayWidth, XEvent, XFlush, XGrabKeyboard, XGrabPointer, XKeyEvent,
    XMotionEvent, XNextEvent, XPending, XQueryPointer, XUngrabKeyboard, XUngrabPointer,
    XWarpPointer,
};

use input_event::{Event, KeyboardEvent, PointerEvent};

use super::{Capture, CaptureError, CaptureEvent, Position, error::X11InputCaptureCreationError};

// ── Request enum (async → thread) ────────────────────────────────────────────

enum Request {
    Create(Position),
    Destroy(Position),
    Release,
    Terminate,
}

// ── Internal thread state ─────────────────────────────────────────────────────

struct X11State {
    display: *mut Display,
    root: Window,
    screen_w: i32,
    screen_h: i32,
    clients: HashSet<Position>,
    active_client: Option<Position>,
    entry_point: (i32, i32),
    prev_pos: (i32, i32),
    event_tx: HookSender,
    release_grabs: fn(&mut X11State),
    #[cfg(test)]
    release_calls: usize,
    request_rx: mpsc::Receiver<Request>,
}

// Safety: display is only accessed from the dedicated X11 thread.
unsafe impl Send for X11State {}

// ── Public struct ─────────────────────────────────────────────────────────────

pub struct X11InputCapture {
    event_rx: HookReceiver,
    request_tx: mpsc::SyncSender<Request>,
    thread: Option<thread::JoinHandle<()>>,
}

impl X11InputCapture {
    pub fn new() -> Result<Self, X11InputCaptureCreationError> {
        let display = unsafe { x11::xlib::XOpenDisplay(std::ptr::null()) };
        if display.is_null() {
            return Err(X11InputCaptureCreationError::OpenDisplayFailed);
        }

        let screen = unsafe { XDefaultScreen(display) };
        let root = unsafe { XDefaultRootWindow(display) };
        let screen_w = unsafe { XDisplayWidth(display, screen) };
        let screen_h = unsafe { XDisplayHeight(display, screen) };

        let (event_tx, event_rx) = x11_channel();
        let (request_tx, request_rx) = mpsc::sync_channel(16);
        let (ready_tx, ready_rx) = mpsc::channel::<()>();

        let state = X11State {
            display,
            root,
            screen_w,
            screen_h,
            clients: HashSet::new(),
            active_client: None,
            entry_point: (0, 0),
            prev_pos: (0, 0),
            event_tx,
            release_grabs: release_native_grabs,
            #[cfg(test)]
            release_calls: 0,
            request_rx,
        };

        let thread = thread::spawn(move || {
            ready_tx.send(()).expect("ready channel closed");
            run_event_loop(state);
        });

        ready_rx.recv().expect("ready channel closed");

        Ok(Self {
            event_rx,
            request_tx,
            thread: Some(thread),
        })
    }
}

impl Drop for X11InputCapture {
    fn drop(&mut self) {
        let _ = self.request_tx.send(Request::Terminate);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

// ── Async trait impl ──────────────────────────────────────────────────────────

#[async_trait]
impl Capture for X11InputCapture {
    fn pending_failure(&self) -> bool {
        self.event_rx.failed()
    }

    async fn create(&mut self, pos: Position) -> Result<(), CaptureError> {
        let _ = self.request_tx.send(Request::Create(pos));
        Ok(())
    }

    async fn destroy(&mut self, pos: Position) -> Result<(), CaptureError> {
        let _ = self.request_tx.send(Request::Destroy(pos));
        Ok(())
    }

    async fn set_enter_only(&mut self, _pos: Position, _enabled: bool) -> Result<(), CaptureError> {
        Ok(())
    }

    async fn release(&mut self) -> Result<(), CaptureError> {
        let _ = self.request_tx.send(Request::Release);
        Ok(())
    }

    async fn release_to(&mut self, _t: f64) -> Result<(), CaptureError> {
        Ok(())
    }

    async fn terminate(&mut self) -> Result<(), CaptureError> {
        let _ = self.request_tx.send(Request::Terminate);
        Ok(())
    }
}

impl Stream for X11InputCapture {
    type Item = Result<(Position, CaptureEvent), CaptureError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.event_rx.poll_recv(cx)
    }
}

// ── Event loop ────────────────────────────────────────────────────────────────

fn run_event_loop(mut state: X11State) {
    loop {
        if !state.event_tx.available() {
            if state.active_client.is_some() {
                do_release(&mut state);
            }
            unsafe {
                XCloseDisplay(state.display);
            }
            return;
        }
        if drain_requests(&mut state) {
            return;
        }

        if state.active_client.is_none() {
            // Phase 1: poll cursor position, detect edge crossing
            let curr = query_pointer(&state);
            if let Some(pos) =
                crossed_boundary(state.prev_pos, curr, state.screen_w, state.screen_h)
            {
                if state.clients.contains(&pos) {
                    let entry = clamp_to_screen(curr, state.screen_w, state.screen_h);
                    do_grab(&mut state, pos, entry);
                }
            }
            state.prev_pos = curr;
            thread::sleep(Duration::from_millis(1));
        } else {
            // Phase 2: drain X11 event queue (populated by XGrabPointer)
            let pending = unsafe { XPending(state.display) };
            if pending > 0 {
                let mut ev = unsafe { std::mem::zeroed::<XEvent>() };
                unsafe { XNextEvent(state.display, &mut ev) };
                handle_event(&mut state, ev);
            } else {
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

/// Drains pending requests. Returns `true` if the thread should terminate.
fn drain_requests(state: &mut X11State) -> bool {
    loop {
        match state.request_rx.try_recv() {
            Ok(Request::Create(pos)) => {
                state.clients.insert(pos);
            }
            Ok(Request::Destroy(pos)) => {
                state.clients.remove(&pos);
                if state.active_client == Some(pos) {
                    do_release(state);
                }
            }
            Ok(Request::Release) => do_release(state),
            Ok(Request::Terminate) => {
                unsafe { XCloseDisplay(state.display) };
                return true;
            }
            Err(_) => return false,
        }
    }
}

fn query_pointer(state: &X11State) -> (i32, i32) {
    let mut root_return: Window = 0;
    let mut child_return: Window = 0;
    let mut root_x: i32 = 0;
    let mut root_y: i32 = 0;
    let mut win_x: i32 = 0;
    let mut win_y: i32 = 0;
    let mut mask: u32 = 0;
    unsafe {
        XQueryPointer(
            state.display,
            state.root,
            &mut root_return,
            &mut child_return,
            &mut root_x,
            &mut root_y,
            &mut win_x,
            &mut win_y,
            &mut mask,
        )
    };
    (root_x, root_y)
}

fn do_grab(state: &mut X11State, pos: Position, entry: (i32, i32)) {
    if !state.event_tx.available() {
        return;
    }
    let grab_mask =
        (PointerMotionMask | ButtonPressMask | ButtonReleaseMask | ButtonMotionMask) as u32;
    let result = unsafe {
        XGrabPointer(
            state.display,
            state.root,
            False,
            grab_mask,
            GrabModeAsync,
            GrabModeAsync,
            0, // no confinement
            0, // no cursor change
            CurrentTime,
        )
    };
    if result != GrabSuccess {
        log::warn!("x11: XGrabPointer failed with code {result}");
        return;
    }
    unsafe {
        XGrabKeyboard(
            state.display,
            state.root,
            False,
            GrabModeAsync,
            GrabModeAsync,
            CurrentTime,
        );
        XWarpPointer(state.display, 0, state.root, 0, 0, 0, 0, entry.0, entry.1);
        XFlush(state.display);
    }
    state.entry_point = entry;
    state.active_client = Some(pos);
    let t = match pos {
        Position::Left | Position::Right => {
            normalized_cross_axis(entry.1 as f64, 0.0, state.screen_h as f64)
        }
        Position::Top | Position::Bottom => {
            normalized_cross_axis(entry.0 as f64, 0.0, state.screen_w as f64)
        }
    };
    send_event(state, pos, CaptureEvent::Begin(t));
    if state.active_client == Some(pos) {
        log::debug!("x11: grabbed pointer for client {pos:?} at {entry:?}");
    }
}

fn release_native_grabs(state: &mut X11State) {
    unsafe {
        XUngrabPointer(state.display, CurrentTime);
        XUngrabKeyboard(state.display, CurrentTime);
        XFlush(state.display);
    }
}

fn do_release(state: &mut X11State) {
    (state.release_grabs)(state);
    log::debug!("x11: released pointer (was {:?})", state.active_client);
    state.active_client = None;
}

fn send_event(state: &mut X11State, pos: Position, event: CaptureEvent) {
    if state.event_tx.send(pos, event).is_err() && state.active_client.is_some() {
        do_release(state);
    }
}

#[allow(non_upper_case_globals)]
fn handle_event(state: &mut X11State, ev: XEvent) {
    match unsafe { ev.type_ } {
        MotionNotify => {
            let m: XMotionEvent = unsafe { ev.motion };
            handle_motion(state, m);
        }
        ButtonPress | ButtonRelease => {
            if let Some(pos) = state.active_client {
                let b: XButtonEvent = unsafe { ev.button };
                let pressed = u32::from(unsafe { ev.type_ } == ButtonPress);
                if let Some(pointer) = x11_pointer_button_event(b.button, pressed) {
                    send_event(state, pos, CaptureEvent::Input(Event::Pointer(pointer)));
                }
            }
        }
        KeyPress => {
            if let Some(pos) = state.active_client {
                let k: XKeyEvent = unsafe { ev.key };
                send_event(
                    state,
                    pos,
                    CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key {
                        time: 0,
                        key: k.keycode.saturating_sub(8),
                        state: 1,
                    })),
                );
            }
        }
        KeyRelease => {
            if let Some(pos) = state.active_client {
                let k: XKeyEvent = unsafe { ev.key };
                send_event(
                    state,
                    pos,
                    CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key {
                        time: 0,
                        key: k.keycode.saturating_sub(8),
                        state: 0,
                    })),
                );
            }
        }
        _ => {}
    }
}

fn handle_motion(state: &mut X11State, m: XMotionEvent) {
    let curr = (m.x_root, m.y_root);
    let entry = state.entry_point;
    let dx = (curr.0 - entry.0) as f64;
    let dy = (curr.1 - entry.1) as f64;
    // Skip warp-back echo events (XWarpPointer generates a synthetic MotionNotify)
    if dx == 0.0 && dy == 0.0 {
        return;
    }
    unsafe {
        XWarpPointer(state.display, 0, state.root, 0, 0, 0, 0, entry.0, entry.1);
        XFlush(state.display);
    }
    if let Some(pos) = state.active_client {
        send_event(
            state,
            pos,
            CaptureEvent::Input(Event::Pointer(PointerEvent::Motion { time: 0, dx, dy })),
        );
    }
}

// ── Pure logic functions ───────────────────────────────────────────────────────

/// Normalizes `coord` to `0.0..=1.0` within `min..=max`, falling back to the
/// midpoint for degenerate bounds.
fn normalized_cross_axis(coord: f64, min: f64, max: f64) -> f64 {
    if max <= min {
        return 0.5;
    }
    ((coord - min) / (max - min)).clamp(0.0, 1.0)
}

pub(crate) fn crossed_boundary(
    prev: (i32, i32),
    curr: (i32, i32),
    w: i32,
    h: i32,
) -> Option<Position> {
    if prev.0 > 0 && curr.0 <= 0 {
        Some(Position::Left)
    } else if prev.0 < w - 1 && curr.0 >= w - 1 {
        // X11 clamps the cursor to [0, w-1], so >= w is never true.
        // Treat arrival at the rightmost pixel as a right-edge crossing.
        Some(Position::Right)
    } else if prev.1 > 0 && curr.1 <= 0 {
        Some(Position::Top)
    } else if prev.1 < h - 1 && curr.1 >= h - 1 {
        // Same reasoning for the bottom edge.
        Some(Position::Bottom)
    } else {
        None
    }
}

pub(crate) fn clamp_to_screen(pos: (i32, i32), w: i32, h: i32) -> (i32, i32) {
    (pos.0.clamp(0, w - 1), pos.1.clamp(0, h - 1))
}

pub(crate) fn x11_button_to_evdev(button: u32) -> Option<u32> {
    use input_event::{BTN_BACK, BTN_FORWARD, BTN_LEFT, BTN_MIDDLE, BTN_RIGHT};
    match button {
        1 => Some(BTN_LEFT),
        2 => Some(BTN_MIDDLE),
        3 => Some(BTN_RIGHT),
        8 => Some(BTN_BACK),
        9 => Some(BTN_FORWARD),
        _ => None,
    }
}

fn x11_pointer_button_event(button: u32, pressed: u32) -> Option<PointerEvent> {
    let (axis, value) = match button {
        4 => (0, -120),
        5 => (0, 120),
        6 => (1, -120),
        7 => (1, 120),
        _ => {
            return x11_button_to_evdev(button).map(|button| PointerEvent::Button {
                time: 0,
                button,
                state: pressed,
            });
        }
    };
    // Core X11 wheel events are click pairs; forward the press exactly once.
    (pressed == 1).then_some(PointerEvent::AxisDiscrete120 { axis, value })
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn button_fixture() -> (X11State, HookReceiver) {
        let (event_tx, event_rx) = x11_channel();
        let (_request_tx, request_rx) = mpsc::channel();
        (
            X11State {
                display: std::ptr::null_mut(),
                root: 0,
                screen_w: 100,
                screen_h: 100,
                clients: HashSet::new(),
                active_client: Some(Position::Left),
                entry_point: (0, 0),
                prev_pos: (0, 0),
                event_tx,
                release_grabs: |state| state.release_calls += 1,
                release_calls: 0,
                request_rx,
            },
            event_rx,
        )
    }

    fn button_event(kind: i32, button: u32) -> XEvent {
        XEvent {
            button: XButtonEvent {
                type_: kind,
                button,
                ..unsafe { std::mem::zeroed() }
            },
        }
    }

    fn ready_event(rx: &mut HookReceiver) -> Option<(Position, CaptureEvent)> {
        let waker = futures::task::noop_waker();
        match rx.poll_recv(&mut Context::from_waker(&waker)) {
            Poll::Ready(Some(Ok(event))) => Some(event),
            Poll::Pending | Poll::Ready(None) => None,
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn full_discrete_queue_releases_capture_and_reports_failure_before_stale_input() {
        let (mut state, mut events) = button_fixture();
        let key = |kind| XEvent {
            key: XKeyEvent {
                type_: kind,
                keycode: 37,
                ..unsafe { std::mem::zeroed() }
            },
        };
        handle_event(&mut state, key(KeyPress));
        for _ in 0..63 {
            handle_event(&mut state, button_event(ButtonPress, 1));
        }
        handle_event(&mut state, key(KeyRelease));
        assert_eq!(state.active_client, None);
        assert_eq!(state.release_calls, 1);
        assert!(!state.event_tx.available());
        let waker = futures::task::noop_waker();
        assert!(matches!(
            events.poll_recv(&mut Context::from_waker(&waker)),
            Poll::Ready(Some(Err(CaptureError::X11QueueOverloaded)))
        ));
        assert!(matches!(
            events.poll_recv(&mut Context::from_waker(&waker)),
            Poll::Ready(None)
        ));
        do_grab(&mut state, Position::Left, (0, 0)); // latched queue refuses before any native call.
        assert_eq!(state.active_client, None);
        assert_eq!(state.release_calls, 1);
    }

    #[test]
    fn public_capture_prioritizes_queue_failure_over_cached_fanout() {
        let (mut state, events) = button_fixture();
        for _ in 0..64 {
            handle_event(&mut state, button_event(ButtonPress, 1));
        }
        handle_event(&mut state, button_event(ButtonRelease, 1));
        assert_eq!(state.release_calls, 1);
        let (request_tx, _request_rx) = mpsc::sync_channel(16);
        let backend = X11InputCapture {
            event_rx: events,
            request_tx,
            thread: None,
        };
        let mut capture = crate::InputCapture {
            capture: Box::new(backend),
            enter_only_handles: Default::default(),
            enter_only_positions: Default::default(),
            pressed_keys: HashSet::from([input_event::scancode::Linux::KeyLeftCtrl]),
            position_map: std::collections::HashMap::from([(Position::Left, vec![6, 7])]),
            id_map: std::collections::HashMap::from([(6, Position::Left), (7, Position::Left)]),
            pending: std::collections::VecDeque::from([(
                7,
                CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key {
                    time: 0,
                    key: 29,
                    state: 1,
                })),
            )]),
        };
        let waker = futures::task::noop_waker();
        assert!(matches!(
            Pin::new(&mut capture).poll_next(&mut Context::from_waker(&waker)),
            Poll::Ready(Some(Err(CaptureError::X11QueueOverloaded)))
        ));
        assert!(capture.pending.is_empty());
        assert_eq!(
            capture.take_pressed_keys(),
            HashSet::from([input_event::scancode::Linux::KeyLeftCtrl])
        );
    }

    #[test]
    fn coalesced_motion_keeps_release_after_a_large_stalled_burst() {
        let (mut state, mut events) = button_fixture();
        let key = |kind| XEvent {
            key: XKeyEvent {
                type_: kind,
                keycode: 37,
                ..unsafe { std::mem::zeroed() }
            },
        };
        handle_event(&mut state, key(KeyPress));
        for _ in 0..8000 {
            send_event(
                &mut state,
                Position::Left,
                CaptureEvent::Input(Event::Pointer(PointerEvent::Motion {
                    time: 0,
                    dx: 0.25,
                    dy: -0.25,
                })),
            );
        }
        handle_event(&mut state, key(KeyRelease));
        assert_eq!(state.active_client, Some(Position::Left));
        assert_eq!(state.release_calls, 0);
        let mut total = (0.0, 0.0);
        let mut transitions = Vec::new();
        while let Some((_, event)) = ready_event(&mut events) {
            match event {
                CaptureEvent::Input(Event::Pointer(PointerEvent::Motion { dx, dy, .. })) => {
                    total.0 += dx;
                    total.1 += dy;
                }
                CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key { state, .. })) => {
                    transitions.push(state)
                }
                _ => panic!("unexpected event"),
            }
        }
        assert_eq!(total, (2000.0, -2000.0));
        assert_eq!(transitions, vec![1, 0]);
    }

    #[test]
    fn x11_scroll_capture_forwards_one_axis_event_per_wheel_click() {
        let (mut state, mut events) = button_fixture();
        for (button, axis, value) in [(4, 0, -120), (5, 0, 120), (6, 1, -120), (7, 1, 120)] {
            handle_event(&mut state, button_event(ButtonPress, button));
            handle_event(&mut state, button_event(ButtonRelease, button));
            assert!(
                matches!(ready_event(&mut events), Some((Position::Left, CaptureEvent::Input(Event::Pointer(PointerEvent::AxisDiscrete120 { axis: actual_axis, value: actual_value }))))
                if actual_axis == axis && actual_value == value)
            );
            assert!(ready_event(&mut events).is_none());
        }
        for kind in [ButtonPress, ButtonRelease] {
            handle_event(&mut state, button_event(kind, 8));
            assert!(
                matches!(ready_event(&mut events), Some((Position::Left, CaptureEvent::Input(Event::Pointer(PointerEvent::Button { button, state: pressed, .. }))))
                if button == input_event::BTN_BACK && pressed == u32::from(kind == ButtonPress))
            );
        }
        state.active_client = None;
        handle_event(&mut state, button_event(ButtonPress, 5));
        assert!(ready_event(&mut events).is_none());
    }

    #[test]
    fn crosses_left_boundary() {
        assert_eq!(
            crossed_boundary((5, 100), (-1, 100), 1920, 1080),
            Some(Position::Left)
        );
    }

    #[test]
    fn crosses_right_boundary() {
        // X11 clamps to w-1; the cursor arrives at 1919, never at 1920.
        assert_eq!(
            crossed_boundary((1915, 100), (1919, 100), 1920, 1080),
            Some(Position::Right)
        );
    }

    #[test]
    fn crosses_top_boundary() {
        assert_eq!(
            crossed_boundary((100, 5), (100, -1), 1920, 1080),
            Some(Position::Top)
        );
    }

    #[test]
    fn crosses_bottom_boundary() {
        // X11 clamps to h-1; the cursor arrives at 1079, never at 1080.
        assert_eq!(
            crossed_boundary((100, 1075), (100, 1079), 1920, 1080),
            Some(Position::Bottom)
        );
    }

    #[test]
    fn no_crossing_interior_movement() {
        assert_eq!(crossed_boundary((100, 100), (200, 200), 1920, 1080), None);
    }

    #[test]
    fn no_crossing_already_at_left_edge() {
        assert_eq!(crossed_boundary((0, 100), (0, 100), 1920, 1080), None);
    }

    #[test]
    fn no_crossing_already_at_right_edge() {
        // Cursor already at w-1: no prev→curr transition, must not re-trigger.
        assert_eq!(crossed_boundary((1919, 100), (1919, 100), 1920, 1080), None);
    }

    #[test]
    fn clamp_within_bounds_is_identity() {
        assert_eq!(clamp_to_screen((500, 300), 1920, 1080), (500, 300));
    }

    #[test]
    fn clamp_negative_coords() {
        assert_eq!(clamp_to_screen((-10, -5), 1920, 1080), (0, 0));
    }

    #[test]
    fn clamp_over_right_bottom_edge() {
        assert_eq!(clamp_to_screen((2000, 1200), 1920, 1080), (1919, 1079));
    }

    #[test]
    fn left_button_maps_to_btn_left() {
        use input_event::BTN_LEFT;
        assert_eq!(x11_button_to_evdev(1), Some(BTN_LEFT));
    }

    #[test]
    fn right_button_maps_to_btn_right() {
        use input_event::BTN_RIGHT;
        assert_eq!(x11_button_to_evdev(3), Some(BTN_RIGHT));
    }

    #[test]
    fn back_and_forward_buttons_keep_their_identity() {
        assert_eq!(x11_button_to_evdev(8), Some(input_event::BTN_BACK));
        assert_eq!(x11_button_to_evdev(9), Some(input_event::BTN_FORWARD));
    }

    #[test]
    fn unknown_and_wheel_buttons_are_not_pointer_button_transitions() {
        for button in [0, 4, 5, 6, 7, 10, u32::MAX] {
            assert_eq!(x11_button_to_evdev(button), None);
        }
    }
}
