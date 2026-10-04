use std::collections::HashSet;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

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
    ReleaseTo(f64),
}

const STARTUP_TIMEOUT: Duration = Duration::from_secs(2);
const CONTROL_TIMEOUT: Duration = Duration::from_millis(500);
static WORKER_ACTIVE: AtomicBool = AtomicBool::new(false);

struct WorkerLease(&'static AtomicBool);
impl WorkerLease {
    fn acquire(active: &'static AtomicBool) -> Result<Self, X11InputCaptureCreationError> {
        active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| X11InputCaptureCreationError::WorkerStillRunning)?;
        Ok(Self(active))
    }
}
impl Drop for WorkerLease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct ControlRequest {
    request: Request,
    done: oneshot::Sender<()>,
}

fn control_error(kind: std::io::ErrorKind, message: &'static str) -> CaptureError {
    std::io::Error::new(kind, message).into()
}

struct WorkerConfig {
    event_tx: HookSender,
    request_rx: mpsc::Receiver<ControlRequest>,
    stopping: Arc<AtomicBool>,
    lease: WorkerLease,
}

fn initialize_native(config: WorkerConfig) -> Result<X11State, X11InputCaptureCreationError> {
    let display = unsafe { x11::xlib::XOpenDisplay(std::ptr::null()) };
    if display.is_null() {
        return Err(X11InputCaptureCreationError::OpenDisplayFailed);
    }
    let screen = unsafe { XDefaultScreen(display) };
    let root = unsafe { XDefaultRootWindow(display) };
    let screen_w = unsafe { XDisplayWidth(display, screen) };
    let screen_h = unsafe { XDisplayHeight(display, screen) };
    Ok(X11State {
        display,
        root,
        screen_w,
        screen_h,
        clients: HashSet::new(),
        active_client: None,
        entry_point: (0, 0),
        prev_pos: (0, 0),
        event_tx: config.event_tx,
        release_grabs: release_native_grabs,
        warp_pointer: warp_native_pointer,
        #[cfg(test)]
        release_calls: 0,
        #[cfg(test)]
        warp_calls: 0,
        request_rx: config.request_rx,
        stopping: config.stopping,
        _worker_lease: Some(config.lease),
    })
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
    warp_pointer: fn(&mut X11State, (i32, i32)),
    #[cfg(test)]
    release_calls: usize,
    #[cfg(test)]
    warp_calls: usize,
    request_rx: mpsc::Receiver<ControlRequest>,
    stopping: Arc<AtomicBool>,
    _worker_lease: Option<WorkerLease>,
}

// Safety: display is only accessed from the dedicated X11 thread.
unsafe impl Send for X11State {}

// ── Public struct ─────────────────────────────────────────────────────────────

pub struct X11InputCapture {
    event_rx: HookReceiver,
    request_tx: mpsc::Sender<ControlRequest>,
    stopping: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl X11InputCapture {
    pub async fn new() -> Result<Self, X11InputCaptureCreationError> {
        Self::start_with(
            WorkerLease::acquire(&WORKER_ACTIVE)?,
            STARTUP_TIMEOUT,
            initialize_native,
            run_event_loop,
        )
        .await
    }

    async fn start_with<F, R>(
        lease: WorkerLease,
        timeout: Duration,
        initialize: F,
        run: R,
    ) -> Result<Self, X11InputCaptureCreationError>
    where
        F: FnOnce(WorkerConfig) -> Result<X11State, X11InputCaptureCreationError> + Send + 'static,
        R: FnOnce(X11State) + Send + 'static,
    {
        let (event_tx, event_rx) = x11_channel();
        let (request_tx, request_rx) = mpsc::channel(16);
        let stopping = Arc::new(AtomicBool::new(false));
        let config = WorkerConfig {
            event_tx,
            request_rx,
            stopping: stopping.clone(),
            lease,
        };
        let (ready, initialized) = oneshot::channel();
        let thread = thread::Builder::new()
            .name("lan-mouse-x11".into())
            .spawn(move || match initialize(config) {
                Err(error) => {
                    let _ = ready.send(Err(error));
                }
                Ok(state) => {
                    if state.stopping.load(Ordering::Acquire) || ready.is_closed() {
                        return;
                    }
                    if ready.send(Ok(())).is_ok() && !state.stopping.load(Ordering::Acquire) {
                        run(state);
                    }
                }
            })
            .map_err(X11InputCaptureCreationError::ThreadSpawn)?;
        // Keep ownership while awaiting readiness: cancellation drops this guard,
        // requests stop, and leaves the native worker lease alive until cleanup.
        let backend = Self {
            event_rx,
            request_tx,
            stopping,
            thread: Some(thread),
        };
        match tokio::time::timeout(timeout, initialized).await {
            Ok(Ok(Ok(()))) => Ok(backend),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_)) => Err(X11InputCaptureCreationError::InitializationClosed),
            Err(_) => Err(X11InputCaptureCreationError::InitializationTimedOut),
        }
    }

    async fn control(&self, request: Request) -> Result<(), CaptureError> {
        self.control_with_timeout(request, CONTROL_TIMEOUT).await
    }

    async fn control_with_timeout(
        &self,
        request: Request,
        timeout: Duration,
    ) -> Result<(), CaptureError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(control_error(
                std::io::ErrorKind::BrokenPipe,
                "X11 capture is stopping",
            ));
        }
        let result = tokio::time::timeout(timeout, async {
            let (done, completion) = oneshot::channel();
            self.request_tx
                .send(ControlRequest { request, done })
                .await
                .map_err(|_| {
                    control_error(std::io::ErrorKind::BrokenPipe, "X11 capture thread closed")
                })?;
            completion.await.map_err(|_| {
                control_error(
                    std::io::ErrorKind::BrokenPipe,
                    "X11 capture request was not completed",
                )
            })
        })
        .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                self.stopping.store(true, Ordering::Release);
                Err(control_error(
                    std::io::ErrorKind::TimedOut,
                    "X11 capture control timed out; stopping capture",
                ))
            }
        }
    }

    async fn terminate_with_timeout(&mut self, timeout: Duration) -> Result<(), CaptureError> {
        self.stopping.store(true, Ordering::Release);
        tokio::time::timeout(timeout, async {
            while self
                .thread
                .as_ref()
                .is_some_and(|thread| !thread.is_finished())
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            if let Some(thread) = self.thread.take() {
                thread.join().map_err(|_| {
                    control_error(std::io::ErrorKind::Other, "X11 capture thread panicked")
                })?;
            }
            Ok(())
        })
        .await
        .map_err(|_| {
            control_error(
                std::io::ErrorKind::TimedOut,
                "X11 capture thread has not stopped",
            )
        })?
    }
}

impl Drop for X11InputCapture {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            if thread.is_finished() {
                let _ = thread.join();
            } else {
                // The native worker keeps its lease until cleanup actually completes.
                log::warn!("X11 capture worker still stopping; native cleanup is pending");
            }
        }
    }
}

impl Drop for X11State {
    fn drop(&mut self) {
        if !self.display.is_null() {
            if self.active_client.is_some() {
                do_release(self);
            }
            unsafe {
                XCloseDisplay(self.display);
            }
        }
    }
}

// ── Async trait impl ──────────────────────────────────────────────────────────

#[async_trait]
impl Capture for X11InputCapture {
    fn pending_failure(&self) -> bool {
        self.stopping.load(Ordering::Acquire) || self.event_rx.failed()
    }

    async fn create(&mut self, pos: Position) -> Result<(), CaptureError> {
        self.control(Request::Create(pos)).await
    }

    async fn destroy(&mut self, pos: Position) -> Result<(), CaptureError> {
        self.control(Request::Destroy(pos)).await
    }

    async fn set_enter_only(&mut self, _pos: Position, _enabled: bool) -> Result<(), CaptureError> {
        Ok(())
    }

    async fn release(&mut self) -> Result<(), CaptureError> {
        self.control(Request::Release).await
    }

    async fn release_to(&mut self, t: f64) -> Result<(), CaptureError> {
        self.control(Request::ReleaseTo(t)).await
    }

    async fn terminate(&mut self) -> Result<(), CaptureError> {
        self.terminate_with_timeout(CONTROL_TIMEOUT).await
    }
}

impl Stream for X11InputCapture {
    type Item = Result<(Position, CaptureEvent), CaptureError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.stopping.load(Ordering::Acquire) {
            return Poll::Ready(None);
        }
        self.event_rx.poll_recv(cx)
    }
}

// ── Event loop ────────────────────────────────────────────────────────────────

fn run_event_loop(mut state: X11State) {
    loop {
        if state.stopping.load(Ordering::Acquire) || !state.event_tx.available() {
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
    for _ in 0..16 {
        if state.stopping.load(Ordering::Acquire) {
            return true;
        }
        let control = match state.request_rx.try_recv() {
            Ok(control) => control,
            Err(mpsc::error::TryRecvError::Empty) => return false,
            Err(mpsc::error::TryRecvError::Disconnected) => return true,
        };
        if control.done.is_closed() {
            continue;
        }
        match control.request {
            Request::Create(pos) => {
                state.clients.insert(pos);
            }
            Request::Destroy(pos) => {
                state.clients.remove(&pos);
                if state.active_client == Some(pos) {
                    do_release(state);
                }
            }
            Request::Release => do_release(state),
            Request::ReleaseTo(t) => do_release_to(state, t),
        }
        let _ = control.done.send(());
    }
    false
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

struct GrabOps {
    pointer: fn(&mut X11State) -> i32,
    keyboard: fn(&mut X11State) -> i32,
    warp: fn(&mut X11State, (i32, i32)),
}

const NATIVE_GRAB_OPS: GrabOps = GrabOps {
    pointer: grab_native_pointer,
    keyboard: grab_native_keyboard,
    warp: warp_native_pointer,
};

fn grab_native_pointer(state: &mut X11State) -> i32 {
    let grab_mask =
        (PointerMotionMask | ButtonPressMask | ButtonReleaseMask | ButtonMotionMask) as u32;
    unsafe {
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
    }
}

fn grab_native_keyboard(state: &mut X11State) -> i32 {
    unsafe {
        XGrabKeyboard(
            state.display,
            state.root,
            False,
            GrabModeAsync,
            GrabModeAsync,
            CurrentTime,
        )
    }
}

fn warp_native_pointer(state: &mut X11State, entry: (i32, i32)) {
    unsafe {
        XWarpPointer(state.display, 0, state.root, 0, 0, 0, 0, entry.0, entry.1);
        XFlush(state.display);
    }
}

fn do_grab(state: &mut X11State, pos: Position, entry: (i32, i32)) {
    do_grab_with(state, pos, entry, &NATIVE_GRAB_OPS);
}

fn do_grab_with(state: &mut X11State, pos: Position, entry: (i32, i32), ops: &GrabOps) {
    if state.stopping.load(Ordering::Acquire) || !state.event_tx.available() {
        return;
    }
    let result = (ops.pointer)(state);
    if result != GrabSuccess {
        log::warn!("x11: XGrabPointer failed with code {result}");
        return;
    }
    if state.stopping.load(Ordering::Acquire) {
        do_release(state);
        return;
    }
    let result = (ops.keyboard)(state);
    if result != GrabSuccess {
        log::warn!("x11: XGrabKeyboard failed with code {result}; rolling back pointer grab");
        do_release(state);
        return;
    }
    if state.stopping.load(Ordering::Acquire) {
        do_release(state);
        return;
    }
    (ops.warp)(state, entry);
    if state.stopping.load(Ordering::Acquire) {
        do_release(state);
        return;
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

fn do_release_to(state: &mut X11State, t: f64) {
    let Some(pos) = state.active_client else {
        return;
    };
    let target = return_point(pos, t, state.screen_w, state.screen_h);
    // Warp while still grabbed, then ungrab. Seed the next idle crossing check
    // from the returned coordinates rather than the old entry point.
    (state.warp_pointer)(state, target);
    state.prev_pos = target;
    do_release(state);
}

fn send_event(state: &mut X11State, pos: Position, event: CaptureEvent) {
    if (state.stopping.load(Ordering::Acquire) || state.event_tx.send(pos, event).is_err())
        && state.active_client.is_some()
    {
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

fn return_point(pos: Position, t: f64, w: i32, h: i32) -> (i32, i32) {
    let w = w.max(1);
    let h = h.max(1);
    let t = if t.is_finite() {
        t.clamp(0.0, 1.0)
    } else {
        0.5
    };
    let x = (f64::from(w - 1) * t).round() as i32;
    let y = (f64::from(h - 1) * t).round() as i32;
    let x_inset = 16.min((w - 1) / 2);
    let y_inset = 16.min((h - 1) / 2);
    match pos {
        Position::Left => (x_inset, y),
        Position::Right => (w - 1 - x_inset, y),
        Position::Top => (x, y_inset),
        Position::Bottom => (x, h - 1 - y_inset),
    }
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
        let (_request_tx, request_rx) = mpsc::channel(16);
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
                warp_pointer: |state, _| state.warp_calls += 1,
                release_calls: 0,
                warp_calls: 0,
                request_rx,
                stopping: Arc::new(AtomicBool::new(false)),
                _worker_lease: None,
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

    fn initialize_mock(config: WorkerConfig) -> Result<X11State, X11InputCaptureCreationError> {
        let (mut state, _) = button_fixture();
        state.active_client = None;
        state.event_tx = config.event_tx;
        state.request_rx = config.request_rx;
        state.stopping = config.stopping;
        state._worker_lease = Some(config.lease);
        Ok(state)
    }

    async fn wait_for_lease_release(active: &AtomicBool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn delayed_initialization_runs_off_runtime_and_control_is_ready_on_success() {
        static ACTIVE: AtomicBool = AtomicBool::new(false);
        let caller = thread::current().id();
        let (resume, blocked) = std::sync::mpsc::channel();
        let (entered, starting) = oneshot::channel();
        let start = X11InputCapture::start_with(
            WorkerLease::acquire(&ACTIVE).unwrap(),
            Duration::from_secs(1),
            move |config| {
                assert_ne!(thread::current().id(), caller);
                entered.send(()).unwrap();
                blocked.recv().unwrap();
                initialize_mock(config)
            },
            |mut state| {
                while !state.stopping.load(Ordering::Acquire) {
                    if drain_requests(&mut state) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            },
        );
        let (backend, ()) = tokio::join!(start, async {
            starting.await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
            resume.send(()).unwrap();
        });
        let mut backend = backend.unwrap();
        backend.create(Position::Left).await.unwrap();
        backend.release().await.unwrap();
        backend.terminate().await.unwrap();
        assert!(!ACTIVE.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn initialization_timeout_retains_lease_and_never_runs_late_capture() {
        static ACTIVE: AtomicBool = AtomicBool::new(false);
        let ran = Arc::new(AtomicBool::new(false));
        let observed = ran.clone();
        let (resume, blocked) = std::sync::mpsc::channel();
        let result = X11InputCapture::start_with(
            WorkerLease::acquire(&ACTIVE).unwrap(),
            Duration::from_millis(20),
            move |config| {
                blocked.recv().unwrap();
                initialize_mock(config)
            },
            move |_| {
                observed.store(true, Ordering::Release);
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(X11InputCaptureCreationError::InitializationTimedOut)
        ));
        assert!(WorkerLease::acquire(&ACTIVE).is_err());
        resume.send(()).unwrap();
        wait_for_lease_release(&ACTIVE).await;
        assert!(!ran.load(Ordering::Acquire));
        assert!(WorkerLease::acquire(&ACTIVE).is_ok());
    }

    #[tokio::test]
    async fn canceling_initialization_stops_before_late_activation() {
        static ACTIVE: AtomicBool = AtomicBool::new(false);
        let ran = Arc::new(AtomicBool::new(false));
        let observed = ran.clone();
        let (resume, blocked) = std::sync::mpsc::channel();
        let (entered, starting) = oneshot::channel();
        let mut start = Box::pin(X11InputCapture::start_with(
            WorkerLease::acquire(&ACTIVE).unwrap(),
            Duration::from_secs(1),
            move |config| {
                entered.send(()).unwrap();
                blocked.recv().unwrap();
                assert!(config.stopping.load(Ordering::Acquire));
                initialize_mock(config)
            },
            move |_| {
                observed.store(true, Ordering::Release);
            },
        ));
        assert!(futures::poll!(start.as_mut()).is_pending());
        starting.await.unwrap();
        drop(start);
        assert!(WorkerLease::acquire(&ACTIVE).is_err());
        resume.send(()).unwrap();
        wait_for_lease_release(&ACTIVE).await;
        assert!(!ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn initialization_failure_and_panic_report_errors_and_release_lease() {
        static ACTIVE: AtomicBool = AtomicBool::new(false);
        let failure = X11InputCapture::start_with(
            WorkerLease::acquire(&ACTIVE).unwrap(),
            Duration::from_secs(1),
            |_| Err(X11InputCaptureCreationError::OpenDisplayFailed),
            |_| panic!("must not run failed initialization"),
        )
        .await;
        assert!(matches!(
            failure,
            Err(X11InputCaptureCreationError::OpenDisplayFailed)
        ));
        wait_for_lease_release(&ACTIVE).await;
        let failure = X11InputCapture::start_with(
            WorkerLease::acquire(&ACTIVE).unwrap(),
            Duration::from_secs(1),
            |_| panic!("simulated initialization panic"),
            |_| panic!("must not run panicked initialization"),
        )
        .await;
        assert!(matches!(
            failure,
            Err(X11InputCaptureCreationError::InitializationClosed)
        ));
        wait_for_lease_release(&ACTIVE).await;
        assert!(WorkerLease::acquire(&ACTIVE).is_ok());
    }

    fn control_fixture() -> (X11State, X11InputCapture) {
        let (mut state, events) = button_fixture();
        let (request_tx, request_rx) = mpsc::channel(16);
        state.request_rx = request_rx;
        let backend = X11InputCapture {
            event_rx: events,
            request_tx,
            stopping: state.stopping.clone(),
            thread: None,
        };
        (state, backend)
    }

    #[tokio::test]
    async fn full_control_queue_yields_and_times_out_without_synchronous_blocking() {
        let (state, backend) = control_fixture();
        let mut completions = Vec::new();
        for _ in 0..16 {
            let (done, completion) = oneshot::channel();
            completions.push(completion);
            backend
                .request_tx
                .try_send(ControlRequest {
                    request: Request::Create(Position::Left),
                    done,
                })
                .unwrap();
        }
        let (result, ()) = tokio::join!(
            backend.control_with_timeout(Request::Release, Duration::from_millis(20)),
            async {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        );
        assert!(
            matches!(result, Err(CaptureError::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut)
        );
        assert!(state.stopping.load(Ordering::Acquire));
        assert_eq!(backend.request_tx.capacity(), 0);
        assert!(matches!(backend.control(Request::Release).await,
            Err(CaptureError::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe));
        drop(backend); // full queue does not prevent Drop.
    }

    #[tokio::test]
    async fn control_waits_for_ack_and_canceled_return_is_not_replayed() {
        let (mut state, backend) = control_fixture();
        let mut request = Box::pin(backend.control(Request::ReleaseTo(0.75)));
        assert!(futures::poll!(request.as_mut()).is_pending());
        assert_eq!(state.active_client, Some(Position::Left));
        drop(request);
        assert!(!drain_requests(&mut state));
        assert_eq!(state.warp_calls, 0);
        assert_eq!(state.release_calls, 0);
        let mut release = Box::pin(backend.control(Request::Release));
        assert!(futures::poll!(release.as_mut()).is_pending());
        assert_eq!(state.release_calls, 0);
        assert!(!drain_requests(&mut state));
        release.await.unwrap();
        assert_eq!(state.release_calls, 1);
        assert_eq!(state.active_client, None);
    }

    #[tokio::test]
    async fn admitted_control_timeout_stops_before_late_native_work() {
        let (mut state, backend) = control_fixture();
        assert!(
            matches!(backend.control_with_timeout(Request::ReleaseTo(0.75), Duration::from_millis(10)).await,
            Err(CaptureError::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut)
        );
        assert!(drain_requests(&mut state));
        assert_eq!(state.warp_calls, 0);
        assert_eq!(state.release_calls, 0);
    }

    #[tokio::test]
    async fn stalled_shutdown_is_bounded_drop_does_not_join_and_lease_prevents_replacement() {
        static ACTIVE: AtomicBool = AtomicBool::new(false);
        let lease = WorkerLease::acquire(&ACTIVE).unwrap();
        let (_, mut backend) = control_fixture();
        let (resume, blocked) = std::sync::mpsc::channel();
        let (done, completed) = oneshot::channel();
        backend.thread = Some(thread::spawn(move || {
            let _lease = lease;
            blocked.recv().unwrap(); // models an uninterruptible native operation.
            drop(_lease);
            let _ = done.send(());
        }));
        assert!(
            matches!(backend.terminate_with_timeout(Duration::from_millis(20)).await,
            Err(CaptureError::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut)
        );
        assert!(backend.stopping.load(Ordering::Acquire));
        assert!(backend.thread.is_some());
        assert!(WorkerLease::acquire(&ACTIVE).is_err());
        drop(backend); // must not wait for the simulated native operation.
        assert!(WorkerLease::acquire(&ACTIVE).is_err());
        resume.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), completed)
            .await
            .unwrap()
            .unwrap();
        assert!(WorkerLease::acquire(&ACTIVE).is_ok());
    }

    #[tokio::test]
    async fn normal_shutdown_can_be_retried_and_panicked_worker_reports_error() {
        let (_, mut backend) = control_fixture();
        let stopping = backend.stopping.clone();
        let (resume, blocked) = std::sync::mpsc::channel();
        backend.thread = Some(thread::spawn(move || {
            blocked.recv().unwrap();
            assert!(stopping.load(Ordering::Acquire));
        }));
        assert!(
            matches!(backend.terminate_with_timeout(Duration::from_millis(10)).await,
            Err(CaptureError::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut)
        );
        assert!(backend.thread.is_some());
        resume.send(()).unwrap();
        backend.terminate().await.unwrap();
        assert!(backend.thread.is_none());
        backend.terminate().await.unwrap();
        let (_, mut backend) = control_fixture();
        backend.thread = Some(thread::spawn(|| panic!("simulated native worker panic")));
        assert!(matches!(backend.terminate().await,
            Err(CaptureError::Io(error)) if error.kind() == std::io::ErrorKind::Other));
        assert!(backend.thread.is_none());
    }

    #[test]
    fn stopping_during_grab_rolls_back_without_warp_or_begin() {
        let (mut state, mut events) = button_fixture();
        state.active_client = None;
        let ops = GrabOps {
            pointer: |state| {
                state.stopping.store(true, Ordering::Release);
                GrabSuccess
            },
            keyboard: |_| panic!("must not acquire keyboard after stop"),
            warp: |_, _| panic!("must not warp after stop"),
        };
        do_grab_with(&mut state, Position::Left, (0, 25), &ops);
        assert_eq!(state.active_client, None);
        assert_eq!(state.release_calls, 1);
        assert!(ready_event(&mut events).is_none());
    }

    #[test]
    fn stopped_capture_never_delivers_old_or_new_input() {
        let (mut state, mut backend) = control_fixture();
        state
            .event_tx
            .send(Position::Left, CaptureEvent::Begin(0.5))
            .unwrap();
        state.stopping.store(true, Ordering::Release);
        send_event(&mut state, Position::Left, CaptureEvent::Begin(0.75));
        assert_eq!(state.release_calls, 1);
        let waker = futures::task::noop_waker();
        assert!(matches!(
            Pin::new(&mut backend).poll_next(&mut Context::from_waker(&waker)),
            Poll::Ready(None)
        ));
        assert!(backend.pending_failure());
    }

    #[tokio::test]
    async fn release_to_request_releases_at_matching_inset_without_immediate_recapture() {
        for (pos, target) in [
            (Position::Left, (16, 74)),
            (Position::Right, (83, 74)),
            (Position::Top, (74, 16)),
            (Position::Bottom, (74, 83)),
        ] {
            let (mut state, events) = button_fixture();
            state.active_client = Some(pos);
            let (request_tx, request_rx) = mpsc::channel(16);
            state.request_rx = request_rx;
            let mut backend = X11InputCapture {
                event_rx: events,
                request_tx,
                stopping: state.stopping.clone(),
                thread: None,
            };
            let (result, ()) = tokio::join!(backend.release_to(0.75), async {
                tokio::task::yield_now().await;
                assert!(!drain_requests(&mut state));
            });
            result.unwrap();
            assert_eq!(state.active_client, None);
            assert_eq!(state.release_calls, 1);
            assert_eq!(state.warp_calls, 1);
            assert_eq!(state.prev_pos, target);
            assert_eq!(crossed_boundary(state.prev_pos, target, 100, 100), None);
            let (result, ()) = tokio::join!(backend.release_to(0.25), async {
                tokio::task::yield_now().await;
                assert!(!drain_requests(&mut state));
            });
            result.unwrap();
            assert_eq!(state.release_calls, 1);
            assert_eq!(state.warp_calls, 1);
            assert_eq!(state.prev_pos, target);
        }
    }

    #[tokio::test]
    async fn release_to_reports_closed_thread_instead_of_false_success() {
        let (_, events) = button_fixture();
        let (request_tx, request_rx) = mpsc::channel(16);
        drop(request_rx);
        let mut backend = X11InputCapture {
            event_rx: events,
            request_tx,
            stopping: Arc::new(AtomicBool::new(false)),
            thread: None,
        };
        assert!(
            matches!(backend.release_to(0.75).await, Err(CaptureError::Io(error))
            if error.kind() == std::io::ErrorKind::BrokenPipe)
        );
    }

    #[test]
    fn return_point_handles_endpoints_invalid_values_and_tiny_screens() {
        assert_eq!(return_point(Position::Left, 0.0, 100, 100), (16, 0));
        assert_eq!(return_point(Position::Right, 1.0, 100, 100), (83, 99));
        assert_eq!(return_point(Position::Top, -1.0, 100, 100), (0, 16));
        assert_eq!(return_point(Position::Bottom, 2.0, 100, 100), (99, 83));
        for t in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(return_point(Position::Left, t, 100, 100), (16, 50));
        }
        for pos in [
            Position::Left,
            Position::Right,
            Position::Top,
            Position::Bottom,
        ] {
            for (w, h) in [(1, 1), (2, 2), (0, -1), (i32::MAX, i32::MAX)] {
                let target = return_point(pos, 0.75, w, h);
                assert!((0..w.max(1)).contains(&target.0));
                assert!((0..h.max(1)).contains(&target.1));
            }
        }
    }

    #[test]
    fn keyboard_grab_failure_rolls_back_pointer_without_begin_or_warp() {
        for keyboard in [
            (|_: &mut X11State| x11::xlib::AlreadyGrabbed) as fn(&mut X11State) -> i32,
            |_| x11::xlib::GrabInvalidTime,
            |_| x11::xlib::GrabNotViewable,
            |_| x11::xlib::GrabFrozen,
        ] {
            let (mut state, mut events) = button_fixture();
            state.active_client = None;
            state.entry_point = (10, 20);
            let ops = GrabOps {
                pointer: |_| GrabSuccess,
                keyboard,
                warp: |state, _| state.warp_calls += 1,
            };
            do_grab_with(&mut state, Position::Left, (0, 25), &ops);
            assert_eq!(state.active_client, None);
            assert_eq!(state.entry_point, (10, 20));
            assert_eq!(state.warp_calls, 0);
            assert_eq!(state.release_calls, 1);
            assert!(ready_event(&mut events).is_none());
            assert!(state.event_tx.available());
            // A later attempt may recover when the other grab owner is gone.
            let success = GrabOps {
                keyboard: |_| GrabSuccess,
                ..ops
            };
            do_grab_with(&mut state, Position::Left, (0, 25), &success);
            assert_eq!(state.active_client, Some(Position::Left));
            assert_eq!(state.warp_calls, 1);
            assert_eq!(state.release_calls, 1);
            assert!(
                matches!(ready_event(&mut events), Some((Position::Left, CaptureEvent::Begin(t))) if t == 0.25)
            );
        }
    }

    #[test]
    fn pointer_grab_failure_never_attempts_keyboard_or_publishes_begin() {
        let (mut state, mut events) = button_fixture();
        state.active_client = None;
        let ops = GrabOps {
            pointer: |_| x11::xlib::AlreadyGrabbed,
            keyboard: |_| panic!("must not attempt keyboard after pointer refusal"),
            warp: |_, _| panic!("must not warp after pointer refusal"),
        };
        do_grab_with(&mut state, Position::Top, (25, 0), &ops);
        assert_eq!(state.active_client, None);
        assert_eq!(state.release_calls, 0);
        assert!(ready_event(&mut events).is_none());
    }

    #[test]
    fn successful_grabs_roll_back_if_begin_cannot_be_published() {
        let (mut state, mut events) = button_fixture();
        state.active_client = None;
        for _ in 0..64 {
            state
                .event_tx
                .send(Position::Left, CaptureEvent::Begin(0.5))
                .unwrap();
        }
        let ops = GrabOps {
            pointer: |_| GrabSuccess,
            keyboard: |_| GrabSuccess,
            warp: |state, _| state.warp_calls += 1,
        };
        do_grab_with(&mut state, Position::Left, (0, 25), &ops);
        assert_eq!(state.active_client, None);
        assert_eq!(state.release_calls, 1);
        let waker = futures::task::noop_waker();
        assert!(matches!(
            events.poll_recv(&mut Context::from_waker(&waker)),
            Poll::Ready(Some(Err(CaptureError::X11QueueOverloaded)))
        ));
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
        let (request_tx, _request_rx) = mpsc::channel(16);
        let backend = X11InputCapture {
            event_rx: events,
            request_tx,
            stopping: Arc::new(AtomicBool::new(false)),
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
