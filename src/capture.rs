use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures::StreamExt;
use input_capture::{
    CaptureError, CaptureEvent, CaptureHandle, InputCapture, InputCaptureError, Position,
    WindowIdentifier,
};
use input_event::{Event, KeyboardEvent, scancode};
use lan_mouse_proto::ProtoEvent;
use local_channel::mpsc::{Receiver, Sender, channel};
use tokio::task::{JoinHandle, spawn_local};
use tokio_util::sync::CancellationToken;

use crate::connect::LanMouseConnection;
use crate::remap::KeyRemap;
use crate::scroll::ScrollInvert;

pub(crate) struct Capture {
    cancellation_token: CancellationToken,
    request_tx: Sender<CaptureRequest>,
    task: JoinHandle<()>,
    event_rx: Receiver<ICaptureEvent>,
}

pub(crate) enum ICaptureEvent {
    /// a client was entered, at the given normalized cross-axis
    /// position along the edge it was entered at
    CaptureBegin(CaptureHandle, f64),
    /// capture disabled
    CaptureDisabled,
    /// A backend failed and capture was disabled until explicitly re-enabled.
    CaptureFailed(String),
    /// capture disabled
    CaptureEnabled,
    /// A (new) client was entered.
    /// In contrast to [`ICaptureEvent::CaptureBegin`] this
    /// event is only triggered when the capture was
    /// explicitly released in the meantime by
    /// either the remote client leaving its device region,
    /// a new device entering the screen or the release bind.
    ClientEntered(u64),
    /// The previously active client was left, i.e. capture
    /// was released for the given handle. Mirrors
    /// [`ICaptureEvent::ClientEntered`] for the leave side
    /// and fires on every release path (release-bind chord,
    /// remote `Leave`, explicit `Release` request, send
    /// failure, or destroy of the active capture).
    ClientLeft(u64),
    /// clipboard data received from remote
    ClipboardReceived(input_event::ClipboardEvent),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CaptureType {
    /// a normal input capture
    Default,
    /// A capture interested only in begin/return events.
    /// The capture is released immediately, if there is no
    /// Default capture at the same position.
    EnterOnly,
}

#[derive(Clone, Debug)]
enum CaptureRequest {
    /// capture must release the mouse
    Release,
    /// add a capture client
    Create(CaptureHandle, Position, CaptureType),
    /// destory a capture client
    Destroy(CaptureHandle),
    /// reenable input capture
    Reenable,
    /// set release bind
    SetReleaseBind(Vec<scancode::Linux>),
    /// set the mouse-jail bind
    SetJailBind(Vec<scancode::Linux>),
    /// set the binds that enter a client without an edge crossing
    SetEnterBinds(HashMap<lan_mouse_ipc::Position, Vec<scancode::Linux>>),
    /// set the keys rewritten on their way to other devices
    SetRemap(Box<KeyRemap>),
    /// set the scroll axes inverted on their way to other devices
    SetScrollInvert(ScrollInvert),
}

impl Capture {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        backend: Option<input_capture::Backend>,
        conn: LanMouseConnection,
        release_bind: Vec<scancode::Linux>,
        jail_bind: Vec<scancode::Linux>,
        enter_binds: HashMap<lan_mouse_ipc::Position, Vec<scancode::Linux>>,
        window_identifier: Arc<Mutex<Option<WindowIdentifier>>>,
        remap: KeyRemap,
        scroll_invert: ScrollInvert,
    ) -> Self {
        let (request_tx, request_rx) = channel();
        let (event_tx, event_rx) = channel();
        let cancellation_token = CancellationToken::new();
        let capture_task = CaptureTask {
            active_client: None,
            backend,
            cancellation_token: cancellation_token.clone(),
            captures: Default::default(),
            conn,
            event_tx,
            request_rx,
            release_bind: Rc::new(RefCell::new(release_bind)),
            enter_binds,
            remap,
            scroll_invert,
            state: Default::default(),
            jail: Cell::new(false),
            jail_bind: RefCell::new(jail_bind),
            jail_bind_prev_engaged: Cell::new(false),
            window_identifier,
            enter_t: 0.5,
            pending_modifiers: None,
        };
        let task = spawn_local(capture_task.run());
        Self {
            cancellation_token,
            request_tx,
            task,
            event_rx,
        }
    }

    pub(crate) fn reenable(&self) {
        self.request_tx
            .send(CaptureRequest::Reenable)
            .expect("channel closed");
    }

    pub(crate) async fn terminate(&mut self) {
        self.cancellation_token.cancel();
        log::debug!("terminating capture");
        if let Err(e) = (&mut self.task).await {
            log::warn!("{e}");
        }
    }

    pub(crate) fn create(
        &self,
        handle: CaptureHandle,
        pos: lan_mouse_ipc::Position,
        capture_type: CaptureType,
    ) {
        let pos = to_capture_pos(pos);
        self.request_tx
            .send(CaptureRequest::Create(handle, pos, capture_type))
            .expect("channel closed");
    }

    pub(crate) fn destroy(&self, handle: CaptureHandle) {
        self.request_tx
            .send(CaptureRequest::Destroy(handle))
            .expect("channel closed");
    }

    pub(crate) fn release(&self) {
        self.request_tx
            .send(CaptureRequest::Release)
            .expect("channel closed");
    }

    pub(crate) async fn event(&mut self) -> ICaptureEvent {
        self.event_rx.recv().await.expect("channel closed")
    }

    pub(crate) fn set_release_bind(&mut self, bind: Vec<scancode::Linux>) {
        let _ = self.request_tx.send(CaptureRequest::SetReleaseBind(bind));
    }

    pub(crate) fn set_jail_bind(&mut self, bind: Vec<scancode::Linux>) {
        self.request_tx
            .send(CaptureRequest::SetJailBind(bind))
            .expect("channel closed");
    }

    pub(crate) fn set_enter_binds(
        &mut self,
        binds: HashMap<lan_mouse_ipc::Position, Vec<scancode::Linux>>,
    ) {
        let _ = self.request_tx.send(CaptureRequest::SetEnterBinds(binds));
    }

    pub(crate) fn set_remap(&mut self, remap: KeyRemap) {
        let _ = self
            .request_tx
            .send(CaptureRequest::SetRemap(Box::new(remap)));
    }

    pub(crate) fn set_scroll_invert(&mut self, scroll_invert: ScrollInvert) {
        let _ = self
            .request_tx
            .send(CaptureRequest::SetScrollInvert(scroll_invert));
    }
}

/// debounce a statement `$st`, i.e. the statement is executed only if the
/// time since the previous execution is at least `$dur`.
/// `$prev` is used to keep track of this timestamp
macro_rules! debounce {
    ($prev:ident, $dur:expr, $st:stmt) => {
        let exec = match $prev.get() {
            None => true,
            Some(instant) if instant.elapsed() > $dur => true,
            _ => false,
        };
        if exec {
            $prev.replace(Some(Instant::now()));
            $st
        }
    };
}

struct CaptureTask {
    active_client: Option<CaptureHandle>,
    backend: Option<input_capture::Backend>,
    cancellation_token: CancellationToken,
    captures: Vec<(CaptureHandle, Position, CaptureType)>,
    conn: LanMouseConnection,
    event_tx: Sender<ICaptureEvent>,
    release_bind: Rc<RefCell<Vec<scancode::Linux>>>,
    enter_binds: HashMap<lan_mouse_ipc::Position, Vec<scancode::Linux>>,
    remap: KeyRemap,
    scroll_invert: ScrollInvert,
    request_rx: Receiver<CaptureRequest>,
    state: State,
    /// jail the mouse cursor to the local machine
    jail: Cell<bool>,
    jail_bind: RefCell<Vec<scancode::Linux>>,
    /// last observed "jail bind engaged" state, for edge detection
    jail_bind_prev_engaged: Cell<bool>,
    window_identifier: Arc<Mutex<Option<WindowIdentifier>>>,
    /// normalized cross-axis position from the most recent
    /// [`CaptureEvent::Begin`], reused when [`State::WaitingForAck`]
    /// re-sends `Enter` — it's the same logical crossing, just retried.
    enter_t: f64,
    /// last `Modifiers` update observed while still waiting for the
    /// enter ack. The lock-state sync emitted right after
    /// [`CaptureEvent::Begin`] must not be replaced by the `Enter`
    /// retry like a regular input event, so it is queued here and
    /// flushed once the ack arrives.
    pending_modifiers: Option<KeyboardEvent>,
}

impl CaptureTask {
    fn add_capture(&mut self, handle: CaptureHandle, pos: Position, capture_type: CaptureType) {
        self.captures.push((handle, pos, capture_type));
    }

    fn remove_capture(&mut self, handle: CaptureHandle) {
        self.captures.retain(|&(h, ..)| handle != h);
    }

    fn is_default_capture_at(&self, pos: Position) -> bool {
        self.captures
            .iter()
            .any(|&(_, p, t)| p == pos && t == CaptureType::Default)
    }

    fn get_pos(&self, handle: CaptureHandle) -> Position {
        self.captures
            .iter()
            .find(|(h, ..)| *h == handle)
            .expect("no such capture")
            .1
    }

    fn capture_enter_binds(&self) -> HashMap<Position, Vec<scancode::Linux>> {
        self.enter_binds
            .iter()
            .map(|(&pos, bind)| (to_capture_pos(pos), bind.clone()))
            .collect()
    }

    fn get_type(&self, handle: CaptureHandle) -> CaptureType {
        self.captures
            .iter()
            .find(|(h, ..)| *h == handle)
            .expect("no such capture")
            .2
    }

    async fn create_backend_capture(
        capture: &mut InputCapture,
        handle: CaptureHandle,
        pos: Position,
        capture_type: CaptureType,
    ) -> Result<(), CaptureError> {
        match capture_type {
            CaptureType::Default => capture.create(handle, pos).await,
            CaptureType::EnterOnly => capture.create_enter_only(handle, pos).await,
        }
    }

    async fn run(mut self) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        loop {
            if let Err(e) = self.do_capture().await {
                log::warn!("input capture exited: {e}");
            }
            loop {
                tokio::select! {
                    r = self.request_rx.recv() => match r.expect("channel closed") {
                        CaptureRequest::Reenable => break,
                        CaptureRequest::Create(h, p, t) => self.add_capture(h, p, t),
                        CaptureRequest::Destroy(h) => self.remove_capture(h),
                        CaptureRequest::Release => { /* nothing to do */ }
                        CaptureRequest::SetReleaseBind(bind) => {
                            self.release_bind.borrow_mut().clone_from(&bind);
                        }
                        CaptureRequest::SetJailBind(bind) => {
                            *self.jail_bind.borrow_mut() = bind;
                        }
                        CaptureRequest::SetEnterBinds(binds) => self.enter_binds = binds,
                        CaptureRequest::SetRemap(remap) => self.remap = *remap,
                        CaptureRequest::SetScrollInvert(scroll_invert) => {
                            self.scroll_invert = scroll_invert
                        }
                    },
                    _ = self.cancellation_token.cancelled() => return,
                }
            }
        }
    }

    async fn do_capture(&mut self) -> Result<(), InputCaptureError> {
        /* allow cancelling capture request */
        let mut capture = tokio::select! {
            r = InputCapture::new(self.backend, self.window_identifier.clone()) => r?,
            _ = self.cancellation_token.cancelled() => return Ok(()),
        };

        // the backend is recreated whenever a capture session
        // restarts, so the binds have to be re-applied here rather
        // than only when they change
        capture.set_enter_binds(self.capture_enter_binds());

        let _capture_guard = DropGuard::new(
            self.event_tx.clone(),
            ICaptureEvent::CaptureEnabled,
            ICaptureEvent::CaptureDisabled,
        );

        /* create barriers for active clients */
        let r = self.create_captures(&mut capture).await;
        if let Err(e) = r {
            capture.terminate().await?;
            return Err(e.into());
        }

        let r = self.do_capture_session(&mut capture).await;
        if let Err(error) = &r {
            let active = self.active_client;
            // The event queue may contain stale presses. Do not send cleanup
            // input through a failing session or start a new connection.
            if let Err(cleanup) = self.release_capture_with(&mut capture, false, None).await {
                log::warn!("failed to release capture after backend error: {cleanup}");
            }
            self.remap.reset_session();
            if let Some(handle) = active {
                self.conn.abort_capture(handle).await;
            }
            self.event_tx
                .send(ICaptureEvent::CaptureFailed(error.to_string()))
                .expect("channel closed");
        }

        // FIXME replace with async drop when stabilized
        capture.terminate().await?;

        r
    }

    async fn create_captures(&mut self, capture: &mut InputCapture) -> Result<(), CaptureError> {
        let captures = self.captures.clone();
        for (handle, pos, capture_type) in captures {
            tokio::select! {
                r = Self::create_backend_capture(capture, handle, pos, capture_type) => r?,
                _ = self.cancellation_token.cancelled() => return Ok(()),
            }
        }
        Ok(())
    }

    async fn do_capture_session(
        &mut self,
        capture: &mut InputCapture,
    ) -> Result<(), InputCaptureError> {
        loop {
            tokio::select! {
                event = capture.next() => match event {
                    Some(event) => self.handle_capture_event(capture, event?).await?,
                    None => return Ok(()),
                },
                (handle, event) = self.conn.recv() => {
                    // clipboard events are accepted from any client
                    if let ProtoEvent::Input(input_event::Event::Clipboard(clipboard_event)) = &event {
                        self.event_tx.send(ICaptureEvent::ClipboardReceived(clipboard_event.clone())).expect("channel closed");
                        continue;
                    }
                    if let Some(active) = self.active_client {
                        if handle != active {
                            // we only care about events coming from the client we are currently connected to
                            // only `Ack` and `Leave` are relevant
                            continue
                        }
                    }

                    match event {
                        // connection acknowlegded => set state to Sending
                        ProtoEvent::Ack(_) => {
                            log::info!("client {handle} acknowledged the connection!");
                            self.state = State::Sending;
                            if let Some(mods) = self.pending_modifiers.take() {
                                let _ = self
                                    .conn
                                    .send(ProtoEvent::Input(Event::Keyboard(mods)), handle)
                                    .await;
                            }
                        }
                        // client disconnected
                        ProtoEvent::Leave(_, t) => {
                            log::info!("releasing capture: left remote client device region");
                            self.release_capture(capture, Some(t)).await?;
                        },
                        _ => {}
                    }
                },
                e = self.request_rx.recv() => match e.expect("channel closed") {
                    CaptureRequest::Reenable => { /* already active */ },
                    CaptureRequest::Release => self.release_capture(capture, None).await?,
                    CaptureRequest::Create(h, p, t) => {
                        self.add_capture(h, p, t);
                        Self::create_backend_capture(capture, h, p, t).await?;
                    }
                    CaptureRequest::Destroy(h) => {
                        // If the capture we're tearing down is the
                        // currently-active one, treat this as a
                        // release for hook purposes. The release_capture
                        // path also clears active_client and flushes
                        // pressed-key state to the peer; without this,
                        // `cli deactivate` (or a hostname change
                        // re-creating the client) would skip leave_hook.
                        if self.active_client == Some(h) {
                            self.release_capture(capture, None).await?;
                        }
                        self.remove_capture(h);
                        capture.destroy(h).await?;
                    }
                    CaptureRequest::SetReleaseBind(bind) => {
                        self.release_bind.borrow_mut().clone_from(&bind);
                    }
                    CaptureRequest::SetJailBind(bind) => {
                        *self.jail_bind.borrow_mut() = bind;
                    }
                    CaptureRequest::SetEnterBinds(binds) => {
                        self.enter_binds = binds;
                        capture.set_enter_binds(self.capture_enter_binds());
                    }
                    CaptureRequest::SetRemap(remap) => self.remap = *remap,
                    CaptureRequest::SetScrollInvert(scroll_invert) => {
                        self.scroll_invert = scroll_invert
                    }
                },
                _ = self.cancellation_token.cancelled() => break,
            }
        }
        Ok(())
    }

    /// Toggle the "mouse jail" whenever the jail bind is engaged, i.e. all
    /// its keys are currently pressed. This has the caveat of mod keys
    /// toggling to the programmatic state of the jail, not the
    /// physical state of the keys. For example, if ScrollLock is the jail
    /// bind, and it is engaged when the app starts, the jail's state
    /// will be inverted from the physical state of the key.
    /// This is a tradeoff to avoid significant overhead and platform-specific code.
    /// Remembers if the key(s) were already engaged on a previous call, so that
    /// auto-repeat does not toggle the jail repeatedly.
    ///
    /// Returns whether the key(s) of the bind /are currently engaged.
    fn update_jail_from_bind(&mut self, capture: &InputCapture) -> bool {
        let bind = self.jail_bind.borrow();
        if bind.is_empty() {
            return false;
        }
        let engaged = capture.keys_pressed(&bind);
        let jail = jail_bind_edge(engaged, self.jail_bind_prev_engaged.get(), self.jail.get());
        self.jail_bind_prev_engaged.replace(engaged);
        self.jail.replace(jail);
        engaged
    }

    async fn handle_capture_event(
        &mut self,
        capture: &mut InputCapture,
        event: (CaptureHandle, CaptureEvent),
    ) -> Result<(), CaptureError> {
        let (handle, event) = event;
        log::trace!("({handle}): {event:?}");

        if capture.keys_pressed(&self.release_bind.borrow()) {
            log::info!("releasing capture: release-bind pressed");
            return self.release_capture(capture, None).await;
        }

        let capture_type = self.get_type(handle);
        let pos = self.get_pos(handle);

        // arm/disarm the mouse jail whenever the jail bind is engaged (see
        // update_jail_from_bind).
        if matches!(event, CaptureEvent::Input(Event::Keyboard(_)))
            && self.update_jail_from_bind(capture)
        {
            // the jail trigger key(s) must not reach the peer
            return Ok(());
        }

        if let CaptureEvent::Begin(t) = event {
            self.event_tx
                .send(ICaptureEvent::CaptureBegin(handle, t))
                .expect("channel closed");
        }

        // enter only capture (for incoming connections)
        if capture_type == CaptureType::EnterOnly {
            // if there is no active outgoing connection at the current capture,
            // we release the capture
            if !self.is_default_capture_at(pos) {
                log::info!("releasing capture: no active client at this position");
                capture.release().await?;
            }
            // we dont care about events from incoming handles except for releasing the capture
            return Ok(());
        }

        // mouse jail active: confine this machine's input to the local screen,
        // do not transfer it across an edge to a peer
        if self.jail.get() {
            return Ok(());
        }

        // activated a new client
        if matches!(event, CaptureEvent::Begin(_)) && Some(handle) != self.active_client {
            self.state = State::WaitingForAck;
            self.active_client.replace(handle);
            self.event_tx
                .send(ICaptureEvent::ClientEntered(handle))
                .expect("channel closed");
        }

        let opposite_pos = to_proto_pos(self.get_pos(handle).opposite());

        let events: Vec<ProtoEvent> = match event {
            CaptureEvent::Begin(t) => {
                self.enter_t = t;
                vec![ProtoEvent::Enter(opposite_pos, t)]
            }
            CaptureEvent::Input(e) => match self.state {
                // connection not acknowledged, repeat `Enter` event
                State::WaitingForAck => {
                    if let Event::Keyboard(mods @ KeyboardEvent::Modifiers { .. }) = e {
                        self.pending_modifiers = Some(mods);
                    }
                    vec![ProtoEvent::Enter(opposite_pos, self.enter_t)]
                }
                // a single physical event can resolve into 0-2 outgoing
                // events once chord remapping buffers/replays a
                // modifier (see `KeyRemap::apply`)
                State::Sending => self
                    .remap
                    .apply(e)
                    .into_iter()
                    .map(|e| ProtoEvent::Input(self.scroll_invert.apply(e)))
                    .collect(),
            },
        };

        for event in events {
            if let Err(e) = self.conn.send(event, handle).await {
                const DUR: Duration = Duration::from_millis(500);
                debounce!(PREV_LOG, DUR, log::warn!("releasing capture: {e}"));
                // Funnel through release_capture so the leave_hook
                // fires and active_client is cleared (without this the
                // active_client field would stay stale until the next
                // Begin from a different handle). The send just failed, so
                // skip the key-up/Leave messages: they fail the same way and
                // each logs a warning on every edge crossing.
                self.release_capture_with(capture, false, None).await?;
                break;
            }
        }
        Ok(())
    }

    async fn release_capture(
        &mut self,
        capture: &mut InputCapture,
        warp_to: Option<f64>,
    ) -> Result<(), CaptureError> {
        self.release_capture_with(capture, true, warp_to).await
    }

    /// releases the capture, optionally warping the cursor to a
    /// normalized cross-axis position `warp_to` along the edge it was
    /// captured at first — set when the peer told us to leave with a
    /// specific hand-back spot (see [`ProtoEvent::Leave`]), `None` for
    /// a plain release (release-bind, explicit release request, ...)
    async fn release_capture_with(
        &mut self,
        capture: &mut InputCapture,
        notify_peer: bool,
        warp_to: Option<f64>,
    ) -> Result<(), CaptureError> {
        self.pending_modifiers = None;
        // If we have an active client, notify them we're leaving
        if let Some(handle) = self.active_client.take() {
            // Surface the leave to the service layer so it can fire
            // the per-client leave_hook. Sent before the network
            // teardown below so we never race against the peer
            // disappearing.
            self.event_tx
                .send(ICaptureEvent::ClientLeft(handle))
                .expect("channel closed");
            if !notify_peer {
                capture.take_pressed_keys();
                return match warp_to {
                    Some(t) => capture.release_to(t).await,
                    None => capture.release().await,
                };
            }
            // Synthesize key-up events for every key still held in the
            // capture's pressed_keys set BEFORE sending Leave. Without
            // this, pressing the release-bind chord (typically all four
            // modifiers) leaves the peer with phantom held modifiers:
            // the down events were forwarded while capture was active,
            // but the matching up events arrive after the local tap
            // flips to passthrough and never reach the peer. The peer
            // then runs every subsequent keystroke through those held
            // mods until its watchdog times out (1+ s) or our Leave
            // arrives — and Leave can be lost over UDP/DTLS.
            for key in capture.take_pressed_keys() {
                // `pressed_keys` holds the *physical* keys, so these
                // have to go through the same remap the down events
                // did — otherwise the peer is released from a key it
                // was never pressed with and keeps holding the one it
                // actually got. A key still `pending` on an unresolved
                // chord never had a down event sent for it at all —
                // `release_key` reports `None` for those, and no
                // key-up should be synthesized either.
                let Some(target) = self.remap.release_key(key) else {
                    continue;
                };
                let key_up = ProtoEvent::Input(Event::Keyboard(KeyboardEvent::Key {
                    time: 0,
                    key: target as u32,
                    state: 0,
                }));
                if let Err(e) = self.conn.send(key_up, handle).await {
                    log::warn!("failed to send key-up to client {handle}: {e}");
                }
            }
            // Reset the modifier mask too. The peer's input-emulation
            // layer keeps a separate XKB-style modifier state that's
            // updated by KeyboardEvent::Modifiers, distinct from the
            // pressed_keys set drained above. Without this, an
            // already-locked CapsLock would survive the release.
            let mods_zero = ProtoEvent::Input(Event::Keyboard(KeyboardEvent::Modifiers {
                depressed: 0,
                latched: 0,
                locked: 0,
                group: 0,
            }));
            if let Err(e) = self.conn.send(mods_zero, handle).await {
                log::warn!("failed to reset modifiers on client {handle}: {e}");
            }

            log::info!("sending Leave event to client {handle}");
            // the peer doesn't act on this `t` — it's *our* Leave,
            // stopping capture towards them, not a hand-back to us
            if let Err(e) = self.conn.send(ProtoEvent::Leave(0, 0.5), handle).await {
                log::warn!("failed to send Leave to client {handle}: {e}");
            }
        }
        match warp_to {
            Some(t) => capture.release_to(t).await,
            None => capture.release().await,
        }
    }
}

thread_local! {
    static PREV_LOG: Cell<Option<Instant>> = const { Cell::new(None) };
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum State {
    #[default]
    WaitingForAck,
    Sending,
}

/// Whether engaging `bind` (all keys currently pressed) is a *fresh* engagement
/// given the previous state, returning the resulting jail flag. Edge-triggered
/// on the false -> true transition, so auto-repeat does not toggle the jail
/// repeatedly.
fn jail_bind_edge(engaged: bool, prev_engaged: bool, current_jail: bool) -> bool {
    if engaged && !prev_engaged {
        !current_jail
    } else {
        current_jail
    }
}

fn to_capture_pos(pos: lan_mouse_ipc::Position) -> input_capture::Position {
    match pos {
        lan_mouse_ipc::Position::Left => input_capture::Position::Left,
        lan_mouse_ipc::Position::Right => input_capture::Position::Right,
        lan_mouse_ipc::Position::Top => input_capture::Position::Top,
        lan_mouse_ipc::Position::Bottom => input_capture::Position::Bottom,
    }
}

fn to_proto_pos(pos: input_capture::Position) -> lan_mouse_proto::Position {
    match pos {
        input_capture::Position::Left => lan_mouse_proto::Position::Left,
        input_capture::Position::Right => lan_mouse_proto::Position::Right,
        input_capture::Position::Top => lan_mouse_proto::Position::Top,
        input_capture::Position::Bottom => lan_mouse_proto::Position::Bottom,
    }
}

struct DropGuard<T> {
    tx: Sender<T>,
    on_drop: Option<T>,
}

impl<T> DropGuard<T> {
    fn new(tx: Sender<T>, on_new: T, on_drop: T) -> Self {
        tx.send(on_new).expect("channel closed");
        let on_drop = Some(on_drop);
        Self { tx, on_drop }
    }
}

impl<T> Drop for DropGuard<T> {
    fn drop(&mut self) {
        self.tx
            .send(self.on_drop.take().expect("item"))
            .expect("channel closed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jail_bind_toggles_only_on_fresh_engagement() {
        // not engaged => jail unchanged
        assert!(!jail_bind_edge(false, false, false));
        // first press => toggles on
        assert!(jail_bind_edge(true, false, false));
        // repeat press (auto-repeat) => stays on, no re-toggle
        assert!(jail_bind_edge(true, true, true));
        // release => jail stays on
        assert!(jail_bind_edge(false, true, true));
        // next press => toggles off
        assert!(!jail_bind_edge(true, false, true));
    }
}
