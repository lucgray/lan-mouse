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

use crate::connect::{CleanupTarget, LanMouseConnection};
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
    /// Cleanup is still owned by this attempt; capture cannot resume yet.
    CaptureCleanupPending(String),
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReleaseMode {
    NotifyPeer,
    Silent,
    AbortPeer,
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
            let result = self.do_capture().await;
            report_capture_exit(&self.event_tx, &result);
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
            return await_capture_termination(&self.event_tx, Err(e.into()), capture.terminate())
                .await;
        }

        let result = self.do_capture_session(&mut capture).await;
        self.finish_capture_session(&mut capture, result).await
    }

    async fn finish_capture_session(
        &mut self,
        capture: &mut InputCapture,
        r: Result<(), InputCaptureError>,
    ) -> Result<(), InputCaptureError> {
        if r.is_err() {
            // Cancel the failed transport before waiting for native release.
            // This mode snapshots the original generation and never sends stale input.
            if let Err(cleanup) = self
                .release_capture_with(capture, ReleaseMode::AbortPeer, None)
                .await
            {
                log::warn!("failed to release capture after backend error: {cleanup}");
            }
            self.remap.reset_session();
        }

        // FIXME replace with async drop when stabilized
        await_capture_termination(&self.event_tx, r, capture.terminate()).await
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
        let disconnected = self.conn.disconnect_signal();
        loop {
            // Drain closure notices before another input can reconnect this target.
            for handle in self.conn.take_disconnected() {
                self.handle_disconnected(capture, handle).await?;
            }
            tokio::select! {
                _ = disconnected.notified() => {},
                event = capture.next() => match event {
                    Some(event) => self.handle_capture_event(capture, event?).await?,
                    None => return self.capture_stream_ended(),
                },
                received = self.conn.recv() => {
                    let crate::connect::ReceivedEvent { handle, event, .. } = received;
                    if self.active_client != Some(handle) {
                        // Late Ack/Leave cannot change an idle or different capture.
                        continue;
                    }

                    match event {
                        // connection acknowlegded => set state to Sending
                        ProtoEvent::Ack(_) => {
                            log::info!("client {handle} acknowledged the connection!");
                            self.state = State::Sending;
                            if let Some(mods) = self.pending_modifiers.take() {
                                self.forward_input(capture, ProtoEvent::Input(Event::Keyboard(mods)), handle).await?;
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

    fn capture_stream_ended(&self) -> Result<(), InputCaptureError> {
        if self.cancellation_token.is_cancelled() {
            return Ok(());
        }
        Err(CaptureError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "input capture stream closed unexpectedly",
        ))
        .into())
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

        let release_engaged = {
            let bind = self.release_bind.borrow();
            !bind.is_empty() && capture.keys_pressed(&bind)
        };
        if release_engaged {
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

        // Input buffered before release or routed for another handle must not
        // re-enter a peer without a fresh Begin for that target.
        if matches!(event, CaptureEvent::Input(_)) && self.active_client != Some(handle) {
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
            if !self.forward_input(capture, event, handle).await? {
                break;
            }
        }
        Ok(())
    }

    async fn forward_input(
        &mut self,
        capture: &mut InputCapture,
        event: ProtoEvent,
        handle: CaptureHandle,
    ) -> Result<bool, CaptureError> {
        let result = tokio::select! {
            _ = self.cancellation_token.cancelled() => Err(crate::connect::LanMouseConnectionError::NotConnected),
            result = self.conn.send(event, handle) => result,
        };
        if let Err(error) = result {
            const DUR: Duration = Duration::from_millis(500);
            debounce!(PREV_LOG, DUR, log::warn!("releasing capture: {error}"));
            self.release_capture_with(capture, ReleaseMode::Silent, None)
                .await?;
            return Ok(false);
        }
        Ok(true)
    }

    async fn handle_disconnected(
        &mut self,
        capture: &mut InputCapture,
        handle: CaptureHandle,
    ) -> Result<(), CaptureError> {
        if self.active_client == Some(handle) {
            log::info!("releasing capture: client {handle} transport disconnected");
            self.remap.reset_session();
            self.state = State::WaitingForAck;
            self.release_capture_with(capture, ReleaseMode::Silent, None)
                .await?;
        }
        Ok(())
    }

    async fn release_capture(
        &mut self,
        capture: &mut InputCapture,
        warp_to: Option<f64>,
    ) -> Result<(), CaptureError> {
        self.release_capture_with(capture, ReleaseMode::NotifyPeer, warp_to)
            .await
    }

    /// releases the capture, optionally warping the cursor to a
    /// normalized cross-axis position `warp_to` along the edge it was
    /// captured at first — set when the peer told us to leave with a
    /// specific hand-back spot (see [`ProtoEvent::Leave`]), `None` for
    /// a plain release (release-bind, explicit release request, ...)
    async fn release_capture_with(
        &mut self,
        capture: &mut InputCapture,
        mode: ReleaseMode,
        warp_to: Option<f64>,
    ) -> Result<(), CaptureError> {
        self.pending_modifiers = None;
        self.state = State::WaitingForAck;
        let active = self.active_client.take();
        let abort = active.and_then(|handle| {
            self.conn
                .capture_revision(handle)
                .map(|revision| (handle, revision))
        });
        let cleanup = active.and_then(|handle| self.conn.prepare_cleanup(handle));
        let mut events = Vec::new();
        for key in capture.take_pressed_keys() {
            if let Some(target) = self.remap.release_key(key) {
                events.push(ProtoEvent::Input(Event::Keyboard(KeyboardEvent::Key {
                    time: 0,
                    key: target as u32,
                    state: 0,
                })));
            }
        }
        self.remap.reset_session();
        if let Some(handle) = active {
            self.event_tx
                .send(ICaptureEvent::ClientLeft(handle))
                .expect("channel closed");
        }
        // Normal release restores the pointer before peer cleanup. Fatal capture
        // errors cancel the original transport first; neither path reconnects it.
        let release = async {
            match warp_to {
                Some(t) => capture.release_to(t).await,
                None => capture.release().await,
            }
        };
        release_native_capture(
            &self.conn,
            abort,
            cleanup.as_ref(),
            mode == ReleaseMode::AbortPeer,
            release,
        )
        .await?;
        if let Some(cleanup) = cleanup.filter(|_| mode == ReleaseMode::NotifyPeer) {
            events.push(ProtoEvent::Input(Event::Keyboard(
                KeyboardEvent::Modifiers {
                    depressed: 0,
                    latched: 0,
                    locked: 0,
                    group: 0,
                },
            )));
            events.push(ProtoEvent::Leave(0, 0.5));
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {},
                result = self.conn.send_cleanup(cleanup, events) => if let Err(error) = result {
                    log::warn!("capture release network cleanup failed: {error}");
                },
            }
        }
        Ok(())
    }
}

async fn release_native_capture<F>(
    conn: &LanMouseConnection,
    active: Option<(CaptureHandle, u64)>,
    cleanup: Option<&CleanupTarget>,
    abort_before_release: bool,
    release: F,
) -> Result<(), CaptureError>
where
    F: std::future::Future<Output = Result<(), CaptureError>>,
{
    let abort = || {
        if let Some((handle, revision)) = active {
            conn.abort_capture(handle, revision, cleanup);
        } else if let Some(cleanup) = cleanup {
            conn.abort_cleanup(cleanup);
        }
    };
    if abort_before_release {
        abort();
    }
    let result = release.await;
    // The original generation was already canceled on the fatal path. A late
    // release error must not re-resolve the handle or close a replacement.
    if result.is_err() && !abort_before_release {
        abort();
    }
    result
}

async fn await_capture_termination<F>(
    event_tx: &Sender<ICaptureEvent>,
    result: Result<(), InputCaptureError>,
    termination: F,
) -> Result<(), InputCaptureError>
where
    F: std::future::Future<Output = Result<(), CaptureError>>,
{
    tokio::pin!(termination);
    let cleanup = tokio::select! {
        biased;
        cleanup = &mut termination => cleanup,
        _ = tokio::time::sleep(Duration::from_millis(250)) => {
            let reason = match &result {
                Err(error) => error.to_string(),
                Ok(()) => "backend termination has not completed".into(),
            };
            log::warn!("input capture cleanup is still pending: {reason}");
            event_tx.send(ICaptureEvent::CaptureCleanupPending(reason)).expect("channel closed");
            // Keep ownership and keep polling the SAME future. The feedback
            // timer is not cancellation or permission to recreate the backend.
            termination.await
        }
    };
    capture_result_after_termination(result, cleanup)
}

fn capture_result_after_termination(
    result: Result<(), InputCaptureError>,
    termination: Result<(), CaptureError>,
) -> Result<(), InputCaptureError> {
    match (result, termination) {
        (Err(error), Err(cleanup)) => Err(CaptureError::Io(std::io::Error::other(format!(
            "{error}; backend termination also failed: {cleanup}"
        )))
        .into()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup)) => Err(cleanup.into()),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn report_capture_exit(event_tx: &Sender<ICaptureEvent>, result: &Result<(), InputCaptureError>) {
    if let Err(error) = result {
        log::warn!("input capture exited: {error}");
        event_tx
            .send(ICaptureEvent::CaptureFailed(error.to_string()))
            .expect("channel closed");
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

    #[tokio::test(flavor = "current_thread")]
    async fn fatal_release_cancels_original_before_wait_and_preserves_late_replacement() {
        use crate::{client::ClientManager, connect::tests::RefusedConnection};
        use webrtc_dtls::crypto::Certificate;
        tokio::task::LocalSet::new()
            .run_until(async {
                for native_error in [false, true] {
                    let clients = ClientManager::default();
                    let handle = clients.add_client();
                    clients.activate_client(handle);
                    let other = clients.add_client();
                    clients.activate_client(other);
                    let token = clients.target_token(handle).unwrap();
                    let other_token = clients.target_token(other).unwrap();
                    let conn = LanMouseConnection::new(
                        Certificate::generate_self_signed(vec![]).unwrap(),
                        clients.clone(),
                    );
                    let sender = conn.sender();
                    let old = Arc::new(RefusedConnection::default());
                    let healthy = Arc::new(RefusedConnection::default());
                    sender
                        .install_test_connection(
                            handle,
                            "127.0.0.1:2".parse().unwrap(),
                            old.clone(),
                        )
                        .await;
                    sender
                        .install_test_connection(
                            other,
                            "127.0.0.1:3".parse().unwrap(),
                            healthy.clone(),
                        )
                        .await;
                    let revision = conn.capture_revision(handle).unwrap();
                    let cleanup = conn.prepare_cleanup(handle).unwrap();
                    let (done, receiver) = tokio::sync::oneshot::channel();
                    let started = Cell::new(false);
                    let release = release_native_capture(
                        &conn,
                        Some((handle, revision)),
                        Some(&cleanup),
                        true,
                        async {
                            assert!(token.is_cancelled()); // cancellation precedes native future's first poll.
                            assert!(clients.active_addr(handle).is_none());
                            started.set(true);
                            receiver.await.unwrap();
                            if native_error {
                                Err(CaptureError::Io(std::io::Error::other(
                                    "native release failed",
                                )))
                            } else {
                                Ok(())
                            }
                        },
                    );
                    tokio::pin!(release);
                    tokio::select! {
                        _ = &mut release => panic!("native release must still be pending"),
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                    }
                    assert!(started.get());
                    assert!(token.is_cancelled());
                    assert!(old.closed.load(std::sync::atomic::Ordering::SeqCst));
                    assert!(!other_token.is_cancelled());
                    assert!(!healthy.closed.load(std::sync::atomic::Ordering::SeqCst));
                    let replacement = Arc::new(RefusedConnection::default());
                    sender
                        .install_test_connection(
                            handle,
                            "127.0.0.1:2".parse().unwrap(),
                            replacement.clone(),
                        )
                        .await;
                    let fresh = clients.target_token(handle).unwrap();
                    done.send(()).unwrap();
                    assert_eq!(release.await.is_err(), native_error);
                    assert!(!fresh.is_cancelled());
                    assert!(clients.active_addr(handle).is_some());
                    assert!(!replacement.closed.load(std::sync::atomic::Ordering::SeqCst));
                    assert!(old.sent.lock().unwrap().is_empty());
                    sender.terminate().await;
                }
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_capture_cleanup_reports_progress_and_retains_owner_until_completion() {
        struct CleanupGuard<'a>(&'a Cell<bool>);
        impl Drop for CleanupGuard<'_> {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        for primary_error in [false, true] {
            for cleanup_error in [false, true] {
                let (tx, mut events) = channel();
                let (done, receiver) = tokio::sync::oneshot::channel();
                let finished = Cell::new(false);
                let dropped = Cell::new(false);
                let guard = CleanupGuard(&dropped);
                let wait = async {
                    let result = if primary_error {
                        Err(CaptureError::ActivationClosed.into())
                    } else {
                        Ok(())
                    };
                    let result = await_capture_termination(&tx, result, async move {
                        let _guard = guard;
                        receiver.await.unwrap();
                        if cleanup_error {
                            Err(CaptureError::Io(std::io::Error::other("cleanup failed")))
                        } else {
                            Ok(())
                        }
                    })
                    .await;
                    finished.set(true);
                    report_capture_exit(&tx, &result);
                };
                tokio::pin!(wait);
                let notice = tokio::time::timeout(Duration::from_millis(650), async {
                    tokio::select! {
                        _ = &mut wait => panic!("cleanup must remain pending"),
                        event = events.recv() => event.unwrap(),
                    }
                })
                .await
                .unwrap();
                assert!(
                    matches!(notice, ICaptureEvent::CaptureCleanupPending(reason) if
                    reason.contains(if primary_error { "activation stream" } else { "termination has not completed" }))
                );
                assert!(!finished.get());
                assert!(!dropped.get()); // no cancel/drop/replacement of pending cleanup.
                assert!(
                    tokio::time::timeout(Duration::from_millis(10), events.recv())
                        .await
                        .is_err()
                );
                done.send(()).unwrap();
                wait.await;
                assert!(finished.get());
                assert!(dropped.get());
                if primary_error || cleanup_error {
                    let event = events.recv().await.unwrap();
                    assert!(matches!(event, ICaptureEvent::CaptureFailed(message) if
                        (!primary_error || message.contains("activation stream")) &&
                        (!cleanup_error || message.contains("cleanup failed"))));
                }
                assert!(
                    tokio::time::timeout(Duration::from_millis(10), events.recv())
                        .await
                        .is_err()
                );
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completed_capture_cleanup_emits_no_pending_notice() {
        let (tx, mut events) = channel();
        let result =
            await_capture_termination(&tx, Err(CaptureError::ActivationClosed.into()), async {
                Ok(())
            })
            .await;
        report_capture_exit(&tx, &result);
        assert!(matches!(
            events.recv().await.unwrap(),
            ICaptureEvent::CaptureFailed(_)
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), events.recv())
                .await
                .is_err()
        );
    }

    #[test]
    fn capture_exit_preserves_primary_and_cleanup_failures() {
        let result = Err(CaptureError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "barrier creation failed",
        ))
        .into());
        let termination = Err(CaptureError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "worker cleanup timed out",
        )));
        let result = capture_result_after_termination(result, termination).unwrap_err();
        assert!(result.to_string().contains("worker cleanup timed out"));
        assert!(result.to_string().contains("barrier creation failed"));
        for (failed, cleanup_failed) in [(false, false), (true, false), (false, true)] {
            let result = if failed {
                Err(CaptureError::ActivationClosed.into())
            } else {
                Ok(())
            };
            let cleanup = if cleanup_failed {
                Err(CaptureError::EndOfStream)
            } else {
                Ok(())
            };
            let result = capture_result_after_termination(result, cleanup);
            match (failed, cleanup_failed) {
                (false, false) => assert!(result.is_ok()),
                (true, false) => assert!(matches!(
                    result,
                    Err(InputCaptureError::Capture(CaptureError::ActivationClosed))
                )),
                (false, true) => assert!(matches!(
                    result,
                    Err(InputCaptureError::Capture(CaptureError::EndOfStream))
                )),
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn every_capture_exit_error_notifies_once_and_success_is_quiet() {
        let (tx, mut events) = channel();
        for result in [
            Err::<(), InputCaptureError>(
                input_capture::CaptureCreationError::NoAvailableBackend.into(),
            ),
            Err(CaptureError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "barrier creation failed",
            ))
            .into()),
            Err(CaptureError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "capture EOF",
            ))
            .into()),
            Err(CaptureError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "worker cleanup timed out",
            ))
            .into()),
        ] {
            let expected = result.as_ref().unwrap_err().to_string();
            report_capture_exit(&tx, &result);
            assert!(
                matches!(events.recv().await.unwrap(), ICaptureEvent::CaptureFailed(message) if message == expected)
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(10), events.recv())
                    .await
                    .is_err()
            );
        }
        report_capture_exit(&tx, &Ok(()));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), events.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capture_eof_cleans_active_session_and_reports_idle_failure() {
        use crate::{client::ClientManager, connect::tests::RefusedConnection};
        use webrtc_dtls::crypto::Certificate;
        tokio::task::LocalSet::new()
            .run_until(async {
            for active in [false, true] {
            let clients = ClientManager::default(); let handle = clients.add_client(); clients.activate_client(handle);
            let token = clients.target_token(handle).unwrap();
            let conn = LanMouseConnection::new(Certificate::generate_self_signed(vec![]).unwrap(), clients.clone());
            let sender = conn.sender();
            let transport = Arc::new(RefusedConnection::default());
            sender.install_test_connection(handle, "127.0.0.1:2".parse().unwrap(), transport.clone()).await;
            let (event_tx, mut events) = channel(); let (_requests, request_rx) = channel();
            let mut task = CaptureTask {
                active_client: active.then_some(handle), backend: Some(input_capture::Backend::Dummy),
                cancellation_token: CancellationToken::new(), captures: vec![], conn,
                event_tx, request_rx, release_bind: Default::default(), enter_binds: Default::default(),
                remap: KeyRemap::new(Default::default(), vec![crate::remap::ChordRemap {
                    modifier: scancode::Linux::KeyLeftMeta, trigger: scancode::Linux::KeyTab, to: scancode::Linux::KeyLeftAlt,
                }]),
                scroll_invert: Default::default(), state: State::Sending,
                jail: Cell::new(false), jail_bind: Default::default(), jail_bind_prev_engaged: Cell::new(false),
                window_identifier: Default::default(), enter_t: 0.5,
                pending_modifiers: Some(KeyboardEvent::Modifiers { depressed: 64, latched: 0, locked: 0, group: 0 }),
            };
            assert!(task.remap.apply(Event::Keyboard(KeyboardEvent::Key { time: 0, key: scancode::Linux::KeyLeftMeta as u32, state: 1 })).is_empty());
            let mut capture = InputCapture::new(Some(input_capture::Backend::Dummy), Default::default()).await.unwrap();
            let result = task.capture_stream_ended();
            let result = task.finish_capture_session(&mut capture, result).await;
            report_capture_exit(&task.event_tx, &result);
            assert!(matches!(result, Err(InputCaptureError::Capture(CaptureError::Io(error))) if error.kind() == std::io::ErrorKind::UnexpectedEof));
            assert!(task.active_client.is_none());
            assert!(task.pending_modifiers.is_none());
            assert_eq!(task.state, State::WaitingForAck);
            assert_eq!(token.is_cancelled(), active);
            assert_eq!(clients.active_addr(handle).is_none(), active);
            assert_eq!(task.remap.release_key(scancode::Linux::KeyLeftMeta), Some(scancode::Linux::KeyLeftMeta));
            assert!(transport.sent.lock().unwrap().is_empty());
            if active {
                assert!(matches!(events.recv().await.unwrap(), ICaptureEvent::ClientLeft(h) if h == handle));
            }
            assert!(matches!(events.recv().await.unwrap(), ICaptureEvent::CaptureFailed(message) if message.contains("stream closed unexpectedly")));
            assert!(tokio::time::timeout(Duration::from_millis(10), events.recv()).await.is_err());
            // EOF during requested shutdown remains successful; service shutdown owns connection cleanup.
            task.cancellation_token.cancel();
            assert!(task.capture_stream_ended().is_ok());
            sender.terminate().await;
            }
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fatal_capture_abort_preserves_replacement_and_never_waits_for_close() {
        use crate::{client::ClientManager, connect::tests::RefusedConnection};
        use webrtc_dtls::crypto::Certificate;
        tokio::task::LocalSet::new()
            .run_until(async {
                let clients = ClientManager::default();
                let handle = clients.add_client();
                clients.activate_client(handle);
                let conn = LanMouseConnection::new(
                    Certificate::generate_self_signed(vec![]).unwrap(),
                    clients.clone(),
                );
                let sender = conn.sender();
                let old = Arc::new(RefusedConnection::default());
                sender
                    .install_test_connection(handle, "127.0.0.1:2".parse().unwrap(), old.clone())
                    .await;
                let revision = conn.capture_revision(handle).unwrap();
                let cleanup = conn.prepare_cleanup(handle).unwrap();
                // A replacement may arrive while native release is awaited.
                clients.invalidate_target(handle);
                let replacement = Arc::new(RefusedConnection::default());
                sender
                    .install_test_connection(
                        handle,
                        "127.0.0.1:2".parse().unwrap(),
                        replacement.clone(),
                    )
                    .await;
                let fresh = clients.target_token(handle).unwrap();
                conn.abort_capture(handle, revision, Some(&cleanup));
                assert!(!fresh.is_cancelled());
                assert!(!replacement.closed.load(std::sync::atomic::Ordering::SeqCst));
                assert!(clients.active_addr(handle).is_some());
                tokio::time::timeout(Duration::from_millis(100), async {
                    while !old.closed.load(std::sync::atomic::Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                // Current-session fatal abort also returns without waiting for close.
                let stalled = Arc::new(RefusedConnection {
                    stall_close: true,
                    ..Default::default()
                });
                sender
                    .install_test_connection(
                        handle,
                        "127.0.0.1:2".parse().unwrap(),
                        stalled.clone(),
                    )
                    .await;
                let revision = conn.capture_revision(handle).unwrap();
                let cleanup = conn.prepare_cleanup(handle).unwrap();
                let token = clients.target_token(handle).unwrap();
                tokio::time::timeout(Duration::from_millis(50), async {
                    conn.abort_capture(handle, revision, Some(&cleanup));
                })
                .await
                .unwrap();
                assert!(token.is_cancelled());
                assert!(clients.active_addr(handle).is_none());
                sender.terminate().await;
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_release_error_aborts_pinned_peer_after_active_handle_is_taken() {
        use crate::{client::ClientManager, connect::tests::RefusedConnection};
        use webrtc_dtls::crypto::Certificate;
        tokio::task::LocalSet::new().run_until(async {
            let clients = ClientManager::default();
            let handle = clients.add_client(); clients.activate_client(handle);
            let other = clients.add_client(); clients.activate_client(other);
            let other_token = clients.target_token(other).unwrap();
            let old_token = clients.target_token(handle).unwrap();
            let conn = LanMouseConnection::new(Certificate::generate_self_signed(vec![]).unwrap(), clients.clone());
            let sender = conn.sender();
            let transport = Arc::new(RefusedConnection { stall_close: true, ..Default::default() });
            let healthy = Arc::new(RefusedConnection::default());
            sender.install_test_connection(handle, "127.0.0.1:2".parse().unwrap(), transport.clone()).await;
            sender.install_test_connection(other, "127.0.0.1:3".parse().unwrap(), healthy.clone()).await;
            let (event_tx, _events) = channel(); let (_requests, request_rx) = channel();
            let mut task = CaptureTask {
                active_client: Some(handle), backend: Some(input_capture::Backend::Dummy),
                cancellation_token: CancellationToken::new(), captures: vec![], conn,
                event_tx, request_rx, release_bind: Default::default(), enter_binds: Default::default(),
                remap: Default::default(), scroll_invert: Default::default(), state: State::Sending,
                jail: Cell::new(false), jail_bind: Default::default(), jail_bind_prev_engaged: Cell::new(false),
                window_identifier: Default::default(), enter_t: 0.5, pending_modifiers: None,
            };
            let active = task.active_client.take();
            let abort = active.and_then(|h| task.conn.capture_revision(h).map(|revision| (h, revision)));
            let cleanup = active.and_then(|h| task.conn.prepare_cleanup(h));
            let result = tokio::time::timeout(Duration::from_millis(50), release_native_capture(&task.conn, abort, cleanup.as_ref(), false, std::future::ready(
                Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "native release failed").into())))).await.unwrap();
            assert!(matches!(result, Err(CaptureError::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut));
            assert!(task.active_client.is_none());
            assert!(old_token.is_cancelled());
            assert!(clients.active_addr(handle).is_none());
            assert!(!other_token.is_cancelled());
            assert!(clients.active_addr(other).is_some());
            assert!(!healthy.closed.load(std::sync::atomic::Ordering::SeqCst));
            assert!(transport.sent.lock().unwrap().is_empty()); // failure sends no stale cleanup input.
            sender.terminate().await;
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_input_releases_on_deadline_and_shutdown_cancels_without_waiting() {
        use crate::{client::ClientManager, connect::tests::RefusedConnection};
        use webrtc_dtls::crypto::Certificate;
        tokio::task::LocalSet::new().run_until(async {
            for shutdown in [false, true] {
                let clients = ClientManager::default(); let handle = clients.add_client(); clients.activate_client(handle);
                let conn = LanMouseConnection::new(Certificate::generate_self_signed(vec![]).unwrap(), clients.clone());
                let sender = conn.sender();
                let transport = Arc::new(RefusedConnection { stall_send: true, stall_close: true, ..Default::default() });
                sender.install_test_connection(handle, "127.0.0.1:2".parse().unwrap(), transport.clone()).await;
                let (event_tx, mut events) = channel(); let (requests, request_rx) = channel();
                let token = CancellationToken::new();
                let mut task = CaptureTask {
                    active_client: Some(handle), backend: Some(input_capture::Backend::Dummy),
                    cancellation_token: token.clone(), captures: vec![(handle, Position::Left, CaptureType::Default)], conn,
                    event_tx, request_rx, release_bind: Default::default(), enter_binds: Default::default(),
                    remap: Default::default(), scroll_invert: Default::default(), state: State::Sending,
                    jail: Cell::new(false), jail_bind: Default::default(), jail_bind_prev_engaged: Cell::new(false),
                    window_identifier: Default::default(), enter_t: 0.5,
                    pending_modifiers: Some(KeyboardEvent::Modifiers { depressed: 64, latched: 0, locked: 0, group: 0 }),
                };
                let mut capture = InputCapture::new(Some(input_capture::Backend::Dummy), Default::default()).await.unwrap();
                capture.create(handle, Position::Left).await.unwrap();
                tokio::time::timeout(Duration::from_millis(700), async {
                    let session = task.do_capture_session(&mut capture); tokio::pin!(session);
                    let control = async {
                        while transport.sent.lock().unwrap().is_empty() { tokio::task::yield_now().await; }
                        if shutdown { token.cancel(); }
                        else {
                            requests.send(CaptureRequest::Release).unwrap();
                            loop { if matches!(events.recv().await.unwrap(), ICaptureEvent::ClientLeft(h) if h == handle) { break; } }
                            // Dummy keeps emitting input after release. Those old
                            // events cannot send Enter or reconnect the failed peer.
                            tokio::time::sleep(Duration::from_millis(30)).await;
                            token.cancel();
                        }
                    };
                    tokio::pin!(control);
                    tokio::select! {
                        result = &mut session => { assert!(shutdown); result.unwrap(); },
                        _ = &mut control => { session.await.unwrap(); },
                    }
                }).await.unwrap();
                assert!(task.active_client.is_none()); assert!(task.pending_modifiers.is_none());
                assert_eq!(task.state, State::WaitingForAck);
                assert_eq!(transport.sent.lock().unwrap().len(), 1); // no key-up reconnect/extra sends after failure.
                assert!(matches!(lan_mouse_proto::decode_event_frame(&transport.sent.lock().unwrap()[0]).unwrap(), ProtoEvent::Enter(..))); // actual input path, not empty-bind cleanup.
                if shutdown {
                    assert!(matches!(events.recv().await.unwrap(), ICaptureEvent::CaptureBegin(..)));
                    assert!(matches!(events.recv().await.unwrap(), ICaptureEvent::ClientLeft(h) if h == handle));
                }
                assert!(tokio::time::timeout(Duration::from_millis(10), events.recv()).await.is_err());
                let healthy = Arc::new(RefusedConnection { succeed_send: true, ..Default::default() });
                sender.install_test_connection(handle, "127.0.0.1:2".parse().unwrap(), healthy.clone()).await;
                task.active_client = Some(handle);
                task.cancellation_token = CancellationToken::new();
                task.release_capture(&mut capture, Some(0.25)).await.unwrap();
                let packets: Vec<_> = healthy.sent.lock().unwrap().iter().map(|bytes| lan_mouse_proto::decode_event_frame(bytes).unwrap()).collect();
                assert_eq!(packets.len(), 2);
                assert!(matches!(packets[0], ProtoEvent::Input(Event::Keyboard(KeyboardEvent::Modifiers { depressed: 0, latched: 0, locked: 0, group: 0 }))));
                assert!(matches!(packets[1], ProtoEvent::Leave(..)));
                assert!(task.active_client.is_none());
                capture.terminate().await.unwrap(); sender.terminate().await;
            }
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn real_dtls_close_releases_idle_capture_without_input_or_leave() {
        use crate::{
            client::ClientManager,
            crypto,
            listen::{LanMouseListener, ListenEvent},
        };
        use std::sync::RwLock;
        use webrtc_dtls::crypto::Certificate;
        tokio::task::LocalSet::new().run_until(async {
            let clients = ClientManager::default();
            let handle = clients.add_client();
            let cert = Certificate::generate_self_signed(vec![]).unwrap();
            let keys = Arc::new(RwLock::new(HashMap::from([
                (crypto::certificate_fingerprint(&cert), "fixture".into())
            ])));
            let mut peer = LanMouseListener::new(0, Certificate::generate_self_signed(vec![]).unwrap(), keys).await.unwrap();
            clients.set_fix_ips(handle, vec!["127.0.0.1".parse().unwrap()]);
            clients.set_port(handle, peer.port());
            clients.activate_client(handle);
            let conn = LanMouseConnection::new(cert, clients.clone());
            let sender = conn.sender();
            assert!(sender.send(ProtoEvent::Ping, handle).await.is_err());
            let transport = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(ListenEvent::Accept { conn, .. }) = peer.next().await { break conn; }
                }
            }).await.unwrap();
            sender.clipboard_ready_signal().notified().await;
            let (event_tx, mut events) = channel();
            let (_requests, request_rx) = channel();
            let cancellation_token = CancellationToken::new();
            let mut task = CaptureTask {
                active_client: Some(handle), backend: Some(input_capture::Backend::Dummy),
                cancellation_token: cancellation_token.clone(), captures: vec![], conn,
                event_tx, request_rx, release_bind: Default::default(), enter_binds: Default::default(),
                remap: Default::default(), scroll_invert: Default::default(), state: State::Sending,
                jail: Cell::new(false), jail_bind: Default::default(), jail_bind_prev_engaged: Cell::new(false),
                window_identifier: Default::default(), enter_t: 0.5,
                pending_modifiers: Some(KeyboardEvent::Modifiers { depressed: 64, latched: 0, locked: 0, group: 0 }),
            };
            let mut capture = InputCapture::new(Some(input_capture::Backend::Dummy), Default::default()).await.unwrap();
            // No backend barriers: no input events can drive a failed send/release.
            // Terminate the peer without ever sending Leave or Ack.
            transport.close().await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                let session = task.do_capture_session(&mut capture);
                tokio::pin!(session);
                tokio::select! {
                    result = &mut session => panic!("capture exited unexpectedly: {result:?}"),
                    event = events.recv() => assert!(matches!(event.unwrap(), ICaptureEvent::ClientLeft(h) if h == handle)),
                }
                cancellation_token.cancel();
                session.await.unwrap();
            }).await.unwrap();
            assert!(task.active_client.is_none());
            assert!(task.pending_modifiers.is_none());
            assert_eq!(task.state, State::WaitingForAck);
            assert!(clients.active_addr(handle).is_none());
            assert!(tokio::time::timeout(Duration::from_millis(10), events.recv()).await.is_err());
            capture.terminate().await.unwrap();
            sender.terminate().await;
            peer.terminate().await;
        }).await;
    }

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
