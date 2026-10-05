use crate::clipboard_network::{ClipboardCompletion, ClipboardJobs, ClipboardRequest};
use crate::config::local_commit;
use crate::listen::ArcConn;
use crate::listen::{ClipboardSendError, LanMouseListener, ListenEvent, ListenerCreationError};
use futures::StreamExt;
use input_emulation::{
    EmulationHandle, EmulationOptions, InputConfig, InputEmulation, InputEmulationError,
};
use input_event::Event;
use lan_mouse_proto::{Position, ProtoEvent};
use local_channel::mpsc::{Receiver, Sender, channel};
use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
    net::SocketAddr,
    rc::Rc,
    time::{Duration, Instant},
};
use std::{
    cell::RefCell,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    select,
    task::{JoinHandle, spawn_local},
};
use tokio_util::sync::CancellationToken;

/// emulation handling events received from a listener
pub(crate) struct Emulation {
    task: JoinHandle<CleanupState<InputEmulation>>,
    request_tx: Sender<EmulationRequest>,
    event_rx: Receiver<EmulationEvent>,
    clipboard_tx: tokio::sync::mpsc::Sender<ClipboardRequest>,
    port_requests: tokio::sync::watch::Sender<Option<u16>>,
    clipboard_conns: Rc<RefCell<Vec<(SocketAddr, ArcConn)>>>,
    authorization: crate::listen::IncomingAuthorization,
    clipboard_generation: AtomicU64,
    clipboard_cancel: Mutex<CancellationToken>,
    terminated: bool,
    cleanup: CleanupState<InputEmulation>,
}

pub(crate) enum EmulationEvent {
    InputRejected {
        addr: SocketAddr,
        reason: String,
        admission: Option<crate::listen::ReaderLease>,
    },
    Connected {
        addr: SocketAddr,
        fingerprint: String,
        conn: ArcConn,
        admission: Option<crate::listen::ReaderLease>,
    },
    /// new connection
    Entered {
        /// address of the connection
        addr: SocketAddr,
        /// position of the connection
        pos: lan_mouse_ipc::Position,
        /// certificate fingerprint of the connection
        fingerprint: String,
        conn: ArcConn,
        input: Option<crate::input_budget::InputAdmission>,
        control: Option<std::sync::Arc<crate::input_budget::ControlLease>>,
    },
    /// connection closed
    Disconnected { addr: SocketAddr },
    /// actual DTLS connection ended or was replaced
    ConnectionClosed {
        addr: SocketAddr,
        admission: Option<crate::listen::ReaderLease>,
    },
    /// the port of the listener has changed
    PortChanged(Result<u16, ListenerCreationError>),
    /// emulation was disabled
    EmulationDisabled,
    /// backend operation failed; surfaced separately from the disabled status
    BackendFailed(String),
    InputCleanupFailed {
        addr: SocketAddr,
        input: Option<crate::input_budget::InputAdmission>,
    },
    InputOverloaded {
        addr: SocketAddr,
        admission: Option<crate::listen::ReaderLease>,
        input: Option<crate::input_budget::InputAdmission>,
    },
    /// emulation was enabled
    EmulationEnabled,
    /// capture should be released
    ReleaseNotify {
        addr: SocketAddr,
        conn: ArcConn,
        input: Option<crate::input_budget::InputAdmission>,
    },
    /// peer sent us a Hello with its build commit hash. Used to
    /// populate `client_manager.peer_commit` from the listen side
    /// too — without this, peer-version visibility silently fails
    /// whenever the outgoing connection in the *other* direction is
    /// broken (one-way setups, asymmetric NAT, peer's TCP listener
    /// down). The connect-side path stays as the primary source;
    /// this is the defensive fallback.
    PeerHello {
        addr: SocketAddr,
        commit: [u8; 8],
        conn: ArcConn,
        control: Option<std::sync::Arc<crate::input_budget::ControlLease>>,
    },
    /// clipboard data received from remote
    ClipboardReceived {
        event: input_event::ClipboardEvent,
        addr: SocketAddr,
        conn: ArcConn,
        control: Option<std::sync::Arc<crate::input_budget::ControlLease>>,
    },
    /// Completion of a network send, not acknowledgement of a remote OS write.
    ClipboardSendCompleted(ClipboardCompletion),
}

enum EmulationRequest {
    Reenable,
    /// release the peer's capture, handing the cursor back at the
    /// given normalized cross-axis position
    Release(SocketAddr, f64),
    Terminate,
    UpdateScrollingInversion(bool),
    UpdateMouseSensitivity(f64),
}

impl Emulation {
    pub(crate) fn new(
        backend: Option<input_emulation::Backend>,
        options: EmulationOptions,
        listener: LanMouseListener,
        input_config: (bool, f64),
    ) -> Self {
        let input_config = InputConfig {
            invert_scroll: input_config.0,
            mouse_sensitivity: input_config.1,
        };
        let emulation_proxy = EmulationProxy::new(backend, options, input_config);
        let (request_tx, request_rx) = channel();
        let (event_tx, event_rx) = channel();
        let (clipboard_tx, clipboard_rx) = tokio::sync::mpsc::channel(32);
        let clipboard_conns = listener.clipboard_connections();
        let authorization = listener.authorization();
        let port_requests = listener.port_requests();
        let emulation_task = ListenTask {
            listener,
            emulation_proxy,
            request_rx,
            event_tx,
            clipboard_rx,
        };
        let task = spawn_local(emulation_task.run());
        Self {
            task,
            request_tx,
            event_rx,
            clipboard_tx,
            clipboard_conns,
            authorization,
            port_requests,
            clipboard_generation: AtomicU64::new(0),
            clipboard_cancel: Mutex::new(CancellationToken::new()),
            terminated: false,
            cleanup: CleanupState::Complete,
        }
    }

    pub(crate) fn send_leave_event(&self, addr: SocketAddr, t: f64) {
        self.request_tx
            .send(EmulationRequest::Release(addr, t))
            .expect("channel closed");
    }

    pub(crate) fn send_clipboard(
        &self,
        addr: SocketAddr,
        event: input_event::ClipboardEvent,
    ) -> Result<(), &'static str> {
        if event.content_len() > lan_mouse_proto::MAX_CLIPBOARD_SIZE {
            return Err("Clipboard is too large to send");
        }
        let conn = self
            .clipboard_conns
            .borrow()
            .iter()
            .find(|(a, _)| *a == addr)
            .map(|(_, conn)| conn.clone());
        let request = ClipboardRequest {
            addr,
            conn: conn.clone(),
            event,
            generation: self.clipboard_generation(),
            outgoing: None,
            session_cancellation: conn
                .as_ref()
                .and_then(|conn| self.authorization.token(addr, conn)),
            cancellation: self
                .clipboard_cancel
                .lock()
                .expect("clipboard token")
                .child_token(),
        };
        self.clipboard_tx
            .try_send(request)
            .map_err(|_| "Clipboard send is busy or stopped; copy again")
    }

    pub(crate) fn clipboard_sessions(&self) -> Vec<(SocketAddr, ArcConn)> {
        self.clipboard_conns.borrow().clone()
    }

    pub(crate) fn clipboard_session_is_current(&self, addr: SocketAddr, conn: &ArcConn) -> bool {
        self.clipboard_conns
            .borrow()
            .iter()
            .any(|(a, current)| *a == addr && std::sync::Arc::ptr_eq(current, conn))
    }

    pub(crate) fn clipboard_scope(&self) -> (u64, CancellationToken) {
        (
            self.clipboard_generation(),
            self.clipboard_cancel
                .lock()
                .expect("clipboard token")
                .child_token(),
        )
    }

    pub(crate) fn clipboard_generation(&self) -> u64 {
        self.clipboard_generation.load(Ordering::Acquire)
    }

    pub(crate) fn clear_clipboard(&self) {
        self.clipboard_generation.fetch_add(1, Ordering::AcqRel);
        let mut token = self.clipboard_cancel.lock().expect("clipboard token");
        token.cancel();
        *token = CancellationToken::new();
    }

    pub(crate) fn reenable(&self) {
        self.request_tx
            .send(EmulationRequest::Reenable)
            .expect("channel closed");
    }

    pub(crate) fn request_port_change(&self, port: u16) {
        self.port_requests.send_replace(Some(port));
    }

    #[cfg(all(test, unix))]
    pub(crate) fn last_port_request(&self) -> Option<u16> {
        *self.port_requests.borrow()
    }

    pub(crate) fn request_scrolling_inversion(&self, invert_scroll: bool) {
        self.request_tx
            .send(EmulationRequest::UpdateScrollingInversion(invert_scroll))
            .expect("channel closed")
    }

    pub(crate) fn request_mouse_sensitivity_change(&self, mouse_sensitivity: f64) {
        self.request_tx
            .send(EmulationRequest::UpdateMouseSensitivity(mouse_sensitivity))
            .expect("channel closed")
    }

    pub(crate) async fn event(&mut self) -> EmulationEvent {
        self.event_rx.recv().await.expect("channel closed")
    }

    /// Failed cleanup remains owned here so repeated termination can retry.
    pub(crate) async fn terminate(&mut self) -> bool {
        if !self.terminated {
            self.clear_clipboard();
            log::debug!("terminating emulation");
            let _ = self.request_tx.send(EmulationRequest::Terminate);
            self.cleanup = match (&mut self.task).await {
                Ok(cleanup) => cleanup,
                Err(e) => {
                    log::warn!("emulation task failed during cleanup: {e}");
                    CleanupState::Failed
                }
            };
            self.terminated = true;
        }
        self.cleanup.retry().await
    }
}

impl Drop for Emulation {
    fn drop(&mut self) {
        // Also cancel independent sends if the owner exits without terminate.
        self.clipboard_cancel
            .get_mut()
            .expect("clipboard token")
            .cancel();
    }
}

struct ListenTask {
    listener: LanMouseListener,
    emulation_proxy: EmulationProxy,
    request_rx: Receiver<EmulationRequest>,
    event_tx: Sender<EmulationEvent>,
    clipboard_rx: tokio::sync::mpsc::Receiver<ClipboardRequest>,
}

impl ListenTask {
    async fn run(mut self) -> CleanupState<InputEmulation> {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        let mut last_response = HashMap::new();
        // peers that entered this device: addr -> (edge, fingerprint).
        // kept across temporary silence so a resuming sender does not
        // need to repeat Enter for the return edge to work again
        let mut entered_clients: HashMap<SocketAddr, (Position, String)> = HashMap::new();
        // addrs whose emulation session timed out while entered
        let mut dormant: HashSet<SocketAddr> = HashSet::new();
        let mut accepted_clients = HashSet::new();
        let mut clipboard_jobs = ClipboardJobs::default();
        let mut control_jobs = crate::control_network::ControlJobs::default();
        let revoked = self.listener.revoked_signal();
        loop {
            for (addr, _, admission) in self.listener.take_revoked() {
                let current = self.listener.clipboard_connection(addr);
                clipboard_jobs.cancel_stale(addr, current.as_ref());
                control_jobs.cancel_stale(addr, current.as_ref());
                if current.is_some() {
                    continue;
                }
                accepted_clients.remove(&addr);
                forget_peer(addr, &mut entered_clients, &mut dormant, &mut last_response);
                self.emulation_proxy
                    .remove_with_admission(addr, None, admission.clone());
                self.event_tx
                    .send(EmulationEvent::ConnectionClosed { addr, admission })
                    .expect("channel closed");
            }
            select! {
                _ = revoked.notified() => {},
                Some(request) = self.clipboard_rx.recv() => {
                    if request.cancellation.is_cancelled() { continue; }
                    if let Err(request) = clipboard_jobs.submit(request) {
                        self.event_tx.send(EmulationEvent::ClipboardSendCompleted(ClipboardCompletion {
                            addr: request.addr, generation: request.generation,
                            kind: request.event.kind(), bytes: request.event.content_len(),
                            outgoing: None, conn: request.conn,
                            result: Err(ClipboardSendError::Transport(std::io::Error::new(
                                std::io::ErrorKind::WouldBlock, "clipboard send is busy; copy again"
                            ).into())),
                        })).expect("channel closed");
                    }
                },
                completed = control_jobs.completed() => {
                    self.listener.finish_control_reply(completed);
                },
                completed = clipboard_jobs.completed() => {
                    self.event_tx.send(EmulationEvent::ClipboardSendCompleted(completed)).expect("channel closed");
                },
                e = self.listener.next() => {match e {
                    Some(ListenEvent::InputRejected { addr, reason, admission }) => { self.event_tx.send(EmulationEvent::InputRejected { addr, reason, admission }).expect("channel closed"); },
                    Some(ListenEvent::InputOverloaded { addr, admission }) => { self.event_tx.send(EmulationEvent::InputOverloaded { addr, admission, input: None }).expect("channel closed"); },
                    Some(ListenEvent::Msg { event, addr, conn, mut budget, control: _control }) => {
                        if !self.listener.is_current(addr, &conn) { continue; }
                        log::trace!("{event} <-<-<-<-<- {addr}");
                        last_response.insert(addr, Instant::now());
                        // a sender whose session timed out may resume without
                        // repeating Enter — restore its incoming registration
                        // so the return edge still works. Only input/Ping
                        // can resume it; Enter registers itself and Leave tears down.
                        if let Some((pos, fingerprint)) = resumed_edge(&mut dormant, &entered_clients, addr, &event) {
                            log::info!("incoming connection resumed: {addr}");
                            self.event_tx.send(EmulationEvent::Entered {
                                addr,
                                pos: to_ipc_pos(pos),
                                fingerprint,
                                conn: conn.clone(),
                                control: _control.clone(),
                                input: budget.as_mut().map(crate::input_budget::InputLease::share_admission),
                            }).expect("channel closed");
                        }
                        match event {
                            ProtoEvent::Enter(pos, t) => {
                                if let Some(fingerprint) = self.listener.get_certificate_fingerprint(addr, &conn) {
                                    if !self.listener.is_current(addr, &conn) { continue; }
                                    log::info!("releasing capture: {addr} entered this device");
                                    dormant.remove(&addr);
                                    entered_clients.insert(addr, (pos, fingerprint.clone()));
                                    let admission = budget.as_mut().map(crate::input_budget::InputLease::share_admission);
                                    self.event_tx.send(EmulationEvent::ReleaseNotify { addr, conn: conn.clone(), input: admission.clone() }).expect("channel closed");
                                    self.listener.reply(&mut control_jobs, addr, ProtoEvent::Ack(0));
                                    if !self.listener.is_current(addr, &conn) { continue; }
                                    self.emulation_proxy.warp(addr, to_emulation_pos(pos), t, self.listener.authorization().token(addr, &conn), budget);
                                    self.event_tx.send(EmulationEvent::Entered{addr, pos: to_ipc_pos(pos), fingerprint, conn: conn.clone(), control: _control.clone(), input: admission}).expect("channel closed");
                                }
                            }
                            ProtoEvent::Leave(..) => {
                                entered_clients.remove(&addr);
                                dormant.remove(&addr);
                                self.emulation_proxy.remove_with_control(addr, _control);
                                self.listener.reply(&mut control_jobs, addr, ProtoEvent::Ack(0));
                            }
                            ProtoEvent::Input(input_event) => {
                                // Clipboard events bypass the emulation
                                // backend: they are handled by the service's
                                // clipboard emulation module instead.
                                match input_event {
                                    input_event::Event::Clipboard(clipboard_event) => {
                                        self.event_tx
                                            .send(EmulationEvent::ClipboardReceived { event: clipboard_event, addr, conn, control: _control })
                                            .expect("channel closed");
                                    }
                                    _ => {
                                        self.emulation_proxy.consume(input_event, addr, self.listener.authorization().token(addr, &conn), budget);
                                    }
                                }
                            }
                            ProtoEvent::Ping => self.listener.reply(&mut control_jobs, addr, ProtoEvent::Pong(self.emulation_proxy.emulation_active.get())),
                            // Peer's version handshake. Echo our own
                            // commit back so the peer's connect-side
                            // receive_loop populates its `peer_commit`,
                            // AND publish a PeerHello upward so our
                            // service can populate ours from the listen
                            // side too — the connect side is the primary
                            // path, but if the outbound direction is
                            // broken (one-way setup, NAT, peer's TCP
                            // listener down) the version display would
                            // otherwise silently say "unknown" while
                            // the peer is in fact happily talking to us.
                            ProtoEvent::Hello { commit } => {
                                self.listener.reply(&mut control_jobs, addr, ProtoEvent::Hello { commit: local_commit() });
                                self.event_tx.send(EmulationEvent::PeerHello { addr, commit, conn: conn.clone(), control: _control }).expect("channel closed");
                            }
                            _ => {}
                        }
                    }
                    Some(ListenEvent::Accept { addr, fingerprint, conn, admission }) => {
                        if !self.listener.is_current(addr, &conn) { continue; }
                        clipboard_jobs.cancel_stale(addr, Some(&conn));
                        control_jobs.cancel_stale(addr, Some(&conn));
                        let remembered = forget_peer(addr, &mut entered_clients, &mut dormant, &mut last_response);
                        if !accepted_clients.insert(addr) || remembered {
                            self.emulation_proxy.remove_with_admission(addr, None, admission.clone());
                            self.event_tx.send(EmulationEvent::ConnectionClosed { addr, admission: admission.clone() }).expect("channel closed");
                        }
                        self.event_tx.send(EmulationEvent::Connected { addr, fingerprint, conn, admission }).expect("channel closed");
                    }
                    Some(ListenEvent::Disconnected { addr, admission }) => {
                        let current = self.listener.clipboard_connection(addr);
                        clipboard_jobs.cancel_stale(addr, current.as_ref());
                        control_jobs.cancel_stale(addr, current.as_ref());
                        // A new connection may already have replaced this one
                        // while its disconnect notification was queued.
                        if self.listener.has_connection(addr) { continue; }
                        accepted_clients.remove(&addr);
                        forget_peer(addr, &mut entered_clients, &mut dormant, &mut last_response);
                        self.emulation_proxy.remove_with_admission(addr, None, admission.clone());
                        self.event_tx.send(EmulationEvent::ConnectionClosed { addr, admission }).expect("channel closed");
                    }
                    Some(ListenEvent::PortChanged(result)) => {
                        self.event_tx.send(EmulationEvent::PortChanged(result)).expect("channel closed");
                    }
                    None => break
                }}
                event = self.emulation_proxy.event() => {
                    self.event_tx.send(event).expect("channel closed");
                }
                request = self.request_rx.recv() => match request.expect("channel closed") {
                    // reenable emulation
                    EmulationRequest::Reenable => self.emulation_proxy.reenable(),
                    // notify the other end that we hit a barrier (should release capture)
                    EmulationRequest::Release(addr, t) => self.listener.reply(&mut control_jobs, addr, ProtoEvent::Leave(0, t)),
                    EmulationRequest::UpdateScrollingInversion(invert_scroll) => {
                        self.emulation_proxy.input_config.invert_scroll = invert_scroll;
                        self.emulation_proxy.update_config();
                    }
                    EmulationRequest::UpdateMouseSensitivity(mouse_sensitivity) => {
                        self.emulation_proxy.input_config.mouse_sensitivity = mouse_sensitivity;
                        self.emulation_proxy.update_config();
                    }
                    EmulationRequest::Terminate => break,
                },
                _ = interval.tick() => {
                    last_response.retain(|&addr,instant| {
                        if instant.elapsed() > Duration::from_secs(1) {
                            log::warn!("releasing keys: {addr} not responding!");
                            self.emulation_proxy.remove(addr);
                            // remember a timed-out entered peer so its return
                            // edge can be rebuilt if it starts sending again
                            if entered_clients.contains_key(&addr) {
                                dormant.insert(addr);
                            }
                            self.event_tx.send(EmulationEvent::Disconnected { addr }).expect("channel closed");
                            false
                        } else {
                            true
                        }
                    });
                }
            }
        }
        drop(control_jobs);
        drop(clipboard_jobs);
        let cleanup = self.emulation_proxy.terminate().await;
        self.listener.terminate().await;
        cleanup
    }
}

fn forget_peer(
    addr: SocketAddr,
    entered: &mut HashMap<SocketAddr, (Position, String)>,
    dormant: &mut HashSet<SocketAddr>,
    last_response: &mut HashMap<SocketAddr, Instant>,
) -> bool {
    let entered = entered.remove(&addr).is_some();
    let dormant = dormant.remove(&addr);
    let response = last_response.remove(&addr).is_some();
    entered || dormant || response
}

fn resumed_edge(
    dormant: &mut HashSet<SocketAddr>,
    entered: &HashMap<SocketAddr, (Position, String)>,
    addr: SocketAddr,
    event: &ProtoEvent,
) -> Option<(Position, String)> {
    // A late Leave must not re-register a timed-out sender just before teardown.
    // Hello/Ack are bookkeeping: leave the dormant state for real input or Ping.
    if matches!(event, ProtoEvent::Input(_) | ProtoEvent::Ping) && dormant.remove(&addr) {
        entered.get(&addr).cloned()
    } else {
        None
    }
}

/// proxy handling the actual input emulation,
/// discarding events when it is disabled
pub(crate) struct EmulationProxy {
    emulation_active: Rc<Cell<bool>>,
    exit_requested: Rc<Cell<bool>>,
    request_tx: Sender<ProxyRequest>,
    event_rx: Receiver<EmulationEvent>,
    task: JoinHandle<CleanupState<InputEmulation>>,
    input_config: InputConfig,
}

enum ProxyRequest {
    Input(
        Event,
        SocketAddr,
        Option<CancellationToken>,
        Option<crate::input_budget::InputLease>,
    ),
    /// warp the cursor to a normalized cross-axis position along an edge
    Warp(
        SocketAddr,
        input_emulation::Position,
        f64,
        Option<CancellationToken>,
        Option<crate::input_budget::InputLease>,
    ),
    Remove(
        SocketAddr,
        Option<std::sync::Arc<crate::input_budget::ControlLease>>,
        Option<crate::listen::ReaderLease>,
    ),
    Terminate,
    Reenable,
    UpdateConfig(InputConfig),
}

impl EmulationProxy {
    fn new(
        backend: Option<input_emulation::Backend>,
        options: EmulationOptions,
        input_config: InputConfig,
    ) -> Self {
        let (request_tx, request_rx) = channel();
        let (event_tx, event_rx) = channel();
        let emulation_active = Rc::new(Cell::new(false));
        let exit_requested = Rc::new(Cell::new(false));
        let emulation_task = EmulationTask {
            backend,
            options,
            exit_requested: exit_requested.clone(),
            request_rx,
            event_tx,
            handles: Default::default(),
            handle_sessions: Default::default(),
            next_id: 0,
            operation_timeout: Duration::from_millis(500),
            cleanup: CleanupState::Complete,
            input_config,
        };
        let task = spawn_local(emulation_task.run());
        Self {
            emulation_active,
            exit_requested,
            request_tx,
            task,
            event_rx,
            input_config,
        }
    }

    async fn event(&mut self) -> EmulationEvent {
        let event = self.event_rx.recv().await.expect("channel closed");
        if let EmulationEvent::EmulationEnabled = event {
            self.emulation_active.replace(true);
        }
        if let EmulationEvent::EmulationDisabled = event {
            self.emulation_active.replace(false);
        }
        event
    }

    fn consume(
        &self,
        event: Event,
        addr: SocketAddr,
        session: Option<CancellationToken>,
        budget: Option<crate::input_budget::InputLease>,
    ) {
        // ignore events if emulation is currently disabled
        if self.emulation_active.get() {
            self.request_tx
                .send(ProxyRequest::Input(event, addr, session, budget))
                .expect("channel closed");
        } else {
            log::warn!("emulation inactive, dropping event: {:?}", event);
        }
    }

    fn warp(
        &self,
        addr: SocketAddr,
        pos: input_emulation::Position,
        t: f64,
        session: Option<CancellationToken>,
        budget: Option<crate::input_budget::InputLease>,
    ) {
        // ignore if emulation is currently disabled
        if self.emulation_active.get() {
            self.request_tx
                .send(ProxyRequest::Warp(addr, pos, t, session, budget))
                .expect("channel closed");
        }
    }

    fn remove(&self, addr: SocketAddr) {
        self.remove_with_control(addr, None);
    }

    fn remove_with_control(
        &self,
        addr: SocketAddr,
        control: Option<std::sync::Arc<crate::input_budget::ControlLease>>,
    ) {
        self.remove_with_admission(addr, control, None);
    }

    fn remove_with_admission(
        &self,
        addr: SocketAddr,
        control: Option<std::sync::Arc<crate::input_budget::ControlLease>>,
        admission: Option<crate::listen::ReaderLease>,
    ) {
        self.request_tx
            .send(ProxyRequest::Remove(addr, control, admission))
            .expect("channel closed");
    }

    fn reenable(&self) {
        self.request_tx
            .send(ProxyRequest::Reenable)
            .expect("channel closed");
    }

    fn update_config(&self) {
        self.request_tx
            .send(ProxyRequest::UpdateConfig(self.input_config))
            .expect("channel closed");
    }

    async fn terminate(&mut self) -> CleanupState<InputEmulation> {
        self.exit_requested.replace(true);
        let _ = self.request_tx.send(ProxyRequest::Terminate);
        match (&mut self.task).await {
            Ok(cleanup) => cleanup,
            Err(e) => {
                log::warn!("input worker failed during cleanup: {e}");
                CleanupState::Failed
            }
        }
    }
}

trait CleanupBackend {
    async fn cleanup(&mut self) -> bool;
}

impl CleanupBackend for InputEmulation {
    async fn cleanup(&mut self) -> bool {
        self.terminate_bounded().await
    }
}

// Carry the original native instance and its ledger through worker shutdown.
// Never construct a replacement while its predecessor still needs release.
enum CleanupState<T> {
    Complete,
    Pending(T),
    Failed,
}

impl<T: CleanupBackend> CleanupState<T> {
    async fn retry(&mut self) -> bool {
        match self {
            Self::Complete => true,
            Self::Failed => false,
            Self::Pending(backend) => {
                if !backend.cleanup().await {
                    return false;
                }
                *self = Self::Complete;
                true
            }
        }
    }
}

// Generic only at the internal worker boundary, allowing backend stalls to be
// exercised through the same request loop without a native display server.
trait ProxyBackend {
    async fn create(&mut self, handle: EmulationHandle);
    async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), input_emulation::EmulationError>;
    async fn warp(&mut self, handle: EmulationHandle, pos: input_emulation::Position, t: f64);
    async fn destroy_bounded(&mut self, handle: EmulationHandle) -> bool;
    fn update_config(&mut self, config: InputConfig);
}

impl ProxyBackend for InputEmulation {
    async fn create(&mut self, handle: EmulationHandle) {
        InputEmulation::create(self, handle).await;
    }
    async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), input_emulation::EmulationError> {
        InputEmulation::consume(self, event, handle).await
    }
    async fn warp(&mut self, handle: EmulationHandle, pos: input_emulation::Position, t: f64) {
        InputEmulation::warp(self, handle, pos, t).await;
    }
    async fn destroy_bounded(&mut self, handle: EmulationHandle) -> bool {
        InputEmulation::destroy_bounded(self, handle).await
    }
    fn update_config(&mut self, config: InputConfig) {
        InputEmulation::update_config(self, config);
    }
}

async fn backend_operation<T>(
    timeout: Duration,
    operation: &'static str,
    future: impl std::future::Future<Output = T>,
) -> Result<T, InputEmulationError> {
    tokio::time::timeout(timeout, future).await.map_err(|_| {
        input_emulation::EmulationError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("input emulation {operation} exceeded its deadline"),
        ))
        .into()
    })
}

#[derive(Debug, thiserror::Error)]
enum ProxyInputError {
    #[error(transparent)]
    Backend(#[from] InputEmulationError),
    #[error("previous input session cleanup failed")]
    PreviousSessionCleanup,
}

enum InputDelivery {
    Completed(Result<(), ProxyInputError>),
    Canceled,
    Discarded,
    Expired,
}

async fn deliver_before_deadline(
    lease: Option<&crate::input_budget::InputLease>,
    operation: impl std::future::Future<Output = Result<(), ProxyInputError>>,
) -> InputDelivery {
    let Some(lease) = lease else {
        return InputDelivery::Completed(operation.await);
    };
    if lease.cancellation().is_cancelled() {
        return InputDelivery::Discarded;
    }
    // timeout_at polls its inner future first; do not start already stale work.
    if tokio::time::Instant::now() >= lease.deadline() {
        return InputDelivery::Expired;
    }
    tokio::select! {
        biased;
        _ = lease.cancellation().cancelled() => InputDelivery::Canceled,
        result = tokio::time::timeout_at(lease.deadline(), operation) => {
            if tokio::time::Instant::now() >= lease.deadline() { InputDelivery::Expired }
            else { match result { Ok(result) => InputDelivery::Completed(result), Err(_) => InputDelivery::Expired } }
        },
    }
}

struct EmulationTask {
    backend: Option<input_emulation::Backend>,
    options: EmulationOptions,
    exit_requested: Rc<Cell<bool>>,
    request_rx: Receiver<ProxyRequest>,
    event_tx: Sender<EmulationEvent>,
    handles: HashMap<SocketAddr, EmulationHandle>,
    handle_sessions: HashMap<SocketAddr, std::sync::Weak<()>>,
    next_id: EmulationHandle,
    operation_timeout: Duration,
    input_config: InputConfig,
    cleanup: CleanupState<InputEmulation>,
}

impl EmulationTask {
    async fn run(mut self) -> CleanupState<InputEmulation> {
        loop {
            if let Err(e) = self.do_emulation().await {
                log::warn!("input emulation exited: {e}");
                self.event_tx
                    .send(EmulationEvent::BackendFailed(e.to_string()))
                    .expect("channel closed");
            }
            if self.exit_requested.get() {
                break;
            }
            // Only explicit reenable may create a replacement backend.
            loop {
                let Some(request) = self.request_rx.recv().await else {
                    let _ = self.cleanup.retry().await;
                    return self.cleanup;
                };
                match request {
                    ProxyRequest::Reenable => break,
                    ProxyRequest::Terminate => {
                        let _ = self.cleanup.retry().await;
                        return self.cleanup;
                    }
                    ProxyRequest::Input(..) | ProxyRequest::Warp(..) => {}
                    ProxyRequest::Remove(addr, _control, _admission) => {
                        if let (CleanupState::Pending(backend), Some(&handle)) =
                            (&mut self.cleanup, self.handles.get(&addr))
                        {
                            if backend.destroy_bounded(handle).await {
                                self.handles.remove(&addr);
                                self.handle_sessions.remove(&addr);
                            }
                        }
                    }
                    ProxyRequest::UpdateConfig(input_config) => {
                        self.input_config = input_config;
                    }
                }
            }
        }
        let _ = self.cleanup.retry().await;
        self.cleanup
    }

    async fn do_emulation(&mut self) -> Result<(), InputEmulationError> {
        if !self.cleanup.retry().await {
            return Err(input_emulation::EmulationError::Io(std::io::Error::other(
                "previous input backend cleanup is incomplete; replacement was refused",
            ))
            .into());
        }
        if self.exit_requested.get() {
            return Ok(());
        }
        log::info!("creating input emulation ...");
        let mut emulation = tokio::select! {
            r = InputEmulation::new(self.backend, self.options, self.input_config) => r?,
            _ = wait_for_termination(&mut self.request_rx) => return Ok(()),
        };

        let _emulation_guard = DropGuard::new(
            self.event_tx.clone(),
            EmulationEvent::EmulationEnabled,
            EmulationEvent::EmulationDisabled,
        );

        let res = match self.create_clients(&mut emulation).await {
            Ok(true) => self.do_emulation_session(&mut emulation).await,
            Ok(false) => Ok(()),
            Err(e) => Err(e),
        };
        self.cleanup = CleanupState::Pending(emulation);
        if !self.cleanup.retry().await {
            log::warn!("input cleanup incomplete; retaining backend and pressed state for retry");
        }
        res
    }

    async fn create_clients(
        &mut self,
        emulation: &mut impl ProxyBackend,
    ) -> Result<bool, InputEmulationError> {
        for handle in self.handles.values() {
            tokio::select! {
                result = backend_operation(self.operation_timeout, "create", emulation.create(*handle)) => { result?; },
                _ = wait_for_termination(&mut self.request_rx) => return Ok(false),
            }
        }
        Ok(true)
    }

    async fn do_emulation_session(
        &mut self,
        emulation: &mut impl ProxyBackend,
    ) -> Result<(), InputEmulationError> {
        loop {
            tokio::select! {
                e = self.request_rx.recv() => match e.expect("channel closed") {
                    ProxyRequest::Input(event, addr, session, mut budget) => {
                        if session.as_ref().is_some_and(CancellationToken::is_cancelled) { continue; }
                        let outcome = deliver_before_deadline(budget.as_ref(), async {
                            let handle = self.handle_for(emulation, addr, budget.as_ref().map(crate::input_budget::InputLease::identity)).await?;
                            if session.as_ref().is_some_and(CancellationToken::is_cancelled) { return Ok(()); }
                            backend_operation(self.operation_timeout, "consume", emulation.consume(event, handle)).await?.map_err(InputEmulationError::from)?;
                            Ok(())
                        }).await;
                        self.finish_input_delivery(outcome, addr, budget.as_mut(), emulation).await?;
                    },
                    ProxyRequest::Warp(addr, pos, t, session, mut budget) => {
                        if session.as_ref().is_some_and(CancellationToken::is_cancelled) { continue; }
                        let outcome = deliver_before_deadline(budget.as_ref(), async {
                            let handle = self.handle_for(emulation, addr, budget.as_ref().map(crate::input_budget::InputLease::identity)).await?;
                            if session.as_ref().is_some_and(CancellationToken::is_cancelled) { return Ok(()); }
                            backend_operation(self.operation_timeout, "warp", emulation.warp(handle, pos, t)).await?;
                            Ok(())
                        }).await;
                        self.finish_input_delivery(outcome, addr, budget.as_mut(), emulation).await?;
                    },
                    ProxyRequest::Remove(addr, _control, _admission) => {
                        if let Some(&handle) = self.handles.get(&addr) {
                            if emulation.destroy_bounded(handle).await {
                                self.handles.remove(&addr);
                                self.handle_sessions.remove(&addr);
                            }
                        }
                    }
                    ProxyRequest::UpdateConfig(input_config) => {
                        self.input_config = input_config;
                        emulation.update_config(input_config);
                    }
                    ProxyRequest::Terminate => break Ok(()),
                    ProxyRequest::Reenable => continue,
                },
            }
        }
    }

    async fn finish_input_delivery(
        &mut self,
        outcome: InputDelivery,
        addr: SocketAddr,
        mut lease: Option<&mut crate::input_budget::InputLease>,
        emulation: &mut impl ProxyBackend,
    ) -> Result<(), InputEmulationError> {
        match outcome {
            InputDelivery::Completed(result) => {
                if let Err(error) = result {
                    if let Some(lease) = lease.as_ref() {
                        lease.cancel();
                    }
                    match error {
                        ProxyInputError::Backend(error) => return Err(error),
                        ProxyInputError::PreviousSessionCleanup => {
                            // Keep the old backend and its cleanup ledger alive.
                            // Only the replacement reader is rejected; other
                            // peers and later cleanup retries can still run.
                            self.event_tx
                                .send(EmulationEvent::InputCleanupFailed {
                                    addr,
                                    input: lease.as_mut().map(|lease| lease.share_admission()),
                                })
                                .expect("channel closed");
                        }
                    }
                }
            }
            InputDelivery::Discarded => {}
            InputDelivery::Canceled | InputDelivery::Expired => {
                if matches!(outcome, InputDelivery::Expired)
                    && lease.as_ref().is_some_and(|lease| lease.cancel())
                {
                    self.event_tx
                        .send(EmulationEvent::InputOverloaded {
                            addr,
                            admission: None,
                            input: lease.as_mut().map(|lease| lease.share_admission()),
                        })
                        .expect("channel closed");
                }
                // Canceling the reader rejects its queued work; release any
                // uncertain press immediately rather than waiting for Remove.
                if let Some(&handle) = self.handles.get(&addr) {
                    if emulation.destroy_bounded(handle).await {
                        self.handles.remove(&addr);
                        self.handle_sessions.remove(&addr);
                    }
                }
            }
        }
        Ok(())
    }

    /// the emulation handle for `addr`, creating one on first use
    async fn handle_for(
        &mut self,
        emulation: &mut impl ProxyBackend,
        addr: SocketAddr,
        session: Option<std::sync::Weak<()>>,
    ) -> Result<EmulationHandle, ProxyInputError> {
        if let Some(&handle) = self.handles.get(&addr) {
            let same_session = match (self.handle_sessions.get(&addr), session.as_ref()) {
                (Some(old), Some(current)) => old.ptr_eq(current),
                (None, None) => true, // internal unscoped callers
                _ => false,
            };
            if same_session {
                return Ok(handle);
            }
            // Preserve failed cleanup for retry, but never let a replacement
            // session inherit uncertain keys/buttons from this native handle.
            if !emulation.destroy_bounded(handle).await {
                return Err(ProxyInputError::PreviousSessionCleanup);
            }
            self.handles.remove(&addr);
            self.handle_sessions.remove(&addr);
        }
        let handle = self.next_id;
        self.next_id += 1;
        // Retain the mapping if create partially succeeds before timing out.
        // Session termination must still be able to release/destroy this handle.
        self.handles.insert(addr, handle);
        if let Some(session) = session {
            self.handle_sessions.insert(addr, session);
        }
        backend_operation(self.operation_timeout, "create", emulation.create(handle)).await?;
        Ok(handle)
    }
}

fn to_ipc_pos(pos: Position) -> lan_mouse_ipc::Position {
    match pos {
        Position::Left => lan_mouse_ipc::Position::Left,
        Position::Right => lan_mouse_ipc::Position::Right,
        Position::Top => lan_mouse_ipc::Position::Top,
        Position::Bottom => lan_mouse_ipc::Position::Bottom,
    }
}

fn to_emulation_pos(pos: Position) -> input_emulation::Position {
    match pos {
        Position::Left => input_emulation::Position::Left,
        Position::Right => input_emulation::Position::Right,
        Position::Top => input_emulation::Position::Top,
        Position::Bottom => input_emulation::Position::Bottom,
    }
}

async fn wait_for_termination(rx: &mut Receiver<ProxyRequest>) {
    loop {
        match rx.recv().await.expect("channel closed") {
            ProxyRequest::Terminate => return,
            ProxyRequest::Input(..) => continue,
            ProxyRequest::Warp(..) => continue,
            ProxyRequest::Remove(..) => continue,
            ProxyRequest::Reenable => continue,
            ProxyRequest::UpdateConfig(_) => continue,
        }
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
mod resume_tests {
    use super::*;

    #[derive(Default)]
    struct StallingBackend {
        stall: Option<&'static str>,
        creates: Vec<EmulationHandle>,
        consumed: Vec<EmulationHandle>,
        removes: usize,
        held: bool,
        consume_delay: Duration,
        fail_all_removes: bool,
        remove_gate: Option<std::sync::Arc<tokio::sync::Semaphore>>,
        remove_started: Option<std::sync::Arc<tokio::sync::Notify>>,
        held_handles: HashSet<EmulationHandle>,
    }

    impl ProxyBackend for StallingBackend {
        async fn create(&mut self, handle: EmulationHandle) {
            self.creates.push(handle);
            if self.stall == Some("create") {
                std::future::pending::<()>().await;
            }
        }
        async fn consume(
            &mut self,
            _: Event,
            handle: EmulationHandle,
        ) -> Result<(), input_emulation::EmulationError> {
            self.consumed.push(handle);
            self.held = true;
            self.held_handles.insert(handle);
            tokio::time::sleep(self.consume_delay).await;
            if self.stall == Some("consume") {
                std::future::pending::<()>().await;
            }
            Ok(())
        }
        async fn warp(&mut self, _: EmulationHandle, _: input_emulation::Position, _: f64) {
            if self.stall == Some("warp") {
                std::future::pending::<()>().await;
            }
        }
        async fn destroy_bounded(&mut self, handle: EmulationHandle) -> bool {
            self.removes += 1;
            if let Some(started) = &self.remove_started {
                started.notify_one();
            }
            if let Some(gate) = &self.remove_gate {
                gate.acquire().await.unwrap().forget();
            }
            if self.fail_all_removes {
                return false;
            }
            if self.stall == Some("remove") && self.removes == 1 {
                return tokio::time::timeout(
                    Duration::from_millis(10),
                    std::future::pending::<bool>(),
                )
                .await
                .unwrap_or(false);
            }
            self.held_handles.remove(&handle);
            self.held = !self.held_handles.is_empty();
            true
        }
        fn update_config(&mut self, _: InputConfig) {}
    }

    fn worker() -> (
        EmulationTask,
        Sender<ProxyRequest>,
        Receiver<EmulationEvent>,
    ) {
        let (tx, request_rx) = channel();
        let (event_tx, event_rx) = channel();
        (
            EmulationTask {
                backend: Some(input_emulation::Backend::Dummy),
                options: Default::default(),
                exit_requested: Default::default(),
                request_rx,
                event_tx,
                handles: Default::default(),
                handle_sessions: Default::default(),
                next_id: 0,
                operation_timeout: Duration::from_millis(20),
                cleanup: CleanupState::Complete,
                input_config: Default::default(),
            },
            tx,
            event_rx,
        )
    }

    #[tokio::test]
    async fn enter_notifications_retain_input_admission_after_proxy_delivery() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let addr = "127.0.0.1:2".parse().unwrap();
                let (listener, conn) = crate::listen::authorized_control_test_listener(addr);
                let incoming = listener.test_sender();
                let mut emulation = Emulation::new(
                    Some(input_emulation::Backend::Dummy),
                    Default::default(),
                    listener,
                    (false, 1.0),
                );
                let budget =
                    crate::input_budget::InputBudget::with_limits(1, 1, Duration::from_millis(50));
                let token = CancellationToken::new();
                let lease = budget.acquire(&token).await.unwrap();
                incoming
                    .send(ListenEvent::Msg {
                        addr,
                        conn,
                        event: ProtoEvent::Enter(Position::Left, 0.5),
                        budget: Some(lease),
                        control: None,
                    })
                    .unwrap_or_else(|_| panic!());
                tokio::time::sleep(Duration::from_millis(10)).await;
                assert_eq!(
                    budget.available(),
                    (0, 0),
                    "proxy completion must not release admission for pending Service notices"
                );
                let mut notices = Vec::new();
                tokio::time::timeout(Duration::from_secs(1), async {
                    while notices.len() < 2 {
                        let event = emulation.event().await;
                        if matches!(
                            event,
                            EmulationEvent::ReleaseNotify { .. } | EmulationEvent::Entered { .. }
                        ) {
                            notices.push(event);
                        }
                    }
                })
                .await
                .unwrap();
                assert_eq!(budget.available(), (0, 0));
                drop(notices.pop());
                assert_eq!(
                    budget.available(),
                    (0, 0),
                    "the other derived notice must retain the same reservation"
                );
                assert!(budget.acquire(&token).await.is_none());
                drop(notices);
                assert_eq!(budget.available(), (1, 1));
                let recovered = budget.acquire(&token).await.unwrap();
                drop(recovered);
                emulation.terminate().await;
            })
            .await;
    }

    #[tokio::test]
    async fn listener_error_reports_keep_generation_through_service_queue() {
        tokio::task::LocalSet::new()
            .run_until(async {
                for rejected in [true, false] {
                    let addr = "127.0.0.1:2".parse().unwrap();
                    let (listener, _) = crate::listen::control_test_listener(addr);
                    let incoming = listener.test_sender();
                    let mut emulation = Emulation::new(
                        Some(input_emulation::Backend::Dummy),
                        Default::default(),
                        listener,
                        (false, 1.0),
                    );
                    let (admission, slots) = crate::listen::reader_slot_for_test();
                    let report = if rejected {
                        ListenEvent::InputRejected {
                            addr,
                            reason: "invalid coordinates".into(),
                            admission: Some(admission),
                        }
                    } else {
                        ListenEvent::InputOverloaded {
                            addr,
                            admission: Some(admission),
                        }
                    };
                    incoming.send(report).unwrap_or_else(|_| panic!());
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    assert_eq!(slots.get(), 1);
                    let report = tokio::time::timeout(Duration::from_secs(1), async {
                        loop {
                            let event = emulation.event().await;
                            if matches!(
                                event,
                                EmulationEvent::InputRejected { .. }
                                    | EmulationEvent::InputOverloaded { .. }
                            ) {
                                break event;
                            }
                        }
                    })
                    .await
                    .unwrap();
                    assert_eq!(slots.get(), 1);
                    drop(report);
                    assert_eq!(slots.get(), 0);
                    emulation.terminate().await;
                }
            })
            .await;
    }

    #[tokio::test]
    async fn connected_notice_keeps_reader_generation_reserved_until_service_consumes() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let addr = "127.0.0.1:2".parse().unwrap();
                let (listener, conn) = crate::listen::authorized_control_test_listener(addr);
                let incoming = listener.test_sender();
                let mut emulation = Emulation::new(
                    Some(input_emulation::Backend::Dummy),
                    Default::default(),
                    listener,
                    (false, 1.0),
                );
                let (admission, slots) = crate::listen::reader_slot_for_test();
                incoming
                    .send(ListenEvent::Accept {
                        addr,
                        conn,
                        fingerprint: "test-peer".into(),
                        admission: Some(admission),
                    })
                    .unwrap_or_else(|_| panic!());
                tokio::time::sleep(Duration::from_millis(10)).await;
                assert_eq!(
                    slots.get(),
                    1,
                    "reader completion must not free a queued Connected notice"
                );
                let event = tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let event = emulation.event().await;
                        if matches!(event, EmulationEvent::Connected { .. }) {
                            break event;
                        }
                    }
                })
                .await
                .unwrap();
                assert_eq!(slots.get(), 1);
                drop(event);
                assert_eq!(slots.get(), 0);
                emulation.terminate().await;
            })
            .await;
    }

    #[tokio::test]
    async fn disconnect_generation_is_retained_by_service_or_discarded_for_current_peer() {
        tokio::task::LocalSet::new().run_until(async {
            for replacement_live in [false, true] {
                let addr = "127.0.0.1:2".parse().unwrap();
                let (listener, conn) = crate::listen::control_test_listener(addr);
                let incoming = listener.test_sender();
                if !replacement_live { listener.clipboard_connections().borrow_mut().clear(); }
                let mut emulation = Emulation::new(Some(input_emulation::Backend::Dummy), Default::default(), listener, (false, 1.0));
                let (admission, slots) = crate::listen::reader_slot_for_test();
                incoming.send(ListenEvent::Disconnected { addr, admission: Some(admission) }).unwrap_or_else(|_| panic!());
                tokio::time::sleep(Duration::from_millis(10)).await;
                if replacement_live {
                    assert_eq!(slots.get(), 0, "obsolete disconnect must release only its owner");
                    assert!(emulation.clipboard_session_is_current(addr, &conn));
                    assert!(tokio::time::timeout(Duration::from_millis(20), async {
                        loop { if matches!(emulation.event().await, EmulationEvent::ConnectionClosed { .. }) { break; } }
                    }).await.is_err());
                } else {
                    assert_eq!(slots.get(), 1, "pending ConnectionClosed must retain the generation after proxy processing");
                    let event = tokio::time::timeout(Duration::from_secs(1), async {
                        loop { let event = emulation.event().await; if matches!(event, EmulationEvent::ConnectionClosed { .. }) { break event; } }
                    }).await.unwrap();
                    assert_eq!(slots.get(), 1);
                    drop(event);
                    assert_eq!(slots.get(), 0);
                }
                emulation.terminate().await;
            }
        }).await;
    }

    #[tokio::test]
    async fn leave_dispatch_keeps_control_permit_in_paused_proxy_queue() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let addr = "127.0.0.1:2".parse().unwrap();
                let (listener, conn) = crate::listen::control_test_listener(addr);
                let incoming = listener.test_sender();
                let (proxy_tx, mut proxy_rx) = channel();
                let (_proxy_events, proxy_event_rx) = channel();
                let worker_done = Rc::new(tokio::sync::Notify::new());
                let done = worker_done.clone();
                let proxy = EmulationProxy {
                    emulation_active: Rc::new(Cell::new(true)),
                    exit_requested: Default::default(),
                    request_tx: proxy_tx,
                    event_rx: proxy_event_rx,
                    input_config: Default::default(),
                    task: spawn_local(async move {
                        done.notified().await;
                        CleanupState::Complete
                    }),
                };
                let (request_tx, request_rx) = channel();
                let (event_tx, _events) = channel();
                let (_clipboard_tx, clipboard_rx) = tokio::sync::mpsc::channel(32);
                let dispatcher = spawn_local(
                    ListenTask {
                        listener,
                        emulation_proxy: proxy,
                        request_rx,
                        event_tx,
                        clipboard_rx,
                    }
                    .run(),
                );
                let budget = crate::input_budget::InputBudget::default();
                let token = CancellationToken::new();
                for _ in 0..32 {
                    let lease = budget.acquire_control(&token).await.unwrap();
                    incoming
                        .send(ListenEvent::Msg {
                            addr,
                            conn: conn.clone(),
                            event: ProtoEvent::Leave(0, 0.5),
                            budget: None,
                            control: Some(std::sync::Arc::new(lease)),
                        })
                        .unwrap_or_else(|_| panic!());
                    tokio::task::yield_now().await;
                }
                tokio::task::yield_now().await;
                assert_eq!(
                    budget.control_available(),
                    (96, 0),
                    "Leave must not free admission while Remove is queued"
                );
                let request = proxy_rx.recv().await.unwrap();
                assert!(matches!(request, ProxyRequest::Remove(..)));
                assert_eq!(budget.control_available(), (96, 0));
                drop(request);
                assert_eq!(budget.control_available(), (97, 1));
                for _ in 0..31 {
                    drop(proxy_rx.recv().await.unwrap());
                }
                assert_eq!(budget.control_available(), (128, 32));
                request_tx.send(EmulationRequest::Terminate).unwrap();
                worker_done.notify_one();
                tokio::time::timeout(Duration::from_secs(1), dispatcher)
                    .await
                    .unwrap()
                    .unwrap();
            })
            .await;
    }

    #[tokio::test]
    async fn remove_request_holds_control_lease_through_async_cleanup() {
        let budget = crate::input_budget::InputBudget::default();
        let lease = std::sync::Arc::new(
            budget
                .acquire_control(&CancellationToken::new())
                .await
                .unwrap(),
        );
        let (mut task, tx, _events) = worker();
        let (lifecycle, slots) = crate::listen::reader_slot_for_test();
        let addr = "127.0.0.1:2".parse().unwrap();
        task.handles.insert(addr, 0);
        let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let started = std::sync::Arc::new(tokio::sync::Notify::new());
        let mut backend = StallingBackend {
            held: true,
            remove_gate: Some(gate.clone()),
            remove_started: Some(started.clone()),
            ..Default::default()
        };
        tx.send(ProxyRequest::Remove(addr, Some(lease), Some(lifecycle)))
            .unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        let control = async {
            started.notified().await;
            assert_eq!(slots.get(), 1);
            assert_eq!(budget.control_available(), (127, 31));
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(
                budget.control_available(),
                (127, 31),
                "pending cleanup must retain request admission"
            );
            assert_eq!(
                slots.get(),
                1,
                "pending replacement cleanup must retain its generation slot"
            );
            gate.add_permits(1);
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            let (result, ()) = tokio::join!(task.do_emulation_session(&mut backend), control);
            result.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(budget.control_available(), (128, 32));
        assert_eq!(slots.get(), 0);
        assert!(task.handles.is_empty());
        assert!(!backend.held);
    }

    #[tokio::test]
    async fn cleanup_ownership_survives_failed_retries_and_moves_to_owner() {
        struct Backend {
            ready: Rc<Cell<bool>>,
            drops: Rc<Cell<usize>>,
            attempts: Rc<Cell<usize>>,
        }
        impl CleanupBackend for Backend {
            async fn cleanup(&mut self) -> bool {
                self.attempts.set(self.attempts.get() + 1);
                self.ready.get()
            }
        }
        impl Drop for Backend {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
            }
        }
        let ready = Rc::new(Cell::new(false));
        let drops = Rc::new(Cell::new(0));
        let attempts = Rc::new(Cell::new(0));
        let mut worker = CleanupState::Pending(Backend {
            ready: ready.clone(),
            drops: drops.clone(),
            attempts: attempts.clone(),
        });
        assert!(!worker.retry().await);
        assert_eq!(drops.get(), 0);
        let mut owner = worker;
        assert!(!owner.retry().await);
        assert_eq!(drops.get(), 0);
        ready.set(true);
        assert!(owner.retry().await);
        assert_eq!(drops.get(), 1);
        assert!(owner.retry().await);
        assert_eq!(attempts.get(), 3);
        let mut failed: CleanupState<Backend> = CleanupState::Failed;
        assert!(!failed.retry().await);
        assert!(!failed.retry().await);
    }

    fn press() -> Event {
        Event::Keyboard(input_event::KeyboardEvent::Key {
            time: 0,
            key: 29,
            state: 1,
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn proxy_backend_stalls_return_error_and_preserve_partial_handle_for_cleanup() {
        for operation in ["create", "consume", "warp"] {
            let (mut task, tx, _events) = worker();
            let addr = "127.0.0.1:2".parse().unwrap();
            let mut backend = StallingBackend {
                stall: Some(operation),
                ..Default::default()
            };
            if operation == "warp" {
                tx.send(ProxyRequest::Warp(
                    addr,
                    input_emulation::Position::Left,
                    0.5,
                    None,
                    None,
                ))
                .unwrap();
            } else {
                tx.send(ProxyRequest::Input(press(), addr, None, None))
                    .unwrap();
            }
            tx.send(ProxyRequest::Terminate).unwrap();
            let error = tokio::time::timeout(
                Duration::from_millis(200),
                task.do_emulation_session(&mut backend),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert!(
                matches!(error, InputEmulationError::Emulate(input_emulation::EmulationError::Io(ref e)) if e.kind() == std::io::ErrorKind::TimedOut)
            );
            assert!(error.to_string().contains(operation));
            assert_eq!(task.handles.get(&addr), Some(&0));
            assert_eq!(backend.creates, vec![0]);
            // Failure leaves shutdown queued for the outer worker; no late input
            // or automatic replay occurs after an uncertain native delivery.
            assert!(matches!(
                task.request_rx.recv().await.unwrap(),
                ProxyRequest::Terminate
            ));
            assert!(backend.destroy_bounded(0).await);
            assert!(!backend.held);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminate_during_handle_creation_never_resumes_input_loop() {
        let (mut task, tx, _events) = worker();
        task.handles.insert("127.0.0.1:2".parse().unwrap(), 0);
        let mut backend = StallingBackend {
            stall: Some("create"),
            ..Default::default()
        };
        tx.send(ProxyRequest::Terminate).unwrap();
        let initialized = tokio::time::timeout(
            Duration::from_millis(200),
            task.create_clients(&mut backend),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            !initialized,
            "caller must exit rather than await an already consumed Terminate"
        );
        assert!(futures::FutureExt::now_or_never(task.request_rx.recv()).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replacement_retries_old_cleanup_before_creating_fresh_handle() {
        let (mut task, tx, _events) = worker();
        let addr = "127.0.0.1:2".parse().unwrap();
        let budget = crate::input_budget::InputBudget::default();
        let old = CancellationToken::new();
        let old_lease = budget.acquire(&old).await.unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(old.clone()),
            Some(old_lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Remove(addr, None, None)).unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        let mut backend = StallingBackend {
            stall: Some("remove"),
            ..Default::default()
        };
        task.do_emulation_session(&mut backend).await.unwrap();
        assert!(backend.held);
        old.cancel();
        let fresh = CancellationToken::new();
        let replacement = budget.for_peer();
        let fresh_lease = replacement.acquire(&fresh).await.unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(fresh),
            Some(fresh_lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        task.do_emulation_session(&mut backend).await.unwrap();
        eprintln!(
            "replacement fixed: creates={:?}, consumed={:?}, cleanup attempts={}",
            backend.creates, backend.consumed, backend.removes
        );
        assert_eq!(backend.creates, vec![0, 1]);
        assert_eq!(backend.consumed, vec![0, 1]);
        assert_eq!(backend.removes, 2);
        assert_eq!(task.handles.get(&addr), Some(&1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replacement_cleanup_failure_cancels_new_reader_and_preserves_old_state_until_recovery()
    {
        let (mut task, tx, mut events) = worker();
        let addr = "127.0.0.1:2".parse().unwrap();
        let other = "127.0.0.1:3".parse().unwrap();
        let budget = crate::input_budget::InputBudget::default();
        let old = CancellationToken::new();
        let lease = budget.acquire(&old).await.unwrap();
        let old_identity = lease.identity();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(old.clone()),
            Some(lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Remove(addr, None, None)).unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        let mut backend = StallingBackend {
            fail_all_removes: true,
            ..Default::default()
        };
        task.do_emulation_session(&mut backend).await.unwrap();
        old.cancel();
        let fresh = CancellationToken::new();
        let replacement = budget.for_peer();
        let fresh_lease = replacement.acquire(&fresh).await.unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(fresh.clone()),
            Some(fresh_lease),
        ))
        .unwrap();
        let healthy = CancellationToken::new();
        let healthy_budget = budget.for_peer();
        let healthy_lease = healthy_budget.acquire(&healthy).await.unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            other,
            Some(healthy.clone()),
            Some(healthy_lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        task.do_emulation_session(&mut backend).await.unwrap();
        assert!(
            fresh.is_cancelled(),
            "failed replacement must wake and close its reader"
        );
        assert!(!healthy.is_cancelled());
        assert_eq!(backend.creates, vec![0, 1]); // only the healthy peer was created
        assert_eq!(backend.consumed, vec![0, 1]);
        assert!(backend.held_handles.contains(&0));
        assert_eq!(task.handles.get(&addr), Some(&0));
        assert!(
            task.handle_sessions
                .get(&addr)
                .unwrap()
                .ptr_eq(&old_identity)
        );
        assert_eq!(budget.available(), (255, 64));
        assert_eq!(replacement.available(), (255, 63));
        let report = events.recv().await.unwrap();
        assert!(
            matches!(report, EmulationEvent::InputCleanupFailed { addr: actual, .. } if actual == addr)
        );
        assert_eq!(replacement.available(), (255, 63));
        drop(report);
        assert_eq!(budget.available(), (256, 64));
        assert_eq!(replacement.available(), (256, 64));
        assert!(futures::FutureExt::now_or_never(events.recv()).is_none());
        backend.fail_all_removes = false;
        let recovered = replacement.for_peer();
        let token = CancellationToken::new();
        let lease = recovered.acquire(&token).await.unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(token.clone()),
            Some(lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        task.do_emulation_session(&mut backend).await.unwrap();
        assert!(!token.is_cancelled());
        assert_eq!(backend.creates, vec![0, 1, 2]);
        assert_eq!(backend.consumed, vec![0, 1, 2]);
        assert!(!backend.held_handles.contains(&0));
        assert!(
            backend.held_handles.contains(&1),
            "replacement cleanup must not release healthy peer"
        );
        assert!(backend.held_handles.contains(&2));
        tx.send(ProxyRequest::Remove(addr, None, None)).unwrap();
        tx.send(ProxyRequest::Remove(other, None, None)).unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        task.do_emulation_session(&mut backend).await.unwrap();
        assert_eq!(backend.removes, 5);
        assert!(!backend.held);
        assert!(task.handles.is_empty());
        assert!(task.handle_sessions.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replacement_uses_fresh_tracked_ctrl_state_after_releasing_old_handle() {
        let (mut task, tx, _events) = worker();
        let addr = "127.0.0.1:2".parse().unwrap();
        let mut emulation = InputEmulation::new(
            Some(input_emulation::Backend::Dummy),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
        let budget = crate::input_budget::InputBudget::default();
        let old = CancellationToken::new();
        let lease = budget.acquire(&old).await.unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(old.clone()),
            Some(lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        task.do_emulation_session(&mut emulation).await.unwrap();
        let old_handle = *task.handles.get(&addr).unwrap();
        assert!(emulation.has_pressed_keys(old_handle));
        old.cancel();
        let replacement = budget.for_peer();
        let token = CancellationToken::new();
        let lease = replacement.acquire(&token).await.unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(token.clone()),
            Some(lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        task.do_emulation_session(&mut emulation).await.unwrap();
        let fresh_handle = *task.handles.get(&addr).unwrap();
        assert_ne!(fresh_handle, old_handle);
        assert!(!emulation.has_pressed_keys(old_handle));
        assert!(emulation.has_pressed_keys(fresh_handle));
        assert!(!token.is_cancelled());
        emulation.terminate().await;
        assert!(!emulation.has_pressed_keys(fresh_handle));
    }

    #[tokio::test]
    async fn expired_input_report_keeps_admission_until_service_consumes() {
        let (mut task, tx, mut events) = worker();
        let budget = crate::input_budget::InputBudget::with_limits(1, 1, Duration::from_millis(50));
        let token = CancellationToken::new();
        let addr = "127.0.0.1:2".parse().unwrap();
        let lease = budget.acquire(&token).await.unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(token.clone()),
            Some(lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        task.do_emulation_session(&mut StallingBackend::default())
            .await
            .unwrap();
        assert!(token.is_cancelled());
        assert_eq!(
            budget.available(),
            (0, 0),
            "queued expiry report must retain its input reservation"
        );
        let report = events.recv().await.unwrap();
        assert!(matches!(report, EmulationEvent::InputOverloaded { .. }));
        assert_eq!(budget.available(), (0, 0));
        drop(report);
        assert_eq!(budget.available(), (1, 1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_backend_expires_session_instead_of_replaying_old_input() {
        let (mut task, tx, mut events) = worker();
        task.operation_timeout = Duration::from_millis(500);
        let budget = crate::input_budget::InputBudget::default();
        let token = CancellationToken::new();
        let addr = "127.0.0.1:2".parse().unwrap();
        for _ in 0..20 {
            let lease = budget.acquire(&token).await.unwrap();
            tx.send(ProxyRequest::Input(
                press(),
                addr,
                Some(token.clone()),
                Some(lease),
            ))
            .unwrap();
        }
        tx.send(ProxyRequest::Terminate).unwrap();
        let mut backend = StallingBackend {
            consume_delay: Duration::from_millis(10),
            ..Default::default()
        };
        let start = tokio::time::Instant::now();
        task.do_emulation_session(&mut backend).await.unwrap();
        eprintln!(
            "freshness protected delivery: {} events, elapsed {:?}",
            backend.consumed.len(),
            start.elapsed()
        );
        assert!(backend.consumed.len() < 20);
        assert!(token.is_cancelled());
        assert_eq!(backend.removes, 1);
        assert!(!backend.held);
        assert!(task.handles.is_empty());
        assert_eq!(budget.available(), (255, 63));
        let report = events.recv().await.unwrap();
        assert!(
            matches!(report, EmulationEvent::InputOverloaded { addr: actual, .. } if actual == addr)
        );
        assert_eq!(budget.available(), (255, 63));
        drop(report);
        assert_eq!(budget.available(), (256, 64));
        assert!(futures::FutureExt::now_or_never(events.recv()).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_release_cleans_tracked_keys_and_stale_warp_cannot_touch_fresh_same_address() {
        let (mut task, tx, mut events) = worker();
        let addr = "127.0.0.1:2".parse().unwrap();
        let mut emulation = InputEmulation::new(
            Some(input_emulation::Backend::Dummy),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
        let old_handle = task.handle_for(&mut emulation, addr, None).await.unwrap();
        emulation.consume(press(), old_handle).await.unwrap();
        assert!(emulation.has_pressed_keys(old_handle));
        let budget = crate::input_budget::InputBudget::default();
        let old = CancellationToken::new();
        let release_lease = budget.acquire(&old).await.unwrap();
        let warp_lease = budget.acquire(&old).await.unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        let release = Event::Keyboard(input_event::KeyboardEvent::Key {
            time: 0,
            key: 29,
            state: 0,
        });
        tx.send(ProxyRequest::Input(
            release,
            addr,
            Some(old.clone()),
            Some(release_lease),
        ))
        .unwrap();
        let fresh = CancellationToken::new();
        let fresh_lease = budget.acquire(&fresh).await.unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(fresh.clone()),
            Some(fresh_lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Warp(
            addr,
            input_emulation::Position::Left,
            0.5,
            Some(old.clone()),
            Some(warp_lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        task.do_emulation_session(&mut emulation).await.unwrap();
        assert!(old.is_cancelled());
        assert!(!fresh.is_cancelled());
        assert!(!emulation.has_pressed_keys(old_handle));
        let new_handle = *task.handles.get(&addr).unwrap();
        assert_ne!(new_handle, old_handle);
        assert!(
            emulation.has_pressed_keys(new_handle),
            "late old warp must not remove fresh handle"
        );
        assert!(matches!(
            events.recv().await,
            Some(EmulationEvent::InputOverloaded { .. })
        ));
        assert!(futures::FutureExt::now_or_never(events.recv()).is_none());
        assert_eq!(budget.available(), (256, 64));
        emulation.terminate().await;
        assert!(!emulation.has_pressed_keys(new_handle));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_active_delivery_releases_uncertain_press_without_waiting_for_backend_deadline()
     {
        let (mut task, tx, mut events) = worker();
        task.operation_timeout = Duration::from_millis(500);
        let budget = crate::input_budget::InputBudget::default();
        let token = CancellationToken::new();
        let addr = "127.0.0.1:2".parse().unwrap();
        let lease = budget.acquire(&token).await.unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(token.clone()),
            Some(lease),
        ))
        .unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        let mut backend = StallingBackend {
            stall: Some("consume"),
            ..Default::default()
        };
        let cancel = async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            token.cancel();
        };
        tokio::time::timeout(Duration::from_millis(200), async {
            let (result, ()) = tokio::join!(task.do_emulation_session(&mut backend), cancel);
            result.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(backend.removes, 1);
        assert!(!backend.held);
        assert!(task.handles.is_empty());
        assert!(
            futures::FutureExt::now_or_never(events.recv()).is_none(),
            "ordinary cancellation is not an overload warning"
        );
        assert_eq!(budget.available(), (256, 64));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forwarded_input_budget_is_held_through_backend_wait_and_released_on_timeout() {
        let (mut task, tx, _events) = worker();
        let addr = "127.0.0.1:2".parse().unwrap();
        let budget = crate::input_budget::InputBudget::with_limits(1, 1, Duration::from_millis(5));
        let token = CancellationToken::new();
        let lease = budget.acquire(&token).await.unwrap();
        tx.send(ProxyRequest::Input(press(), addr, None, Some(lease)))
            .unwrap();
        let mut backend = StallingBackend {
            stall: Some("consume"),
            ..Default::default()
        };
        let operation = task.do_emulation_session(&mut backend);
        tokio::pin!(operation);
        tokio::select! {
            result = &mut operation => panic!("backend should still be pending: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(2)) => {},
        }
        assert_eq!(
            budget.available(),
            (0, 0),
            "forwarding must not release admission"
        );
        assert!(budget.acquire(&token).await.is_none());
        operation.await.unwrap_err();
        assert_eq!(budget.available(), (1, 1));
        assert!(
            token.is_cancelled(),
            "failed delivery must close its reader"
        );
        assert!(budget.acquire(&token).await.is_none());
        let fresh = CancellationToken::new();
        assert!(budget.for_peer().acquire(&fresh).await.is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn proxy_failed_remove_retains_address_and_retry_releases_same_handle() {
        let (mut task, tx, _events) = worker();
        let addr = "127.0.0.1:2".parse().unwrap();
        let mut backend = StallingBackend {
            stall: Some("remove"),
            ..Default::default()
        };
        let budget = crate::input_budget::InputBudget::default();
        let token = CancellationToken::new();
        let first = budget.acquire(&token).await.unwrap();
        let second = budget.acquire(&token).await.unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(token.clone()),
            Some(first),
        ))
        .unwrap();
        tx.send(ProxyRequest::Remove(addr, None, None)).unwrap();
        tx.send(ProxyRequest::Input(
            press(),
            addr,
            Some(token),
            Some(second),
        ))
        .unwrap();
        tx.send(ProxyRequest::Remove(addr, None, None)).unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        tokio::time::timeout(
            Duration::from_millis(200),
            task.do_emulation_session(&mut backend),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(backend.creates, vec![0]);
        assert_eq!(backend.consumed, vec![0, 0]);
        assert_eq!(backend.removes, 2);
        assert!(task.handles.is_empty());
        assert!(task.handle_sessions.is_empty());
        assert!(!backend.held);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn revoked_proxy_input_and_warp_are_filtered_and_removal_releases_pressed_keys() {
        let addr = "127.0.0.1:2".parse().unwrap();
        let other = "127.0.0.1:3".parse().unwrap();
        let (tx, request_rx) = channel();
        let (event_tx, _events) = channel();
        let mut task = EmulationTask {
            backend: Some(input_emulation::Backend::Dummy),
            options: Default::default(),
            exit_requested: Default::default(),
            request_rx,
            event_tx,
            handles: Default::default(),
            handle_sessions: Default::default(),
            next_id: 0,
            operation_timeout: Duration::from_millis(20),
            input_config: Default::default(),
            cleanup: CleanupState::Complete,
        };
        let mut emulation = InputEmulation::new(
            Some(input_emulation::Backend::Dummy),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
        let key = Event::Keyboard(input_event::KeyboardEvent::Key {
            time: 0,
            key: input_event::scancode::Linux::KeyLeftCtrl as u32,
            state: 1,
        });
        let handle = task.handle_for(&mut emulation, addr, None).await.unwrap();
        emulation.consume(key.clone(), handle).await.unwrap();
        assert!(emulation.has_pressed_keys(handle));
        let token = CancellationToken::new();
        token.cancel();
        for _ in 0..1000 {
            tx.send(ProxyRequest::Input(
                key.clone(),
                other,
                Some(token.clone()),
                None,
            ))
            .unwrap();
        }
        tx.send(ProxyRequest::Warp(
            other,
            input_emulation::Position::Left,
            0.5,
            Some(token),
            None,
        ))
        .unwrap();
        tx.send(ProxyRequest::Remove(addr, None, None)).unwrap();
        tx.send(ProxyRequest::Terminate).unwrap();
        task.do_emulation_session(&mut emulation).await.unwrap();
        assert!(!task.handles.contains_key(&other));
        assert!(!task.handles.contains_key(&addr));
        assert!(!emulation.has_pressed_keys(handle));
        emulation.terminate().await;
    }

    #[test]
    fn forgetting_real_connection_clears_only_its_return_metadata() {
        let addr = "127.0.0.1:2".parse().unwrap();
        let other = "127.0.0.1:3".parse().unwrap();
        let mut entered = HashMap::from([
            (addr, (Position::Left, "old".into())),
            (other, (Position::Right, "other".into())),
        ]);
        let mut dormant = HashSet::from([addr, other]);
        let mut responses = HashMap::from([(addr, Instant::now()), (other, Instant::now())]);
        assert!(forget_peer(
            addr,
            &mut entered,
            &mut dormant,
            &mut responses
        ));
        assert!(!entered.contains_key(&addr));
        assert!(!dormant.contains(&addr));
        assert!(!responses.contains_key(&addr));
        assert!(
            entered.contains_key(&other)
                && dormant.contains(&other)
                && responses.contains_key(&other)
        );
        assert!(resumed_edge(&mut dormant, &entered, addr, &ProtoEvent::Ping).is_none());
        assert!(!forget_peer(
            addr,
            &mut entered,
            &mut dormant,
            &mut responses
        ));
    }

    #[test]
    fn bookkeeping_and_leave_do_not_restore_return_edge() {
        let addr = "127.0.0.1:4242".parse().unwrap();
        let entered = HashMap::from([(addr, (Position::Left, "peer".into()))]);
        let mut dormant = HashSet::from([addr]);
        for event in [
            ProtoEvent::Leave(0, 0.5),
            ProtoEvent::Ack(0),
            ProtoEvent::Hello {
                commit: *b"12345678",
            },
            ProtoEvent::Enter(Position::Left, 0.5),
        ] {
            assert!(resumed_edge(&mut dormant, &entered, addr, &event).is_none());
            assert!(dormant.contains(&addr));
        }
        assert!(
            matches!(resumed_edge(&mut dormant, &entered, addr, &ProtoEvent::Ping), Some((Position::Left, fingerprint)) if fingerprint == "peer")
        );
        assert!(resumed_edge(&mut dormant, &entered, addr, &ProtoEvent::Ping).is_none());
        dormant.insert(addr);
        let motion = ProtoEvent::Input(Event::Pointer(input_event::PointerEvent::Motion {
            time: 0,
            dx: 1.0,
            dy: 0.0,
        }));
        assert!(resumed_edge(&mut dormant, &entered, addr, &motion).is_some());
    }
}
