use crate::config::local_commit;
use crate::listen::{LanMouseListener, ListenEvent, ListenerCreationError};
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
use tokio::{
    select,
    task::{JoinHandle, spawn_local},
};

/// emulation handling events received from a listener
pub(crate) struct Emulation {
    task: JoinHandle<()>,
    request_tx: Sender<EmulationRequest>,
    event_rx: Receiver<EmulationEvent>,
}

pub(crate) enum EmulationEvent {
    Connected {
        addr: SocketAddr,
        fingerprint: String,
    },
    ConnectionAttempt {
        fingerprint: String,
    },
    /// new connection
    Entered {
        /// address of the connection
        addr: SocketAddr,
        /// position of the connection
        pos: lan_mouse_ipc::Position,
        /// certificate fingerprint of the connection
        fingerprint: String,
    },
    /// connection closed
    Disconnected {
        addr: SocketAddr,
    },
    /// actual DTLS connection ended or was replaced
    ConnectionClosed {
        addr: SocketAddr,
    },
    /// the port of the listener has changed
    PortChanged(Result<u16, ListenerCreationError>),
    /// emulation was disabled
    EmulationDisabled,
    /// emulation was enabled
    EmulationEnabled,
    /// capture should be released
    ReleaseNotify,
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
    },
    /// clipboard data received from remote
    ClipboardReceived(input_event::ClipboardEvent),
}

enum EmulationRequest {
    Reenable,
    /// release the peer's capture, handing the cursor back at the
    /// given normalized cross-axis position
    Release(SocketAddr, f64),
    ChangePort(u16),
    Terminate,
    UpdateScrollingInversion(bool),
    UpdateMouseSensitivity(f64),
    SendClipboard(SocketAddr, input_event::ClipboardEvent),
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
        let emulation_task = ListenTask {
            listener,
            emulation_proxy,
            request_rx,
            event_tx,
        };
        let task = spawn_local(emulation_task.run());
        Self {
            task,
            request_tx,
            event_rx,
        }
    }

    pub(crate) fn send_leave_event(&self, addr: SocketAddr, t: f64) {
        self.request_tx
            .send(EmulationRequest::Release(addr, t))
            .expect("channel closed");
    }

    pub(crate) fn send_clipboard(&self, addr: SocketAddr, clipboard: input_event::ClipboardEvent) {
        self.request_tx
            .send(EmulationRequest::SendClipboard(addr, clipboard))
            .expect("channel closed");
    }

    pub(crate) fn reenable(&self) {
        self.request_tx
            .send(EmulationRequest::Reenable)
            .expect("channel closed");
    }

    pub(crate) fn request_port_change(&self, port: u16) {
        self.request_tx
            .send(EmulationRequest::ChangePort(port))
            .expect("channel closed")
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

    /// wait for termination
    pub(crate) async fn terminate(&mut self) {
        log::debug!("terminating emulation");
        self.request_tx
            .send(EmulationRequest::Terminate)
            .expect("channel closed");
        if let Err(e) = (&mut self.task).await {
            log::warn!("{e}");
        }
    }
}

struct ListenTask {
    listener: LanMouseListener,
    emulation_proxy: EmulationProxy,
    request_rx: Receiver<EmulationRequest>,
    event_tx: Sender<EmulationEvent>,
}

impl ListenTask {
    async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        let mut last_response = HashMap::new();
        let mut rejected_connections = HashMap::new();
        // peers that entered this device: addr -> (edge, fingerprint).
        // kept across temporary silence so a resuming sender does not
        // need to repeat Enter for the return edge to work again
        let mut entered_clients: HashMap<SocketAddr, (Position, String)> = HashMap::new();
        // addrs whose emulation session timed out while entered
        let mut dormant: HashSet<SocketAddr> = HashSet::new();
        let mut accepted_clients = HashSet::new();
        loop {
            select! {
                e = self.listener.next() => {match e {
                    Some(ListenEvent::Msg { event, addr, conn }) => {
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
                            }).expect("channel closed");
                        }
                        match event {
                            ProtoEvent::Enter(pos, t) => {
                                if let Some(fingerprint) = self.listener.get_certificate_fingerprint(addr).await {
                                    if !self.listener.is_current(addr, &conn) { continue; }
                                    log::info!("releasing capture: {addr} entered this device");
                                    dormant.remove(&addr);
                                    entered_clients.insert(addr, (pos, fingerprint.clone()));
                                    self.event_tx.send(EmulationEvent::ReleaseNotify).expect("channel closed");
                                    self.listener.reply(addr, ProtoEvent::Ack(0)).await;
                                    if !self.listener.is_current(addr, &conn) { continue; }
                                    self.emulation_proxy.warp(addr, to_emulation_pos(pos), t);
                                    self.event_tx.send(EmulationEvent::Entered{addr, pos: to_ipc_pos(pos), fingerprint}).expect("channel closed");
                                }
                            }
                            ProtoEvent::Leave(..) => {
                                entered_clients.remove(&addr);
                                dormant.remove(&addr);
                                self.emulation_proxy.remove(addr);
                                self.listener.reply(addr, ProtoEvent::Ack(0)).await;
                            }
                            ProtoEvent::Input(input_event) => {
                                // Clipboard events bypass the emulation
                                // backend: they are handled by the service's
                                // clipboard emulation module instead.
                                match input_event {
                                    input_event::Event::Clipboard(clipboard_event) => {
                                        self.event_tx
                                            .send(EmulationEvent::ClipboardReceived(
                                                clipboard_event,
                                            ))
                                            .expect("channel closed");
                                    }
                                    _ => {
                                        self.emulation_proxy.consume(input_event, addr);
                                    }
                                }
                            }
                            ProtoEvent::Ping => self.listener.reply(addr, ProtoEvent::Pong(self.emulation_proxy.emulation_active.get())).await,
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
                                self.listener.reply(addr, ProtoEvent::Hello { commit: local_commit() }).await;
                                self.event_tx.send(EmulationEvent::PeerHello { addr, commit }).expect("channel closed");
                            }
                            _ => {}
                        }
                    }
                    Some(ListenEvent::Accept { addr, fingerprint, conn }) => {
                        if !self.listener.is_current(addr, &conn) { continue; }
                        let remembered = forget_peer(addr, &mut entered_clients, &mut dormant, &mut last_response);
                        if !accepted_clients.insert(addr) || remembered {
                            self.emulation_proxy.remove(addr);
                            self.event_tx.send(EmulationEvent::ConnectionClosed { addr }).expect("channel closed");
                        }
                        self.event_tx.send(EmulationEvent::Connected { addr, fingerprint }).expect("channel closed");
                    }
                    Some(ListenEvent::Disconnected { addr }) => {
                        // A new connection may already have replaced this one
                        // while its disconnect notification was queued.
                        if self.listener.has_connection(addr) { continue; }
                        accepted_clients.remove(&addr);
                        forget_peer(addr, &mut entered_clients, &mut dormant, &mut last_response);
                        self.emulation_proxy.remove(addr);
                        self.event_tx.send(EmulationEvent::ConnectionClosed { addr }).expect("channel closed");
                    }
                    Some(ListenEvent::Rejected { fingerprint }) => {
                        if rejected_connections.insert(fingerprint.clone(), Instant::now())
                            .is_none_or(|i| i.elapsed() >= Duration::from_secs(2)) {
                                self.event_tx.send(EmulationEvent::ConnectionAttempt { fingerprint }).expect("channel closed");
                            }
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
                    EmulationRequest::Release(addr, t) => self.listener.reply(addr, ProtoEvent::Leave(0, t)).await,
                    EmulationRequest::UpdateScrollingInversion(invert_scroll) => {
                        self.emulation_proxy.input_config.invert_scroll = invert_scroll;
                        self.emulation_proxy.update_config();
                    }
                    EmulationRequest::UpdateMouseSensitivity(mouse_sensitivity) => {
                        self.emulation_proxy.input_config.mouse_sensitivity = mouse_sensitivity;
                        self.emulation_proxy.update_config();
                    }
                    // send clipboard to a specific address
                    EmulationRequest::SendClipboard(addr, clipboard_event) => {
                        let proto_event = ProtoEvent::Input(input_event::Event::Clipboard(clipboard_event));
                        self.listener.reply_clipboard(addr, proto_event).await;
                    }
                    EmulationRequest::ChangePort(port) => {
                        self.listener.request_port_change(port);
                        let result = self.listener.port_changed().await;
                        self.event_tx.send(EmulationEvent::PortChanged(result)).expect("channel closed");
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
        self.listener.terminate().await;
        self.emulation_proxy.terminate().await;
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
    task: JoinHandle<()>,
    input_config: InputConfig,
}

enum ProxyRequest {
    Input(Event, SocketAddr),
    /// warp the cursor to a normalized cross-axis position along an edge
    Warp(SocketAddr, input_emulation::Position, f64),
    Remove(SocketAddr),
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
            next_id: 0,
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

    fn consume(&self, event: Event, addr: SocketAddr) {
        // ignore events if emulation is currently disabled
        if self.emulation_active.get() {
            self.request_tx
                .send(ProxyRequest::Input(event, addr))
                .expect("channel closed");
        } else {
            log::warn!("emulation inactive, dropping event: {:?}", event);
        }
    }

    fn warp(&self, addr: SocketAddr, pos: input_emulation::Position, t: f64) {
        // ignore if emulation is currently disabled
        if self.emulation_active.get() {
            self.request_tx
                .send(ProxyRequest::Warp(addr, pos, t))
                .expect("channel closed");
        }
    }

    fn remove(&self, addr: SocketAddr) {
        self.request_tx
            .send(ProxyRequest::Remove(addr))
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

    async fn terminate(&mut self) {
        self.exit_requested.replace(true);
        self.request_tx
            .send(ProxyRequest::Terminate)
            .expect("channel closed");
        let _ = (&mut self.task).await;
    }
}

struct EmulationTask {
    backend: Option<input_emulation::Backend>,
    options: EmulationOptions,
    exit_requested: Rc<Cell<bool>>,
    request_rx: Receiver<ProxyRequest>,
    event_tx: Sender<EmulationEvent>,
    handles: HashMap<SocketAddr, EmulationHandle>,
    next_id: EmulationHandle,
    input_config: InputConfig,
}

impl EmulationTask {
    async fn run(mut self) {
        loop {
            if let Err(e) = self.do_emulation().await {
                log::warn!("input emulation exited: {e}");
            }
            if self.exit_requested.get() {
                break;
            }
            // wait for reenable request
            loop {
                match self.request_rx.recv().await.expect("channel closed") {
                    ProxyRequest::Reenable => break,
                    ProxyRequest::Terminate => return,
                    ProxyRequest::Input(..) => { /* emulation inactive => ignore */ }
                    ProxyRequest::Warp(..) => { /* emulation inactive => ignore */ }
                    ProxyRequest::Remove(..) => { /* emulation inactive => ignore */ }
                    ProxyRequest::UpdateConfig(input_config) => {
                        self.input_config = input_config;
                    }
                }
            }
        }
    }

    async fn do_emulation(&mut self) -> Result<(), InputEmulationError> {
        log::info!("creating input emulation ...");
        let mut emulation = tokio::select! {
            r = InputEmulation::new(self.backend, self.options, self.input_config) => r?,
            // allow termination event while requesting input emulation
            _ = wait_for_termination(&mut self.request_rx) => return Ok(()),
        };

        // used to send enabled and disabled events
        let _emulation_guard = DropGuard::new(
            self.event_tx.clone(),
            EmulationEvent::EmulationEnabled,
            EmulationEvent::EmulationDisabled,
        );

        // create active handles
        if let Err(e) = self.create_clients(&mut emulation).await {
            emulation.terminate().await;
            return Err(e);
        }

        let res = self.do_emulation_session(&mut emulation).await;
        // FIXME replace with async drop when stabilized
        emulation.terminate().await;
        res
    }

    async fn create_clients(
        &mut self,
        emulation: &mut InputEmulation,
    ) -> Result<(), InputEmulationError> {
        for handle in self.handles.values() {
            tokio::select! {
                _ = emulation.create(*handle) => {},
                _ = wait_for_termination(&mut self.request_rx) => return Ok(()),
            }
        }
        Ok(())
    }

    async fn do_emulation_session(
        &mut self,
        emulation: &mut InputEmulation,
    ) -> Result<(), InputEmulationError> {
        loop {
            tokio::select! {
                e = self.request_rx.recv() => match e.expect("channel closed") {
                    ProxyRequest::Input(event, addr) => {
                        let handle = self.handle_for(emulation, addr).await;
                        emulation.consume(event, handle).await?;
                    },
                    ProxyRequest::Warp(addr, pos, t) => {
                        let handle = self.handle_for(emulation, addr).await;
                        emulation.warp(handle, pos, t).await;
                    },
                    ProxyRequest::Remove(addr) => {
                        if let Some(&handle) = self.handles.get(&addr) {
                            if emulation.destroy_bounded(handle).await {
                                self.handles.remove(&addr);
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

    /// the emulation handle for `addr`, creating one on first use
    async fn handle_for(
        &mut self,
        emulation: &mut InputEmulation,
        addr: SocketAddr,
    ) -> EmulationHandle {
        if let Some(&handle) = self.handles.get(&addr) {
            return handle;
        }
        let handle = self.next_id;
        self.next_id += 1;
        emulation.create(handle).await;
        self.handles.insert(addr, handle);
        handle
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
            ProxyRequest::Input(_, _) => continue,
            ProxyRequest::Warp(_, _, _) => continue,
            ProxyRequest::Remove(_) => continue,
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
