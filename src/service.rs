use crate::{
    capture::{Capture, CaptureType, ICaptureEvent},
    client::ClientManager,
    clipboard_writer::ClipboardWriter,
    config::{Config, ConfigClient},
    connect::{LanMouseConnection, LanMouseConnectionSender},
    crypto,
    dns::{DnsEvent, DnsResolver},
    emulation::{Emulation, EmulationEvent},
    hooks::{HookKind, HookRunner},
    listen::{LanMouseListener, ListenerCreationError},
    remap::KeyRemap,
    scroll::ScrollInvert,
};
use futures::StreamExt;
use input_capture::clipboard::ClipboardMonitor;
use input_emulation::clipboard::ClipboardEmulation;
use lan_mouse_ipc::{
    AsyncFrontendListener, ClientHandle, FrontendEvent, FrontendRequest, IpcError,
    IpcListenerCreationError, Position, Status,
};
use log;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{signal, sync::Notify};

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("input cleanup incomplete; some native input state may remain pressed")]
    InputCleanupIncomplete,
    #[error(transparent)]
    IpcListen(#[from] IpcListenerCreationError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    ListenError(#[from] ListenerCreationError),
    #[error("failed to load certificate: `{0}`")]
    Certificate(#[from] crypto::Error),
}

pub struct Service {
    /// configuration
    config: Config,
    hooks: HookRunner,
    authentication_notices: crate::authentication::AuthenticationNotices,
    /// input capture
    capture: Capture,
    /// input emulation
    emulation: Emulation,
    /// clipboard monitor
    clipboard_monitor: Option<ClipboardMonitor>,
    /// clipboard emulation
    clipboard_emulation: Option<ClipboardEmulation>,
    clipboard_writer: Option<ClipboardWriter>,
    clipboard_outgoing: crate::clipboard_network::ClipboardJobs,
    clipboard_replay: crate::clipboard_replay::ClipboardReplay,
    clipboard_ready: Arc<Notify>,
    clipboard_received: tokio::sync::watch::Receiver<Option<crate::connect::ReceivedEvent>>,
    clipboard_retry: bool,
    clipboard_write_session: Option<tokio_util::sync::CancellationToken>,
    clipboard_busy_notice: Option<Instant>,
    incoming_clipboard:
        HashMap<SocketAddr, (String, std::sync::Weak<dyn webrtc_util::Conn + Send + Sync>)>,
    /// clipboard enabled
    clipboard_enabled: bool,
    /// dns resolver
    resolver: DnsResolver,
    /// frontend listener
    frontend_listener: AsyncFrontendListener,
    /// authorized public key sha256 fingerprints
    authorized_keys: Arc<RwLock<HashMap<String, String>>>,
    authorization_warning: Option<String>,
    incoming_authorization: crate::listen::IncomingAuthorization,
    /// (outgoing) client information
    client_manager: ClientManager,
    /// lan mouse connection sender (for clipboard)
    conn_sender: LanMouseConnectionSender,
    /// current port
    port: u16,
    /// last configured port; zero is a request, not the running socket port
    configured_port: u16,
    /// the public key fingerprint for (D)TLS
    public_key_fingerprint: String,
    /// notify for pending frontend events
    frontend_event_pending: Notify,
    /// frontend events queued for sending
    pending_frontend_events: VecDeque<FrontendEvent>,
    /// status of input capture (enabled / disabled)
    capture_status: Status,
    /// status of input emulation (enabled / disabled)
    emulation_status: Status,
    /// keep track of registered connections to avoid duplicate barriers
    incoming_conns: HashSet<SocketAddr>,
    /// map from capture handle to connection info
    incoming_conn_info: HashMap<ClientHandle, Incoming>,
    next_trigger_handle: u64,
    window_identifier: Arc<Mutex<Option<input_capture::WindowIdentifier>>>,
}

#[derive(Debug)]
struct Incoming {
    fingerprint: String,
    addr: SocketAddr,
    pos: Position,
}

impl Service {
    pub async fn new(config: Config) -> Result<Self, ServiceError> {
        let client_manager = ClientManager::default();
        for client in config.clients() {
            client_manager.add_with_config(client);
        }

        // load certificate
        let cert = crypto::load_or_generate_key_and_cert(config.cert_path())?;
        let public_key_fingerprint = crypto::certificate_fingerprint(&cert);

        // create frontend communication adapter, exit if already running
        let frontend_listener = AsyncFrontendListener::new().await?;

        let parsed =
            crate::authorization::AuthorizationConfig::new(config.authorized_fingerprints());
        let authorization_warning = parsed.warning();
        if let Some(warning) = &authorization_warning {
            log::warn!("{warning}");
        }
        let authorized_keys = Arc::new(RwLock::new(parsed.trusted));
        // listener + connection
        let listener =
            LanMouseListener::new(config.port(), cert.clone(), authorized_keys.clone()).await?;
        let port = listener.port();
        let configured_port = config.port();
        let authentication_notices = listener.authentication_notices();
        let incoming_authorization = listener.authorization();
        let conn = LanMouseConnection::new(cert.clone(), client_manager.clone());
        let conn_sender = conn.sender();
        let clipboard_ready = conn_sender.clipboard_ready_signal();
        let clipboard_received = conn_sender.clipboard_events();

        // input capture + emulation
        let capture_backend = config.capture_backend().map(|b| b.into());
        let window_identifier = Arc::new(Mutex::new(None));
        let capture = Capture::new(
            capture_backend,
            conn,
            config.release_bind(),
            config.jail_bind(),
            config.enter_binds(),
            window_identifier.clone(),
            KeyRemap::new(config.remap_keys(), config.remap_chords()),
            ScrollInvert::new(
                config.invert_scroll_vertical(),
                config.invert_scroll_horizontal(),
            ),
        );
        let emulation_backend = config.emulation_backend().map(|b| b.into());
        let emulation = Emulation::new(
            emulation_backend,
            config.emulation_options(),
            listener,
            (config.invert_scroll(), config.mouse_sensitivity()),
        );

        // clipboard monitor + emulation
        let clipboard_enabled = config.clipboard_enabled();
        conn_sender.set_clipboard_receiving(clipboard_enabled);
        let (clipboard_monitor, clipboard_emulation) = if clipboard_enabled {
            Self::create_clipboard_parts()
        } else {
            log::info!("Clipboard sharing disabled by configuration");
            (None, None)
        };

        let clipboard_writer = clipboard_emulation.as_ref().map(|emulation| {
            ClipboardWriter::new(
                emulation.clone(),
                clipboard_monitor.as_ref().map(ClipboardMonitor::feedback),
            )
        });

        // create dns resolver
        let resolver = DnsResolver::new()?;

        let service = Self {
            config,
            hooks: HookRunner::new(),
            capture,
            emulation,
            clipboard_monitor,
            clipboard_emulation,
            clipboard_writer,
            clipboard_outgoing: Default::default(),
            clipboard_replay: Default::default(),
            clipboard_ready,
            clipboard_received,
            clipboard_retry: false,
            clipboard_write_session: None,
            clipboard_busy_notice: None,
            incoming_clipboard: Default::default(),
            authentication_notices,
            clipboard_enabled,
            frontend_listener,
            resolver,
            authorized_keys,
            authorization_warning,
            incoming_authorization,
            public_key_fingerprint,
            client_manager: client_manager.clone(),
            conn_sender,
            frontend_event_pending: Default::default(),
            port,
            configured_port,
            pending_frontend_events: Default::default(),
            capture_status: Default::default(),
            emulation_status: Default::default(),
            incoming_conn_info: Default::default(),
            incoming_conns: Default::default(),
            next_trigger_handle: 0,
            window_identifier,
        };
        Ok(service)
    }

    pub async fn run(&mut self) -> Result<(), ServiceError> {
        self.run_with_shutdown(None).await
    }

    pub async fn run_with_shutdown(
        &mut self,
        mut shutdown: Option<tokio::sync::mpsc::UnboundedReceiver<()>>,
    ) -> Result<(), ServiceError> {
        let active = self.client_manager.active_clients();
        for handle in active.iter() {
            // small hack: `activate_client()` checks, if the client
            // is already active in client_manager and does not create a
            // capture barrier in that case so we have to deactivate it first
            self.client_manager.deactivate_client(*handle);
        }

        for handle in active {
            self.activate_client(handle);
        }

        let mut clipboard_retry = tokio::time::interval(Duration::from_millis(250));
        clipboard_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = self.clipboard_received.changed() => {
                    if changed.is_ok() {
                        self.clipboard_received.borrow_and_update();
                        if let Some(received) = self.conn_sender.take_received_clipboard() { self.handle_outgoing_clipboard(received); }
                    }
                },
                _ = self.clipboard_ready.notified() => self.replay_clipboard(),
                _ = clipboard_retry.tick(), if self.clipboard_retry => self.replay_clipboard(),
                request = self.frontend_listener.next() => self.handle_frontend_request(request),
                _ = self.frontend_event_pending.notified() => self.handle_frontend_pending().await,
                fingerprint = self.authentication_notices.next() => self.handle_authentication_attempt(fingerprint),
                event = self.emulation.event() => self.handle_emulation_event(event).await,
                event = self.capture.event() => self.handle_capture_event(event),
                event = self.resolver.event() => self.handle_resolver_event(event),
                result = self.config.changed() => match result {
                    Ok(true) => {
                        if self.config.take_reload_conflict() {
                            self.notify_frontend(FrontendEvent::Error(
                                "External configuration replaced pending settings; review and retry your edits".into()
                            ));
                        }
                        self.handle_config_change();
                    },
                    Ok(false) => {},
                    Err(error) => {
                        log::warn!("could not save or reload configuration: {error}");
                        self.notify_frontend(FrontendEvent::Error(format!(
                            "Failed to save or reload settings: {error}"
                        )));
                    }
                },
                event = async {
                    match &mut self.clipboard_monitor {
                        Some(monitor) => monitor.recv().await,
                        None => std::future::pending().await,
                    }
                } => self.handle_clipboard_event(event),
                completed = self.clipboard_outgoing.completed() => self.handle_clipboard_completion(completed),
                result = async {
                    match &mut self.clipboard_writer {
                        Some(writer) => writer.completed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some((event, result, revision)) = result {
                        self.handle_clipboard_applied(event, result, revision);
                    } else {
                        self.clipboard_writer = None;
                        self.notify_frontend(FrontendEvent::Error("Clipboard writer stopped unexpectedly".into()));
                    }
                },
                r = signal::ctrl_c(), if shutdown.is_none() => break r.expect("failed to wait for CTRL+C"),
                _ = async { shutdown.as_mut().unwrap().recv().await }, if shutdown.is_some() => {
                    log::info!("Shutdown signal received");
                    break;
                },
            }
        }

        log::info!("terminating service ...");
        log::debug!("terminating capture ...");
        self.capture.terminate().await;
        log::debug!("terminating emulation ...");
        let input_cleanup_complete = self.emulation.terminate().await;
        self.conn_sender.terminate().await;
        self.hooks.terminate().await;
        log::debug!("terminating dns resolver ...");
        self.resolver.terminate().await;
        match tokio::time::timeout(Duration::from_secs(2), self.config.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => log::warn!("configuration shutdown save failed: {error}"),
            Err(_) => log::warn!(
                "configuration shutdown save exceeded two seconds; disk completion is unconfirmed"
            ),
        }

        if !input_cleanup_complete {
            let error = ServiceError::InputCleanupIncomplete;
            log::error!("{error}");
            self.notify_frontend(FrontendEvent::Error(error.to_string()));
            self.handle_frontend_pending().await;
            return Err(error);
        }
        Ok(())
    }

    fn handle_frontend_request(&mut self, request: Option<Result<FrontendRequest, IpcError>>) {
        let request = match request.expect("frontend listener closed") {
            Ok(r) => r,
            Err(e) => return log::error!("error receiving request: {e}"),
        };
        match request {
            FrontendRequest::Activate(handle, active) => {
                self.set_client_active(handle, active);
                self.save_config();
            }
            FrontendRequest::AuthorizeKey(desc, fp) => {
                if self.add_authorized_key(desc, fp) {
                    self.save_config();
                }
            }
            FrontendRequest::ChangePort(port) => self.change_port(port),
            FrontendRequest::Create => {
                self.add_client();
                self.save_config();
            }
            FrontendRequest::Delete(handle) => {
                self.remove_client(handle);
                self.save_config();
            }
            FrontendRequest::EnableCapture => self.capture.reenable(),
            FrontendRequest::EnableEmulation => self.emulation.reenable(),
            FrontendRequest::Enumerate() => self.enumerate(),
            FrontendRequest::UpdateFixIps(handle, fix_ips) => {
                self.update_fix_ips(handle, fix_ips);
                self.save_config();
            }
            FrontendRequest::UpdateHostname(handle, host) => {
                self.update_hostname(handle, host);
                self.save_config();
            }
            FrontendRequest::UpdatePort(handle, port) => {
                self.update_port(handle, port);
                self.save_config();
            }
            FrontendRequest::UpdatePosition(handle, pos) => {
                self.update_pos(handle, pos);
                self.save_config();
            }
            FrontendRequest::ResolveDns(handle) => self.resolve(handle),
            FrontendRequest::Sync => self.sync_frontend(),
            FrontendRequest::RemoveAuthorizedKey(key) => {
                self.remove_authorized_key(key);
                self.save_config();
            }
            FrontendRequest::UpdateEnterHook(handle, enter_hook) => {
                self.update_enter_hook(handle, enter_hook)
            }
            FrontendRequest::UpdateLeaveHook(handle, leave_hook) => {
                self.update_leave_hook(handle, leave_hook)
            }
            FrontendRequest::SaveConfiguration => self.save_config(),
            FrontendRequest::UpdateScrollingInversion(invert_scroll) => {
                self.update_scrolling_inversion(invert_scroll)
            }
            FrontendRequest::UpdateMouseSensitivity(mouse_sensitivity) => {
                self.update_mouse_sensitivity(mouse_sensitivity)
            }
            FrontendRequest::SetClipboardEnabled(enabled) => self.set_clipboard_enabled(enabled),
            FrontendRequest::WindowIdentifier(handle) => {
                log::info!("xdg-foreign handle: {handle:?}");
                self.window_identifier
                    .lock()
                    .unwrap()
                    .replace(match handle {
                        lan_mouse_ipc::WindowIdentifier::Wayland(handle) => {
                            input_capture::WindowIdentifier::Wayland(handle)
                        }
                        lan_mouse_ipc::WindowIdentifier::X11(xid) => {
                            input_capture::WindowIdentifier::X11(xid)
                        }
                    });
            }
        }
    }

    fn save_config(&mut self) {
        let clients = self.client_manager.clients();
        let clients = clients
            .into_iter()
            .map(|(c, s)| ConfigClient {
                ips: HashSet::from_iter(c.fix_ips),
                hostname: c.hostname,
                port: c.port,
                pos: c.pos,
                active: s.active,
                enter_hook: c.cmd,
                leave_hook: c.leave_cmd,
            })
            .collect();
        self.config.set_clients(clients);
        // Authorization edits update the original table explicitly. Saving other
        // settings must not rewrite aliases or silently discard malformed entries.
        self.config.queue_write_back();
    }

    fn handle_config_change(&mut self) {
        for h in self.client_manager.registered_clients() {
            self.remove_client(h);
        }
        for c in self.config.clients() {
            let handle = self.client_manager.add_with_config(c);
            log::info!("added client {handle}");
            let (c, s) = self.client_manager.get_state(handle).unwrap();
            if s.active {
                self.client_manager.deactivate_client(handle);
                self.activate_client(handle);
            }
            self.notify_frontend(FrontendEvent::Created(handle, c, s));
        }
        let release_bind = self.config.release_bind();
        self.capture.set_release_bind(release_bind);
        let jail_bind = self.config.jail_bind();
        self.capture.set_jail_bind(jail_bind);
        let enter_binds = self.config.enter_binds();
        self.capture.set_enter_binds(enter_binds);
        // Applying an external snapshot must not invoke GUI setters that save
        // runtime state back over the newly read configuration.
        self.emulation
            .request_scrolling_inversion(self.config.invert_scroll());
        self.emulation
            .request_mouse_sensitivity_change(self.config.mouse_sensitivity());
        self.capture.set_remap(KeyRemap::new(
            self.config.remap_keys(),
            self.config.remap_chords(),
        ));
        self.capture.set_scroll_invert(ScrollInvert::new(
            self.config.invert_scroll_vertical(),
            self.config.invert_scroll_horizontal(),
        ));
        self.reload_authorized_keys();
        self.apply_clipboard_enabled(self.config.clipboard_enabled());
        let configured_port = self.config.port();
        if configured_port != self.configured_port {
            self.configured_port = configured_port;
            self.change_port(configured_port);
        }
        self.sync_frontend();
    }

    async fn handle_frontend_pending(&mut self) {
        while let Some(event) = self.pending_frontend_events.pop_front() {
            self.frontend_listener.broadcast(event).await;
        }
    }

    fn handle_authentication_attempt(&mut self, fingerprint: String) {
        // Authorization may have changed while this bounded prompt was waiting.
        if !self
            .authorized_keys
            .read()
            .expect("lock")
            .contains_key(&fingerprint)
        {
            self.notify_frontend(FrontendEvent::ConnectionAttempt { fingerprint });
        }
    }

    async fn handle_emulation_event(&mut self, event: EmulationEvent) {
        match event {
            EmulationEvent::Entered {
                addr,
                pos,
                fingerprint,
                conn,
                control: _control,
                input: _input,
            } => {
                if !self.emulation.clipboard_session_is_current(addr, &conn) {
                    return;
                }
                // check if already registered
                if !self.incoming_conns.contains(&addr) {
                    self.add_incoming(addr, pos, fingerprint.clone());
                    self.notify_frontend(FrontendEvent::DeviceEntered {
                        fingerprint,
                        addr,
                        pos,
                    });
                } else {
                    self.update_incoming(addr, pos, fingerprint);
                }
            }
            EmulationEvent::ConnectionClosed {
                addr,
                admission: _admission,
            } => {
                self.remove_incoming(addr);
                self.notify_frontend(FrontendEvent::IncomingDisconnected(addr));
            }
            EmulationEvent::Disconnected {
                addr,
                conn,
                timeout: _timeout,
            } => {
                if !self.emulation.clipboard_session_is_current(addr, &conn) {
                    return;
                }
                if let Some(addr) = self.remove_incoming(addr) {
                    self.notify_frontend(FrontendEvent::IncomingDisconnected(addr));
                }
            }
            EmulationEvent::PortChanged(port) => match port {
                Ok(port) => {
                    self.port = port;
                    self.notify_frontend(FrontendEvent::PortChanged(port, None));
                }
                Err(e) => self
                    .notify_frontend(FrontendEvent::PortChanged(self.port, Some(format!("{e}")))),
            },
            EmulationEvent::InputRejected {
                addr,
                reason,
                admission: _admission,
            } => {
                self.notify_frontend(FrontendEvent::Error(format!(
                    "Invalid incoming input from {addr}: {reason}. Its connection was closed."
                )));
            }
            EmulationEvent::InputCleanupFailed {
                addr,
                input: _input,
            } => {
                self.notify_frontend(FrontendEvent::Error(format!("Previous input session at {addr} could not finish cleanup. New input was rejected; retry after the backend recovers.")));
            }
            EmulationEvent::InputOverloaded {
                addr,
                admission: _admission,
                input: _input,
            } => {
                self.notify_frontend(FrontendEvent::Error(format!("Incoming input from {addr} is stalled; its connection was closed. Retry after the input backend recovers.")));
            }
            EmulationEvent::BackendFailed(error) => {
                self.notify_frontend(FrontendEvent::Error(format!(
                    "Input emulation failed: {error}"
                )));
            }
            EmulationEvent::EmulationDisabled => {
                self.emulation_status = Status::Disabled;
                self.notify_frontend(FrontendEvent::EmulationStatus(self.emulation_status));
            }
            EmulationEvent::EmulationEnabled => {
                self.emulation_status = Status::Enabled;
                self.notify_frontend(FrontendEvent::EmulationStatus(self.emulation_status));
            }
            EmulationEvent::ReleaseNotify {
                addr,
                conn,
                input: _input,
            } => {
                if self.emulation.clipboard_session_is_current(addr, &conn) {
                    self.capture.release();
                }
            }
            EmulationEvent::Connected {
                addr,
                fingerprint,
                conn,
                admission: _admission,
            } => {
                if !self.emulation.clipboard_session_is_current(addr, &conn) {
                    return;
                }
                self.incoming_clipboard
                    .insert(addr, (fingerprint.clone(), Arc::downgrade(&conn)));
                self.notify_frontend(FrontendEvent::DeviceConnected { addr, fingerprint });
                self.replay_clipboard();
            }
            EmulationEvent::PeerHello {
                addr,
                commit,
                conn,
                control: _control,
            } => {
                if !self.emulation.clipboard_session_is_current(addr, &conn) {
                    return;
                }
                // Map the peer's source addr back to its client handle
                // and stamp the commit. Skip if we don't have an
                // outgoing client configured for this peer (incoming-
                // only setup) — there's nowhere to display the version
                // in that case anyway.
                if let Some(handle) = self.client_manager.get_client(addr) {
                    self.client_manager.set_peer_commit(handle, Some(commit));
                    self.broadcast_client(handle);
                }
            }
            EmulationEvent::ClipboardReceived {
                event,
                addr,
                conn,
                control: _control,
            } => {
                if !self.emulation.clipboard_session_is_current(addr, &conn) {
                    return;
                }
                let origin = self
                    .incoming_clipboard
                    .get(&addr)
                    .filter(|(_, current)| {
                        current
                            .upgrade()
                            .is_some_and(|current| Arc::ptr_eq(&current, &conn))
                    })
                    .map(|(origin, _)| origin.clone());
                if let Some(origin) = origin {
                    self.receive_clipboard(
                        event,
                        origin,
                        self.incoming_authorization.token(addr, &conn),
                    );
                }
            }
            EmulationEvent::ClipboardSendCompleted(completed) => {
                self.handle_clipboard_completion(completed)
            }
        }
    }

    fn handle_clipboard_completion(
        &mut self,
        completed: crate::clipboard_network::ClipboardCompletion,
    ) {
        if !self.clipboard_enabled || completed.generation != self.emulation.clipboard_generation()
        {
            return;
        }
        if let Some((handle, revision)) = completed.outgoing {
            if !self.client_manager.target_is_current(handle, revision) {
                return;
            }
        }
        if let Some(conn) = &completed.conn {
            let current = match completed.outgoing {
                Some((handle, revision)) => self.conn_sender.clipboard_session_is_current(
                    handle,
                    revision,
                    completed.addr,
                    conn,
                ),
                None => self
                    .emulation
                    .clipboard_session_is_current(completed.addr, conn),
            };
            if !current {
                return;
            }
        }
        match completed.result {
            Ok(()) => self.notify_frontend(FrontendEvent::ClipboardShared {
                received: false,
                kind: completed.kind,
                bytes: completed.bytes,
            }),
            Err(crate::listen::ClipboardSendError::Canceled) => {}
            Err(error) => {
                if let (Some((handle, revision)), Some(conn)) = (completed.outgoing, completed.conn)
                {
                    self.conn_sender
                        .clipboard_send_failed(handle, revision, completed.addr, conn);
                }
                log::warn!("clipboard send to {} failed: {error}", completed.addr);
                self.notify_frontend(FrontendEvent::Error(format!(
                    "Failed to send clipboard to {}: {error}",
                    completed.addr
                )));
            }
        }
    }

    fn handle_capture_event(&mut self, event: ICaptureEvent) {
        match event {
            ICaptureEvent::CaptureBegin(handle, t) => {
                // we entered the capture zone for an incoming connection
                // => notify it that its capture should be released
                if let Some(incoming) = self.incoming_conn_info.get(&handle) {
                    self.emulation.send_leave_event(incoming.addr, t);
                }
            }
            ICaptureEvent::CaptureCleanupPending(reason) => {
                self.capture_status = Status::Disabled;
                self.notify_frontend(FrontendEvent::CaptureStatus(self.capture_status));
                self.notify_frontend(FrontendEvent::Error(format!(
                    "Input capture cleanup is still pending: {reason}. Waiting for backend cleanup before capture can be re-enabled."
                )));
            }
            ICaptureEvent::CaptureFailed(error) => {
                self.notify_frontend(FrontendEvent::Error(format!(
                    "Input capture stopped: {error}"
                )));
            }
            ICaptureEvent::CaptureDisabled => {
                self.capture_status = Status::Disabled;
                self.notify_frontend(FrontendEvent::CaptureStatus(self.capture_status));
            }
            ICaptureEvent::CaptureEnabled => {
                self.capture_status = Status::Enabled;
                self.notify_frontend(FrontendEvent::CaptureStatus(self.capture_status));
            }
            ICaptureEvent::ClientEntered(handle) => {
                log::info!("entering client {handle} ...");
                self.spawn_hook_command(handle, HookKind::Enter);
            }
            ICaptureEvent::ClientLeft(handle) => {
                log::info!("leaving client {handle} ...");
                self.spawn_hook_command(handle, HookKind::Leave);
            }
        }
    }

    fn handle_outgoing_clipboard(&mut self, received: crate::connect::ReceivedEvent) {
        let crate::connect::ReceivedEvent {
            event,
            handle,
            revision,
            addr,
            conn,
        } = received;
        if !self
            .conn_sender
            .clipboard_session_is_current(handle, revision, addr, &conn)
        {
            return;
        }
        if let (
            lan_mouse_proto::ProtoEvent::Input(input_event::Event::Clipboard(event)),
            Some(origin),
        ) = (event, self.conn_sender.clipboard_peer(handle, &conn))
        {
            self.receive_clipboard(event, origin, None);
        }
    }

    fn receive_clipboard(
        &mut self,
        event: input_event::ClipboardEvent,
        origin: String,
        session: Option<tokio_util::sync::CancellationToken>,
    ) {
        if !self.clipboard_enabled {
            return;
        }
        if event.content_len() > lan_mouse_proto::MAX_CLIPBOARD_SIZE {
            return;
        }
        let Some(writer) = &self.clipboard_writer else {
            self.notify_frontend(FrontendEvent::Error(
                "Clipboard writer is unavailable".into(),
            ));
            return;
        };
        // Invalidate accepted local snapshots as soon as a remote intent arrives.
        // Canceled network jobs cannot start a stale local send during OS writing.
        self.emulation.clear_clipboard();
        self.clipboard_retry = false;
        let revision = self.clipboard_replay.begin_remote(origin);
        self.clipboard_write_session = session.clone();
        writer.submit_with_revision(event, revision, session);
    }

    fn handle_clipboard_applied(
        &mut self,
        event: input_event::ClipboardEvent,
        result: Result<(), input_emulation::clipboard::ClipboardError>,
        revision: u64,
    ) {
        let canceled = self
            .clipboard_write_session
            .as_ref()
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled);
        if !self.clipboard_enabled
            || !self
                .clipboard_replay
                .applied(revision, event.clone(), result.is_ok() && !canceled)
        {
            return;
        }
        if canceled {
            return;
        }
        match result {
            Ok(()) => {
                self.notify_clipboard_shared(&event, true);
                self.replay_clipboard();
            }
            Err(error) => {
                log::warn!("Failed to apply remote clipboard: {error}");
                self.notify_frontend(FrontendEvent::Error(format!(
                    "Failed to apply clipboard: {error}"
                )));
            }
        }
    }

    fn handle_clipboard_event(&mut self, event: Option<input_capture::CaptureEvent>) {
        if !self.clipboard_enabled {
            return;
        }
        if let Some(input_capture::CaptureEvent::Input(input_event::Event::Clipboard(event))) =
            event
        {
            let bytes = event.content_len();
            if bytes > lan_mouse_proto::MAX_CLIPBOARD_SIZE {
                self.clipboard_replay.invalidate();
                self.emulation.clear_clipboard();
                self.clipboard_retry = false;
                self.notify_frontend(FrontendEvent::ClipboardTooLarge {
                    bytes,
                    limit: lan_mouse_proto::MAX_CLIPBOARD_SIZE,
                });
                return;
            }
            self.clipboard_replay.local(event);
            self.replay_clipboard();
        }
    }

    fn clipboard_send_busy(&mut self) {
        self.clipboard_retry = true;
        if self
            .clipboard_busy_notice
            .is_none_or(|at| at.elapsed() >= Duration::from_secs(2))
        {
            self.clipboard_busy_notice = Some(Instant::now());
            self.notify_frontend(FrontendEvent::Error(
                "Clipboard send is busy; retrying the latest snapshot".into(),
            ));
        }
    }

    fn replay_clipboard(&mut self) {
        self.clipboard_retry = false;
        if !self.clipboard_enabled {
            return;
        }
        let Some(snapshot) = self.clipboard_replay.snapshot() else {
            return;
        };
        let mut current_peers = self.conn_sender.clipboard_known_peers();
        let mut peers = HashSet::new();
        for handle in self.client_manager.active_clients() {
            let (conn, peer) = match self.conn_sender.clipboard_current(handle) {
                Ok(current) => current,
                Err(crate::connect::LanMouseConnectionError::ClipboardBusy) => {
                    self.clipboard_send_busy();
                    continue;
                }
                Err(_) => continue,
            };
            if !peers.insert(peer.clone())
                || !self.clipboard_replay.should_send(&snapshot, &peer, &conn)
            {
                continue;
            }
            let (generation, token) = self.emulation.clipboard_scope();
            match self.conn_sender.prepare_clipboard(
                (*snapshot.event).clone(),
                handle,
                generation,
                token,
            ) {
                Ok(request) => {
                    self.clipboard_outgoing
                        .cancel_stale(request.addr, Some(&conn));
                    if self.clipboard_outgoing.submit(request).is_ok() {
                        self.clipboard_replay.accepted(&snapshot, peer, &conn);
                    } else {
                        peers.remove(&peer);
                        self.clipboard_send_busy();
                    }
                }
                Err(crate::connect::LanMouseConnectionError::ClipboardBusy) => {
                    peers.remove(&peer);
                    self.clipboard_send_busy();
                }
                Err(_) => {
                    peers.remove(&peer);
                }
            }
        }
        // Authenticated incoming transport is ready even before Enter establishes
        // a return edge. Validate queued Connected metadata against the exact Arc.
        let incoming: HashMap<_, _> = self.emulation.clipboard_sessions().into_iter().collect();
        self.incoming_clipboard.retain(|addr, (_, conn)| {
            incoming.get(addr).is_some_and(|current| {
                conn.upgrade()
                    .is_some_and(|conn| Arc::ptr_eq(current, &conn))
            })
        });
        let targets: Vec<_> = self
            .incoming_clipboard
            .iter()
            .map(|(addr, (peer, _))| (*addr, peer.clone(), incoming[addr].clone()))
            .collect();
        for (addr, peer, conn) in targets {
            current_peers.insert(peer.clone());
            if !peers.insert(peer.clone())
                || !self.clipboard_replay.should_send(&snapshot, &peer, &conn)
            {
                continue;
            }
            if self
                .emulation
                .send_clipboard(addr, (*snapshot.event).clone())
                .is_ok()
            {
                self.clipboard_replay.accepted(&snapshot, peer, &conn);
            } else {
                self.clipboard_send_busy();
            }
        }
        self.clipboard_replay.retain_peers(&current_peers);
    }

    fn handle_resolver_event(&mut self, event: DnsEvent) {
        let handle = match event {
            DnsEvent::Resolving(handle, revision) => {
                if !self.resolver.is_current(handle, revision) {
                    return;
                }
                self.client_manager.set_resolving(handle, true);
                handle
            }
            DnsEvent::Resolved(handle, revision, hostname, ips) => {
                if !self.resolver.is_current(handle, revision)
                    || self.client_manager.get_hostname(handle).as_deref() != Some(&hostname)
                {
                    return;
                }
                self.client_manager.set_resolving(handle, false);
                match ips {
                    Ok(ips) => self.client_manager.set_dns_ips(handle, ips),
                    Err(error) => {
                        log::warn!("could not resolve {hostname}: {error}");
                        self.notify_frontend(FrontendEvent::Error(format!(
                            "Could not resolve {hostname}: {error}"
                        )));
                    }
                }
                handle
            }
        };
        self.broadcast_client(handle);
    }

    fn resolve(&self, handle: ClientHandle) {
        if let Some(hostname) = self.client_manager.get_hostname(handle) {
            self.resolver.resolve(handle, hostname);
        }
    }

    fn sync_frontend(&mut self) {
        self.enumerate();
        self.notify_frontend(FrontendEvent::EmulationStatus(self.emulation_status));
        self.notify_frontend(FrontendEvent::CaptureStatus(self.capture_status));
        self.notify_frontend(FrontendEvent::PortChanged(self.port, None));
        self.notify_frontend(FrontendEvent::PublicKeyFingerprint(
            self.public_key_fingerprint.clone(),
        ));
        self.notify_authorized_keys();
        self.notify_settings();
    }

    const ENTER_HANDLE_BEGIN: u64 = u64::MAX / 2 + 1;

    fn add_incoming(&mut self, addr: SocketAddr, pos: Position, fingerprint: String) {
        let handle = Self::ENTER_HANDLE_BEGIN + self.next_trigger_handle;
        self.next_trigger_handle += 1;
        self.capture.create(handle, pos, CaptureType::EnterOnly);
        self.incoming_conns.insert(addr);
        self.incoming_conn_info.insert(
            handle,
            Incoming {
                fingerprint,
                addr,
                pos,
            },
        );
    }

    fn update_incoming(&mut self, addr: SocketAddr, pos: Position, fingerprint: String) {
        let incoming = self
            .incoming_conn_info
            .iter_mut()
            .find(|(_, i)| i.addr == addr)
            .map(|(_, i)| i)
            .expect("no such client");
        let mut changed = false;
        if incoming.fingerprint != fingerprint {
            incoming.fingerprint = fingerprint.clone();
            changed = true;
        }
        if incoming.pos != pos {
            incoming.pos = pos;
            changed = true;
        }
        if changed {
            self.remove_incoming(addr);
            self.add_incoming(addr, pos, fingerprint.clone());
            self.notify_frontend(FrontendEvent::IncomingDisconnected(addr));
            self.notify_frontend(FrontendEvent::DeviceEntered {
                fingerprint,
                addr,
                pos,
            });
        }
    }

    fn remove_incoming(&mut self, addr: SocketAddr) -> Option<SocketAddr> {
        let handle = self
            .incoming_conn_info
            .iter()
            .find(|(_, incoming)| incoming.addr == addr)
            .map(|(k, _)| *k)?;
        self.capture.destroy(handle);
        self.incoming_conns.remove(&addr);
        self.incoming_conn_info
            .remove(&handle)
            .map(|incoming| incoming.addr)
    }

    fn notify_frontend(&mut self, event: FrontendEvent) {
        self.pending_frontend_events.push_back(event);
        self.frontend_event_pending.notify_one();
    }

    fn reload_authorized_keys(&mut self) {
        let parsed =
            crate::authorization::AuthorizationConfig::new(self.config.authorized_fingerprints());
        self.authorization_warning = parsed.warning();
        if let Some(warning) = &self.authorization_warning {
            log::warn!("{warning}");
        }
        *self.authorized_keys.write().expect("lock") = parsed.trusted;
        for addr in self.incoming_authorization.revoke_untrusted() {
            self.remove_incoming(addr);
            self.incoming_clipboard.remove(&addr);
            self.notify_frontend(FrontendEvent::IncomingDisconnected(addr));
        }
    }

    fn notify_authorized_keys(&mut self) {
        let keys = self.authorized_keys.read().expect("lock").clone();
        self.notify_frontend(FrontendEvent::AuthorizedUpdated(keys));
        if let Some(warning) = self.authorization_warning.clone() {
            self.notify_frontend(FrontendEvent::Error(warning));
        }
    }

    fn add_authorized_key(&mut self, desc: String, fp: String) -> bool {
        let fp = match lan_mouse_ipc::normalize_fingerprint(&fp) {
            Ok(fp) => fp,
            Err(error) => {
                self.notify_frontend(FrontendEvent::Error(error.to_string()));
                return false;
            }
        };
        let mut keys = self.config.authorized_fingerprints();
        keys.retain(|key, _| lan_mouse_ipc::normalize_fingerprint(key).ok().as_ref() != Some(&fp));
        keys.insert(fp, desc);
        self.config.set_authorized_keys(keys);
        self.reload_authorized_keys();
        self.notify_authorized_keys();
        true
    }

    fn remove_authorized_key(&mut self, fp: String) {
        let mut keys = self.config.authorized_fingerprints();
        if let Ok(canonical) = lan_mouse_ipc::normalize_fingerprint(&fp) {
            // Removing a visible digest must remove every spelling, or a legacy
            // alias could restore authorization on the next reload.
            keys.retain(|key, _| {
                lan_mouse_ipc::normalize_fingerprint(key).ok().as_ref() != Some(&canonical)
            });
        } else {
            keys.remove(&fp);
        }
        self.config.set_authorized_keys(keys);
        self.reload_authorized_keys();
        self.notify_authorized_keys();
    }

    fn enumerate(&mut self) {
        let clients = self.client_manager.get_client_states();
        self.notify_frontend(FrontendEvent::Enumerate(clients));
    }

    fn add_client(&mut self) {
        let handle = self.client_manager.add_client();
        log::info!("added client {handle}");
        let (c, s) = self.client_manager.get_state(handle).unwrap();
        self.notify_frontend(FrontendEvent::Created(handle, c, s));
    }

    fn set_client_active(&mut self, handle: ClientHandle, active: bool) {
        if active {
            self.activate_client(handle);
        } else {
            self.deactivate_client(handle);
        }
    }

    fn deactivate_client(&mut self, handle: ClientHandle) {
        log::debug!("deactivating client {handle}");
        if self.client_manager.deactivate_client(handle) {
            self.capture.destroy(handle);
            self.broadcast_client(handle);
            log::info!("deactivated client {handle}");
        }
    }

    fn activate_client(&mut self, handle: ClientHandle) {
        log::debug!("activating client {handle}");

        /* resolve dns on activate */
        self.resolve(handle);

        /* deactivate potential other client at this position */
        let Some(pos) = self.client_manager.get_pos(handle) else {
            return;
        };

        if let Some(other) = self.client_manager.client_at(pos) {
            if other != handle {
                self.deactivate_client(other);
            }
        }

        /* activate the client */
        if self.client_manager.activate_client(handle) {
            /* notify capture and frontends */
            self.capture.create(handle, pos, CaptureType::Default);
            self.broadcast_client(handle);
            log::info!("activated client {handle} ({pos})");
        }
    }

    fn change_port(&mut self, port: u16) {
        // Even the current port supersedes an in-flight request for another port.
        self.emulation.request_port_change(port);
    }

    fn remove_client(&mut self, handle: ClientHandle) {
        self.resolver.cancel(handle);
        self.hooks.cancel_client(handle);
        if self
            .client_manager
            .remove_client(handle)
            .map(|(_, s)| s.active)
            .unwrap_or(false)
        {
            self.capture.destroy(handle);
        }
        self.notify_frontend(FrontendEvent::Deleted(handle));
    }

    fn update_fix_ips(&mut self, handle: ClientHandle, fix_ips: Vec<IpAddr>) {
        self.client_manager.set_fix_ips(handle, fix_ips);
        self.broadcast_client(handle);
    }

    fn update_hostname(&mut self, handle: ClientHandle, hostname: Option<String>) {
        log::info!("hostname changed: {hostname:?}");
        if self.client_manager.set_hostname(handle, hostname.clone()) {
            self.resolver.cancel(handle);
            self.resolve(handle);
        }
        self.broadcast_client(handle);
    }

    fn update_port(&mut self, handle: ClientHandle, port: u16) {
        self.client_manager.set_port(handle, port);
        self.broadcast_client(handle);
    }

    fn update_pos(&mut self, handle: ClientHandle, pos: Position) {
        // update state in event input emulator & input capture
        if self.client_manager.set_pos(handle, pos) {
            self.deactivate_client(handle);
            self.activate_client(handle);
        }
        self.broadcast_client(handle);
    }

    fn update_enter_hook(&mut self, handle: ClientHandle, enter_hook: Option<String>) {
        self.client_manager.set_enter_hook(handle, enter_hook);
        self.broadcast_client(handle);
    }

    fn update_leave_hook(&mut self, handle: ClientHandle, leave_hook: Option<String>) {
        self.client_manager.set_leave_hook(handle, leave_hook);
        self.broadcast_client(handle);
    }

    fn broadcast_client(&mut self, handle: ClientHandle) {
        let event = self
            .client_manager
            .get_state(handle)
            .map(|(c, s)| FrontendEvent::State(handle, c, s))
            .unwrap_or(FrontendEvent::NoSuchClient(handle));
        self.notify_frontend(event);
    }

    /// lazily create the clipboard monitor + emulation pair; both are
    /// `None` when the platform clipboard is unavailable (headless, no
    /// X/Wayland clipboard access)
    fn create_clipboard_parts() -> (Option<ClipboardMonitor>, Option<ClipboardEmulation>) {
        let monitor = match ClipboardMonitor::new() {
            Ok(m) => {
                log::info!("Clipboard monitoring enabled");
                Some(m)
            }
            Err(e) => {
                log::warn!("Failed to create clipboard monitor: {}", e);
                None
            }
        };
        let emulation = match ClipboardEmulation::new() {
            Ok(e) => {
                log::info!("Clipboard emulation enabled");
                Some(e)
            }
            Err(e) => {
                log::warn!("Failed to create clipboard emulation: {}", e);
                None
            }
        };
        (monitor, emulation)
    }

    fn set_clipboard_enabled(&mut self, enabled: bool) {
        if self.clipboard_enabled == enabled {
            self.notify_settings();
            return;
        }
        self.apply_clipboard_enabled(enabled);
        self.config.set_clipboard_enabled(enabled);
        self.save_config();
        self.notify_settings();
    }

    /// Update runtime state without writing an externally loaded snapshot back.
    fn apply_clipboard_enabled(&mut self, enabled: bool) {
        if self.clipboard_enabled == enabled {
            return;
        }
        log::info!(
            "clipboard sharing {}",
            if enabled { "enabled" } else { "disabled" }
        );
        self.clipboard_enabled = enabled;
        self.conn_sender.set_clipboard_receiving(enabled);
        if enabled {
            // lazily create the monitor/emulation if they were missing
            // (e.g. clipboard unavailable at daemon startup)
            if self.clipboard_monitor.is_none() || self.clipboard_emulation.is_none() {
                let (monitor, emulation) = Self::create_clipboard_parts();
                if self.clipboard_monitor.is_none() {
                    self.clipboard_monitor = monitor;
                }
                if self.clipboard_emulation.is_none() {
                    self.clipboard_emulation = emulation;
                }
            }
            if self.clipboard_writer.is_none() {
                self.clipboard_writer = self.clipboard_emulation.as_ref().map(|emulation| {
                    ClipboardWriter::new(
                        emulation.clone(),
                        self.clipboard_monitor
                            .as_ref()
                            .map(ClipboardMonitor::feedback),
                    )
                });
            }
            if let Some(monitor) = &self.clipboard_monitor {
                monitor.enable();
            }
        } else {
            self.clipboard_replay.invalidate();
            self.clipboard_retry = false;
            self.emulation.clear_clipboard();
            if let Some(monitor) = &self.clipboard_monitor {
                monitor.disable();
            }
            if let Some(writer) = &self.clipboard_writer {
                writer.clear_pending();
            }
        }
    }

    fn update_scrolling_inversion(&mut self, invert_scroll: bool) {
        self.emulation.request_scrolling_inversion(invert_scroll);
        self.config.set_invert_scroll(invert_scroll);
        self.save_config();
        self.notify_settings();
    }

    fn update_mouse_sensitivity(&mut self, mouse_sensitivity: f64) {
        if !mouse_sensitivity.is_finite() {
            self.notify_frontend(FrontendEvent::Error(
                "Mouse sensitivity must be finite".into(),
            ));
            self.notify_settings();
            return;
        }
        self.emulation
            .request_mouse_sensitivity_change(mouse_sensitivity);
        self.config.set_mouse_sensitivity(mouse_sensitivity);
        self.save_config();
        self.notify_settings();
    }

    /// push the current settings to the frontend
    fn notify_settings(&mut self) {
        self.notify_frontend(FrontendEvent::Settings {
            clipboard_enabled: self.clipboard_enabled,
            invert_scroll: self.config.invert_scroll(),
            mouse_sensitivity: self.config.mouse_sensitivity(),
        });
    }

    /// let the frontend know clipboard content travelled in either direction
    fn notify_clipboard_shared(&mut self, event: &input_event::ClipboardEvent, received: bool) {
        self.notify_frontend(FrontendEvent::ClipboardShared {
            received,
            kind: event.kind(),
            bytes: event.content_len(),
        });
    }

    fn spawn_hook_command(&mut self, handle: ClientHandle, kind: HookKind) {
        let command = match kind {
            HookKind::Enter => self.client_manager.get_enter_cmd(handle),
            HookKind::Leave => self.client_manager.get_leave_cmd(handle),
        };
        if let Some(command) = command {
            if let Some(notice) = self.hooks.submit(handle, kind, command) {
                log::warn!("{notice}");
                self.notify_frontend(FrontendEvent::Error(notice));
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires isolated LAN_MOUSE_SERVICE_TEST_RUNTIME_DIR / XDG_RUNTIME_DIR"]
    async fn real_dtls_clipboard_replay_both_routes_origin_order_disable_and_reconnect() {
        use crate::listen::{ArcConn, ListenEvent};
        use futures::StreamExt;
        use input_event::{ClipboardEvent, Event};
        use lan_mouse_proto::ProtoEvent;
        use webrtc_dtls::{
            config::{Config as DtlsConfig, ExtendedMasterSecretType},
            conn::DTLSConn,
            crypto::Certificate,
        };
        fn text(value: &str) -> ClipboardEvent {
            ClipboardEvent::Text(value.into())
        }
        fn local(service: &mut Service, event: ClipboardEvent) {
            service.handle_clipboard_event(Some(input_capture::CaptureEvent::Input(
                Event::Clipboard(event),
            )));
        }
        async fn send(conn: &ArcConn, event: ClipboardEvent) {
            crate::listen::send_clipboard_reply(
                Some(conn.clone()),
                ProtoEvent::Input(Event::Clipboard(event)),
            )
            .await
            .unwrap();
        }
        async fn receive(conn: &ArcConn) -> ClipboardEvent {
            let mut bytes = vec![0; lan_mouse_proto::MAX_CLIPBOARD_SIZE + 5];
            let count = tokio::time::timeout(Duration::from_secs(2), conn.recv(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            match lan_mouse_proto::decode_event_frame(&bytes[..count]).unwrap() {
                ProtoEvent::Input(Event::Clipboard(event)) => event,
                _ => panic!("expected clipboard snapshot"),
            }
        }
        async fn incoming(service: &mut Service, cert: Certificate) -> ArcConn {
            service
                .authorized_keys
                .write()
                .unwrap()
                .insert(crypto::certificate_fingerprint(&cert), "fixture".into());
            let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
            socket
                .connect((std::net::Ipv4Addr::LOCALHOST, service.port))
                .await
                .unwrap();
            let conn: ArcConn = Arc::new(
                tokio::time::timeout(
                    Duration::from_secs(2),
                    DTLSConn::new(
                        socket,
                        DtlsConfig {
                            certificates: vec![cert],
                            insecure_skip_verify: true,
                            extended_master_secret: ExtendedMasterSecretType::Require,
                            ..Default::default()
                        },
                        true,
                        None,
                    ),
                )
                .await
                .unwrap()
                .unwrap(),
            );
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let event = service.emulation.event().await;
                    let accepted = matches!(event, EmulationEvent::Connected { .. });
                    service.handle_emulation_event(event).await;
                    if accepted {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            conn
        }
        async fn outgoing_receive(service: &mut Service) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    service.clipboard_received.changed().await.unwrap();
                    service.clipboard_received.borrow_and_update();
                    if let Some(received) = service.conn_sender.take_received_clipboard() {
                        service.handle_outgoing_clipboard(received);
                        break;
                    }
                }
            })
            .await
            .unwrap();
        }
        async fn network_done(service: &mut Service) {
            let completed = tokio::time::timeout(
                Duration::from_secs(2),
                service.clipboard_outgoing.completed(),
            )
            .await
            .unwrap();
            service.handle_clipboard_completion(completed);
        }
        async fn applied(service: &mut Service) {
            let (event, result, revision) = tokio::time::timeout(
                Duration::from_secs(2),
                service.clipboard_writer.as_mut().unwrap().completed(),
            )
            .await
            .unwrap()
            .unwrap();
            service.handle_clipboard_applied(event, result, revision);
        }
        let runtime = std::env::var("LAN_MOUSE_SERVICE_TEST_RUNTIME_DIR").unwrap();
        assert_eq!(std::env::var("XDG_RUNTIME_DIR").unwrap(), runtime);
        assert!(std::path::Path::new(&runtime).starts_with(std::env::temp_dir()));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        let cert = directory.path().join("test.pem");
        std::fs::write(&path, "port = 0\nenable_clipboard = false\n").unwrap();
        let config = Config::new_with_args([
            "lan-mouse",
            "--config",
            path.to_str().unwrap(),
            "--cert-path",
            cert.to_str().unwrap(),
            "--capture-backend",
            "dummy",
            "--emulation-backend",
            "dummy",
        ])
        .unwrap();
        tokio::task::LocalSet::new().run_until(async move {
            let mut service = Service::new(config).await.unwrap();
            service.clipboard_enabled = true;
            service.conn_sender.set_clipboard_receiving(true);
            let (started, mut starts) = tokio::sync::mpsc::channel(4);
            let release = Arc::new(Notify::new());
            let gate = release.clone();
            service.clipboard_writer = Some(ClipboardWriter::with_apply(None, move |event| {
                let started = started.clone(); let gate = gate.clone();
                async move {
                    started.send(event.clone()).await.unwrap();
                    if event == text("first remote") { gate.notified().await; }
                    if event == text("refused remote") { return Err(input_emulation::clipboard::ClipboardError::Set("fixture refusal".into())); }
                    Ok(())
                }
            }));
            let peer_cert = Certificate::generate_self_signed(vec![]).unwrap();
            let peer_fingerprint = crypto::certificate_fingerprint(&peer_cert);
            let mut peer = LanMouseListener::new(0, peer_cert.clone(), Arc::new(RwLock::new(HashMap::from([(service.public_key_fingerprint.clone(), "service".into())])))).await.unwrap();
            let peer_port = peer.port();
            let (accept_tx, mut accepts) = tokio::sync::mpsc::channel(4);
            let (clip_tx, mut clips) = tokio::sync::mpsc::channel(8);
            let cancel = tokio_util::sync::CancellationToken::new();
            let peer_cancel = cancel.clone();
            let peer_task = tokio::task::spawn_local(async move {
                loop {
                    tokio::select! {
                        _ = peer_cancel.cancelled() => break,
                        event = peer.next() => match event {
                            Some(ListenEvent::Accept { conn, .. }) => accept_tx.send(conn).await.unwrap(),
                            Some(ListenEvent::Msg { event: ProtoEvent::Input(Event::Clipboard(event)), .. }) => clip_tx.send(event).await.unwrap(),
                            Some(ListenEvent::Msg { event: ProtoEvent::Ping, conn, .. }) => { let (bytes, count): ([u8; lan_mouse_proto::MAX_EVENT_SIZE], usize) = ProtoEvent::Pong(false).into(); conn.send(&bytes[..count]).await.unwrap(); },
                            None => break,
                            _ => {}
                        }
                    }
                }
                peer.terminate().await;
            });
            let handle = service.client_manager.add_client();
            service.client_manager.set_fix_ips(handle, vec![std::net::Ipv4Addr::LOCALHOST.into()]);
            service.client_manager.set_port(handle, peer_port);
            service.client_manager.activate_client(handle);
            local(&mut service, text("offline old"));
            local(&mut service, text("offline latest"));
            assert!(service.conn_sender.send(ProtoEvent::Ping, handle).await.is_err());
            let server_conn = tokio::time::timeout(Duration::from_secs(2), accepts.recv()).await.unwrap().unwrap();
            tokio::time::timeout(Duration::from_secs(2), service.clipboard_ready.notified()).await.unwrap();
            service.replay_clipboard();
            assert_eq!(tokio::time::timeout(Duration::from_secs(2), clips.recv()).await.unwrap().unwrap(), text("offline latest"));
            network_done(&mut service).await;
            assert!(!service.client_manager.alive(handle)); // clipboard does not require input emulation.
            for _ in 0..1000 { service.replay_clipboard(); }
            assert!(tokio::time::timeout(Duration::from_millis(20), clips.recv()).await.is_err());
            // A second, incoming route uses the same authenticated certificate.
            // No Enter is sent; established transport alone is sufficient.
            let incoming_route = incoming(&mut service, peer_cert.clone()).await;
            let mut no_data = [0u8; 32];
            assert!(tokio::time::timeout(Duration::from_millis(20), incoming_route.recv(&mut no_data)).await.is_err());
            send(&server_conn, text("first remote")).await;
            outgoing_receive(&mut service).await;
            assert!(service.clipboard_replay.snapshot().is_none());
            assert_eq!(starts.recv().await.unwrap(), text("first remote"));
            send(&server_conn, text("latest remote")).await;
            outgoing_receive(&mut service).await;
            release.notify_one();
            applied(&mut service).await;
            assert!(service.clipboard_replay.snapshot().is_none()); // first completion cannot commit newer intent.
            applied(&mut service).await;
            assert_eq!(*service.clipboard_replay.snapshot().unwrap().event, text("latest remote"));
            assert_eq!(service.clipboard_replay.snapshot().unwrap().origin.as_deref(), Some(peer_fingerprint.as_str()));
            assert!(tokio::time::timeout(Duration::from_millis(20), clips.recv()).await.is_err());
            assert!(tokio::time::timeout(Duration::from_millis(20), incoming_route.recv(&mut no_data)).await.is_err()); // no echo on either route.
            send(&server_conn, text("refused remote")).await;
            outgoing_receive(&mut service).await;
            applied(&mut service).await;
            assert!(service.clipboard_replay.snapshot().is_none());
            let image = ClipboardEvent::Image(input_event::encode_image_rgba(1, 1, &[255, 0, 0, 255]).unwrap());
            send(&server_conn, image.clone()).await;
            outgoing_receive(&mut service).await;
            applied(&mut service).await;
            assert_eq!(*service.clipboard_replay.snapshot().unwrap().event, image);
            send(&incoming_route, image.clone()).await;
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let event = service.emulation.event().await;
                    let received = matches!(event, EmulationEvent::ClipboardReceived { .. });
                    service.handle_emulation_event(event).await;
                    if received { break; }
                }
            }).await.unwrap();
            applied(&mut service).await;
            assert_eq!(*service.clipboard_replay.snapshot().unwrap().event, image);
            let old_revision = service.client_manager.target_revision(handle).unwrap();
            let old_client_conn = service.conn_sender.clipboard_current(handle).unwrap().0;
            service.client_manager.invalidate_target(handle);
            local(&mut service, text("offline after disconnect"));
            service.handle_outgoing_clipboard(crate::connect::ReceivedEvent { handle, revision: old_revision, addr: (std::net::Ipv4Addr::LOCALHOST, peer_port).into(), conn: old_client_conn, event: ProtoEvent::Input(Event::Clipboard(text("stale queued remote"))) });
            assert_eq!(*service.clipboard_replay.snapshot().unwrap().event, text("offline after disconnect"));
            // Incoming-only fallback received the fresh local snapshot even with no Enter.
            assert_eq!(receive(&incoming_route).await, text("offline after disconnect"));
            local(&mut service, image.clone());
            assert_eq!(receive(&incoming_route).await, image);
            local(&mut service, text("offline after disconnect"));
            assert_eq!(receive(&incoming_route).await, text("offline after disconnect"));
            assert!(service.conn_sender.send(ProtoEvent::Ping, handle).await.is_err());
            let _replacement = tokio::time::timeout(Duration::from_secs(2), accepts.recv()).await.unwrap().unwrap();
            tokio::time::timeout(Duration::from_secs(2), service.clipboard_ready.notified()).await.unwrap();
            service.replay_clipboard();
            assert_eq!(tokio::time::timeout(Duration::from_secs(2), clips.recv()).await.unwrap().unwrap(), text("offline after disconnect"));
            network_done(&mut service).await;
            local(&mut service, image.clone());
            assert_eq!(tokio::time::timeout(Duration::from_secs(2), clips.recv()).await.unwrap().unwrap(), image);
            let completed = service.clipboard_outgoing.completed().await;
            service.handle_clipboard_completion(completed);
            // Oversize content must not leave a previously valid replay value.
            local(&mut service, ClipboardEvent::Text("x".repeat(lan_mouse_proto::MAX_CLIPBOARD_SIZE + 1)));
            assert!(service.clipboard_replay.snapshot().is_none());
            service.apply_clipboard_enabled(false);
            assert!(service.clipboard_replay.snapshot().is_none());
            let independent = incoming(&mut service, Certificate::generate_self_signed(vec![]).unwrap()).await;
            assert!(tokio::time::timeout(Duration::from_millis(20), independent.recv(&mut no_data)).await.is_err());
            // Re-enable is modeled without creating/reading the user's OS clipboard.
            service.clipboard_enabled = true;
            service.conn_sender.set_clipboard_receiving(true);
            assert!(service.clipboard_replay.snapshot().is_none());
            local(&mut service, text("fresh after enable"));
            assert_eq!(receive(&independent).await, text("fresh after enable"));
            // Occupy the real 32-request ingress before the worker is polled.
            let missing: SocketAddr = "127.0.0.1:1".parse().unwrap();
            for _ in 0..32 { service.emulation.send_clipboard(missing, text("filler")).unwrap(); }
            local(&mut service, text("busy older"));
            local(&mut service, text("busy latest"));
            assert!(service.clipboard_retry);
            tokio::time::timeout(Duration::from_secs(2), async {
                while service.clipboard_retry {
                    tokio::task::yield_now().await;
                    service.replay_clipboard();
                }
            }).await.unwrap();
            assert_eq!(receive(&independent).await, text("busy latest")); // retry requires no new copy and uses only the latest snapshot.
            independent.close().await.unwrap(); incoming_route.close().await.unwrap();
            service.capture.terminate().await;
            service.emulation.terminate().await;
            service.conn_sender.terminate().await;
            service.hooks.terminate().await;
            service.resolver.terminate().await;
            cancel.cancel(); peer_task.await.unwrap();
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires isolated LAN_MOUSE_SERVICE_TEST_RUNTIME_DIR / XDG_RUNTIME_DIR"]
    async fn pending_capture_cleanup_disables_status_and_forwards_progress_then_failure() {
        let runtime = std::env::var("LAN_MOUSE_SERVICE_TEST_RUNTIME_DIR").unwrap();
        assert_eq!(std::env::var("XDG_RUNTIME_DIR").unwrap(), runtime);
        assert!(std::path::Path::new(&runtime).starts_with(std::env::temp_dir()));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        let cert = directory.path().join("test.pem");
        std::fs::write(&path, "port = 0\nenable_clipboard = false\n").unwrap();
        let config = Config::new_with_args([
            "lan-mouse",
            "--config",
            path.to_str().unwrap(),
            "--cert-path",
            cert.to_str().unwrap(),
            "--capture-backend",
            "dummy",
            "--emulation-backend",
            "dummy",
        ])
        .unwrap();
        tokio::task::LocalSet::new().run_until(async move {
            let mut service = Service::new(config).await.unwrap();
            service.handle_capture_event(ICaptureEvent::CaptureEnabled);
            service.pending_frontend_events.clear();
            service.handle_capture_event(ICaptureEvent::CaptureCleanupPending("activation stream closed unexpectedly".into()));
            assert!(matches!(service.capture_status, Status::Disabled));
            assert_eq!(service.pending_frontend_events.len(), 2);
            assert!(matches!(&service.pending_frontend_events[0], FrontendEvent::CaptureStatus(Status::Disabled)));
            assert!(matches!(&service.pending_frontend_events[1], FrontendEvent::Error(message) if
                message.contains("cleanup is still pending") && message.contains("activation stream")));
            service.pending_frontend_events.clear();
            service.handle_capture_event(ICaptureEvent::CaptureFailed("activation stream closed unexpectedly; backend termination also failed: cleanup failed".into()));
            assert_eq!(service.pending_frontend_events.len(), 1);
            assert!(matches!(&service.pending_frontend_events[0], FrontendEvent::Error(message) if
                message.contains("Input capture stopped") && message.contains("cleanup failed")));
            service.capture.terminate().await;
            service.emulation.terminate().await;
            service.conn_sender.terminate().await;
            service.hooks.terminate().await;
            service.resolver.terminate().await;
        }).await;
    }

    // This constructs the real service. Explicit isolation is required so the
    // fixture never binds to the user's frontend socket or uses input hardware.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires isolated LAN_MOUSE_SERVICE_TEST_RUNTIME_DIR / XDG_RUNTIME_DIR"]
    async fn external_reload_preserves_file_and_applies_authorization_and_clipboard() {
        async fn handshake(
            port: u16,
            cert: webrtc_dtls::crypto::Certificate,
        ) -> webrtc_dtls::conn::DTLSConn {
            let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
            socket
                .connect((std::net::Ipv4Addr::LOCALHOST, port))
                .await
                .unwrap();
            tokio::time::timeout(
                Duration::from_secs(2),
                webrtc_dtls::conn::DTLSConn::new(
                    socket,
                    webrtc_dtls::config::Config {
                        certificates: vec![cert],
                        insecure_skip_verify: true,
                        extended_master_secret:
                            webrtc_dtls::config::ExtendedMasterSecretType::Require,
                        ..Default::default()
                    },
                    true,
                    None,
                ),
            )
            .await
            .unwrap()
            .unwrap()
        }
        let runtime = std::env::var("LAN_MOUSE_SERVICE_TEST_RUNTIME_DIR").unwrap();
        assert_eq!(std::env::var("XDG_RUNTIME_DIR").unwrap(), runtime);
        assert!(std::path::Path::new(&runtime).starts_with(std::env::temp_dir()));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        let cert = directory.path().join("test.pem");
        let startup_cert = webrtc_dtls::crypto::Certificate::generate_self_signed(vec![]).unwrap();
        let startup_fp = crypto::certificate_fingerprint(&startup_cert);
        let startup_alias = startup_fp.replace(':', "").to_uppercase();
        std::fs::write(&path, format!("port = 0\nenable_clipboard = false\n[authorized_fingerprints]\nold = 'old-peer'\n\"{startup_alias}\" = 'alias description'\n\"{startup_fp}\" = 'preferred description'\n")).unwrap();
        let config = Config::new_with_args([
            "lan-mouse",
            "--config",
            path.to_str().unwrap(),
            "--cert-path",
            cert.to_str().unwrap(),
            "--capture-backend",
            "dummy",
            "--emulation-backend",
            "dummy",
        ])
        .unwrap();
        tokio::task::LocalSet::new().run_until(async move {
            let mut service = Service::new(config).await.unwrap();
            let initial_port = service.port;
            assert_ne!(initial_port, 0);
            assert_eq!(service.config.port(), 0);
            service.sync_frontend();
            assert!(service.pending_frontend_events.iter().any(|event| matches!(event,
                FrontendEvent::PortChanged(port, None) if *port == initial_port
            )));
            assert_eq!(*service.authorized_keys.read().unwrap(), HashMap::from([(startup_fp.clone(), "preferred description".into())]));
            assert!(service.pending_frontend_events.iter().any(|event| matches!(event, FrontendEvent::Error(message) if message.contains("1 malformed") && message.contains("1 alias"))));
            let accepted = handshake(service.port, startup_cert).await;
            webrtc_util::Conn::close(&accepted).await.unwrap();
            let original_raw = service.config.authorized_fingerprints();
            service.update_mouse_sensitivity(1.15);
            service.config.flush().await.unwrap();
            let settings_on_disk = std::fs::read_to_string(&path).unwrap();
            for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                service.update_mouse_sensitivity(invalid);
                assert_eq!(service.config.mouse_sensitivity(), 1.15);
                assert!(service.pending_frontend_events.iter().any(|event| matches!(event,
                    FrontendEvent::Error(message) if message.contains("sensitivity must be finite")
                )));
                assert!(service.pending_frontend_events.iter().any(|event| matches!(event,
                    FrontendEvent::Settings { mouse_sensitivity, .. } if *mouse_sensitivity == 1.15
                )));
                service.config.flush().await.unwrap();
                assert_eq!(std::fs::read_to_string(&path).unwrap(), settings_on_disk);
            }
            let persisted = Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
            assert_eq!(persisted.authorized_fingerprints(), original_raw); // no implicit authorization migration.
            // Invalid raw IPC requests cannot change trust or persist a bad key.
            service.pending_frontend_events.clear();
            let before = std::fs::read(&path).unwrap();
            let old_keys = service.authorized_keys.read().unwrap().clone();
            for invalid in ["", "bad", &"A".repeat(63), &"gg".repeat(32)] {
                service.handle_frontend_request(Some(Ok(FrontendRequest::AuthorizeKey("bad input".into(), invalid.into()))));
            }
            service.config.flush().await.unwrap();
            assert_eq!(*service.authorized_keys.read().unwrap(), old_keys);
            assert_eq!(std::fs::read(&path).unwrap(), before);
            assert!(service.pending_frontend_events.iter().all(|event| matches!(event, FrontendEvent::Error(_))));
            assert_eq!(service.pending_frontend_events.len(), 4);
            let canonical = service.public_key_fingerprint.clone();
            service.handle_frontend_request(Some(Ok(FrontendRequest::AuthorizeKey("valid peer".into(), canonical.replace(':', "").to_uppercase()))));
            service.config.flush().await.unwrap();
            assert_eq!(service.authorized_keys.read().unwrap().get(&canonical).unwrap(), "valid peer");
            let persisted = Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
            assert_eq!(persisted.authorized_fingerprints().get(&canonical).unwrap(), "valid peer");
            service.handle_frontend_request(Some(Ok(FrontendRequest::RemoveAuthorizedKey(canonical.to_uppercase()))));
            assert!(!service.authorized_keys.read().unwrap().contains_key(&canonical));
            let legacy = canonical.to_uppercase();
            let mut raw = service.config.authorized_fingerprints();
            raw.insert(legacy.clone(), "legacy uppercase".into());
            service.config.set_authorized_keys(raw);
            service.reload_authorized_keys();
            assert!(service.authorized_keys.read().unwrap().contains_key(&canonical));
            service.handle_frontend_request(Some(Ok(FrontendRequest::RemoveAuthorizedKey(legacy.clone()))));
            assert!(!service.authorized_keys.read().unwrap().contains_key(&canonical));
            assert!(!service.config.authorized_fingerprints().contains_key(&legacy));
            service.handle_frontend_request(Some(Ok(FrontendRequest::RemoveAuthorizedKey("old".into()))));
            assert!(!service.authorized_keys.read().unwrap().contains_key("old"));
            service.config.flush().await.unwrap();
            service.pending_frontend_events.clear();
            // Model an already enabled session without starting OS clipboard
            // resources: the external snapshot must disable it on reload.
            service.clipboard_enabled = true;
            let reload_cert = webrtc_dtls::crypto::Certificate::generate_self_signed(vec![]).unwrap();
            let reload_fp = crypto::certificate_fingerprint(&reload_cert);
            let reload_alias = reload_fp.replace(':', "").to_uppercase();
            let external = format!("# external edit must survive reload\nport = 0\nenable_clipboard = false\n[authorized_fingerprints]\nnew = 'invalid-key'\n\"{reload_alias}\" = 'new-peer'\n[input_post_processing]\ninvert_scroll = true\nmouse_sensitivity = 1.75\n");
            std::fs::write(&path, &external).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while !service.config.changed().await.unwrap() {}
            }).await.unwrap();
            service.handle_config_change();
            // Reloading unrelated settings must not request a new ephemeral port.
            assert_eq!(service.emulation.last_port_request(), None);
            assert_eq!(service.port, initial_port);
            assert_eq!(service.configured_port, 0);
            assert_eq!(std::fs::read_to_string(&path).unwrap(), external);
            assert_eq!(*service.authorized_keys.read().unwrap(), HashMap::from([(reload_fp.clone(), "new-peer".into())]));
            assert!(!service.authorized_keys.read().unwrap().contains_key(&startup_fp));
            let accepted = handshake(service.port, reload_cert.clone()).await;
            let (old_addr, old_server) = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let event = service.emulation.event().await;
                    let current = match &event {
                        EmulationEvent::Connected { addr, fingerprint, conn, .. } if fingerprint == &reload_fp => Some((*addr, conn.clone())),
                        _ => None,
                    };
                    service.handle_emulation_event(event).await;
                    if let Some(current) = current { break current; }
                }
            }).await.unwrap();
            let (bytes, len): ([u8; lan_mouse_proto::MAX_EVENT_SIZE], usize) = lan_mouse_proto::ProtoEvent::Enter(lan_mouse_proto::Position::Left, 0.5).into();
            webrtc_util::Conn::send(&accepted, &bytes[..len]).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let event = service.emulation.event().await;
                    let entered = matches!(&event, EmulationEvent::Entered { addr, .. } if *addr == old_addr);
                    service.handle_emulation_event(event).await;
                    if entered { break; }
                }
            }).await.unwrap();
            assert!(service.incoming_conns.contains(&old_addr));
            // A delayed timeout belonging to a different connection at this
            // address cannot remove the current return edge.
            let (mut obsolete_listener, obsolete_conn) = crate::listen::control_test_listener(old_addr);
            let (timeout, timeout_slots) = crate::emulation::timeout_lease_for_test(&obsolete_conn);
            let notices_before = service.pending_frontend_events.len();
            service.handle_emulation_event(EmulationEvent::Disconnected { addr: old_addr, conn: obsolete_conn, timeout }).await;
            assert!(service.incoming_conns.contains(&old_addr));
            assert_eq!(service.pending_frontend_events.len(), notices_before);
            assert_eq!(timeout_slots.get(), 0);
            obsolete_listener.terminate().await;
            let (timeout, timeout_slots) = crate::emulation::timeout_lease_for_test(&old_server);
            service.handle_emulation_event(EmulationEvent::Disconnected { addr: old_addr, conn: old_server.clone(), timeout }).await;
            assert!(!service.incoming_conns.contains(&old_addr));
            assert_eq!(timeout_slots.get(), 0);
            service.handle_emulation_event(EmulationEvent::Entered { input: None, control: None, addr: old_addr, pos: lan_mouse_ipc::Position::Left, fingerprint: reload_fp.clone(), conn: old_server.clone() }).await;
            assert!(service.incoming_conns.contains(&old_addr));
            let old_token = service.incoming_authorization.token(old_addr, &old_server).unwrap();
            let mut reply = [0u8; lan_mouse_proto::MAX_EVENT_SIZE];
            let count = tokio::time::timeout(Duration::from_secs(2), webrtc_util::Conn::recv(&accepted, &mut reply)).await.unwrap().unwrap();
            assert!(matches!(lan_mouse_proto::decode_event_frame(&reply[..count]).unwrap(), lan_mouse_proto::ProtoEvent::Ack(_)));
            assert!(!service.clipboard_enabled);
            assert!(service.pending_frontend_events.iter().any(|event| matches!(event,
                FrontendEvent::Settings { clipboard_enabled: false, invert_scroll: true, mouse_sensitivity } if *mouse_sensitivity == 1.75
            )));
            service.update_mouse_sensitivity(2.25);
            service.config.flush().await.unwrap();
            let persisted = Config::new_with_args([
                "lan-mouse", "--config", path.to_str().unwrap(),
            ]).unwrap();
            assert_eq!(persisted.mouse_sensitivity(), 2.25);
            assert!(!persisted.clipboard_enabled());
            assert_eq!(persisted.authorized_fingerprints(), HashMap::from([("new".into(), "invalid-key".into()), (reload_alias.clone(), "new-peer".into())]));
            // Queue a network clipboard write but revoke before its worker runs.
            service.emulation.send_clipboard(old_addr, input_event::ClipboardEvent::Text("must not share after revocation".into())).unwrap();
            service.handle_frontend_request(Some(Ok(FrontendRequest::RemoveAuthorizedKey(reload_fp.clone()))));
            assert!(old_token.is_cancelled());
            assert!(!service.emulation.clipboard_session_is_current(old_addr, &old_server));
            assert!(!service.incoming_conns.contains(&old_addr));
            assert!(!service.incoming_clipboard.contains_key(&old_addr));
            service.pending_frontend_events.clear();
            let stale_budget = crate::input_budget::InputBudget::with_limits(1, 1, Duration::from_millis(50));
            let mut stale_lease = stale_budget.acquire(&tokio_util::sync::CancellationToken::new()).await.unwrap();
            let stale_admission = stale_lease.share_admission();
            drop(stale_lease);
            service.handle_emulation_event(EmulationEvent::Entered { input: Some(stale_admission.clone()), control: None, addr: old_addr, pos: lan_mouse_ipc::Position::Left, fingerprint: reload_fp.clone(), conn: old_server.clone() }).await;
            assert_eq!(stale_budget.available(), (0, 0));
            let (stale_generation, generation_slots) = crate::listen::reader_slot_for_test();
            service.handle_emulation_event(EmulationEvent::Connected { admission: Some(stale_generation), addr: old_addr, fingerprint: reload_fp.clone(), conn: old_server.clone() }).await;
            assert_eq!(generation_slots.get(), 0, "rejecting stale Connected must release generation ownership");
            service.handle_emulation_event(EmulationEvent::ClipboardReceived { control: None, addr: old_addr, conn: old_server.clone(), event: input_event::ClipboardEvent::Text("old receipt".into()) }).await;
            service.handle_emulation_event(EmulationEvent::ReleaseNotify { input: Some(stale_admission), addr: old_addr, conn: old_server.clone() }).await;
            assert_eq!(stale_budget.available(), (1, 1), "rejecting both stale notices must return admission");
            assert!(service.pending_frontend_events.is_empty());
            let (report_generation, report_slots) = crate::listen::reader_slot_for_test();
            let report_budget = crate::input_budget::InputBudget::with_limits(1, 1, Duration::from_millis(50));
            let mut report_lease = report_budget.acquire(&tokio_util::sync::CancellationToken::new()).await.unwrap();
            let report_input = report_lease.share_admission();
            drop(report_lease);
            service.handle_emulation_event(EmulationEvent::InputRejected { addr: old_addr, reason: "invalid fixture".into(), admission: Some(report_generation.clone()) }).await;
            assert_eq!(report_slots.get(), 1);
            service.handle_emulation_event(EmulationEvent::InputOverloaded { addr: old_addr, admission: Some(report_generation), input: Some(report_input.clone()) }).await;
            assert_eq!(report_slots.get(), 0);
            assert_eq!(report_budget.available(), (0, 0));
            service.handle_emulation_event(EmulationEvent::InputCleanupFailed { addr: old_addr, input: Some(report_input) }).await;
            assert_eq!(report_budget.available(), (1, 1));
            assert_eq!(service.pending_frontend_events.iter().filter(|event| matches!(event, FrontendEvent::Error(_))).count(), 3);
            service.pending_frontend_events.clear();
            assert!(!service.incoming_conns.contains(&old_addr));
            service.config.flush().await.unwrap();
            let result = tokio::time::timeout(Duration::from_millis(300), webrtc_util::Conn::recv(&accepted, &mut reply)).await;
            if let Ok(Ok(count)) = result {
                if count > 0 { assert!(!matches!(lan_mouse_proto::decode_event_frame(&reply[..count]).unwrap(), lan_mouse_proto::ProtoEvent::Input(input_event::Event::Clipboard(_)))); }
            }
            service.reload_authorized_keys();
            assert!(service.authorized_keys.read().unwrap().is_empty());
            assert!(!service.config.authorized_fingerprints().contains_key(&reload_alias));
            assert!(service.config.authorized_fingerprints().contains_key("new"));
            let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
            socket.connect((std::net::Ipv4Addr::LOCALHOST, service.port)).await.unwrap();
            let denied_cert = reload_cert.clone();
            let revoked = tokio::task::spawn_local(async move {
                webrtc_dtls::conn::DTLSConn::new(socket, webrtc_dtls::config::Config {
                    certificates: vec![denied_cert], insecure_skip_verify: true,
                    extended_master_secret: webrtc_dtls::config::ExtendedMasterSecretType::Require, ..Default::default()
                }, true, None).await
            });
            let notice = tokio::time::timeout(Duration::from_secs(2), service.authentication_notices.next()).await.unwrap();
            assert_eq!(notice, reload_fp); // same certificate is now rejected, alias cannot restore trust.
            revoked.abort(); let _ = revoked.await;
            service.handle_frontend_request(Some(Ok(FrontendRequest::AuthorizeKey("regranted peer".into(), reload_alias.clone()))));
            assert!(old_token.is_cancelled());
            let fresh = handshake(service.port, reload_cert).await;
            assert!(!service.emulation.clipboard_session_is_current(old_addr, &old_server));
            service.handle_emulation_event(EmulationEvent::Entered { input: None, control: None, addr: old_addr, pos: lan_mouse_ipc::Position::Left, fingerprint: reload_fp, conn: old_server }).await;
            assert!(!service.incoming_conns.contains(&old_addr));
            webrtc_util::Conn::close(&fresh).await.unwrap();
            webrtc_util::Conn::close(&accepted).await.unwrap();
            service.clipboard_enabled = true;
            let clipboard = input_event::ClipboardEvent::Text("fixture clipboard".into());
            let missing_addr = "127.0.0.1:1".parse().unwrap();
            service.emulation.send_clipboard(missing_addr, clipboard.clone()).unwrap();
            let completed = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let event = service.emulation.event().await;
                    if matches!(&event, EmulationEvent::ClipboardSendCompleted(completed) if completed.addr == missing_addr) {
                        break event;
                    }
                    service.handle_emulation_event(event).await;
                }
            }).await.unwrap();
            assert!(matches!(&completed, EmulationEvent::ClipboardSendCompleted(completed)
                if completed.addr == missing_addr && matches!(completed.result, Err(crate::listen::ClipboardSendError::NotConnected))));
            service.handle_emulation_event(completed).await;
            assert!(!service.pending_frontend_events.iter().any(|event| matches!(event,
                FrontendEvent::ClipboardShared { .. }
            )));
            assert!(service.pending_frontend_events.iter().any(|event| matches!(event,
                FrontendEvent::Error(message) if message.contains("Failed to send clipboard")
            )));
            service.handle_emulation_event(EmulationEvent::ClipboardSendCompleted(crate::clipboard_network::ClipboardCompletion {
                addr: missing_addr, kind: clipboard.kind(), bytes: clipboard.content_len(),
                generation: service.emulation.clipboard_generation(), outgoing: None, conn: None, result: Ok(()),
            })).await;
            assert!(service.pending_frontend_events.iter().any(|event| matches!(event,
                FrontendEvent::ClipboardShared { .. }
            )));
            service.pending_frontend_events.clear();
            service.apply_clipboard_enabled(false);
            service.handle_emulation_event(EmulationEvent::ClipboardSendCompleted(crate::clipboard_network::ClipboardCompletion {
                addr: missing_addr, kind: clipboard.kind(), bytes: clipboard.content_len(),
                generation: service.emulation.clipboard_generation(), outgoing: None, conn: None, result: Ok(()),
            })).await;
            assert!(service.pending_frontend_events.is_empty());
            let oversized = input_event::ClipboardEvent::Image(vec![0; lan_mouse_proto::MAX_CLIPBOARD_SIZE + 1]);
            service.clipboard_enabled = true;
            service.handle_clipboard_event(Some(input_capture::CaptureEvent::Input(input_event::Event::Clipboard(oversized))));
            assert!(service.pending_frontend_events.iter().any(|event| matches!(event,
                FrontendEvent::ClipboardTooLarge { bytes, .. } if *bytes == lan_mouse_proto::MAX_CLIPBOARD_SIZE + 1
            )));
            assert_eq!(service.clipboard_outgoing.sizes(), (0, 0));
            service.pending_frontend_events.clear();
            let old_generation = service.emulation.clipboard_generation();
            for _ in 0..32 {
                service.emulation.send_clipboard(missing_addr, clipboard.clone()).unwrap();
            }
            assert!(service.emulation.send_clipboard(missing_addr, clipboard.clone()).is_err());
            service.emulation.clear_clipboard();
            service.clipboard_enabled = true;
            service.handle_emulation_event(EmulationEvent::ClipboardSendCompleted(crate::clipboard_network::ClipboardCompletion {
                addr: missing_addr, kind: clipboard.kind(), bytes: clipboard.content_len(),
                generation: old_generation, outgoing: None, conn: None, result: Ok(()),
            })).await;
            assert!(service.pending_frontend_events.is_empty());
            service.change_port(0);
            let changed = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let event = service.emulation.event().await;
                    if matches!(event, EmulationEvent::PortChanged(_)) { break event; }
                }
            }).await.unwrap();
            service.handle_emulation_event(changed).await;
            assert_ne!(service.port, 0);
            assert_ne!(service.port, initial_port);
            assert_eq!(service.config.port(), 0);
            assert!(service.pending_frontend_events.iter().any(|event| matches!(event,
                FrontendEvent::PortChanged(port, None) if *port == service.port
            )));
            // Real unauthorized DTLS handshake -> verifier notice -> Service prompt.
            // The notice path is independent of ListenTask's input/error queues.
            service.pending_frontend_events.clear();
            let peer_cert = webrtc_dtls::crypto::Certificate::generate_self_signed(vec![]).unwrap();
            let peer_fingerprint = crypto::certificate_fingerprint(&peer_cert);
            let retry_cert = peer_cert.clone();
            let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
            socket.connect((std::net::Ipv4Addr::LOCALHOST, service.port)).await.unwrap();
            let attempt = tokio::task::spawn_local(async move {
                tokio::time::timeout(Duration::from_secs(2), webrtc_dtls::conn::DTLSConn::new(socket, webrtc_dtls::config::Config {
                    certificates: vec![peer_cert],
                    insecure_skip_verify: true,
                    extended_master_secret: webrtc_dtls::config::ExtendedMasterSecretType::Require,
                    ..Default::default()
                }, true, None)).await
            });
            let fingerprint = tokio::time::timeout(Duration::from_secs(2), service.authentication_notices.next()).await.unwrap();
            assert_eq!(fingerprint, peer_fingerprint);
            service.handle_authentication_attempt(fingerprint.clone());
            assert!(service.pending_frontend_events.iter().any(|event| matches!(event,
                FrontendEvent::ConnectionAttempt { fingerprint: key } if key == &fingerprint
            )));
            service.pending_frontend_events.clear();
            for _ in 0..1000 { service.authentication_notices.record(fingerprint.clone()); }
            assert!(tokio::time::timeout(Duration::from_millis(10), service.authentication_notices.next()).await.is_err());
            service.handle_frontend_request(Some(Ok(FrontendRequest::AuthorizeKey("accepted peer".into(), fingerprint.replace(':', "").to_uppercase()))));
            assert_eq!(service.authorized_keys.read().unwrap().get(&fingerprint).unwrap(), "accepted peer");
            service.pending_frontend_events.clear();
            service.handle_authentication_attempt(fingerprint);
            assert!(service.pending_frontend_events.is_empty());
            attempt.abort();
            let _ = attempt.await;
            let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
            socket.connect((std::net::Ipv4Addr::LOCALHOST, service.port)).await.unwrap();
            let accepted = tokio::time::timeout(Duration::from_secs(3), webrtc_dtls::conn::DTLSConn::new(socket, webrtc_dtls::config::Config {
                certificates: vec![retry_cert], insecure_skip_verify: true,
                extended_master_secret: webrtc_dtls::config::ExtendedMasterSecretType::Require, ..Default::default()
            }, true, None)).await.unwrap().unwrap();
            webrtc_util::Conn::close(&accepted).await.unwrap();
            service.config.flush().await.unwrap();
            service.capture.terminate().await;
            service.emulation.terminate().await;
            service.conn_sender.terminate().await;
            service.hooks.terminate().await;
            service.resolver.terminate().await;
        }).await;
    }
}
