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
    time::Duration,
};
use thiserror::Error;
use tokio::{signal, sync::Notify};

#[derive(Debug, Error)]
pub enum ServiceError {
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
    /// clipboard enabled
    clipboard_enabled: bool,
    /// dns resolver
    resolver: DnsResolver,
    /// frontend listener
    frontend_listener: AsyncFrontendListener,
    /// authorized public key sha256 fingerprints
    authorized_keys: Arc<RwLock<HashMap<String, String>>>,
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

        let authorized_keys = Arc::new(RwLock::new(config.authorized_fingerprints()));
        // listener + connection
        let listener =
            LanMouseListener::new(config.port(), cert.clone(), authorized_keys.clone()).await?;
        let port = listener.port();
        let configured_port = config.port();
        let authentication_notices = listener.authentication_notices();
        let conn = LanMouseConnection::new(cert.clone(), client_manager.clone());
        let conn_sender = conn.sender();

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
            authentication_notices,
            clipboard_enabled,
            frontend_listener,
            resolver,
            authorized_keys,
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

        loop {
            tokio::select! {
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
                    if let Some((event, result)) = result {
                        match result {
                            Ok(()) if self.clipboard_enabled => self.notify_clipboard_shared(&event, true),
                            Ok(()) => {},
                            Err(e) => { log::warn!("Failed to apply remote clipboard: {e}");
                                self.notify_frontend(FrontendEvent::Error(format!("Failed to apply clipboard: {e}"))); }
                        }
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
        self.emulation.terminate().await;
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
                self.add_authorized_key(desc, fp);
                self.save_config();
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
        let authorized_keys = self.authorized_keys.read().expect("lock").clone();
        self.config.set_authorized_keys(authorized_keys);
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
        let authorized_keys = self.config.authorized_fingerprints();
        self.authorized_keys
            .write()
            .unwrap()
            .clone_from(&authorized_keys);
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
            } => {
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
            EmulationEvent::ConnectionClosed { addr } => {
                self.remove_incoming(addr);
                self.notify_frontend(FrontendEvent::IncomingDisconnected(addr));
            }
            EmulationEvent::Disconnected { addr } => {
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
            EmulationEvent::EmulationDisabled => {
                self.emulation_status = Status::Disabled;
                self.notify_frontend(FrontendEvent::EmulationStatus(self.emulation_status));
            }
            EmulationEvent::EmulationEnabled => {
                self.emulation_status = Status::Enabled;
                self.notify_frontend(FrontendEvent::EmulationStatus(self.emulation_status));
            }
            EmulationEvent::ReleaseNotify => self.capture.release(),
            EmulationEvent::Connected { addr, fingerprint } => {
                self.notify_frontend(FrontendEvent::DeviceConnected { addr, fingerprint });
            }
            EmulationEvent::PeerHello { addr, commit } => {
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
            EmulationEvent::ClipboardReceived(event) => self.receive_clipboard(event),
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
            ICaptureEvent::ClipboardReceived(event) => self.receive_clipboard(event),
        }
    }

    fn receive_clipboard(&self, event: input_event::ClipboardEvent) {
        if self.clipboard_enabled {
            if let Some(writer) = &self.clipboard_writer {
                writer.submit(event);
            }
        }
    }

    fn handle_clipboard_event(&mut self, event: Option<input_capture::CaptureEvent>) {
        if !self.clipboard_enabled {
            return;
        }
        use input_capture::CaptureEvent;
        use input_event::Event;

        if let Some(CaptureEvent::Input(Event::Clipboard(clipboard_event))) = event {
            // The protocol limit is payload bytes. Check before cloning or
            // encoding a potentially huge image; encoding belongs in the job.
            let bytes = clipboard_event.content_len();
            if bytes > lan_mouse_proto::MAX_CLIPBOARD_SIZE {
                self.notify_frontend(FrontendEvent::ClipboardTooLarge {
                    bytes,
                    limit: lan_mouse_proto::MAX_CLIPBOARD_SIZE,
                });
                return;
            }

            log::info!("Clipboard changed locally, sending to all connected peers");

            // Send clipboard to all active clients (machines we're controlling)
            let active_clients: Vec<_> = self.client_manager.active_clients().into_iter().collect();

            for handle in active_clients {
                let (generation, cancellation) = self.emulation.clipboard_scope();
                match self.conn_sender.prepare_clipboard(
                    clipboard_event.clone(),
                    handle,
                    generation,
                    cancellation,
                ) {
                    Ok(request) => {
                        if self.clipboard_outgoing.submit(request).is_err() {
                            self.notify_frontend(FrontendEvent::Error(
                                "Clipboard send is busy; copy again".into(),
                            ));
                        }
                    }
                    Err(crate::connect::LanMouseConnectionError::ClipboardBusy) => {
                        self.notify_frontend(FrontendEvent::Error(
                            "Clipboard send is busy; copy again".into(),
                        ));
                    }
                    Err(error) => log::debug!("clipboard target {handle} unavailable: {error}"),
                }
            }

            // Also send clipboard to all incoming connections (machines controlling us)
            let incoming_addrs: Vec<_> = self
                .incoming_conn_info
                .values()
                .map(|incoming| incoming.addr)
                .collect();

            for addr in incoming_addrs {
                log::info!("Sending clipboard to incoming connection {}", addr);
                if let Err(error) = self.emulation.send_clipboard(addr, clipboard_event.clone()) {
                    self.notify_frontend(FrontendEvent::Error(error.into()));
                }
            }
        }
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
        let keys = self.authorized_keys.read().expect("lock").clone();
        self.notify_frontend(FrontendEvent::AuthorizedUpdated(keys));
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

    fn add_authorized_key(&mut self, desc: String, fp: String) {
        self.authorized_keys.write().expect("lock").insert(fp, desc);
        let keys = self.authorized_keys.read().expect("lock").clone();
        self.notify_frontend(FrontendEvent::AuthorizedUpdated(keys));
    }

    fn remove_authorized_key(&mut self, fp: String) {
        self.authorized_keys.write().expect("lock").remove(&fp);
        let keys = self.authorized_keys.read().expect("lock").clone();
        self.notify_frontend(FrontendEvent::AuthorizedUpdated(keys));
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

    // This constructs the real service. Explicit isolation is required so the
    // fixture never binds to the user's frontend socket or uses input hardware.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires isolated LAN_MOUSE_SERVICE_TEST_RUNTIME_DIR / XDG_RUNTIME_DIR"]
    async fn external_reload_preserves_file_and_applies_authorization_and_clipboard() {
        let runtime = std::env::var("LAN_MOUSE_SERVICE_TEST_RUNTIME_DIR").unwrap();
        assert_eq!(std::env::var("XDG_RUNTIME_DIR").unwrap(), runtime);
        assert!(std::path::Path::new(&runtime).starts_with(std::env::temp_dir()));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        let cert = directory.path().join("test.pem");
        std::fs::write(
            &path,
            "port = 0\nenable_clipboard = false\n[authorized_fingerprints]\nold = 'old-peer'\n",
        )
        .unwrap();
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
            // Model an already enabled session without starting OS clipboard
            // resources: the external snapshot must disable it on reload.
            service.clipboard_enabled = true;
            let external = "# external edit must survive reload\nport = 0\nenable_clipboard = false\n[authorized_fingerprints]\nnew = 'new-peer'\n[input_post_processing]\ninvert_scroll = true\nmouse_sensitivity = 1.75\n";
            std::fs::write(&path, external).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while !service.config.changed().await.unwrap() {}
            }).await.unwrap();
            service.handle_config_change();
            // Reloading unrelated settings must not request a new ephemeral port.
            assert_eq!(service.emulation.last_port_request(), None);
            assert_eq!(service.port, initial_port);
            assert_eq!(service.configured_port, 0);
            assert_eq!(std::fs::read_to_string(&path).unwrap(), external);
            assert_eq!(*service.authorized_keys.read().unwrap(), HashMap::from([("new".into(), "new-peer".into())]));
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
            assert_eq!(persisted.authorized_fingerprints(), HashMap::from([("new".into(), "new-peer".into())]));
            service.clipboard_enabled = true;
            let clipboard = input_event::ClipboardEvent::Text("fixture clipboard".into());
            let missing_addr = "127.0.0.1:1".parse().unwrap();
            service.emulation.send_clipboard(missing_addr, clipboard.clone()).unwrap();
            let completed = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let event = service.emulation.event().await;
                    if matches!(event, EmulationEvent::ClipboardSendCompleted(_)) {
                        break event;
                    }
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
            service.authorized_keys.write().unwrap().insert(fingerprint.clone(), "accepted peer".into());
            service.handle_authentication_attempt(fingerprint);
            assert!(service.pending_frontend_events.is_empty());
            attempt.abort();
            let _ = attempt.await;
            service.capture.terminate().await;
            service.emulation.terminate().await;
            service.conn_sender.terminate().await;
            service.hooks.terminate().await;
            service.resolver.terminate().await;
        }).await;
    }
}
