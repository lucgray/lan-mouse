use crate::{
    capture::{Capture, CaptureType, ICaptureEvent},
    client::ClientManager,
    config::{Config, ConfigClient},
    connect::{LanMouseConnection, LanMouseConnectionSender},
    crypto,
    dns::{DnsEvent, DnsResolver},
    emulation::{Emulation, EmulationEvent},
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
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{process::Command, signal, sync::Notify};

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
    /// a subsystem died on its own — the daemon exits nonzero so a
    /// supervisor (Restart=on-failure) brings it back instead of
    /// treating the death as a clean shutdown
    #[error("subsystem exited: {0}")]
    SubsystemExited(&'static str),
}

pub struct Service {
    /// configuration
    config: Config,
    /// input capture
    capture: Capture,
    /// input emulation
    emulation: Emulation,
    /// clipboard monitor
    clipboard_monitor: Option<ClipboardMonitor>,
    /// clipboard emulation
    clipboard_emulation: Option<ClipboardEmulation>,
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
    /// outgoing clipboard transfer progress (fed by send loops)
    clipboard_progress_tx: local_channel::mpsc::Sender<(u64, u64)>,
    clipboard_progress_rx: local_channel::mpsc::Receiver<(u64, u64)>,
    /// completion reports for clipboard sends (`batch id`, `ok`) — the
    /// "shared" hint fires only once every send of a batch reported back
    clipboard_send_done_tx: local_channel::mpsc::Sender<(u64, bool)>,
    clipboard_send_done_rx: local_channel::mpsc::Receiver<(u64, bool)>,
    /// a local clipboard change fans out to N peers; each batch resolves
    /// when all its sends report
    pending_clipboard_batches: HashMap<u64, PendingSendBatch>,
    next_clipboard_batch: u64,
    /// result reports from detached local-clipboard-write tasks:
    /// `Ok` carries (kind, bytes) for the "received" hint, `Err` the
    /// failure text for a user-visible error
    clipboard_applied_tx:
        local_channel::mpsc::Sender<Result<(input_event::ClipboardContentKind, usize), String>>,
    clipboard_applied_rx:
        local_channel::mpsc::Receiver<Result<(input_event::ClipboardContentKind, usize), String>>,
}

/// one local clipboard change fanned out to N peers
struct PendingSendBatch {
    kind: input_event::ClipboardContentKind,
    bytes: usize,
    expected: u32,
    done: u32,
    ok: u32,
    started: Instant,
}

/// a peer that never finishes receiving leaves the batch (and its
/// "shared/failed" toast) pending forever — expire stale batches
const CLIPBOARD_BATCH_TIMEOUT: Duration = Duration::from_secs(300);

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
            let parts = Self::create_clipboard_parts();
            if let Some(ref e) = parts.1 {
                e.set_download_dir(config.download_dir());
            }
            parts
        } else {
            log::info!("Clipboard sharing disabled by configuration");
            (None, None)
        };

        // create dns resolver
        let resolver = DnsResolver::new()?;

        let port = config.port();
        let (clipboard_progress_tx, clipboard_progress_rx) = local_channel::mpsc::channel();
        let (clipboard_send_done_tx, clipboard_send_done_rx) = local_channel::mpsc::channel();
        let (clipboard_applied_tx, clipboard_applied_rx) = local_channel::mpsc::channel();
        let service = Self {
            config,
            capture,
            emulation,
            clipboard_monitor,
            clipboard_emulation,
            clipboard_enabled,
            frontend_listener,
            resolver,
            authorized_keys,
            public_key_fingerprint,
            client_manager: client_manager.clone(),
            conn_sender,
            frontend_event_pending: Default::default(),
            port,
            pending_frontend_events: Default::default(),
            capture_status: Default::default(),
            emulation_status: Default::default(),
            incoming_conn_info: Default::default(),
            incoming_conns: Default::default(),
            next_trigger_handle: 0,
            window_identifier,
            clipboard_progress_tx,
            clipboard_progress_rx,
            clipboard_send_done_tx,
            clipboard_send_done_rx,
            pending_clipboard_batches: Default::default(),
            next_clipboard_batch: 0,
            clipboard_applied_tx,
            clipboard_applied_rx,
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

        let mut liveness_tick = tokio::time::interval(Duration::from_secs(1));
        // set when the loop exits because a subsystem died rather than
        // via a normal shutdown signal — turns the exit into Err so a
        // supervisor can tell a crash from a requested stop
        let mut fatal: Option<&'static str> = None;
        loop {
            tokio::select! {
                request = self.frontend_listener.next() => match request {
                    Some(_) => self.handle_frontend_request(request),
                    None => {
                        log::error!("frontend listener channel closed, shutting down");
                        fatal = Some("frontend listener");
                        break;
                    }
                },
                _ = self.frontend_event_pending.notified() => self.handle_frontend_pending().await,
                event = self.emulation.event() => match event {
                    Some(event) => self.handle_emulation_event(event).await,
                    None => {
                        log::error!("emulation task exited, shutting down");
                        fatal = Some("emulation");
                        break;
                    }
                },
                event = self.capture.event() => match event {
                    Some(event) => self.handle_capture_event(event),
                    None => {
                        log::error!("capture task exited, shutting down");
                        fatal = Some("capture");
                        break;
                    }
                },
                event = self.resolver.event() => match event {
                    Some(event) => self.handle_resolver_event(event),
                    None => {
                        log::error!("dns resolver task exited, shutting down");
                        fatal = Some("dns resolver");
                        break;
                    }
                },
                // watch the subsystem tasks themselves: a dead task is a
                // dead subsystem even if a spawned child keeps an event
                // channel sender alive and the None arms never fire
                _ = liveness_tick.tick() => {
                    if !self.emulation.is_alive() {
                        log::error!("emulation task exited, shutting down");
                        fatal = Some("emulation");
                        break;
                    }
                    if !self.capture.is_alive() {
                        log::error!("capture task exited, shutting down");
                        fatal = Some("capture");
                        break;
                    }
                    if !self.resolver.is_alive() {
                        log::error!("dns resolver task exited, shutting down");
                        fatal = Some("dns resolver");
                        break;
                    }
                    // the clipboard monitor is degraded, not fatal:
                    // its death disables clipboard sharing but input
                    // keeps working
                    if let Some(monitor) = self.clipboard_monitor.as_ref() {
                        if !monitor.is_alive() {
                            log::error!("clipboard monitor exited, clipboard sharing disabled");
                            self.clipboard_monitor = None;
                            self.notify_frontend(FrontendEvent::Error(
                                "clipboard monitor stopped, clipboard sharing disabled".to_string(),
                            ));
                        }
                    }
                    self.expire_clipboard_batches();
                },
                r = self.config.changed() => match r {
                    Ok(()) => self.handle_config_change(),
                    Err(e) => {
                        log::error!("config watcher failed: {e}");
                    }
                },
                event = async {
                    match &mut self.clipboard_monitor {
                        Some(monitor) => monitor.recv().await,
                        None => std::future::pending().await,
                    }
                } => match event {
                    Some(_) => self.handle_clipboard_event(event).await,
                    None => {
                        // the monitor thread died (e.g. a panic inside
                        // it) - without this arm the recv keeps returning
                        // None instantly and spins the loop at 100% CPU
                        log::error!("clipboard monitor exited, clipboard sharing disabled");
                        self.clipboard_monitor = None;
                        self.notify_frontend(FrontendEvent::Error(
                            "clipboard monitor stopped, clipboard sharing disabled".to_string(),
                        ));
                    }
                },
                r = signal::ctrl_c(), if shutdown.is_none() => match r {
                    Ok(()) => break,
                    Err(e) => {
                        log::error!("failed to wait for CTRL+C: {e}");
                        fatal = Some("signal handling");
                        break;
                    }
                },
                _ = async { shutdown.as_mut().unwrap().recv().await }, if shutdown.is_some() => {
                    log::info!("Shutdown signal received");
                    break;
                },
                progress = self.clipboard_progress_rx.recv() => {
                    if let Some((received, total)) = progress {
                        self.notify_frontend(FrontendEvent::ClipboardProgress {
                            incoming: false,
                            received,
                            total,
                        });
                    }
                },
                done = self.clipboard_send_done_rx.recv() => {
                    if let Some((batch, ok)) = done {
                        self.record_clipboard_send_done(batch, ok);
                    }
                },
                applied = self.clipboard_applied_rx.recv() => {
                    match applied {
                        Some(Ok((kind, bytes))) => {
                            self.notify_frontend(FrontendEvent::ClipboardShared {
                                received: true,
                                kind,
                                bytes,
                            });
                        }
                        Some(Err(e)) => {
                            self.notify_frontend(FrontendEvent::Error(e));
                        }
                        None => {}
                    }
                },
            }
        }

        log::info!("terminating service ...");
        log::debug!("terminating capture ...");
        self.capture.terminate().await;
        log::debug!("terminating emulation ...");
        self.emulation.terminate().await;
        log::debug!("terminating dns resolver ...");
        self.resolver.terminate().await;

        match fatal {
            Some(subsystem) => Err(ServiceError::SubsystemExited(subsystem)),
            None => Ok(()),
        }
    }

    fn handle_frontend_request(&mut self, request: Option<Result<FrontendRequest, IpcError>>) {
        let Some(request) = request else {
            return;
        };
        let request = match request {
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
            FrontendRequest::SetDownloadDir(dir) => self.set_download_dir(dir),
            FrontendRequest::SetLanguage(lang) => self.set_language(lang),
            FrontendRequest::SetNotificationMode(mode) => self.set_notification_mode(mode),
            FrontendRequest::SetKeyRepeat { delay, interval } => {
                self.set_key_repeat(delay, interval)
            }
            FrontendRequest::WindowIdentifier(handle) => {
                log::info!("xdg-foreign handle: {handle:?}");
                self.window_identifier
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
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
        let authorized_keys = self
            .authorized_keys
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        self.config.set_authorized_keys(authorized_keys);
        if let Err(e) = self.config.write_back() {
            log::warn!("failed to write config: {e}");
        }
    }

    fn handle_config_change(&mut self) {
        for h in self.client_manager.registered_clients() {
            self.remove_client(h);
        }
        for c in self.config.clients() {
            let handle = self.client_manager.add_with_config(c);
            log::info!("added client {handle}");
            let Some((c, s)) = self.client_manager.get_state(handle) else {
                log::error!("client {handle} missing after registration");
                continue;
            };
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
        self.update_scrolling_inversion(self.config.invert_scroll());
        self.update_mouse_sensitivity(self.config.mouse_sensitivity());
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
            .unwrap_or_else(|e| e.into_inner())
            .clone_from(&authorized_keys);
        self.sync_frontend();
    }

    async fn handle_frontend_pending(&mut self) {
        while let Some(event) = self.pending_frontend_events.pop_front() {
            self.frontend_listener.broadcast(event).await;
        }
    }

    async fn handle_emulation_event(&mut self, event: EmulationEvent) {
        match event {
            EmulationEvent::ConnectionAttempt { fingerprint } => {
                self.notify_frontend(FrontendEvent::ConnectionAttempt { fingerprint });
            }
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
            EmulationEvent::Disconnected { addr } => {
                if let Some(addr) = self.remove_incoming(addr) {
                    self.notify_frontend(FrontendEvent::IncomingDisconnected(addr));
                }
            }
            EmulationEvent::PortChanged(port) => match port {
                Ok(port) => {
                    self.port = port;
                    self.notify_frontend(FrontendEvent::PortChanged(port, None));
                    self.notify_settings();
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
            EmulationEvent::ClipboardProgress { received, total } => {
                self.notify_frontend(FrontendEvent::ClipboardProgress {
                    incoming: true,
                    received,
                    total,
                });
            }
            EmulationEvent::ClipboardReceived(clipboard_event) => {
                // Received clipboard data from a remote machine - set it locally.
                // Same detached path as ICaptureEvent::ClipboardReceived:
                // writing a large file must not stall the service loop.
                if self.clipboard_enabled {
                    if let Some(ref monitor) = self.clipboard_monitor {
                        // record the incoming content so our own monitor
                        // does not echo it right back to the peer
                        monitor.update_last_content(clipboard_event.clone());
                    }
                    if let Some(ref clipboard_emulation) = self.clipboard_emulation {
                        let clipboard_emulation = clipboard_emulation.clone();
                        let applied_tx = self.clipboard_applied_tx.clone();
                        tokio::task::spawn_local(async move {
                            let result =
                                match clipboard_emulation.set(clipboard_event.clone()).await {
                                    Ok(()) => {
                                        Ok((clipboard_event.kind(), clipboard_event.content_len()))
                                    }
                                    Err(e) => {
                                        log::warn!("Failed to set clipboard: {}", e);
                                        Err(format!("failed to apply received clipboard: {e}"))
                                    }
                                };
                            let _ = applied_tx.send(result);
                        });
                    }
                }
            }
            EmulationEvent::ClipboardSendDone { batch, ok } => {
                self.record_clipboard_send_done(batch, ok);
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
            ICaptureEvent::ClipboardProgress { received, total } => {
                self.notify_frontend(FrontendEvent::ClipboardProgress {
                    incoming: true,
                    received,
                    total,
                });
            }
            ICaptureEvent::ConnectFailed { handle, error } => {
                log::warn!("connection to client {handle} failed: {error}");
                self.notify_frontend(FrontendEvent::Error(format!(
                    "could not connect to client {handle}: {error}"
                )));
            }
            ICaptureEvent::ClipboardReceived(clipboard_event) => {
                // Received clipboard data from a remote machine - set it locally
                if self.clipboard_enabled {
                    if let Some(ref monitor) = self.clipboard_monitor {
                        monitor.update_last_content(clipboard_event.clone());
                    }
                    if let Some(ref clipboard_emulation) = self.clipboard_emulation {
                        // the "received" hint fires when the write task
                        // reports back — before that, success is unknown
                        let clipboard_emulation = clipboard_emulation.clone();
                        let applied_tx = self.clipboard_applied_tx.clone();
                        tokio::task::spawn_local(async move {
                            let result =
                                match clipboard_emulation.set(clipboard_event.clone()).await {
                                    Ok(()) => {
                                        Ok((clipboard_event.kind(), clipboard_event.content_len()))
                                    }
                                    Err(e) => {
                                        log::warn!("Failed to set clipboard: {}", e);
                                        Err(format!("failed to apply received clipboard: {e}"))
                                    }
                                };
                            let _ = applied_tx.send(result);
                        });
                    }
                }
            }
        }
    }

    async fn handle_clipboard_event(&mut self, event: Option<input_capture::CaptureEvent>) {
        use input_capture::CaptureEvent;
        use input_event::Event;

        if let Some(CaptureEvent::Input(Event::Clipboard(clipboard_event))) = event {
            use lan_mouse_proto::{
                MAX_CLIPBOARD_TRANSFER_SIZE, ProtocolError, encode_clipboard_event,
            };

            let proto_event = lan_mouse_proto::ProtoEvent::Input(input_event::Event::Clipboard(
                clipboard_event.clone(),
            ));

            // encode once up-front: an oversized payload is dropped with a
            // friendly hint instead of failing per-connection further down
            match encode_clipboard_event(&proto_event) {
                Err(ProtocolError::ClipboardTooLarge(bytes)) => {
                    log::warn!(
                        "clipboard content too large to share: {} bytes ({} byte limit)",
                        bytes,
                        MAX_CLIPBOARD_TRANSFER_SIZE
                    );
                    self.notify_frontend(FrontendEvent::ClipboardTooLarge {
                        bytes,
                        limit: MAX_CLIPBOARD_TRANSFER_SIZE,
                    });
                    return;
                }
                Err(e) => {
                    log::warn!("failed to encode clipboard event: {e}");
                    return;
                }
                Ok(_) => {}
            }

            log::info!("Clipboard changed locally, sending to all connected peers");

            // Send clipboard to all active clients (machines we're controlling)
            let active_clients: Vec<_> = self.client_manager.active_clients().into_iter().collect();

            let batch_id = self.next_clipboard_batch;
            self.next_clipboard_batch += 1;
            let mut expected = 0u32;
            for handle in active_clients {
                // large payloads take seconds on the wire — run each
                // send detached so the service loop (and the progress
                // events it reports) keeps running; every send reports
                // its outcome so the "shared" hint is honest
                let sender = self.conn_sender.clone();
                let progress_tx = self.clipboard_progress_tx.clone();
                let done_tx = self.clipboard_send_done_tx.clone();
                let event = proto_event.clone();
                tokio::task::spawn_local(async move {
                    let ok = match sender
                        .send_clipboard(event, handle, Some(&progress_tx))
                        .await
                    {
                        Ok(()) => true,
                        Err(e) => {
                            log::warn!("Failed to send clipboard to client {}: {}", handle, e);
                            false
                        }
                    };
                    let _ = done_tx.send((batch_id, ok));
                });
                expected += 1;
            }

            // Also send clipboard to all incoming connections (machines controlling us)
            let incoming_addrs: Vec<_> = self
                .incoming_conn_info
                .values()
                .map(|incoming| incoming.addr)
                .collect();

            for addr in incoming_addrs {
                log::info!("Sending clipboard to incoming connection {}", addr);
                self.emulation.send_clipboard(
                    addr,
                    clipboard_event.clone(),
                    Some(self.clipboard_progress_tx.clone()),
                    batch_id,
                );
                expected += 1;
            }

            if expected > 0 {
                self.pending_clipboard_batches.insert(
                    batch_id,
                    PendingSendBatch {
                        kind: clipboard_event.kind(),
                        bytes: clipboard_event.content_len(),
                        expected,
                        done: 0,
                        ok: 0,
                        started: Instant::now(),
                    },
                );
            }
        }
    }

    /// drop sends whose peers stopped reporting; unresolved batches
    /// count as failed so the user can retry the same content
    fn expire_clipboard_batches(&mut self) {
        let now = Instant::now();
        let stale: Vec<u64> = self
            .pending_clipboard_batches
            .iter()
            .filter(|(_, b)| now.duration_since(b.started) > CLIPBOARD_BATCH_TIMEOUT)
            .map(|(id, _)| *id)
            .collect();
        if stale.is_empty() {
            return;
        }
        let mut cleared_sig = false;
        for id in stale {
            let Some(b) = self.pending_clipboard_batches.remove(&id) else {
                continue;
            };
            log::warn!(
                "clipboard batch {id} timed out: {}/{} sends reported",
                b.done,
                b.expected
            );
            cleared_sig = true;
            self.notify_frontend(FrontendEvent::Error(format!(
                "clipboard share timed out — {}/{} peer(s) responded",
                b.done, b.expected
            )));
        }
        if cleared_sig {
            if let Some(ref monitor) = self.clipboard_monitor {
                monitor.clear_last_sig();
            }
        }
    }

    /// one send of a clipboard batch finished. The "shared" hint fires
    /// only when every send reported — as success when at least one
    /// peer got it, as an error (with retry allowed) when all failed.
    fn record_clipboard_send_done(&mut self, batch: u64, ok: bool) {
        let Some(mut b) = self.pending_clipboard_batches.remove(&batch) else {
            return;
        };
        b.done += 1;
        if ok {
            b.ok += 1;
        }
        if b.done < b.expected {
            self.pending_clipboard_batches.insert(batch, b);
            return;
        }
        if b.ok > 0 {
            self.notify_frontend(FrontendEvent::ClipboardShared {
                received: false,
                kind: b.kind,
                bytes: b.bytes,
            });
        } else {
            // every send failed — forget the recorded signature so the
            // user can retry by copying the same content again
            if let Some(ref monitor) = self.clipboard_monitor {
                monitor.clear_last_sig();
            }
            self.notify_frontend(FrontendEvent::Error(format!(
                "clipboard share failed — could not reach any of {} peer(s)",
                b.expected
            )));
        }
    }

    fn handle_resolver_event(&mut self, event: DnsEvent) {
        let handle = match event {
            DnsEvent::Resolving(handle) => {
                self.client_manager.set_resolving(handle, true);
                handle
            }
            DnsEvent::Resolved(handle, hostname, ips) => {
                self.client_manager.set_resolving(handle, false);
                if let Err(e) = &ips {
                    log::warn!("could not resolve {hostname}: {e}");
                }
                let ips = ips.unwrap_or_default();
                self.client_manager.set_dns_ips(handle, ips);
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
        let keys = self
            .authorized_keys
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
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
        // incoming_conns and incoming_conn_info can desync — e.g. a
        // destroy raced a re-Enter — so a missing entry is a warn,
        // not a panic
        let Some(incoming) = self
            .incoming_conn_info
            .iter_mut()
            .find(|(_, i)| i.addr == addr)
            .map(|(_, i)| i)
        else {
            log::warn!("update_incoming: {addr} not registered, ignoring");
            return;
        };
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
        // headless fallback: without a frontend there is no window to
        // host a banner — surface user-facing events as OS notifications
        if !self.frontend_listener.frontend_connected() {
            crate::notify::notify_for_event(&event);
        }
        self.pending_frontend_events.push_back(event);
        self.frontend_event_pending.notify_one();
    }

    fn add_authorized_key(&mut self, desc: String, fp: String) {
        self.authorized_keys
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(fp, desc);
        let keys = self
            .authorized_keys
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        self.notify_frontend(FrontendEvent::AuthorizedUpdated(keys));
    }

    fn remove_authorized_key(&mut self, fp: String) {
        self.authorized_keys
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&fp);
        let keys = self
            .authorized_keys
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        self.notify_frontend(FrontendEvent::AuthorizedUpdated(keys));
    }

    fn enumerate(&mut self) {
        let clients = self.client_manager.get_client_states();
        self.notify_frontend(FrontendEvent::Enumerate(clients));
    }

    fn add_client(&mut self) {
        let handle = self.client_manager.add_client();
        log::info!("added client {handle}");
        let Some((c, s)) = self.client_manager.get_state(handle) else {
            log::error!("client {handle} missing after registration");
            return;
        };
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
        if self.port != port {
            self.emulation.request_port_change(port);
        } else {
            self.notify_frontend(FrontendEvent::PortChanged(self.port, None));
        }
    }

    fn remove_client(&mut self, handle: ClientHandle) {
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
                if let Some(ref e) = emulation {
                    e.set_download_dir(self.config.download_dir());
                }
                if self.clipboard_monitor.is_none() {
                    self.clipboard_monitor = monitor;
                }
                if self.clipboard_emulation.is_none() {
                    self.clipboard_emulation = emulation;
                }
            }
            if let Some(monitor) = &self.clipboard_monitor {
                monitor.enable();
            }
        } else if let Some(monitor) = &self.clipboard_monitor {
            monitor.disable();
        }
        self.config.set_clipboard_enabled(enabled);
        self.save_config();
        self.notify_settings();
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

    /// effective directory received clipboard files are written to
    fn download_dir(&self) -> PathBuf {
        self.config
            .download_dir()
            .or_else(input_emulation::clipboard::download_dir)
            .unwrap_or_else(|| PathBuf::from("."))
    }

    /// update the directory received clipboard files are written to
    fn set_download_dir(&mut self, dir: Option<String>) {
        let dir = dir.map(PathBuf::from);
        self.config.set_download_dir(dir.clone());
        self.save_config();
        if let Some(ref e) = self.clipboard_emulation {
            e.set_download_dir(dir);
        }
        self.notify_settings();
    }

    fn set_language(&mut self, language: Option<String>) {
        log::info!("ui language set to {language:?} (applies on next frontend start)");
        self.config.set_language(language);
        self.save_config();
        self.notify_settings();
    }

    /// key-repeat timing in ms — applied live on backends that synthesize
    /// repeats themselves (Windows, macOS) and persisted for the rest
    fn set_key_repeat(&mut self, delay: u64, interval: u64) {
        log::info!("key repeat set to {delay}ms delay / {interval}ms interval");
        self.emulation.request_key_repeat(
            Duration::from_millis(delay),
            Duration::from_millis(interval),
        );
        self.config.set_key_repeat(Some(delay), Some(interval));
        self.save_config();
        self.notify_settings();
    }

    /// app/system/both — validated against the known modes, anything
    /// else is dropped with a warning so a typo can't silence hints
    fn set_notification_mode(&mut self, mode: String) {
        const MODES: [&str; 3] = ["app", "system", "both"];
        if !MODES.contains(&mode.as_str()) {
            self.notify_frontend(FrontendEvent::Error(format!(
                "invalid notification mode '{mode}' — expected one of {MODES:?}"
            )));
            return;
        }
        log::info!("notification mode set to {mode}");
        self.config.set_notification_mode(Some(mode));
        self.save_config();
        self.notify_settings();
    }

    /// push the current settings to the frontend
    fn notify_settings(&mut self) {
        self.notify_frontend(FrontendEvent::Settings {
            clipboard_enabled: self.clipboard_enabled,
            invert_scroll: self.config.invert_scroll(),
            mouse_sensitivity: self.config.mouse_sensitivity(),
            download_dir: self.download_dir().display().to_string(),
            port: self.port,
            language: self.config.language().unwrap_or_default(),
            key_repeat_delay: self.config.emulation_options().key_repeat_delay.as_millis() as u64,
            key_repeat_interval: self
                .config
                .emulation_options()
                .key_repeat_interval
                .as_millis() as u64,
            notification_mode: self.notification_mode(),
        });
    }

    /// "app" | "system" | "both" — the GTK default is "app"
    fn notification_mode(&self) -> String {
        self.config
            .notification_mode()
            .unwrap_or_else(|| "app".to_string())
    }

    fn spawn_hook_command(&self, handle: ClientHandle, kind: HookKind) {
        let cmd = match kind {
            HookKind::Enter => self.client_manager.get_enter_cmd(handle),
            HookKind::Leave => self.client_manager.get_leave_cmd(handle),
        };
        let Some(cmd) = cmd else { return };
        tokio::task::spawn_local(async move {
            log::info!("spawning {kind} hook for client {handle}");
            let mut child = match Command::new("sh").arg("-c").arg(cmd.as_str()).spawn() {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("could not execute {kind} hook for client {handle}: {e}");
                    return;
                }
            };
            match child.wait().await {
                Ok(s) => {
                    if s.success() {
                        log::info!("{kind} hook for client {handle} ({cmd}) exited successfully");
                    } else {
                        log::warn!("{kind} hook for client {handle} ({cmd}) exited with {s}");
                    }
                }
                Err(e) => log::warn!("{kind} hook for client {handle} ({cmd}): {e}"),
            }
        });
    }
}

#[derive(Clone, Copy, Debug)]
enum HookKind {
    Enter,
    Leave,
}

impl std::fmt::Display for HookKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HookKind::Enter => f.write_str("enter"),
            HookKind::Leave => f.write_str("leave"),
        }
    }
}
