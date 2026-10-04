use futures::{FutureExt, Stream, StreamExt};
use lan_mouse_proto::ProtoEvent;
use local_channel::mpsc::{Receiver, Sender, channel};
use rustls::pki_types::CertificateDer;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    rc::Rc,
    sync::{Arc, RwLock},
    time::Duration,
};
use thiserror::Error;
use tokio::task::{JoinHandle, spawn_local};
use tokio_util::sync::CancellationToken;
use webrtc_dtls::{
    config::{ClientAuthType::RequireAnyClientCert, Config, ExtendedMasterSecretType},
    conn::DTLSConn,
    crypto::Certificate,
    listener::listen,
};
use webrtc_util::{Conn, Error, conn::Listener};

use crate::crypto;

#[derive(Error, Debug)]
pub enum ListenerCreationError {
    #[error(transparent)]
    WebrtcUtil(#[from] webrtc_util::Error),
    #[error(transparent)]
    WebrtcDtls(#[from] webrtc_dtls::Error),
    #[error("listener binding exceeded two seconds")]
    BindTimeout,
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
    #[error("listeners must report one nonzero bound port")]
    InvalidBoundPort,
}

#[derive(Error, Debug)]
pub(crate) enum ClipboardSendError {
    #[error("clipboard peer is no longer connected")]
    NotConnected,
    #[error("clipboard send canceled")]
    Canceled,
    #[error("clipboard send timed out")]
    Timeout,
    #[error(transparent)]
    Encode(#[from] lan_mouse_proto::ProtocolError),
    #[error(transparent)]
    Transport(#[from] webrtc_util::Error),
    #[error("clipboard send wrote {sent} of {expected} bytes")]
    Incomplete { sent: usize, expected: usize },
}

pub(crate) type ArcConn = Arc<dyn Conn + Send + Sync>;

/// Create a DTLS listener per address family.
///
/// A socket bound to `[::]` accepts IPv4-mapped peers only on platforms
/// with dual-stack sockets. Socket defaults depend on the platform and
/// configuration; v6-only sockets cannot accept an IPv4-only peer.
/// A separate IPv4 listener is therefore always attempted;
/// where `[::]` is already dual-stack its bind fails with EADDRINUSE
/// and the attempt is skipped.
async fn bind_dtls(
    port: u16,
    cfg: &Config,
) -> Result<Vec<Box<dyn Listener>>, ListenerCreationError> {
    let mut listeners: Vec<Box<dyn Listener>> = Vec::new();
    let mut last_err = None;
    let mut selected_port = port;
    for ip in [
        IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
    ] {
        match listen(SocketAddr::new(ip, selected_port), cfg.clone()).await {
            Ok(l) => {
                // Port zero asks the OS once; the other family must bind the
                // same actual port, including on v6-only platforms.
                if selected_port == 0 {
                    selected_port = l.addr().await?.port();
                }
                listeners.push(Box::new(l));
            }
            Err(e) => {
                log::debug!("dtls listen on {ip}: {e}");
                last_err = Some(e);
            }
        }
    }
    if listeners.is_empty() {
        Err(last_err.expect("at least one bind attempted").into())
    } else {
        Ok(listeners)
    }
}

/// Wait for an incoming connection on any of the bound listeners.
async fn accept_any(
    listeners: &[Box<dyn Listener>],
) -> Result<(ArcConn, SocketAddr), webrtc_util::Error> {
    futures::future::select_all(listeners.iter().map(|l| l.accept()))
        .await
        .0
}

pub(crate) enum ListenEvent {
    Msg {
        event: ProtoEvent,
        addr: SocketAddr,
        conn: ArcConn,
    },
    Disconnected {
        addr: SocketAddr,
    },
    Accept {
        addr: SocketAddr,
        fingerprint: String,
        conn: ArcConn,
    },
    PortChanged(Result<u16, ListenerCreationError>),
}

pub(crate) struct LanMouseListener {
    listen_rx: Receiver<ListenEvent>,
    listen_tx: Sender<ListenEvent>,
    listen_task: JoinHandle<()>,
    cancellation: CancellationToken,
    conns: Rc<RefCell<Vec<(SocketAddr, ArcConn)>>>,
    request_port_change: tokio::sync::watch::Sender<Option<u16>>,
    port: Rc<Cell<u16>>,
    authentication_notices: crate::authentication::AuthenticationNotices,
}

type BoundListeners = Vec<Box<dyn Listener>>;
struct BoundListenerSet {
    port: u16,
    listeners: BoundListeners,
}
type BindingResult = (u16, Result<BoundListenerSet, ListenerCreationError>);
type ListenerBinder = Rc<
    dyn Fn(
        u16,
        Config,
    ) -> futures::future::LocalBoxFuture<
        'static,
        Result<BoundListeners, ListenerCreationError>,
    >,
>;

async fn bind_and_resolve_port(
    binder: &ListenerBinder,
    port: u16,
    cfg: Config,
) -> Result<BoundListenerSet, ListenerCreationError> {
    let listeners = binder(port, cfg).await?;
    let actual = async {
        let first = listeners
            .first()
            .ok_or(ListenerCreationError::InvalidBoundPort)?;
        let actual_port = first.addr().await?.port();
        if actual_port == 0 {
            return Err(ListenerCreationError::InvalidBoundPort);
        }
        for listener in &listeners {
            if listener.addr().await?.port() != actual_port {
                return Err(ListenerCreationError::InvalidBoundPort);
            }
        }
        Ok(actual_port)
    }
    .await;
    match actual {
        Ok(port) => Ok(BoundListenerSet { port, listeners }),
        Err(error) => {
            close_listeners(listeners).await;
            Err(error)
        }
    }
}

// Aborting the owner also aborts binding/cleanup; JoinHandle drop alone detaches.
struct OwnedListenerTask<T>(JoinHandle<T>);
impl<T> Drop for OwnedListenerTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn listener_task_completed<T>(
    task: &mut Option<OwnedListenerTask<T>>,
) -> Result<T, tokio::task::JoinError> {
    match task {
        Some(task) => (&mut task.0).await,
        None => std::future::pending().await,
    }
}
async fn close_listeners(listeners: Vec<Box<dyn Listener>>) {
    futures::future::join_all(listeners.iter().map(|listener| async move {
        match tokio::time::timeout(Duration::from_secs(1), listener.close()).await {
            Err(_) => log::warn!("listener cleanup exceeded one second"),
            Ok(Err(error)) => log::warn!("listener cleanup failed: {error}"),
            Ok(Ok(())) => {}
        }
    }))
    .await;
}

type VerifyPeerCertificateFn = Arc<
    dyn (Fn(&[Vec<u8>], &[CertificateDer<'static>]) -> Result<(), webrtc_dtls::Error>)
        + Send
        + Sync,
>;

impl LanMouseListener {
    pub(crate) async fn new(
        port: u16,
        cert: Certificate,
        authorized_keys: Arc<RwLock<HashMap<String, String>>>,
    ) -> Result<Self, ListenerCreationError> {
        Self::new_with_binder(
            port,
            cert,
            authorized_keys,
            Rc::new(|port, cfg| async move { bind_dtls(port, &cfg).await }.boxed_local()),
        )
        .await
    }

    async fn new_with_binder(
        port: u16,
        cert: Certificate,
        authorized_keys: Arc<RwLock<HashMap<String, String>>>,
        binder: ListenerBinder,
    ) -> Result<Self, ListenerCreationError> {
        let (listen_tx, listen_rx) = channel();
        let (request_port_change, mut request_port_change_rx) = tokio::sync::watch::channel(None);
        let authentication_notices = crate::authentication::AuthenticationNotices::default();

        let authorized = authorized_keys.clone();
        let verify_peer_certificate: Option<VerifyPeerCertificateFn> = {
            let authentication_notices = authentication_notices.clone();
            Some(Arc::new(
                move |certs: &[Vec<u8>], _chains: &[CertificateDer<'static>]| {
                    // Authorize the leaf certificate, not intermediates. An empty
                    // chain is invalid input, never a process assertion failure.
                    let Some(cert) = certs.first() else {
                        return Err(webrtc_dtls::Error::ErrVerifyDataMismatch);
                    };
                    let fingerprint = crypto::generate_fingerprint(cert);
                    if authorized.read().expect("lock").contains_key(&fingerprint) {
                        Ok(())
                    } else {
                        authentication_notices.record(fingerprint);
                        Err(webrtc_dtls::Error::ErrVerifyDataMismatch)
                    }
                },
            ))
        };
        let cfg = Config {
            certificates: vec![cert.clone()],
            extended_master_secret: ExtendedMasterSecretType::Require,
            client_auth: RequireAnyClientCert,
            verify_peer_certificate,
            ..Default::default()
        };

        let bound = tokio::time::timeout(
            Duration::from_secs(2),
            bind_and_resolve_port(&binder, port, cfg.clone()),
        )
        .await
        .map_err(|_| ListenerCreationError::BindTimeout)??;
        let mut listeners = bound.listeners;
        let running_port = Rc::new(Cell::new(bound.port));
        let task_port = running_port.clone();

        let conns: Rc<RefCell<Vec<(SocketAddr, ArcConn)>>> = Rc::new(RefCell::new(Vec::new()));

        let conns_clone = conns.clone();
        let cancellation = CancellationToken::new();
        let readers_cancel = cancellation.clone();
        let listen_task: JoinHandle<()> = {
            let listen_tx = listen_tx.clone();
            spawn_local(async move {
                let current_port = task_port;
                let mut binding: Option<OwnedListenerTask<BindingResult>> = None;
                let mut cleanup = None;
                loop {
                    let sleep = tokio::time::sleep(Duration::from_secs(2));
                    tokio::select! {
                        /* workaround for https://github.com/webrtc-rs/webrtc/issues/614 */
                        _ = sleep => continue,
                        c = accept_any(&listeners) => match c {
                            Ok((conn, addr)) => {
                                log::info!("dtls client connected, ip: {addr}");
                                let dtls_conn: &DTLSConn = conn.as_any().downcast_ref().expect("dtls conn");
                                let certs = dtls_conn.connection_state().await.peer_certificates;
                                let cert = certs.first().expect("cert");
                                let fingerprint = crypto::generate_fingerprint(cert);
                                let previous = {
                                    let mut current = conns_clone.borrow_mut();
                                    let index = current.iter().position(|(a, _)| *a == addr);
                                    let previous = index.map(|index| current.remove(index).1);
                                    current.push((addr, conn.clone()));
                                    previous
                                };
                                listen_tx.send(ListenEvent::Accept { addr, fingerprint, conn: conn.clone() }).expect("channel closed");
                                spawn_local(read_loop(conns_clone.clone(), addr, conn, listen_tx.clone(), readers_cancel.clone()));
                                if let Some(previous) = previous { spawn_local(async move { close_incoming(&previous).await; }); }
                            },
                            Err(e) => {
                                if let Error::Std(ref e) = e {
                                    if let Some(e) = e.0.downcast_ref::<webrtc_dtls::Error>() {
                                        match e {
                                            webrtc_dtls::Error::ErrVerifyDataMismatch => {
                                                // The verifier already recorded the exact fingerprint.
                                                // Accept errors carry no identity and must not dequeue one.
                                            }
                                            _ => log::warn!("accept: {e}"),
                                        }
                                    } else {
                                        log::warn!("accept: {e:?}");
                                    }
                                } else {
                                    log::warn!("accept: {e:?}");
                                }
                            }
                        },
                        _ = readers_cancel.cancelled() => break,
                        changed = request_port_change_rx.changed(), if binding.is_none() && cleanup.is_none() => {
                            if changed.is_err() { break; }
                            let port = (*request_port_change_rx.borrow_and_update()).expect("port request");
                            if port == current_port.get() {
                                let _ = listen_tx.send(ListenEvent::PortChanged(Ok(port)));
                                continue;
                            }
                            let cfg = cfg.clone();
                            let binder = binder.clone();
                            binding = Some(OwnedListenerTask(spawn_local(async move {
                                let result = tokio::time::timeout(Duration::from_secs(2), bind_and_resolve_port(&binder, port, cfg))
                                    .await.unwrap_or(Err(ListenerCreationError::BindTimeout));
                                (port, result)
                            })));
                        },
                        completed = listener_task_completed(&mut binding) => {
                            binding = None;
                            match completed {
                                Ok((port, Ok(new_bound))) => {
                                    let new_listeners = new_bound.listeners;
                                    if *request_port_change_rx.borrow() != Some(port) {
                                        // A newer request superseded this bind. Dispose of its
                                        // sockets before admitting another bind/cleanup batch.
                                        cleanup = Some(OwnedListenerTask(spawn_local(close_listeners(new_listeners))));
                                        continue;
                                    }
                                    let previous = std::mem::replace(&mut listeners, new_listeners);
                                    current_port.set(new_bound.port);
                                    cleanup = Some(OwnedListenerTask(spawn_local(close_listeners(previous))));
                                    let _ = listen_tx.send(ListenEvent::PortChanged(Ok(new_bound.port)));
                                }
                                Ok((port, Err(error))) => {
                                    if *request_port_change_rx.borrow() == Some(port) {
                                        log::warn!("unable to change port: {error}");
                                        let _ = listen_tx.send(ListenEvent::PortChanged(Err(error)));
                                    }
                                }
                                Err(error) => { let _ = listen_tx.send(ListenEvent::PortChanged(Err(error.into()))); }
                            }
                        },
                        _ = listener_task_completed(&mut cleanup) => { cleanup = None; },
                    };
                }
                drop(binding);
                drop(cleanup);
                close_listeners(listeners).await;
            })
        };

        Ok(Self {
            conns,
            listen_rx,
            listen_tx,
            listen_task,
            cancellation,
            request_port_change,
            port: running_port,
            authentication_notices,
        })
    }

    pub(crate) fn is_current(&self, addr: SocketAddr, conn: &ArcConn) -> bool {
        is_current(&self.conns.borrow(), addr, conn)
    }

    pub(crate) fn has_connection(&self, addr: SocketAddr) -> bool {
        self.conns.borrow().iter().any(|(a, _)| *a == addr)
    }

    pub(crate) fn authentication_notices(&self) -> crate::authentication::AuthenticationNotices {
        self.authentication_notices.clone()
    }

    pub(crate) fn port(&self) -> u16 {
        self.port.get()
    }

    pub(crate) fn port_requests(&self) -> tokio::sync::watch::Sender<Option<u16>> {
        self.request_port_change.clone()
    }

    pub(crate) async fn terminate(&mut self) {
        self.cancellation.cancel();

        let conns: Vec<_> = self
            .conns
            .borrow_mut()
            .drain(..)
            .map(|(_, conn)| conn)
            .collect();
        let task = async {
            if tokio::time::timeout(Duration::from_secs(2), &mut self.listen_task)
                .await
                .is_err()
            {
                self.listen_task.abort();
                let _ = (&mut self.listen_task).await;
                log::warn!("listener shutdown exceeded two seconds");
            }
        };
        let connections = futures::future::join_all(conns.iter().map(close_incoming));
        tokio::join!(task, connections);
        self.listen_tx.close();
    }

    pub(crate) fn reply(
        &self,
        jobs: &mut crate::control_network::ControlJobs,
        addr: SocketAddr,
        event: ProtoEvent,
    ) {
        log::trace!("reply {event} >=>=>=>=>=> {addr}");
        let Some(conn) = self.clipboard_connection(addr) else {
            log::debug!("control reply to {addr} skipped: connection missing");
            return;
        };
        if let Err(error) = jobs.submit(addr, conn.clone(), event) {
            self.finish_control_reply(crate::control_network::ControlCompletion {
                addr,
                conn,
                result: Err(error),
            });
            jobs.cancel_stale(addr, None);
        }
    }

    pub(crate) fn finish_control_reply(
        &self,
        completed: crate::control_network::ControlCompletion,
    ) {
        if completed.result.is_ok()
            || matches!(
                completed.result,
                Err(crate::control_network::ControlSendError::Canceled)
            )
            || !self.is_current(completed.addr, &completed.conn)
        {
            return;
        }
        log::warn!(
            "control reply to {} failed: {}",
            completed.addr,
            completed.result.unwrap_err()
        );
        // Remove before notifying so queued input from this failed session is
        // ignored. Releasing its emulation state does not wait for socket close.
        self.conns
            .borrow_mut()
            .retain(|(addr, conn)| *addr != completed.addr || !Arc::ptr_eq(conn, &completed.conn));
        let _ = self.listen_tx.send(ListenEvent::Disconnected {
            addr: completed.addr,
        });
        spawn_local(async move {
            close_incoming(&completed.conn).await;
        });
    }

    pub(crate) fn clipboard_connection(&self, addr: SocketAddr) -> Option<ArcConn> {
        self.conns
            .borrow()
            .iter()
            .find(|(a, _)| *a == addr)
            .map(|(_, conn)| conn.clone())
    }

    pub(crate) fn clipboard_connections(&self) -> Rc<RefCell<Vec<(SocketAddr, ArcConn)>>> {
        self.conns.clone()
    }

    pub(crate) async fn get_certificate_fingerprint(&self, addr: SocketAddr) -> Option<String> {
        let conn = self
            .conns
            .borrow()
            .iter()
            .find(|(a, _)| *a == addr)
            .map(|(_, c)| c.clone());
        if let Some(conn) = conn {
            let conn: &DTLSConn = conn.as_any().downcast_ref().expect("dtls conn");
            let certs = conn.connection_state().await.peer_certificates;
            let cert = certs.first()?;
            let fingerprint = crypto::generate_fingerprint(cert);
            Some(fingerprint)
        } else {
            None
        }
    }
}

pub(crate) async fn send_clipboard_reply(
    conn: Option<ArcConn>,
    event: ProtoEvent,
) -> Result<(), ClipboardSendError> {
    let conn = conn.ok_or(ClipboardSendError::NotConnected)?;
    let bytes = lan_mouse_proto::encode_clipboard_event(&event)?;
    let sent = conn.send(&bytes).await?;
    if sent != bytes.len() {
        return Err(ClipboardSendError::Incomplete {
            sent,
            expected: bytes.len(),
        });
    }
    Ok(())
}

impl Drop for LanMouseListener {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.listen_task.abort();
    }
}

impl Stream for LanMouseListener {
    type Item = ListenEvent;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.listen_rx.poll_next_unpin(cx)
    }
}

async fn read_loop(
    conns: Rc<RefCell<Vec<(SocketAddr, ArcConn)>>>,
    addr: SocketAddr,
    conn: ArcConn,
    dtls_tx: Sender<ListenEvent>,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    use lan_mouse_proto::{MAX_CLIPBOARD_SIZE, decode_event_frame};

    // Buffer needs to be large enough for clipboard data
    // Use Vec instead of array for large buffers to avoid stack overflow
    let mut b = vec![0u8; MAX_CLIPBOARD_SIZE + 5];

    loop {
        // Read first byte to determine event type
        let received = tokio::select! {
            _ = cancellation.cancelled() => break,
            received = conn.recv(&mut b) => received,
        };
        let n = match received {
            Ok(n) => n,
            Err(e) => {
                log::warn!("recv error from {}: {:?}", addr, e);
                break;
            }
        };

        if n == 0 {
            break;
        }

        if !is_current(&conns.borrow(), addr, &conn) {
            break;
        }
        log::trace!("Received {} bytes from {}", n, addr);

        let event = match decode_event_frame(&b[..n]) {
            Ok(event) => event,
            Err(e) => {
                log::debug!("ignoring undecodable event from {addr}: {e}");
                continue;
            }
        };

        if dtls_tx
            .send(ListenEvent::Msg {
                event,
                addr,
                conn: conn.clone(),
            })
            .is_err()
        {
            break;
        }
    }

    let removed = {
        let mut current = conns.borrow_mut();
        current
            .iter()
            .position(|(a, c)| *a == addr && Arc::ptr_eq(c, &conn))
            .map(|index| current.remove(index))
            .is_some()
    };
    if removed {
        let _ = dtls_tx.send(ListenEvent::Disconnected { addr });
    }
    close_incoming(&conn).await;
    Ok(())
}

fn is_current(conns: &[(SocketAddr, ArcConn)], addr: SocketAddr, conn: &ArcConn) -> bool {
    conns
        .iter()
        .any(|(a, c)| *a == addr && Arc::ptr_eq(c, conn))
}

async fn close_incoming(conn: &ArcConn) {
    if tokio::time::timeout(Duration::from_secs(1), conn.close())
        .await
        .is_err()
    {
        log::warn!("incoming connection cleanup exceeded one second");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lan_mouse_proto::MAX_EVENT_SIZE;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestConn {
        packet: Mutex<Option<Vec<u8>>>,
        closed: AtomicBool,
        send_result: Mutex<Option<webrtc_util::Result<usize>>>,
        sent: Mutex<Vec<Vec<u8>>>,
        send_gate: Option<Arc<tokio::sync::Semaphore>>,
        sending: AtomicBool,
        send_entered: tokio::sync::Notify,
    }
    impl TestConn {
        fn new(packet: Option<Vec<u8>>) -> Self {
            Self {
                packet: Mutex::new(packet),
                closed: AtomicBool::new(false),
                send_result: Mutex::new(None),
                sent: Mutex::new(Vec::new()),
                send_gate: None,
                sending: AtomicBool::new(false),
                send_entered: tokio::sync::Notify::new(),
            }
        }
    }
    #[async_trait::async_trait]
    impl Conn for TestConn {
        async fn connect(&self, _: SocketAddr) -> webrtc_util::Result<()> {
            Ok(())
        }
        async fn recv(&self, buffer: &mut [u8]) -> webrtc_util::Result<usize> {
            if let Some(packet) = self.packet.lock().unwrap().take() {
                buffer[..packet.len()].copy_from_slice(&packet);
                Ok(packet.len())
            } else {
                Ok(0)
            }
        }
        async fn recv_from(&self, _: &mut [u8]) -> webrtc_util::Result<(usize, SocketAddr)> {
            unreachable!()
        }
        async fn send(&self, bytes: &[u8]) -> webrtc_util::Result<usize> {
            struct SendGuard<'a>(&'a AtomicBool);
            impl Drop for SendGuard<'_> {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::SeqCst);
                }
            }
            self.sending.store(true, Ordering::SeqCst);
            let _guard = SendGuard(&self.sending);
            self.send_entered.notify_one();
            self.sent.lock().unwrap().push(bytes.to_vec());
            if let Some(gate) = &self.send_gate {
                gate.acquire().await.unwrap().forget();
            }
            self.send_result
                .lock()
                .unwrap()
                .take()
                .unwrap_or(Ok(bytes.len()))
        }
        async fn send_to(&self, _: &[u8], _: SocketAddr) -> webrtc_util::Result<usize> {
            unreachable!()
        }
        fn local_addr(&self) -> webrtc_util::Result<SocketAddr> {
            Ok("127.0.0.1:1".parse().unwrap())
        }
        fn remote_addr(&self) -> Option<SocketAddr> {
            Some("127.0.0.1:2".parse().unwrap())
        }
        async fn close(&self) -> webrtc_util::Result<()> {
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
            self
        }
    }

    fn control_listener(conns: Vec<(SocketAddr, ArcConn)>) -> LanMouseListener {
        let (listen_tx, listen_rx) = channel();
        let (request_port_change, _) = tokio::sync::watch::channel(None);
        let cancellation = CancellationToken::new();
        let task_cancel = cancellation.clone();
        LanMouseListener {
            listen_rx,
            listen_tx,
            listen_task: spawn_local(async move {
                task_cancel.cancelled().await;
            }),
            cancellation,
            conns: Rc::new(RefCell::new(conns)),
            request_port_change,
            port: Rc::new(Cell::new(2)),
            authentication_notices: Default::default(),
        }
    }

    #[tokio::test]
    async fn verifier_reports_exact_leaf_without_accept_error_matching_or_chain_assertions() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let server = Certificate::generate_self_signed(vec![]).unwrap();
                let peer = Certificate::generate_self_signed(vec![]).unwrap();
                let other = Certificate::generate_self_signed(vec![]).unwrap();
                let peer_bytes = peer.certificate[0].as_ref().to_vec();
                let other_bytes = other.certificate[0].as_ref().to_vec();
                let keys = Arc::new(RwLock::new(HashMap::new()));
                let verifier = Arc::new(Mutex::new(None));
                let captured = verifier.clone();
                let binder: ListenerBinder = Rc::new(move |port, cfg| {
                    *captured.lock().unwrap() = cfg.verify_peer_certificate.clone();
                    async move { bind_dtls(port, &cfg).await }.boxed_local()
                });
                let mut listener =
                    LanMouseListener::new_with_binder(0, server, keys.clone(), binder)
                        .await
                        .unwrap();
                let verify: VerifyPeerCertificateFn = verifier.lock().unwrap().take().unwrap();
                assert!(verify(&[], &[]).is_err());
                assert!(verify(&[peer_bytes.clone(), other_bytes.clone()], &[]).is_err());
                assert_eq!(
                    listener.authentication_notices().next().await,
                    crypto::certificate_fingerprint(&peer)
                );
                // Leaf authorization accepts an accompanying chain, but an authorized
                // intermediate never authorizes a different leaf.
                keys.write()
                    .unwrap()
                    .insert(crypto::certificate_fingerprint(&peer), "peer".into());
                assert!(verify(&[peer_bytes.clone(), other_bytes.clone()], &[]).is_ok());
                assert!(verify(&[other_bytes, peer_bytes], &[]).is_err());
                assert_eq!(
                    listener.authentication_notices().next().await,
                    crypto::certificate_fingerprint(&other)
                );
                // No failed accept has occurred; reporting is directly from verification.
                assert!(
                    tokio::time::timeout(Duration::from_millis(10), listener.next())
                        .await
                        .is_err()
                );
                listener.terminate().await;
            })
            .await;
    }

    #[tokio::test]
    async fn ephemeral_port_is_shared_by_bound_families_and_reported_after_switch() {
        tokio::task::LocalSet::new().run_until(async {
            let certificate = Certificate::generate_self_signed(vec![]).unwrap();
            let cfg = Config { certificates: vec![certificate.clone()], ..Default::default() };
            let listeners = bind_dtls(0, &cfg).await.unwrap();

            let first = listeners[0].addr().await.unwrap().port();
            assert_ne!(first, 0);
            for listener in &listeners { assert_eq!(listener.addr().await.unwrap().port(), first); }
            // Verify both families functionally, whether the OS uses one dual-stack
            // socket or two v6-only/v4 sockets. Socket count is not the contract.
            for ip in [IpAddr::V4(Ipv4Addr::LOCALHOST), IpAddr::V6(Ipv6Addr::LOCALHOST)] {
                let bind_ip = if ip.is_ipv4() { IpAddr::V4(Ipv4Addr::UNSPECIFIED) } else { IpAddr::V6(Ipv6Addr::UNSPECIFIED) };
                let socket = Arc::new(tokio::net::UdpSocket::bind(SocketAddr::new(bind_ip, 0)).await.unwrap());
                socket.connect(SocketAddr::new(ip, first)).await.unwrap();
                let client_cfg = Config {
                    certificates: vec![certificate.clone()],
                    insecure_skip_verify: true,
                    ..Default::default()
                };
                let (client, server) = tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::join!(DTLSConn::new(socket, client_cfg, true, None), accept_any(&listeners))
                }).await.expect("both address families must complete a DTLS handshake");
                let client = client.unwrap();
                let (server, _) = server.unwrap();
                client.close().await.unwrap();
                server.close().await.unwrap();
            }
            close_listeners(listeners).await;
            let mut listener = LanMouseListener::new(0, certificate, Default::default()).await.unwrap();
            let initial = listener.port();
            assert_ne!(initial, 0);
            listener.port_requests().send_replace(Some(0));
            let actual = match tokio::time::timeout(Duration::from_secs(3), listener.next()).await.unwrap() {
                Some(ListenEvent::PortChanged(Ok(port))) => port,
                _ => panic!("ephemeral switch failed"),
            };
            assert_ne!(actual, 0);
            assert_ne!(actual, initial); // Old sockets remained bound during replacement.
            assert_eq!(actual, listener.port());
            listener.port_requests().send_replace(Some(actual));
            assert!(matches!(tokio::time::timeout(Duration::from_secs(3), listener.next()).await.unwrap(), Some(ListenEvent::PortChanged(Ok(port))) if port == actual));
            listener.terminate().await;
        }).await;
    }

    #[tokio::test]
    async fn inconsistent_bound_ports_are_closed_without_replacing_running_listener() {
        use std::sync::atomic::AtomicUsize;
        tokio::task::LocalSet::new()
            .run_until(async {
                let initial = Arc::new(AtomicUsize::new(0));
                let rejected = Arc::new(AtomicUsize::new(0));
                let binder: ListenerBinder = {
                    let (initial, rejected) = (initial.clone(), rejected.clone());
                    Rc::new(move |port, _| {
                        let (initial, rejected) = (initial.clone(), rejected.clone());
                        async move {
                            if port == 4000 {
                                Ok(vec![
                                    Box::new(TrackedListener(initial, 4000)) as Box<dyn Listener>
                                ])
                            } else {
                                Ok(vec![
                                    Box::new(TrackedListener(rejected.clone(), 5000))
                                        as Box<dyn Listener>,
                                    Box::new(TrackedListener(rejected, 5001)) as Box<dyn Listener>,
                                ])
                            }
                        }
                        .boxed_local()
                    })
                };
                let mut listener = LanMouseListener::new_with_binder(
                    4000,
                    Certificate::generate_self_signed(vec![]).unwrap(),
                    Default::default(),
                    binder,
                )
                .await
                .unwrap();
                listener.port_requests().send_replace(Some(5000));
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(3), listener.next())
                        .await
                        .unwrap(),
                    Some(ListenEvent::PortChanged(Err(
                        ListenerCreationError::InvalidBoundPort
                    )))
                ));
                assert_eq!(listener.port(), 4000);
                assert_eq!(initial.load(Ordering::SeqCst), 0);
                assert_eq!(rejected.load(Ordering::SeqCst), 2);
                listener.terminate().await;
                assert_eq!(initial.load(Ordering::SeqCst), 1);
            })
            .await;
    }

    #[tokio::test]
    async fn port_requests_coalesce_without_blocking_real_input_dispatch() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let conn: ArcConn = Arc::new(TestConn::new(None));
                let addr = "127.0.0.1:2".parse().unwrap();
                let listener = control_listener(vec![(addr, conn.clone())]);
                let incoming = listener.listen_tx.clone();
                let mut requested = listener.port_requests().subscribe();
                let mut emulation = crate::emulation::Emulation::new(
                    Some(input_emulation::Backend::Dummy),
                    Default::default(),
                    listener,
                    (false, 1.0),
                );
                for port in 4000..5000 {
                    emulation.request_port_change(port);
                }
                requested.changed().await.unwrap();
                assert_eq!(*requested.borrow_and_update(), Some(4999));
                assert!(!requested.has_changed().unwrap());
                // No completion is delivered yet. The real dispatcher still has
                // to process messages rather than waiting for the port operation.
                incoming
                    .send(ListenEvent::Msg {
                        addr,
                        conn,
                        event: ProtoEvent::Input(input_event::Event::Clipboard(
                            input_event::ClipboardEvent::Text("during port change".into()),
                        )),
                    })
                    .unwrap_or_else(|_| panic!());
                tokio::time::timeout(Duration::from_millis(100), async {
                    loop {
                        if let crate::emulation::EmulationEvent::ClipboardReceived { .. } =
                            emulation.event().await
                        {
                            break;
                        }
                    }
                })
                .await
                .unwrap();
                incoming
                    .send(ListenEvent::PortChanged(Err(
                        ListenerCreationError::BindTimeout,
                    )))
                    .unwrap_or_else(|_| panic!());
                tokio::time::timeout(Duration::from_millis(100), async {
                    loop {
                        if let crate::emulation::EmulationEvent::PortChanged(Err(
                            ListenerCreationError::BindTimeout,
                        )) = emulation.event().await
                        {
                            break;
                        }
                    }
                })
                .await
                .unwrap();
                tokio::time::timeout(Duration::from_millis(100), emulation.terminate())
                    .await
                    .unwrap();
            })
            .await;
    }

    struct SlowListener(Arc<AtomicBool>);
    #[async_trait::async_trait]
    impl Listener for SlowListener {
        async fn accept(&self) -> webrtc_util::Result<(ArcConn, SocketAddr)> {
            std::future::pending().await
        }
        async fn addr(&self) -> webrtc_util::Result<SocketAddr> {
            Ok("127.0.0.1:2".parse().unwrap())
        }
        async fn close(&self) -> webrtc_util::Result<()> {
            struct Guard(Arc<AtomicBool>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::SeqCst);
                }
            }
            self.0.store(true, Ordering::SeqCst);
            let _guard = Guard(self.0.clone());
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn port_cleanup_has_independent_parallel_deadline_and_owner_cancellation() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let one = Arc::new(AtomicBool::new(false));
                let two = Arc::new(AtomicBool::new(false));
                let mut cleanup = Some(OwnedListenerTask(spawn_local(close_listeners(vec![
                    Box::new(SlowListener(one.clone())),
                    Box::new(SlowListener(two.clone())),
                ]))));
                tokio::task::yield_now().await;
                assert!(one.load(Ordering::SeqCst) && two.load(Ordering::SeqCst));
                // No polling of the manager; both deadlines still finish together.
                tokio::time::sleep(Duration::from_millis(1100)).await;
                assert!(!one.load(Ordering::SeqCst) && !two.load(Ordering::SeqCst));
                listener_task_completed(&mut cleanup).await.unwrap();
                drop(cleanup);
                let task = OwnedListenerTask(spawn_local(close_listeners(vec![Box::new(
                    SlowListener(one.clone()),
                )])));
                tokio::task::yield_now().await;
                assert!(one.load(Ordering::SeqCst));
                drop(task);
                tokio::task::yield_now().await;
                assert!(!one.load(Ordering::SeqCst));
            })
            .await;
    }

    struct TrackedListener(Arc<std::sync::atomic::AtomicUsize>, u16);
    #[async_trait::async_trait]
    impl Listener for TrackedListener {
        async fn accept(&self) -> webrtc_util::Result<(ArcConn, SocketAddr)> {
            std::future::pending().await
        }
        async fn addr(&self) -> webrtc_util::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], self.1)))
        }
        async fn close(&self) -> webrtc_util::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn port_switch_discards_obsolete_bind_and_applies_latest_pending_request() {
        use std::sync::atomic::AtomicUsize;
        tokio::task::LocalSet::new()
            .run_until(async {
                let started = Arc::new(tokio::sync::Notify::new());
                let gate = Arc::new(tokio::sync::Semaphore::new(0));
                let initial = Arc::new(AtomicUsize::new(0));
                let obsolete = Arc::new(AtomicUsize::new(0));
                let latest = Arc::new(AtomicUsize::new(0));
                let calls = Arc::new(AtomicUsize::new(0));
                let binder: ListenerBinder = {
                    let (started, gate, initial, obsolete, latest, calls) = (
                        started.clone(),
                        gate.clone(),
                        initial.clone(),
                        obsolete.clone(),
                        latest.clone(),
                        calls.clone(),
                    );
                    Rc::new(move |port, _| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let (started, gate) = (started.clone(), gate.clone());
                        let closed = match port {
                            4000 => initial.clone(),
                            5000 => obsolete.clone(),
                            6000 => latest.clone(),
                            _ => panic!("intermediate port was not coalesced"),
                        };
                        async move {
                            if port == 5000 {
                                started.notify_one();
                                gate.acquire().await.unwrap().forget();
                            }
                            Ok(vec![
                                Box::new(TrackedListener(closed, port)) as Box<dyn Listener>
                            ])
                        }
                        .boxed_local()
                    })
                };
                let mut listener = LanMouseListener::new_with_binder(
                    4000,
                    Certificate::generate_self_signed(vec![]).unwrap(),
                    Default::default(),
                    binder,
                )
                .await
                .unwrap();
                let requests = listener.port_requests();
                requests.send_replace(Some(5000));
                tokio::time::timeout(Duration::from_millis(100), started.notified())
                    .await
                    .unwrap();
                for port in 5001..=6000 {
                    requests.send_replace(Some(port));
                }
                gate.add_permits(1);
                // No completion for the obsolete successful bind is published.
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(1), listener.next())
                        .await
                        .unwrap(),
                    Some(ListenEvent::PortChanged(Ok(6000)))
                ));
                assert_eq!(calls.load(Ordering::SeqCst), 3);
                assert_eq!(obsolete.load(Ordering::SeqCst), 1);
                tokio::task::yield_now().await;
                assert_eq!(initial.load(Ordering::SeqCst), 1);
                assert_eq!(latest.load(Ordering::SeqCst), 0);
                // Restoring the running port supersedes an unfinished bind.
                requests.send_replace(Some(5000));
                tokio::time::timeout(Duration::from_millis(100), started.notified())
                    .await
                    .unwrap();
                requests.send_replace(Some(6000));
                gate.add_permits(1);
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(1), listener.next())
                        .await
                        .unwrap(),
                    Some(ListenEvent::PortChanged(Ok(6000)))
                ));
                assert_eq!(calls.load(Ordering::SeqCst), 4);
                assert_eq!(obsolete.load(Ordering::SeqCst), 2);
                assert_eq!(latest.load(Ordering::SeqCst), 0);
                listener.terminate().await;
                assert_eq!(latest.load(Ordering::SeqCst), 1);
            })
            .await;
    }

    #[tokio::test]
    async fn port_binding_timeout_keeps_current_listener_and_allows_retry() {
        use std::sync::atomic::AtomicUsize;
        tokio::task::LocalSet::new()
            .run_until(async {
                let running = Arc::new(AtomicBool::new(false));
                let started = Arc::new(tokio::sync::Notify::new());
                let initial = Arc::new(AtomicUsize::new(0));
                let replacement = Arc::new(AtomicUsize::new(0));
                let binder: ListenerBinder = {
                    let (running, started, initial, replacement) = (
                        running.clone(),
                        started.clone(),
                        initial.clone(),
                        replacement.clone(),
                    );
                    Rc::new(move |port, _| {
                        let (running, started) = (running.clone(), started.clone());
                        let closed = if port == 4000 {
                            initial.clone()
                        } else {
                            replacement.clone()
                        };
                        async move {
                            if port == 5000 {
                                struct Guard(Arc<AtomicBool>);
                                impl Drop for Guard {
                                    fn drop(&mut self) {
                                        self.0.store(false, Ordering::SeqCst);
                                    }
                                }
                                running.store(true, Ordering::SeqCst);
                                let _guard = Guard(running);
                                started.notify_one();
                                std::future::pending::<()>().await;
                            }
                            Ok(vec![
                                Box::new(TrackedListener(closed, port)) as Box<dyn Listener>
                            ])
                        }
                        .boxed_local()
                    })
                };
                let mut listener = LanMouseListener::new_with_binder(
                    4000,
                    Certificate::generate_self_signed(vec![]).unwrap(),
                    Default::default(),
                    binder,
                )
                .await
                .unwrap();
                let requests = listener.port_requests();
                requests.send_replace(Some(5000));
                tokio::time::timeout(Duration::from_millis(100), started.notified())
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(2100)).await;
                assert!(!running.load(Ordering::SeqCst));
                assert!(matches!(
                    listener.next().await,
                    Some(ListenEvent::PortChanged(Err(
                        ListenerCreationError::BindTimeout
                    )))
                ));
                assert_eq!(initial.load(Ordering::SeqCst), 0);
                requests.send_replace(Some(6000));
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(1), listener.next())
                        .await
                        .unwrap(),
                    Some(ListenEvent::PortChanged(Ok(6000)))
                ));
                listener.terminate().await;
                assert_eq!(initial.load(Ordering::SeqCst), 1);
                assert_eq!(replacement.load(Ordering::SeqCst), 1);
            })
            .await;
    }

    #[tokio::test]
    async fn port_switch_real_sockets_keep_old_port_on_failure_and_release_on_shutdown() {
        tokio::task::LocalSet::new().run_until(async {
            let initial_socket = tokio::net::UdpSocket::bind("[::]:0").await.unwrap();
            let initial_port = initial_socket.local_addr().unwrap().port();
            drop(initial_socket);
            let mut listener = LanMouseListener::new(initial_port, Certificate::generate_self_signed(vec![]).unwrap(), Default::default()).await.unwrap();
            let requests = listener.port_requests();
            // Holding both wildcard families ensures the requested port cannot bind.
            let occupied_v6 = tokio::net::UdpSocket::bind("[::]:0").await.unwrap();
            let occupied_port = occupied_v6.local_addr().unwrap().port();
            let _occupied_v4 = tokio::net::UdpSocket::bind(SocketAddr::from(([0,0,0,0], occupied_port))).await.ok();
            requests.send_replace(Some(occupied_port));
            let failure = tokio::time::timeout(Duration::from_secs(3), listener.next()).await.unwrap();
            assert!(matches!(failure, Some(ListenEvent::PortChanged(Err(_)))));
            // Returning to the running port is a no-op success, superseding the
            // failed target without closing the original sockets.
            assert!(tokio::net::UdpSocket::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), initial_port)).await.is_err());
            requests.send_replace(Some(initial_port));
            assert!(matches!(tokio::time::timeout(Duration::from_secs(3), listener.next()).await.unwrap(), Some(ListenEvent::PortChanged(Ok(p))) if p == initial_port));
            let candidate = tokio::net::UdpSocket::bind("[::]:0").await.unwrap();
            let new_port = candidate.local_addr().unwrap().port();
            drop(candidate);
            requests.send_replace(Some(new_port));
            assert!(matches!(tokio::time::timeout(Duration::from_secs(3), listener.next()).await.unwrap(), Some(ListenEvent::PortChanged(Ok(p))) if p == new_port));
            listener.terminate().await;
            // The dependency's UDP receive task exits after close, not at the
            // exact instant the API returns. Verify actual socket release.
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if let Ok(socket) = tokio::net::UdpSocket::bind(SocketAddr::from(([0,0,0,0], new_port))).await {
                        drop(socket);
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
        }).await;
    }

    #[tokio::test]
    async fn control_replies_keep_fifo_and_allow_other_peer_during_slow_send() {
        use crate::control_network::{ControlJobs, ControlSendError};
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let slow = Arc::new(TestConn {
            send_gate: Some(gate.clone()),
            ..TestConn::new(None)
        });
        let healthy = Arc::new(TestConn::new(None));
        let addr = "127.0.0.1:2".parse().unwrap();
        let other = "127.0.0.1:3".parse().unwrap();
        let mut jobs = ControlJobs::default();
        jobs.submit(addr, slow.clone(), ProtoEvent::Ack(0)).unwrap();
        jobs.submit(other, healthy.clone(), ProtoEvent::Pong(true))
            .unwrap();
        for serial in 1..=32 {
            jobs.submit(addr, slow.clone(), ProtoEvent::Ack(serial))
                .unwrap();
        }
        assert_eq!(jobs.sizes(), (2, 32));
        assert!(matches!(
            jobs.submit(addr, slow.clone(), ProtoEvent::Leave(0, 0.5)),
            Err(ControlSendError::Busy)
        ));
        let completed = tokio::time::timeout(Duration::from_millis(100), jobs.completed())
            .await
            .unwrap();
        assert_eq!(completed.addr, other);
        completed.result.unwrap();
        // Completion is independent of a slow peer. Every accepted Ack must
        // retain its position; no latest-value replacement is valid here.
        gate.add_permits(33);
        for _ in 0..33 {
            jobs.completed().await.result.unwrap();
        }
        let expected: Vec<_> = (0..=32)
            .map(|serial| {
                let (buf, len): ([u8; lan_mouse_proto::MAX_EVENT_SIZE], usize) =
                    ProtoEvent::Ack(serial).into();
                buf[..len].to_vec()
            })
            .collect();
        assert_eq!(*slow.sent.lock().unwrap(), expected);
        assert_eq!(jobs.sizes(), (0, 0));
        // Multiple peers still cannot exceed the global capacity.
        let mut full = ControlJobs::default();
        for port in 2..6 {
            let conn = Arc::new(TestConn {
                send_gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
                ..TestConn::new(None)
            });
            for serial in 0..=32 {
                full.submit(
                    SocketAddr::from(([127, 0, 0, 1], port)),
                    conn.clone(),
                    ProtoEvent::Ack(serial),
                )
                .unwrap();
            }
        }
        assert_eq!(full.sizes(), (4, 128));
        assert!(matches!(
            full.submit(
                "127.0.0.1:6".parse().unwrap(),
                healthy,
                ProtoEvent::Pong(true)
            ),
            Err(ControlSendError::Busy)
        ));
    }

    #[tokio::test]
    async fn control_deadline_runs_without_manager_polling_and_discards_failed_tail() {
        use crate::control_network::{ControlJobs, ControlSendError};
        let slow = Arc::new(TestConn {
            send_gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
            ..TestConn::new(None)
        });
        let addr = "127.0.0.1:2".parse().unwrap();
        let mut jobs = ControlJobs::default();
        jobs.submit(addr, slow.clone(), ProtoEvent::Ack(0)).unwrap();
        jobs.submit(addr, slow.clone(), ProtoEvent::Leave(0, 0.5))
            .unwrap();
        slow.send_entered.notified().await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(!slow.sending.load(Ordering::SeqCst));
        assert!(matches!(
            jobs.completed().await.result,
            Err(ControlSendError::Timeout)
        ));
        assert_eq!(slow.sent.lock().unwrap().len(), 1);
        assert_eq!(jobs.sizes(), (0, 0));
        assert!(!slow.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn control_queue_deadline_prevents_late_send_to_waiting_peer() {
        use crate::control_network::{ControlJobs, ControlSendError};
        let mut jobs = ControlJobs::default();
        for port in 2..6 {
            let conn = Arc::new(TestConn {
                send_gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
                ..TestConn::new(None)
            });
            jobs.submit(
                SocketAddr::from(([127, 0, 0, 1], port)),
                conn,
                ProtoEvent::Ack(0),
            )
            .unwrap();
        }
        let waiting = Arc::new(TestConn::new(None));
        jobs.submit(
            "127.0.0.1:6".parse().unwrap(),
            waiting.clone(),
            ProtoEvent::Leave(0, 0.5),
        )
        .unwrap();
        // All slots are occupied; this packet's own deadline starts at admission.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        for _ in 0..5 {
            assert!(matches!(
                jobs.completed().await.result,
                Err(ControlSendError::Timeout)
            ));
        }
        assert!(waiting.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn control_replacement_cancels_old_work_and_drop_aborts_sends() {
        use crate::control_network::{ControlJobs, ControlSendError};
        let slow = Arc::new(TestConn {
            send_gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
            ..TestConn::new(None)
        });
        let replacement: ArcConn = Arc::new(TestConn::new(None));
        let addr = "127.0.0.1:2".parse().unwrap();
        let mut jobs = ControlJobs::default();
        jobs.submit(addr, slow.clone(), ProtoEvent::Ack(0)).unwrap();
        jobs.submit(addr, slow.clone(), ProtoEvent::Leave(0, 0.5))
            .unwrap();
        jobs.submit(
            addr,
            replacement.clone(),
            ProtoEvent::Hello {
                commit: *b"new-test",
            },
        )
        .unwrap();
        slow.send_entered.notified().await;
        jobs.cancel_stale(addr, Some(&replacement));
        assert!(matches!(
            jobs.completed().await.result,
            Err(ControlSendError::Canceled)
        ));
        let completed = jobs.completed().await;
        assert!(Arc::ptr_eq(&completed.conn, &replacement));
        completed.result.unwrap();
        assert_eq!(slow.sent.lock().unwrap().len(), 1);
        jobs.submit(addr, slow.clone(), ProtoEvent::Ack(1)).unwrap();
        slow.send_entered.notified().await;
        drop(jobs);
        tokio::task::yield_now().await;
        assert!(!slow.sending.load(Ordering::SeqCst));
        assert!(!slow.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn control_failures_remove_only_current_session_and_notify_release() {
        use crate::control_network::{ControlCompletion, ControlJobs, ControlSendError};
        tokio::task::LocalSet::new().run_until(async {
            for short in [false, true] {
                let failed = Arc::new(TestConn::new(None));
                *failed.send_result.lock().unwrap() = Some(if short { Ok(0) } else {
                    Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "refused").into())
                });
                let addr = "127.0.0.1:2".parse().unwrap();
                let other = "127.0.0.1:3".parse().unwrap();
                let healthy: ArcConn = Arc::new(TestConn::new(None));
                let mut listener = control_listener(vec![(addr, failed.clone()), (other, healthy.clone())]);
                let mut jobs = ControlJobs::default();
                listener.reply(&mut jobs, addr, ProtoEvent::Ack(0));
                let completed = jobs.completed().await;
                assert!(if short { matches!(completed.result, Err(ControlSendError::Incomplete { .. })) }
                    else { matches!(completed.result, Err(ControlSendError::Transport(_))) });
                listener.finish_control_reply(completed);
                assert!(!listener.has_connection(addr));
                assert!(listener.is_current(other, &healthy));
                assert!(matches!(listener.next().await, Some(ListenEvent::Disconnected { addr: a }) if a == addr));
                tokio::task::yield_now().await;
                assert!(failed.closed.load(Ordering::SeqCst));
                // A stale error cannot remove a newer connection at the address.
                listener.conns.borrow_mut().push((addr, healthy.clone()));
                listener.finish_control_reply(ControlCompletion {
                    addr, conn: failed.clone(), result: Err(ControlSendError::Timeout),
                });
                assert!(listener.is_current(addr, &healthy));
            }
        }).await;
    }

    #[tokio::test]
    async fn control_overload_disconnects_only_rejected_session() {
        use crate::control_network::ControlJobs;
        tokio::task::LocalSet::new().run_until(async {
            let slow = Arc::new(TestConn {
                send_gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
                ..TestConn::new(None)
            });
            let rejected = Arc::new(TestConn::new(None));
            let addr = "127.0.0.1:2".parse().unwrap();
            let other = "127.0.0.1:3".parse().unwrap();
            let mut listener = control_listener(vec![(addr, slow.clone()), (other, rejected.clone())]);
            let mut jobs = ControlJobs::default();
            for serial in 0..=32 {
                jobs.submit(addr, slow.clone(), ProtoEvent::Ack(serial)).unwrap();
            }
            listener.reply(&mut jobs, addr, ProtoEvent::Leave(0, 0.5));
            assert!(!listener.has_connection(addr));
            assert!(listener.has_connection(other));
            assert!(matches!(listener.next().await, Some(ListenEvent::Disconnected { addr: a }) if a == addr));
            tokio::task::yield_now().await;
            assert!(slow.closed.load(Ordering::SeqCst));
            assert!(!rejected.closed.load(Ordering::SeqCst));
            assert!(rejected.sent.lock().unwrap().is_empty());
        }).await;
    }

    #[tokio::test]
    async fn control_wait_does_not_block_real_listen_dispatch_or_termination() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let slow = Arc::new(TestConn {
                    send_gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
                    ..TestConn::new(None)
                });
                let addr = "127.0.0.1:2".parse().unwrap();
                let listener = control_listener(vec![(addr, slow.clone())]);
                let incoming = listener.listen_tx.clone();
                let mut emulation = crate::emulation::Emulation::new(
                    Some(input_emulation::Backend::Dummy),
                    Default::default(),
                    listener,
                    (false, 1.0),
                );
                incoming
                    .send(ListenEvent::Msg {
                        addr,
                        conn: slow.clone(),
                        event: ProtoEvent::Hello {
                            commit: *b"peertest",
                        },
                    })
                    .unwrap_or_else(|_| panic!("listener stopped"));
                tokio::time::timeout(Duration::from_millis(100), slow.send_entered.notified())
                    .await
                    .unwrap();
                // Actual ListenTask must continue consuming messages while the
                // Hello response is held inside Conn::send.
                incoming
                    .send(ListenEvent::Msg {
                        addr,
                        conn: slow.clone(),
                        event: ProtoEvent::Input(input_event::Event::Clipboard(
                            input_event::ClipboardEvent::Text("after blocked hello".into()),
                        )),
                    })
                    .unwrap_or_else(|_| panic!("listener stopped"));
                tokio::time::timeout(Duration::from_millis(100), async {
                    loop {
                        if let crate::emulation::EmulationEvent::ClipboardReceived {
                            event: input_event::ClipboardEvent::Text(text),
                            ..
                        } = emulation.event().await
                        {
                            assert_eq!(text, "after blocked hello");
                            break;
                        }
                    }
                })
                .await
                .unwrap();
                emulation.send_leave_event(addr, 0.5);
                tokio::time::timeout(Duration::from_millis(100), emulation.terminate())
                    .await
                    .unwrap();
                assert!(!slow.sending.load(Ordering::SeqCst));
                assert!(slow.closed.load(Ordering::SeqCst));
            })
            .await;
    }

    #[tokio::test]
    async fn clipboard_reply_reports_missing_refused_short_and_valid_sends() {
        let event = ProtoEvent::Input(input_event::Event::Clipboard(
            input_event::ClipboardEvent::Text("test clipboard".into()),
        ));
        assert!(matches!(
            send_clipboard_reply(None, event.clone()).await,
            Err(ClipboardSendError::NotConnected)
        ));
        let conn = Arc::new(TestConn::new(None));
        *conn.send_result.lock().unwrap() = Some(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "test refusal",
        )
        .into()));
        assert!(matches!(
            send_clipboard_reply(Some(conn.clone()), event.clone()).await,
            Err(ClipboardSendError::Transport(_))
        ));
        *conn.send_result.lock().unwrap() = Some(Ok(0));
        assert!(
            matches!(send_clipboard_reply(Some(conn.clone()), event.clone()).await,
            Err(ClipboardSendError::Incomplete { sent: 0, expected }) if expected > 0)
        );
        send_clipboard_reply(Some(conn.clone()), event.clone())
            .await
            .unwrap();
        assert_eq!(
            conn.sent.lock().unwrap().last().unwrap(),
            &lan_mouse_proto::encode_clipboard_event(&event).unwrap()
        );
        let before = conn.sent.lock().unwrap().len();
        let oversized = ProtoEvent::Input(input_event::Event::Clipboard(
            input_event::ClipboardEvent::Text("x".repeat(lan_mouse_proto::MAX_CLIPBOARD_SIZE + 1)),
        ));
        assert!(matches!(
            send_clipboard_reply(Some(conn.clone()), oversized).await,
            Err(ClipboardSendError::Encode(
                lan_mouse_proto::ProtocolError::ClipboardTooLarge(_)
            ))
        ));
        assert_eq!(conn.sent.lock().unwrap().len(), before);
    }

    fn clipboard_job_request(
        addr: SocketAddr,
        conn: Option<ArcConn>,
        text: String,
    ) -> crate::clipboard_network::ClipboardRequest {
        crate::clipboard_network::ClipboardRequest {
            addr,
            conn,
            event: input_event::ClipboardEvent::Text(text),
            generation: 0,
            cancellation: CancellationToken::new(),
            outgoing: None,
            session_cancellation: None,
        }
    }

    #[tokio::test]
    async fn clipboard_jobs_bound_active_pending_and_keep_latest_per_peer() {
        let mut jobs = crate::clipboard_network::ClipboardJobs::default();
        for port in 1..=36 {
            jobs.submit(clipboard_job_request(
                format!("127.0.0.1:{port}").parse().unwrap(),
                None,
                "test".into(),
            ))
            .unwrap_or_else(|_| panic!("unexpected admission failure"));
        }
        assert_eq!(jobs.sizes(), (4, 32));
        assert!(
            jobs.submit(clipboard_job_request(
                "127.0.0.1:37".parse().unwrap(),
                None,
                "new peer".into()
            ))
            .is_err()
        );
        jobs.submit(clipboard_job_request(
            "127.0.0.1:36".parse().unwrap(),
            None,
            "latest".into(),
        ))
        .unwrap_or_else(|_| panic!("existing pending peer must coalesce"));
        assert_eq!(jobs.sizes(), (4, 32));
    }

    #[tokio::test]
    async fn clipboard_jobs_slow_send_allows_timer_and_coalesces_a_thousand_edits() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let conn = Arc::new(TestConn {
            send_gate: Some(gate.clone()),
            ..TestConn::new(None)
        });
        let addr = "127.0.0.1:2".parse().unwrap();
        let mut jobs = crate::clipboard_network::ClipboardJobs::default();
        jobs.submit(clipboard_job_request(
            addr,
            Some(conn.clone()),
            "first".into(),
        ))
        .unwrap_or_else(|_| panic!());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), jobs.completed())
                .await
                .is_err()
        );
        assert_eq!(conn.sent.lock().unwrap().len(), 1);
        for index in 1..=1000 {
            jobs.submit(clipboard_job_request(
                addr,
                Some(conn.clone()),
                format!("latest-{index}"),
            ))
            .unwrap_or_else(|_| panic!());
        }
        assert_eq!(jobs.sizes(), (1, 1));
        gate.add_permits(2);
        jobs.completed().await.result.unwrap();
        jobs.completed().await.result.unwrap();
        assert_eq!(jobs.sizes(), (0, 0));
        let sent = conn.sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(
            sent.last().unwrap(),
            &lan_mouse_proto::encode_clipboard_event(&ProtoEvent::Input(
                input_event::Event::Clipboard(input_event::ClipboardEvent::Text(
                    "latest-1000".into()
                ))
            ))
            .unwrap()
        );
    }

    #[tokio::test]
    async fn clipboard_jobs_timeout_does_not_hold_up_another_peer_or_close_input_connection() {
        let conn = Arc::new(TestConn {
            send_gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
            ..TestConn::new(None)
        });
        let healthy = Arc::new(TestConn::new(None));
        let slow_addr = "127.0.0.1:2".parse().unwrap();
        let healthy_addr = "127.0.0.1:3".parse().unwrap();
        let mut jobs = crate::clipboard_network::ClipboardJobs::default();
        jobs.submit(clipboard_job_request(
            slow_addr,
            Some(conn.clone()),
            "slow".into(),
        ))
        .unwrap_or_else(|_| panic!());
        jobs.submit(clipboard_job_request(
            healthy_addr,
            Some(healthy),
            "healthy".into(),
        ))
        .unwrap_or_else(|_| panic!());
        let first = tokio::time::timeout(Duration::from_millis(500), jobs.completed())
            .await
            .unwrap();
        assert_eq!(first.addr, healthy_addr);
        first.result.unwrap();
        conn.send_entered.notified().await;
        // No polling of the manager during this wait: a separate dispatch
        // await must not prevent the background send's deadline from firing.
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert!(!conn.sending.load(Ordering::SeqCst));
        let slow = tokio::time::timeout(Duration::from_secs(3), jobs.completed())
            .await
            .unwrap();
        assert!(matches!(slow.result, Err(ClipboardSendError::Timeout)));
        assert!(!conn.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn clipboard_jobs_cancel_only_old_session_and_preserve_new_pending_connection() {
        let old = Arc::new(TestConn {
            send_gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
            ..TestConn::new(None)
        });
        let new = Arc::new(TestConn::new(None));
        let addr = "127.0.0.1:2".parse().unwrap();
        let mut jobs = crate::clipboard_network::ClipboardJobs::default();
        jobs.submit(clipboard_job_request(addr, Some(old.clone()), "old".into()))
            .unwrap_or_else(|_| panic!());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), jobs.completed())
                .await
                .is_err()
        );
        let new_conn: ArcConn = new.clone();
        jobs.submit(clipboard_job_request(
            addr,
            Some(new_conn.clone()),
            "new".into(),
        ))
        .unwrap_or_else(|_| panic!());
        jobs.cancel_stale(addr, Some(&new_conn));
        assert!(matches!(
            jobs.completed().await.result,
            Err(ClipboardSendError::Canceled)
        ));
        jobs.completed().await.result.unwrap();
        assert_eq!(old.sent.lock().unwrap().len(), 1);
        assert_eq!(new.sent.lock().unwrap().len(), 1);
        assert!(!old.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn old_reader_cannot_deliver_or_disconnect_replacement() {
        let addr = "127.0.0.1:2".parse().unwrap();
        let (packet, len): ([u8; MAX_EVENT_SIZE], usize) = ProtoEvent::Ping.into();
        let old = Arc::new(TestConn::new(Some(packet[..len].to_vec())));
        let replacement = Arc::new(TestConn::new(None));
        let current: ArcConn = replacement.clone();
        let conns = Rc::new(RefCell::new(vec![(addr, current.clone())]));
        let (tx, mut rx) = channel();
        read_loop(
            conns.clone(),
            addr,
            old.clone(),
            tx,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(is_current(&conns.borrow(), addr, &current));
        assert!(old.closed.load(Ordering::SeqCst));
        assert!(!replacement.closed.load(Ordering::SeqCst));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn real_end_emits_disconnect_once_and_repeated_cleanup_does_not_panic() {
        let addr = "127.0.0.1:2".parse().unwrap();
        let conn = Arc::new(TestConn::new(None));
        let current: ArcConn = conn.clone();
        let conns = Rc::new(RefCell::new(vec![(addr, current)]));
        let (tx, mut rx) = channel();
        read_loop(
            conns.clone(),
            addr,
            conn.clone(),
            tx.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(
            matches!(rx.recv().await, Some(ListenEvent::Disconnected { addr: actual }) if actual == addr)
        );
        read_loop(conns.clone(), addr, conn, tx, CancellationToken::new())
            .await
            .unwrap();
        assert!(rx.recv().await.is_none());
        assert!(conns.borrow().is_empty());
    }
}
