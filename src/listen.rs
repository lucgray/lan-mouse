use futures::{Stream, StreamExt};
use lan_mouse_proto::{MAX_EVENT_SIZE, ProtoEvent};
use local_channel::mpsc::{Receiver, Sender, channel};
use rustls::pki_types::CertificateDer;
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    rc::Rc,
    sync::{Arc, Mutex, RwLock},
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
}

type ArcConn = Arc<dyn Conn + Send + Sync>;

/// Create a DTLS listener per address family.
///
/// A socket bound to `[::]` accepts IPv4-mapped peers only on platforms
/// with dual-stack sockets (Linux). Windows and macOS default IPv6
/// sockets to `IPV6_V6ONLY`, so an IPv4-only peer would never be able
/// to connect. A separate IPv4 listener is therefore always created;
/// where `[::]` is already dual-stack its bind fails with EADDRINUSE
/// and the attempt is skipped.
async fn bind_dtls(
    port: u16,
    cfg: &Config,
) -> Result<Vec<Box<dyn Listener>>, ListenerCreationError> {
    let mut listeners: Vec<Box<dyn Listener>> = Vec::new();
    let mut last_err = None;
    for ip in [
        IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
    ] {
        match listen(SocketAddr::new(ip, port), cfg.clone()).await {
            Ok(l) => listeners.push(Box::new(l)),
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
    Rejected {
        fingerprint: String,
    },
}

pub(crate) struct LanMouseListener {
    listen_rx: Receiver<ListenEvent>,
    listen_tx: Sender<ListenEvent>,
    listen_task: JoinHandle<()>,
    cancellation: CancellationToken,
    conns: Rc<RefCell<Vec<(SocketAddr, ArcConn)>>>,
    request_port_change: Sender<u16>,
    port_changed: Receiver<Result<u16, ListenerCreationError>>,
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
        let (listen_tx, listen_rx) = channel();
        let (request_port_change, mut request_port_change_rx) = channel();
        let (port_changed_tx, port_changed) = channel();
        let connection_attempts: Arc<Mutex<VecDeque<String>>> = Default::default();

        let authorized = authorized_keys.clone();
        let verify_peer_certificate: Option<VerifyPeerCertificateFn> = {
            let connection_attempts = connection_attempts.clone();
            Some(Arc::new(
                move |certs: &[Vec<u8>], _chains: &[CertificateDer<'static>]| {
                    assert!(certs.len() == 1);
                    let fingerprints = certs
                        .iter()
                        .map(|c| crypto::generate_fingerprint(c))
                        .collect::<Vec<_>>();
                    if authorized
                        .read()
                        .expect("lock")
                        .contains_key(&fingerprints[0])
                    {
                        Ok(())
                    } else {
                        let fingerprint = fingerprints.into_iter().next().expect("fingerprint");
                        connection_attempts
                            .lock()
                            .expect("lock")
                            .push_back(fingerprint);
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

        let mut listeners = bind_dtls(port, &cfg).await?;

        let conns: Rc<RefCell<Vec<(SocketAddr, ArcConn)>>> = Rc::new(RefCell::new(Vec::new()));

        let conns_clone = conns.clone();
        let cancellation = CancellationToken::new();
        let readers_cancel = cancellation.clone();
        let listen_task: JoinHandle<()> = {
            let listen_tx = listen_tx.clone();
            let connection_attempts = connection_attempts.clone();
            spawn_local(async move {
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
                                                if let Some(fingerprint) = connection_attempts.lock().expect("lock").pop_front() {
                                                    listen_tx.send(ListenEvent::Rejected { fingerprint }).expect("channel closed");
                                                }
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
                        port = request_port_change_rx.recv() => {
                            let port = port.expect("channel closed");
                            match bind_dtls(port, &cfg).await {
                                Ok(new_listeners) => {
                                    for l in &listeners {
                                        let _ = l.close().await;
                                    }
                                    listeners = new_listeners;
                                    port_changed_tx.send(Ok(port)).expect("channel closed");
                                }
                                Err(e) => {
                                    log::warn!("unable to change port: {e}");
                                    port_changed_tx.send(Err(e)).expect("channel closed");
                                }
                            };
                        },
                    };
                }
            })
        };

        Ok(Self {
            conns,
            listen_rx,
            listen_tx,
            listen_task,
            cancellation,
            port_changed,
            request_port_change,
        })
    }

    pub(crate) fn is_current(&self, addr: SocketAddr, conn: &ArcConn) -> bool {
        is_current(&self.conns.borrow(), addr, conn)
    }

    pub(crate) fn has_connection(&self, addr: SocketAddr) -> bool {
        self.conns.borrow().iter().any(|(a, _)| *a == addr)
    }

    pub(crate) fn request_port_change(&mut self, port: u16) {
        self.request_port_change.send(port).expect("channel closed");
    }

    pub(crate) async fn port_changed(&mut self) -> Result<u16, ListenerCreationError> {
        self.port_changed.recv().await.expect("channel closed")
    }

    pub(crate) async fn terminate(&mut self) {
        self.cancellation.cancel();
        self.listen_task.abort();
        let conns: Vec<_> = self
            .conns
            .borrow_mut()
            .drain(..)
            .map(|(_, conn)| conn)
            .collect();
        for conn in conns {
            close_incoming(&conn).await;
        }
        self.listen_tx.close();
    }

    pub(crate) async fn reply(&self, addr: SocketAddr, event: ProtoEvent) {
        log::trace!("reply {event} >=>=>=>=>=> {addr}");
        let (buf, len): ([u8; MAX_EVENT_SIZE], usize) = event.into();
        let conn = self
            .conns
            .borrow()
            .iter()
            .find(|(a, _)| *a == addr)
            .map(|(_, conn)| conn.clone());
        match conn {
            Some(conn) => {
                if let Err(e) = conn.send(&buf[..len]).await {
                    log::warn!("control reply to {addr} failed: {e}");
                }
            }
            None => log::debug!("control reply to {addr} skipped: connection missing"),
        }
    }

    pub(crate) async fn reply_clipboard(&self, addr: SocketAddr, event: ProtoEvent) {
        use lan_mouse_proto::encode_clipboard_event;

        let buf = match encode_clipboard_event(&event) {
            Ok(b) => b,
            Err(e) => {
                log::error!("Failed to encode clipboard event: {}", e);
                return;
            }
        };

        log::info!(
            "Sending clipboard ({} bytes) >=>=>=>=>=> {}",
            buf.len(),
            addr
        );

        let conn = self
            .conns
            .borrow()
            .iter()
            .find(|(a, _)| *a == addr)
            .map(|(_, conn)| conn.clone());
        if let Some(conn) = conn {
            if let Err(error) = conn.send(&buf).await {
                log::warn!("clipboard reply to {addr} failed: {error}");
            }
        }
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
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestConn {
        packet: Mutex<Option<Vec<u8>>>,
        closed: AtomicBool,
    }
    impl TestConn {
        fn new(packet: Option<Vec<u8>>) -> Self {
            Self {
                packet: Mutex::new(packet),
                closed: AtomicBool::new(false),
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
            Ok(bytes.len())
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
