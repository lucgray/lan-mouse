use futures::{Stream, StreamExt};
use lan_mouse_proto::{MAX_EVENT_SIZE, ProtoEvent};
use local_channel::mpsc::{Receiver, Sender, channel};
use rustls::pki_types::CertificateDer;
use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    rc::Rc,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::Mutex as AsyncMutex,
    task::{JoinHandle, spawn_local},
};
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
    },
    Accept {
        addr: SocketAddr,
        fingerprint: String,
    },
    Rejected {
        fingerprint: String,
    },
    /// fragment progress of a clipboard transfer the peer is sending us
    ClipboardProgress {
        received: u64,
        total: u64,
    },
}

pub(crate) struct LanMouseListener {
    listen_rx: Receiver<ListenEvent>,
    listen_tx: Sender<ListenEvent>,
    listen_task: JoinHandle<()>,
    conns: Rc<AsyncMutex<Vec<(SocketAddr, ArcConn)>>>,
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
                    let fingerprint = peer_cert_fingerprint(certs)?;
                    // a poisoned lock still holds valid data — recover the
                    // guard rather than panicking inside the DTLS handshake
                    let authorized = authorized.read().unwrap_or_else(|e| e.into_inner());
                    if authorized.contains_key(&fingerprint) {
                        Ok(())
                    } else {
                        connection_attempts
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
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

        let conns: Rc<AsyncMutex<Vec<(SocketAddr, ArcConn)>>> =
            Rc::new(AsyncMutex::new(Vec::new()));

        let conns_clone = conns.clone();
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
                                let mut conns = conns_clone.lock().await;
                                conns.push((addr, conn.clone()));
                                drop(conns);
                                // the listener only accepts DTLS connections, but
                                // keep these graceful — a panic here kills the
                                // accept loop while the daemon keeps running
                                let Some(dtls_conn) = conn.as_any().downcast_ref::<DTLSConn>() else {
                                    log::error!("accepted connection is not a DTLS conn, dropping {addr}");
                                    continue;
                                };
                                let peer_certs = dtls_conn.connection_state().await.peer_certificates;
                                let Some(cert) = peer_certs.first() else {
                                    log::warn!("peer {addr} presented no certificate, dropping");
                                    continue;
                                };
                                let fingerprint = crypto::generate_fingerprint(cert);
                                if listen_tx.send(ListenEvent::Accept { addr, fingerprint }).is_err() {
                                    log::error!("service event channel closed, listener exiting");
                                    return;
                                }
                                spawn_local(read_loop(conns_clone.clone(), addr, conn, listen_tx.clone()));
                            },
                            Err(e) => {
                                if let Error::Std(ref e) = e {
                                    if let Some(e) = e.0.downcast_ref::<webrtc_dtls::Error>() {
                                        match e {
                                            webrtc_dtls::Error::ErrVerifyDataMismatch => {
                                                let fingerprint = connection_attempts
                                                    .lock()
                                                    .unwrap_or_else(|e| e.into_inner())
                                                    .pop_front();
                                                if let Some(fingerprint) = fingerprint {
                                                    if listen_tx.send(ListenEvent::Rejected { fingerprint }).is_err() {
                                                        log::error!("service event channel closed, listener exiting");
                                                        return;
                                                    }
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
                            let Some(port) = port else {
                                log::error!("service request channel closed, listener exiting");
                                return;
                            };
                            match bind_dtls(port, &cfg).await {
                                Ok(new_listeners) => {
                                    for l in &listeners {
                                        let _ = l.close().await;
                                    }
                                    listeners = new_listeners;
                                    if port_changed_tx.send(Ok(port)).is_err() {
                                        log::error!("service channel closed, listener exiting");
                                        return;
                                    }
                                }
                                Err(e) => {
                                    log::warn!("unable to change port: {e}");
                                    if port_changed_tx.send(Err(e)).is_err() {
                                        log::error!("service channel closed, listener exiting");
                                        return;
                                    }
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
            port_changed,
            request_port_change,
        })
    }

    pub(crate) fn request_port_change(&mut self, port: u16) {
        if self.request_port_change.send(port).is_err() {
            log::error!("listener task gone, cannot request port change");
        }
    }

    /// `None` when the listener task exited without answering.
    pub(crate) async fn port_changed(&mut self) -> Option<Result<u16, ListenerCreationError>> {
        self.port_changed.recv().await
    }

    /// whether the DTLS accept task is still running — polled by the
    /// listen task's tick since the event channel stays open on death
    /// (`listen_tx` is a field of this struct, so sender drop never
    /// happens) and task death would otherwise be invisible
    pub(crate) fn is_alive(&self) -> bool {
        !self.listen_task.is_finished()
    }

    pub(crate) async fn terminate(&mut self) {
        self.listen_task.abort();
        let conns = self.conns.lock().await;
        for (_, conn) in conns.iter() {
            let _ = conn.close().await;
        }
        self.listen_tx.close();
    }

    pub(crate) async fn reply(&self, addr: SocketAddr, event: ProtoEvent) {
        log::trace!("reply {event} >=>=>=>=>=> {addr}");
        let event_str = format!("{event}");
        let (buf, len): ([u8; MAX_EVENT_SIZE], usize) = event.into();
        let conns = self.conns.lock().await;
        for (a, conn) in conns.iter() {
            if *a == addr {
                if let Err(e) = conn.send(&buf[..len]).await {
                    log::warn!("reply {event_str} to {addr} failed: {e}");
                }
            }
        }
    }

    /// Reply to an incoming peer's clipboard share. Returns `true` when
    /// the payload was handed to the connection, `false` when the send
    /// failed (encode error, peer not connected or a datagram write
    /// error) so the service can surface it to the user.
    pub(crate) async fn reply_clipboard(
        &self,
        addr: SocketAddr,
        event: ProtoEvent,
        progress: Option<&Sender<(u64, u64)>>,
    ) -> bool {
        use lan_mouse_proto::encode_clipboard_event;

        let buf = match encode_clipboard_event(&event) {
            Ok(b) => b,
            Err(e) => {
                log::error!("Failed to encode clipboard event: {}", e);
                return false;
            }
        };

        log::info!(
            "Sending clipboard ({} bytes) >=>=>=>=>=> {}",
            buf.len(),
            addr
        );

        let conns = self.conns.lock().await;
        for (a, conn) in conns.iter() {
            if *a == addr {
                return match crate::connect::send_clipboard_datagrams(conn, &buf, progress).await {
                    Ok(_) => {
                        log::debug!("Clipboard sent successfully to {}", addr);
                        true
                    }
                    Err(e) => {
                        log::error!("Failed to send clipboard to {}: {:?}", addr, e);
                        false
                    }
                };
            }
        }
        log::warn!("cannot reply clipboard to {addr}: peer not connected");
        false
    }

    pub(crate) async fn get_certificate_fingerprint(&self, addr: SocketAddr) -> Option<String> {
        if let Some(conn) = self
            .conns
            .lock()
            .await
            .iter()
            .find(|(a, _)| *a == addr)
            .map(|(_, c)| c.clone())
        {
            let Some(dtls_conn) = conn.as_any().downcast_ref::<DTLSConn>() else {
                log::error!("connection for {addr} is not a DTLS conn");
                return None;
            };
            let certs = dtls_conn.connection_state().await.peer_certificates;
            let cert = certs.first()?;
            let fingerprint = crypto::generate_fingerprint(cert);
            Some(fingerprint)
        } else {
            None
        }
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
    conns: Rc<AsyncMutex<Vec<(SocketAddr, ArcConn)>>>,
    addr: SocketAddr,
    conn: ArcConn,
    dtls_tx: Sender<ListenEvent>,
) -> Result<(), Error> {
    use lan_mouse_proto::{
        ClipboardReassembler, MAX_CLIPBOARD_SIZE, decode_clipboard_event, is_clipboard_event_type,
        is_clipboard_fragment_type,
    };

    // Buffer needs to be large enough for a single legacy-format clipboard
    // datagram or one clipboard fragment. Use Vec instead of array for
    // large buffers to avoid stack overflow
    let mut b = vec![0u8; MAX_CLIPBOARD_SIZE + 5];
    let mut reassembler = ClipboardReassembler::new();
    let mut last_reported = 0u64;

    loop {
        // Read first byte to determine event type
        let n = tokio::select! {
            r = conn.recv(&mut b) => match r {
                Ok(n) => n,
                Err(e) => {
                    log::warn!("recv error from {}: {:?}", addr, e);
                    break;
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
                if reassembler.wants_request() {
                    let id = reassembler.transfer_id();
                    let missing = reassembler
                        .missing_seqs(lan_mouse_proto::FRAGMENT_REQUEST_MAX_SEQS * 20);
                    for chunk in missing.chunks(lan_mouse_proto::FRAGMENT_REQUEST_MAX_SEQS)
                    {
                        let _ = conn
                            .send(&lan_mouse_proto::encode_fragment_request(id, chunk))
                            .await;
                    }
                }
                if reassembler.stalled() {
                    let (got, want) = reassembler.progress().unwrap_or((0, 0));
                    log::warn!(
                        "clipboard transfer from {addr} stalled at {got}/{want} bytes - aborting"
                    );
                    reassembler.reset();
                    let _ = dtls_tx.send(ListenEvent::ClipboardProgress {
                        received: 0,
                        total: 0,
                    });
                }
                continue;
            }
        };

        if n == 0 {
            break;
        }

        log::trace!("Received {} bytes from {}", n, addr);

        // a peer asking for dropped fragments of a transfer we sent
        if lan_mouse_proto::is_clipboard_fragment_request_type(b[0]) {
            if let Some((id, seqs)) = lan_mouse_proto::decode_fragment_request(&b[..n]) {
                for dgram in crate::connect::pending_resends(id, &seqs) {
                    if conn.send(&dgram).await.is_err() {
                        break;
                    }
                }
            }
            continue;
        }

        // Check if this is a clipboard event (variable length)
        let event_type = b[0];
        let event = if is_clipboard_fragment_type(event_type) {
            match reassembler.push(&b[..n]) {
                Ok(Some(encoded)) => {
                    last_reported = 0;
                    let _ = dtls_tx.send(ListenEvent::ClipboardProgress {
                        received: encoded.len() as u64,
                        total: encoded.len() as u64,
                    });
                    match decode_clipboard_event(&encoded) {
                        Ok(event) => event,
                        Err(e) => {
                            log::warn!("error decoding clipboard event: {e}");
                            continue;
                        }
                    }
                }
                Ok(None) => {
                    if let Some((received, total)) = reassembler.progress() {
                        // a smaller `received` means a new transfer started
                        if received < last_reported {
                            last_reported = 0;
                        }
                        // ~1% steps only — the frontend channel is small
                        if received - last_reported >= (total / 100).max(1) {
                            last_reported = received;
                            let _ =
                                dtls_tx.send(ListenEvent::ClipboardProgress { received, total });
                        }
                    }
                    continue;
                }
                Err(e) => {
                    log::warn!("bad clipboard fragment from {addr}: {e}");
                    continue;
                }
            }
        } else if is_clipboard_event_type(event_type) {
            // This is a clipboard event - need to read full message
            if n < 5 {
                log::warn!("Clipboard event too short: {} bytes", n);
                break;
            }

            // Parse length from bytes 1-4
            let length = u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as usize;

            // payloads larger than MAX_CLIPBOARD_SIZE always arrive as
            // fragments; a bigger length in the single-datagram format
            // can only come from a broken peer
            if length > MAX_CLIPBOARD_SIZE {
                log::warn!("Clipboard data too large: {} bytes", length);
                break;
            }

            let total_size = 5 + length;
            log::debug!(
                "Clipboard event: received {} bytes, total expected {}",
                n,
                total_size
            );

            // Check if we already have all the data
            if n >= total_size {
                // All data received in one packet
                match decode_clipboard_event(&b[..total_size]) {
                    Ok(event) => event,
                    Err(e) => {
                        log::warn!("error decoding clipboard event: {e}");
                        break;
                    }
                }
            } else {
                // Need to read more data
                let mut clipboard_buf = vec![0u8; total_size];
                clipboard_buf[..n].copy_from_slice(&b[..n]);

                let mut read_so_far = n;
                let mut read_failed = false;
                while read_so_far < total_size {
                    match conn.recv(&mut clipboard_buf[read_so_far..]).await {
                        Ok(n) if n > 0 => read_so_far += n,
                        _ => {
                            log::warn!("Connection closed while reading clipboard data");
                            read_failed = true;
                            break;
                        }
                    }
                }

                if read_failed || read_so_far < total_size {
                    log::warn!("Incomplete clipboard data received, closing connection");
                    break;
                }

                match decode_clipboard_event(&clipboard_buf) {
                    Ok(event) => event,
                    Err(e) => {
                        log::warn!("error decoding clipboard event: {e}");
                        break;
                    }
                }
            }
        } else {
            // Standard fixed-size event - need to convert to fixed-size array
            // Pad with zeros if message is smaller than MAX_EVENT_SIZE
            let mut fixed_buf = [0u8; MAX_EVENT_SIZE];
            let copy_len = n.min(MAX_EVENT_SIZE);
            fixed_buf[..copy_len].copy_from_slice(&b[..copy_len]);
            match fixed_buf.try_into() {
                Ok(event) => event,
                Err(e) => {
                    // Skip the malformed/unknown datagram and keep
                    // listening. Each DTLS recv returns one full
                    // datagram, so a parse error here can't desync a
                    // stream; the next call gets a fresh, framed
                    // message. This makes the protocol forward-
                    // compatible: a peer running a newer Lan Mouse
                    // version can introduce additional event types
                    // and old peers will simply ignore them rather
                    // than dropping the connection.
                    log::debug!("ignoring undecodable event from {addr}: {e}");
                    continue;
                }
            }
        };

        if dtls_tx.send(ListenEvent::Msg { event, addr }).is_err() {
            log::error!("service event channel closed, dropping message from {addr}");
            break;
        }
    }

    log::info!("dtls client disconnected {addr:?}");
    let mut conns = conns.lock().await;
    if let Some(index) = conns.iter().position(|(a, _)| *a == addr) {
        conns.remove(index);
    } else {
        log::warn!("{addr} was not registered in conns on disconnect");
    }
    Ok(())
}

/// fingerprint of the single certificate a peer must present — any
/// other count is a malformed handshake and gets a controlled
/// rejection instead of panicking on the DTLS callback thread
fn peer_cert_fingerprint(certs: &[Vec<u8>]) -> Result<String, webrtc_dtls::Error> {
    if certs.len() != 1 {
        log::warn!(
            "rejecting peer: expected 1 certificate, got {}",
            certs.len()
        );
        return Err(webrtc_dtls::Error::ErrVerifyDataMismatch);
    }
    Ok(crypto::generate_fingerprint(&certs[0]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_cert_fingerprint_rejects_invalid_counts() {
        // zero certificates
        assert!(matches!(
            peer_cert_fingerprint(&[]),
            Err(webrtc_dtls::Error::ErrVerifyDataMismatch)
        ));
        // multiple certificates — the old code panicked here
        let cert = vec![7u8; 32];
        assert!(matches!(
            peer_cert_fingerprint(&[cert.clone(), cert]),
            Err(webrtc_dtls::Error::ErrVerifyDataMismatch)
        ));
    }

    #[test]
    fn peer_cert_fingerprint_accepts_single_cert() {
        let cert = vec![7u8; 32];
        let fp = peer_cert_fingerprint(std::slice::from_ref(&cert)).expect("single cert");
        assert_eq!(fp, crypto::generate_fingerprint(&cert));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn listener_is_alive_reflects_task_death() {
        // fault injection: the accept task dying must be observable —
        // the event channel stays open (listen_tx lives on the
        // listener struct) so is_alive is the only detection path
        tokio::task::LocalSet::new()
            .run_until(async {
                let keys = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
                let cert = Certificate::generate_self_signed(["ignored".to_owned()])
                    .expect("self-signed cert");
                let listener = LanMouseListener::new(0, cert, keys)
                    .await
                    .expect("listener");
                assert!(listener.is_alive());
                listener.listen_task.abort();
                // the abort is only acted on once the executor runs
                tokio::task::yield_now().await;
                assert!(!listener.is_alive());
            })
            .await;
    }
}
