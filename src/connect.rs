use crate::client::ClientManager;
use crate::config::local_commit;
use lan_mouse_ipc::{ClientHandle, DEFAULT_PORT};
use lan_mouse_proto::{MAX_EVENT_SIZE, ProtoEvent};
use local_channel::mpsc::{Receiver, Sender, channel};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    io,
    net::SocketAddr,
    rc::Rc,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::Mutex,
    task::{JoinSet, spawn_local},
};
use webrtc_dtls::{
    config::{Config, ExtendedMasterSecretType},
    conn::DTLSConn,
    crypto::Certificate,
};
use webrtc_util::Conn;

#[derive(Debug, Error)]
pub(crate) enum LanMouseConnectionError {
    #[error(transparent)]
    Bind(#[from] io::Error),
    #[error(transparent)]
    Dtls(#[from] webrtc_dtls::Error),
    #[error(transparent)]
    Webrtc(#[from] webrtc_util::Error),
    #[error("not connected")]
    NotConnected,
    #[error("emulation is disabled on the target device")]
    TargetEmulationDisabled,
    #[error("Connection timed out")]
    Timeout,
}

const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

// encoded clipboard payloads retained briefly after sending so the
// peer can ask for retransmission of fragments lost on the wire,
// keyed by `transfer_id`. Tasks all live on one single-threaded
// runtime, so a thread_local map is enough.
thread_local! {
    static PENDING_TRANSFERS: RefCell<HashMap<u32, (Vec<u8>, std::time::Instant)>> =
        RefCell::new(HashMap::new());
}

/// how long a sent payload stays available for retransmit requests
const PENDING_TTL: Duration = Duration::from_secs(30);

/// total payload bytes retained across all pending transfers
const PENDING_MAX_BYTES: usize = 512 * 1024 * 1024;

fn pending_store(encoded: &[u8], id: u32) {
    PENDING_TRANSFERS.with(|p| {
        let mut m = p.borrow_mut();
        m.insert(id, (encoded.to_vec(), std::time::Instant::now()));
        m.retain(|_, (_, t)| t.elapsed() < PENDING_TTL);
        let mut bytes: usize = m.values().map(|(v, _)| v.len()).sum();
        while bytes > PENDING_MAX_BYTES && m.len() > 1 {
            let Some(oldest) = m
                .iter()
                .max_by_key(|(_, (_, t))| t.elapsed())
                .map(|(k, _)| *k)
            else {
                break;
            };
            if let Some((v, _)) = m.remove(&oldest) {
                bytes -= v.len();
            }
        }
    });
}

/// fragment datagrams answering a peer's retransmit request
pub(crate) fn pending_resends(id: u32, seqs: &[u32]) -> Vec<Vec<u8>> {
    PENDING_TRANSFERS.with(|p| {
        let mut m = p.borrow_mut();
        match m.get_mut(&id) {
            Some((encoded, touched)) => {
                *touched = std::time::Instant::now();
                seqs.iter()
                    .filter_map(|s| lan_mouse_proto::clipboard_fragment_at(encoded, id, *s))
                    .collect()
            }
            None => Vec::new(),
        }
    })
}

/// send an encoded clipboard event over `conn`. Payloads that fit the
/// legacy single-datagram format go out as one message; larger ones are
/// split into [`lan_mouse_proto::EventType::ClipboardFragment`] datagrams
/// the peer reassembles. Fragment datagrams stay below the typical LAN
/// MTU so no IP fragmentation is involved and a lost fragment aborts
/// just this transfer, not the connection.
pub(crate) async fn send_clipboard_datagrams(
    conn: &Arc<dyn Conn + Send + Sync>,
    encoded: &[u8],
    progress: Option<&Sender<(u64, u64)>>,
) -> Result<(), webrtc_util::Error> {
    use lan_mouse_proto::ClipboardFragmenter;
    // largest message that still fits a single datagram without
    // IP fragmentation: the same size budget as a fragment datagram.
    // A bigger single datagram gets sliced by the IP layer and one
    // lost slice drops the whole transfer with no retransmit, so
    // anything that doesn't fit is sent via the reliable fragment
    // path instead.
    const MAX_CLIPBOARD_DATAGRAM: usize = lan_mouse_proto::CLIPBOARD_FRAGMENT_PAYLOAD
        + lan_mouse_proto::CLIPBOARD_FRAGMENT_HEADER;
    if encoded.len() <= MAX_CLIPBOARD_DATAGRAM {
        conn.send(encoded).await?;
        return Ok(());
    }
    let total = encoded.len() as u64;
    let mut last_reported = 0u64;
    for (i, dgram) in ClipboardFragmenter::new(encoded).enumerate() {
        conn.send(&dgram).await?;
        if i % 64 == 63 {
            // pace the burst: flooding the receiver's socket buffer
            // faster than its DTLS read loop drains it drops datagrams
            // and a single missing fragment kills the whole transfer.
            // ~10MB/s wire rate - comfortably below even debug-build
            // decrypt speed, and the transfer bar wants to be seen anyway.
            if let Some(progress) = progress {
                let done = ((i as u64 + 1) * lan_mouse_proto::CLIPBOARD_FRAGMENT_PAYLOAD as u64)
                    .min(total);
                // throttle to ~1% steps: every fragment would flood the
                // frontend channel and bury the bar under stale events
                if done - last_reported >= (total / 100).max(1) {
                    last_reported = done;
                    let _ = progress.send((done, total));
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(8)).await;
        }
    }
    // keep the payload so dropped fragments can be re-sent on request
    pending_store(encoded, lan_mouse_proto::transfer_id(encoded));
    if let Some(progress) = progress {
        let _ = progress.send((total, total));
    }
    Ok(())
}

/// bind a socket matching the target's address family — an IPv4-bound
/// socket can't send to an IPv6 peer ("address family not supported")
fn bind_addr_for(addr: SocketAddr) -> SocketAddr {
    if addr.is_ipv6() {
        "[::]:0".parse().expect("invalid ip")
    } else {
        "0.0.0.0:0".parse().expect("invalid ip")
    }
}

async fn connect(
    addr: SocketAddr,
    cert: Certificate,
) -> Result<(Arc<dyn Conn + Sync + Send>, SocketAddr), (SocketAddr, LanMouseConnectionError)> {
    log::info!("connecting to {addr} ...");
    let bind_addr = bind_addr_for(addr);
    let conn = Arc::new(
        UdpSocket::bind(bind_addr)
            .await
            .map_err(|e| (addr, e.into()))?,
    );
    conn.connect(addr).await.map_err(|e| (addr, e.into()))?;
    let config = Config {
        certificates: vec![cert],
        server_name: "ignored".to_owned(),
        insecure_skip_verify: true,
        extended_master_secret: ExtendedMasterSecretType::Require,
        ..Default::default()
    };
    let timeout = tokio::time::sleep(DEFAULT_CONNECTION_TIMEOUT);
    tokio::select! {
        _ = timeout => Err((addr, LanMouseConnectionError::Timeout)),
        result = DTLSConn::new(conn, config, true, None) => match result {
            Ok(dtls_conn) => Ok((Arc::new(dtls_conn), addr)),
            Err(e) => Err((addr, e.into())),
        }
    }
}

async fn connect_any(
    addrs: &[SocketAddr],
    cert: Certificate,
) -> Result<(Arc<dyn Conn + Send + Sync>, SocketAddr), LanMouseConnectionError> {
    let mut joinset = JoinSet::new();
    for &addr in addrs {
        joinset.spawn_local(connect(addr, cert.clone()));
    }
    loop {
        match joinset.join_next().await {
            None => return Err(LanMouseConnectionError::NotConnected),
            Some(r) => match r.expect("join error") {
                Ok(conn) => return Ok(conn),
                Err((a, e)) => {
                    log::warn!("failed to connect to {a}: `{e}`")
                }
            },
        };
    }
}

/// messages an outgoing connection produces for the rest of the daemon
pub(crate) enum IncomingEvent {
    Event(ProtoEvent),
    /// fragment progress of a clipboard transfer the peer is sending us
    ClipboardProgress {
        received: u64,
        total: u64,
    },
}

#[derive(Clone)]
pub(crate) struct LanMouseConnectionSender {
    cert: Certificate,
    client_manager: ClientManager,
    conns: Rc<Mutex<HashMap<SocketAddr, Arc<dyn Conn + Send + Sync>>>>,
    connecting: Rc<Mutex<HashSet<ClientHandle>>>,
    recv_tx: Sender<(ClientHandle, IncomingEvent)>,
    ping_response: Rc<RefCell<HashSet<SocketAddr>>>,
}

pub(crate) struct LanMouseConnection {
    sender: LanMouseConnectionSender,
    recv_rx: Receiver<(ClientHandle, IncomingEvent)>,
}

impl LanMouseConnection {
    pub(crate) fn new(cert: Certificate, client_manager: ClientManager) -> Self {
        let (recv_tx, recv_rx) = channel();
        let sender = LanMouseConnectionSender {
            cert,
            client_manager,
            conns: Default::default(),
            connecting: Default::default(),
            recv_tx,
            ping_response: Default::default(),
        };
        Self { sender, recv_rx }
    }

    pub(crate) fn sender(&self) -> LanMouseConnectionSender {
        self.sender.clone()
    }

    pub(crate) async fn recv(&mut self) -> (ClientHandle, IncomingEvent) {
        self.recv_rx.recv().await.expect("channel closed")
    }

    pub(crate) async fn send(
        &self,
        event: ProtoEvent,
        handle: ClientHandle,
    ) -> Result<(), LanMouseConnectionError> {
        self.sender.send(event, handle).await
    }
}

impl LanMouseConnectionSender {
    pub(crate) async fn send(
        &self,
        event: ProtoEvent,
        handle: ClientHandle,
    ) -> Result<(), LanMouseConnectionError> {
        let event_str = format!("{event}");
        let (buf, len): ([u8; MAX_EVENT_SIZE], usize) = event.into();
        let buf = &buf[..len];
        if let Some(addr) = self.client_manager.active_addr(handle) {
            let conn = {
                let conns = self.conns.lock().await;
                conns.get(&addr).cloned()
            };
            if let Some(conn) = conn {
                if !self.client_manager.alive(handle) {
                    return Err(LanMouseConnectionError::TargetEmulationDisabled);
                }
                match conn.send(buf).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::warn!("client {handle} failed to send: {e}");
                        disconnect(&self.client_manager, handle, addr, &self.conns).await;
                    }
                }
                log::trace!("{event_str} >->->->->- {addr}");
                return Ok(());
            }
        }

        // check if we are already trying to connect
        let mut connecting = self.connecting.lock().await;
        if !connecting.contains(&handle) {
            connecting.insert(handle);
            // connect in the background
            spawn_local(connect_to_handle(
                self.client_manager.clone(),
                self.cert.clone(),
                handle,
                self.conns.clone(),
                self.connecting.clone(),
                self.recv_tx.clone(),
                self.ping_response.clone(),
            ));
        }
        Err(LanMouseConnectionError::NotConnected)
    }

    /// Send clipboard event with variable-length encoding.
    /// `progress` (if given) receives (transferred, total) byte counts
    /// while a fragmented transfer runs.
    pub(crate) async fn send_clipboard(
        &self,
        event: ProtoEvent,
        handle: ClientHandle,
        progress: Option<&Sender<(u64, u64)>>,
    ) -> Result<(), LanMouseConnectionError> {
        use lan_mouse_proto::encode_clipboard_event;

        let buf = encode_clipboard_event(&event).map_err(|e| {
            log::error!("Failed to encode clipboard event: {}", e);
            LanMouseConnectionError::NotConnected
        })?;

        if let Some(addr) = self.client_manager.active_addr(handle) {
            let conn = {
                let conns = self.conns.lock().await;
                conns.get(&addr).cloned()
            };
            if let Some(conn) = conn {
                if !self.client_manager.alive(handle) {
                    return Err(LanMouseConnectionError::TargetEmulationDisabled);
                }
                if let Err(e) = send_clipboard_datagrams(&conn, &buf, progress).await {
                    log::warn!("client {handle} failed to send clipboard: {e}");
                    disconnect(&self.client_manager, handle, addr, &self.conns).await;
                }
                log::trace!("{event} >->->->->- {addr}");
                return Ok(());
            }
        }

        // Not connected yet - clipboard will sync when connection is established
        log::debug!(
            "Client {} not connected, clipboard will sync when connection is established",
            handle
        );
        Ok(())
    }
}

async fn connect_to_handle(
    client_manager: ClientManager,
    cert: Certificate,
    handle: ClientHandle,
    conns: Rc<Mutex<HashMap<SocketAddr, Arc<dyn Conn + Send + Sync>>>>,
    connecting: Rc<Mutex<HashSet<ClientHandle>>>,
    tx: Sender<(ClientHandle, IncomingEvent)>,
    ping_response: Rc<RefCell<HashSet<SocketAddr>>>,
) -> Result<(), LanMouseConnectionError> {
    log::info!("client {handle} connecting ...");
    // sending did not work, figure out active conn.
    if let Some(addrs) = client_manager.get_ips(handle) {
        let port = client_manager.get_port(handle).unwrap_or(DEFAULT_PORT);
        let addrs = addrs
            .into_iter()
            .map(|a| SocketAddr::new(a, port))
            .collect::<Vec<_>>();
        log::info!("client ({handle}) connecting ... (ips: {addrs:?})");
        let res = connect_any(&addrs, cert).await;
        let (conn, addr) = match res {
            Ok(c) => c,
            Err(e) => {
                connecting.lock().await.remove(&handle);
                return Err(e);
            }
        };
        log::info!("client ({handle}) connected @ {addr}");
        client_manager.set_active_addr(handle, Some(addr));
        conns.lock().await.insert(addr, conn.clone());
        connecting.lock().await.remove(&handle);

        // Best-effort version handshake. Send our commit hash once
        // immediately after the DTLS handshake; the listen side
        // mirrors a Hello back so the receive loop can populate
        // `peer_commit`. Old peers will silently skip this event
        // per the forward-compat handler in [`receive_loop`].
        let (buf, len) = ProtoEvent::Hello {
            commit: local_commit(),
        }
        .into();
        if let Err(e) = conn.send(&buf[..len]).await {
            log::debug!("hello send to {addr} failed: {e}");
        }

        // poll connection for active
        spawn_local(ping_pong(addr, conn.clone(), ping_response.clone()));

        // receiver
        spawn_local(receive_loop(
            client_manager,
            handle,
            addr,
            conn,
            conns,
            tx,
            ping_response.clone(),
        ));
        return Ok(());
    }
    connecting.lock().await.remove(&handle);
    Err(LanMouseConnectionError::NotConnected)
}

async fn ping_pong(
    addr: SocketAddr,
    conn: Arc<dyn Conn + Send + Sync>,
    ping_response: Rc<RefCell<HashSet<SocketAddr>>>,
) {
    loop {
        let (buf, len) = ProtoEvent::Ping.into();

        // send 4 pings, at least one must be answered
        for _ in 0..4 {
            if let Err(e) = conn.send(&buf[..len]).await {
                log::warn!("{addr}: send error `{e}`, closing connection");
                let _ = conn.close().await;
                break;
            }
            log::trace!("PING >->->->->- {addr}");

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        if !ping_response.borrow_mut().remove(&addr) {
            log::warn!("{addr} did not respond, closing connection");
            let _ = conn.close().await;
            return;
        }
    }
}

async fn receive_loop(
    client_manager: ClientManager,
    handle: ClientHandle,
    addr: SocketAddr,
    conn: Arc<dyn Conn + Send + Sync>,
    conns: Rc<Mutex<HashMap<SocketAddr, Arc<dyn Conn + Send + Sync>>>>,
    tx: Sender<(ClientHandle, IncomingEvent)>,
    ping_response: Rc<RefCell<HashSet<SocketAddr>>>,
) {
    use lan_mouse_proto::{
        ClipboardReassembler, MAX_CLIPBOARD_SIZE, decode_clipboard_event, is_clipboard_event_type,
        is_clipboard_fragment_type,
    };

    // Buffer needs to be large enough for a single clipboard datagram
    // (legacy format, up to MAX_CLIPBOARD_SIZE + header) or one
    // clipboard fragment. Fragmented transfers are reassembled below.
    // Use Vec instead of array for large buffers to avoid stack overflow.
    let mut buf = vec![0u8; MAX_CLIPBOARD_SIZE + 5];
    let mut reassembler = ClipboardReassembler::new();
    let mut last_reported = 0u64;
    loop {
        let n = tokio::select! {
            r = conn.recv(&mut buf) => match r {
                Ok(n) => n,
                Err(_) => break,
            },
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
                if reassembler.wants_request() {
                    let id = reassembler.transfer_id();
                    let missing = reassembler.missing_seqs(
                        lan_mouse_proto::FRAGMENT_REQUEST_MAX_SEQS * 20,
                    );
                    for chunk in
                        missing.chunks(lan_mouse_proto::FRAGMENT_REQUEST_MAX_SEQS)
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
                    let _ = tx.send((
                        handle,
                        IncomingEvent::ClipboardProgress {
                            received: 0,
                            total: 0,
                        },
                    ));
                }
                continue;
            }
        };
        if n == 0 {
            break;
        }
        // a peer asking for dropped fragments of a transfer we sent
        if lan_mouse_proto::is_clipboard_fragment_request_type(buf[0]) {
            if let Some((id, seqs)) = lan_mouse_proto::decode_fragment_request(&buf[..n]) {
                for dgram in pending_resends(id, &seqs) {
                    if conn.send(&dgram).await.is_err() {
                        break;
                    }
                }
            }
            continue;
        }
        // Clipboard events use variable-length encoding
        let event = if is_clipboard_fragment_type(buf[0]) {
            match reassembler.push(&buf[..n]) {
                Ok(Some(encoded)) => {
                    last_reported = 0;
                    // full payload — tell the frontend the bar can hide
                    let _ = tx.send((
                        handle,
                        IncomingEvent::ClipboardProgress {
                            received: encoded.len() as u64,
                            total: encoded.len() as u64,
                        },
                    ));
                    match decode_clipboard_event(&encoded) {
                        Ok(event) => event,
                        Err(e) => {
                            log::warn!("Failed to decode clipboard from {addr}: {e:?}");
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
                            let _ = tx.send((
                                handle,
                                IncomingEvent::ClipboardProgress { received, total },
                            ));
                        }
                    }
                    continue;
                }
                Err(e) => {
                    log::warn!("bad clipboard fragment from {addr}: {e}");
                    continue;
                }
            }
        } else if is_clipboard_event_type(buf[0]) {
            match decode_clipboard_event(&buf[..n]) {
                Ok(event) => event,
                Err(e) => {
                    log::warn!("Failed to decode clipboard from {addr}: {e:?}");
                    continue;
                }
            }
        } else {
            // Pad with zeros if message is smaller than MAX_EVENT_SIZE
            let mut fixed_buf = [0u8; MAX_EVENT_SIZE];
            let copy_len = n.min(MAX_EVENT_SIZE);
            fixed_buf[..copy_len].copy_from_slice(&buf[..copy_len]);
            match fixed_buf.try_into() {
                Ok(event) => event,
                // Skip undecodable datagrams without dropping the
                // connection. Each DTLS recv is one framed message, so
                // skipping is safe and keeps us forward-compatible with
                // peers that send event types we don't yet know about.
                Err(e) => {
                    log::debug!("ignoring undecodable event from {addr}: {e}");
                    continue;
                }
            }
        };

        log::trace!("{addr} <==<==<== {event}");
        match event {
            ProtoEvent::Pong(b) => {
                client_manager.set_active_addr(handle, Some(addr));
                client_manager.set_alive(handle, b);
                ping_response.borrow_mut().insert(addr);
            }
            ProtoEvent::Hello { commit } => {
                client_manager.set_peer_commit(handle, Some(commit));
            }
            event => tx
                .send((handle, IncomingEvent::Event(event)))
                .expect("channel closed"),
        }
    }

    log::warn!("recv error");
    disconnect(&client_manager, handle, addr, &conns).await;
}

async fn disconnect(
    client_manager: &ClientManager,
    handle: ClientHandle,
    addr: SocketAddr,
    conns: &Mutex<HashMap<SocketAddr, Arc<dyn Conn + Send + Sync>>>,
) {
    log::warn!("client ({handle}) @ {addr} connection closed");
    conns.lock().await.remove(&addr);
    client_manager.set_active_addr(handle, None);
    client_manager.set_peer_commit(handle, None);
    let active: Vec<SocketAddr> = conns.lock().await.keys().copied().collect();
    log::info!("active connections: {active:?}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_addr_matches_target_family() {
        let v4: SocketAddr = "192.168.1.1:4242".parse().unwrap();
        assert!(bind_addr_for(v4).is_ipv4());
        let v6: SocketAddr = "[fe80::1]:4242".parse().unwrap();
        assert!(bind_addr_for(v6).is_ipv6());
    }
}
