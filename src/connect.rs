use crate::client::ClientManager;
use crate::config::local_commit;
use lan_mouse_ipc::{ClientHandle, DEFAULT_PORT};
use lan_mouse_proto::{MAX_EVENT_SIZE, ProtoEvent};
use local_channel::mpsc::{Receiver, Sender, channel};
use std::{
    cell::Cell, collections::HashMap, io, net::SocketAddr, rc::Rc, sync::Arc, time::Duration,
};
use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::Mutex,
    task::{JoinSet, spawn_local},
};
use tokio_util::sync::CancellationToken;
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

type Connection = Arc<dyn Conn + Send + Sync>;
type Connections = Mutex<HashMap<ClientHandle, (SocketAddr, Connection)>>;
type Attempts = Mutex<HashMap<ClientHandle, (u64, Rc<()>)>>;

async fn close_connection(conn: &Connection) {
    match tokio::time::timeout(Duration::from_secs(1), conn.close()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => log::debug!("connection cleanup failed: {error}"),
        Err(_) => log::warn!("connection cleanup exceeded one second"),
    }
}

#[derive(Clone)]
struct Target {
    handle: ClientHandle,
    revision: u64,
    cancellation: CancellationToken,
}

#[derive(Clone)]
pub(crate) struct LanMouseConnectionSender {
    cert: Certificate,
    client_manager: ClientManager,
    conns: Rc<Connections>,
    connecting: Rc<Attempts>,
    recv_tx: Sender<(ClientHandle, ProtoEvent)>,
}

pub(crate) struct LanMouseConnection {
    sender: LanMouseConnectionSender,
    recv_rx: Receiver<(ClientHandle, ProtoEvent)>,
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
        };
        Self { sender, recv_rx }
    }

    pub(crate) fn sender(&self) -> LanMouseConnectionSender {
        self.sender.clone()
    }

    pub(crate) async fn recv(&mut self) -> (ClientHandle, ProtoEvent) {
        self.recv_rx.recv().await.expect("channel closed")
    }

    /// End only the failed capture's transport and heartbeat. A fresh token
    /// permits reconnect after capture is explicitly re-enabled.
    pub(crate) async fn abort_capture(&self, handle: ClientHandle) {
        self.sender.client_manager.invalidate_target(handle);
        let connection = self.sender.conns.lock().await.get(&handle).cloned();
        if let Some((addr, conn)) = connection {
            disconnect(
                &self.sender.client_manager,
                handle,
                addr,
                &conn,
                &self.sender.conns,
            )
            .await;
        }
    }

    pub(crate) async fn send(
        &self,
        event: ProtoEvent,
        handle: ClientHandle,
    ) -> Result<(), LanMouseConnectionError> {
        self.sender.send(event, handle).await
    }
}

impl Drop for LanMouseConnection {
    fn drop(&mut self) {
        self.sender.client_manager.cancel_targets();
    }
}

impl LanMouseConnectionSender {
    fn target(&self, handle: ClientHandle) -> Option<Target> {
        let revision = self.client_manager.target_revision(handle)?;
        let cancellation = self.client_manager.target_token(handle)?;
        self.client_manager
            .target_is_current(handle, revision)
            .then_some(Target {
                handle,
                revision,
                cancellation,
            })
    }

    pub(crate) async fn terminate(&self) {
        self.client_manager.cancel_targets();
        let connections: Vec<_> = self
            .conns
            .lock()
            .await
            .drain()
            .map(|(_, (_, conn))| conn)
            .collect();
        for conn in connections {
            close_connection(&conn).await;
        }
    }

    pub(crate) async fn send(
        &self,
        event: ProtoEvent,
        handle: ClientHandle,
    ) -> Result<(), LanMouseConnectionError> {
        let target = self
            .target(handle)
            .ok_or(LanMouseConnectionError::NotConnected)?;
        log::trace!("sending {event} to client {handle}");
        let (buf, len): ([u8; MAX_EVENT_SIZE], usize) = event.into();
        let buf = &buf[..len];
        if let Some(addr) = self.client_manager.active_addr(handle) {
            let conn = {
                let conns = self.conns.lock().await;
                conns
                    .get(&handle)
                    .filter(|(a, _)| *a == addr)
                    .map(|(_, c)| c.clone())
            };
            if let Some(conn) = conn {
                if !self.client_manager.alive(handle) {
                    return Err(LanMouseConnectionError::TargetEmulationDisabled);
                }
                let sent = tokio::select! {
                    _ = target.cancellation.cancelled() => return Err(LanMouseConnectionError::NotConnected),
                    result = conn.send(buf) => result,
                };
                match sent {
                    Ok(_) => {}
                    Err(e) => {
                        log::warn!("client {handle} failed to send: {e}");
                        disconnect(&self.client_manager, handle, addr, &conn, &self.conns).await;
                        return Err(e.into());
                    }
                }
                return Ok(());
            }
        }

        let mut connecting = self.connecting.lock().await;
        if !self
            .client_manager
            .target_is_current(handle, target.revision)
        {
            return Err(LanMouseConnectionError::NotConnected);
        }
        if !connecting
            .get(&handle)
            .is_some_and(|(revision, _)| *revision == target.revision)
        {
            let marker = Rc::new(());
            connecting.insert(handle, (target.revision, marker.clone()));
            let sender = self.clone();
            spawn_local(async move {
                let result = tokio::select! {
                    _ = target.cancellation.cancelled() => Err(LanMouseConnectionError::NotConnected),
                    result = connect_to_handle(&sender, target.clone()) => result,
                };
                let mut attempts = sender.connecting.lock().await;
                finish_attempt(&mut attempts, handle, &marker);
                if let Err(error) = result {
                    log::debug!("client {handle} connection attempt ended: {error}");
                }
            });
        }
        Err(LanMouseConnectionError::NotConnected)
    }

    /// Send clipboard event with variable-length encoding
    pub(crate) async fn send_clipboard(
        &self,
        event: ProtoEvent,
        handle: ClientHandle,
    ) -> Result<(), LanMouseConnectionError> {
        let target = self
            .target(handle)
            .ok_or(LanMouseConnectionError::NotConnected)?;
        use lan_mouse_proto::encode_clipboard_event;

        let buf = encode_clipboard_event(&event).map_err(|e| {
            log::error!("Failed to encode clipboard event: {}", e);
            LanMouseConnectionError::NotConnected
        })?;

        if let Some(addr) = self.client_manager.active_addr(handle) {
            let conn = {
                let conns = self.conns.lock().await;
                conns
                    .get(&handle)
                    .filter(|(a, _)| *a == addr)
                    .map(|(_, c)| c.clone())
            };
            if let Some(conn) = conn {
                if !self.client_manager.alive(handle) {
                    return Err(LanMouseConnectionError::TargetEmulationDisabled);
                }
                let sent = tokio::select! {
                    _ = target.cancellation.cancelled() => return Err(LanMouseConnectionError::NotConnected),
                    result = conn.send(&buf) => result,
                };
                match sent {
                    Ok(_) => {}
                    Err(e) => {
                        log::warn!("client {handle} failed to send clipboard: {e}");
                        disconnect(&self.client_manager, handle, addr, &conn, &self.conns).await;
                        return Err(e.into());
                    }
                }
                log::trace!("{event} >->->->->- {addr}");
                return Ok(());
            }
        }

        Err(LanMouseConnectionError::NotConnected)
    }
}

fn finish_attempt(
    attempts: &mut HashMap<ClientHandle, (u64, Rc<()>)>,
    handle: ClientHandle,
    marker: &Rc<()>,
) {
    if attempts
        .get(&handle)
        .is_some_and(|(_, current)| Rc::ptr_eq(current, marker))
    {
        attempts.remove(&handle);
    }
}

async fn connect_to_handle(
    sender: &LanMouseConnectionSender,
    target: Target,
) -> Result<(), LanMouseConnectionError> {
    let Target {
        handle, revision, ..
    } = target.clone();
    let client_manager = &sender.client_manager;
    let addrs = client_manager
        .get_ips(handle)
        .ok_or(LanMouseConnectionError::NotConnected)?;
    let port = client_manager.get_port(handle).unwrap_or(DEFAULT_PORT);
    let addrs: Vec<_> = addrs
        .into_iter()
        .map(|ip| SocketAddr::new(ip, port))
        .collect();
    let (conn, addr) = connect_any(&addrs, sender.cert.clone()).await?;
    let mut current = sender.conns.lock().await;
    if !client_manager.target_is_current(handle, revision) {
        drop(current);
        close_connection(&conn).await;
        return Err(LanMouseConnectionError::NotConnected);
    }
    client_manager.set_active_addr(handle, Some(addr));
    let previous = current.insert(handle, (addr, conn.clone()));
    drop(current);

    // Install cancellation-aware consumers before any post-publication await.
    // A cancellation during Hello/old-session cleanup must not orphan this conn.
    let ping_response = Rc::new(Cell::new(false));
    spawn_local(receive_loop(
        client_manager.clone(),
        target.clone(),
        addr,
        conn.clone(),
        sender.conns.clone(),
        sender.recv_tx.clone(),
        ping_response.clone(),
    ));
    let ping_conn = conn.clone();
    let cancellation = target.cancellation.clone();
    spawn_local(async move {
        tokio::select! {
            _ = cancellation.cancelled() => { close_connection(&ping_conn).await; },
            _ = ping_pong(addr, ping_conn.clone(), ping_response) => {},
        }
    });
    let (buf, len) = ProtoEvent::Hello {
        commit: local_commit(),
    }
    .into();
    match tokio::time::timeout(Duration::from_secs(1), conn.send(&buf[..len])).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => log::debug!("hello send to {addr} failed: {error}"),
        Err(_) => log::debug!("hello send to {addr} timed out"),
    }
    if let Some((_, old)) = previous {
        close_connection(&old).await;
    }
    Ok(())
}

async fn ping_pong(
    addr: SocketAddr,
    conn: Arc<dyn Conn + Send + Sync>,
    ping_response: Rc<Cell<bool>>,
) {
    loop {
        let (buf, len) = ProtoEvent::Ping.into();

        // send 4 pings, at least one must be answered
        for _ in 0..4 {
            if let Err(e) = conn.send(&buf[..len]).await {
                log::warn!("{addr}: send error `{e}`, closing connection");
                close_connection(&conn).await;
                return;
            }
            log::trace!("PING >->->->->- {addr}");

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        if !ping_response.replace(false) {
            log::warn!("{addr} did not respond, closing connection");
            close_connection(&conn).await;
            return;
        }
    }
}

async fn receive_loop(
    client_manager: ClientManager,
    target: Target,
    addr: SocketAddr,
    conn: Arc<dyn Conn + Send + Sync>,
    conns: Rc<Connections>,
    tx: Sender<(ClientHandle, ProtoEvent)>,
    ping_response: Rc<Cell<bool>>,
) {
    let Target {
        handle,
        revision,
        cancellation,
    } = target;
    use lan_mouse_proto::{MAX_CLIPBOARD_SIZE, decode_event_frame};

    // Buffer needs to be large enough for clipboard data.
    // Use Vec instead of array for large buffers to avoid stack overflow.
    let mut buf = vec![0u8; MAX_CLIPBOARD_SIZE + 5];
    loop {
        let received = tokio::select! {
            _ = cancellation.cancelled() => break,
            result = conn.recv(&mut buf) => result,
        };
        let Ok(n) = received else {
            break;
        };
        if n == 0 {
            break;
        }
        let current = conns.lock().await;
        let ours = current
            .get(&handle)
            .is_some_and(|(_, c)| Arc::ptr_eq(c, &conn));
        drop(current);
        if !ours || !client_manager.target_is_current(handle, revision) {
            break;
        }
        let event = match decode_event_frame(&buf[..n]) {
            Ok(event) => event,
            Err(e) => {
                log::debug!("ignoring undecodable event from {addr}: {e}");
                continue;
            }
        };

        log::trace!("{addr} <==<==<== {event}");
        match event {
            ProtoEvent::Pong(b) => {
                client_manager.set_active_addr(handle, Some(addr));
                client_manager.set_alive(handle, b);
                ping_response.set(true);
            }
            ProtoEvent::Hello { commit } => {
                client_manager.set_peer_commit(handle, Some(commit));
            }
            event => tx.send((handle, event)).expect("channel closed"),
        }
    }

    log::debug!("client {handle} receive task ended @ {addr}");
    disconnect(&client_manager, handle, addr, &conn, &conns).await;
}

async fn disconnect(
    client_manager: &ClientManager,
    handle: ClientHandle,
    addr: SocketAddr,
    conn: &Connection,
    conns: &Connections,
) {
    let mut current = conns.lock().await;
    if current
        .get(&handle)
        .is_some_and(|(_, c)| Arc::ptr_eq(c, conn))
    {
        current.remove(&handle);
        if client_manager.active_addr(handle) == Some(addr) {
            client_manager.set_active_addr(handle, None);
            client_manager.set_peer_commit(handle, None);
            client_manager.set_alive(handle, false);
        }
    }
    drop(current);
    close_connection(conn).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RefusedConnection {
        closed: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl Conn for RefusedConnection {
        async fn connect(&self, _: SocketAddr) -> webrtc_util::Result<()> {
            Ok(())
        }
        async fn recv(&self, _: &mut [u8]) -> webrtc_util::Result<usize> {
            std::future::pending().await
        }
        async fn recv_from(&self, _: &mut [u8]) -> webrtc_util::Result<(usize, SocketAddr)> {
            unreachable!()
        }
        async fn send(&self, _: &[u8]) -> webrtc_util::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "test refusal").into())
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
            self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
            self
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_input_and_clipboard_sends_return_errors() {
        let clients = ClientManager::default();
        let handle = clients.add_client();
        clients.activate_client(handle);
        let addr = "127.0.0.1:2".parse().unwrap();
        let connection = LanMouseConnection::new(
            Certificate::generate_self_signed(vec![]).unwrap(),
            clients.clone(),
        );
        let sender = connection.sender();
        for clipboard in [false, true] {
            clients.set_active_addr(handle, Some(addr));
            clients.set_alive(handle, true);
            sender
                .conns
                .lock()
                .await
                .insert(handle, (addr, Arc::new(RefusedConnection::default())));
            let result = if clipboard {
                sender
                    .send_clipboard(
                        ProtoEvent::Input(input_event::Event::Clipboard(
                            input_event::ClipboardEvent::Text("test".into()),
                        )),
                        handle,
                    )
                    .await
            } else {
                sender.send(ProtoEvent::Ping, handle).await
            };
            assert!(result.is_err());
            assert!(clients.active_addr(handle).is_none());
        }
        assert!(
            sender
                .send_clipboard(
                    ProtoEvent::Input(input_event::Event::Clipboard(
                        input_event::ClipboardEvent::Text("offline".into())
                    )),
                    handle
                )
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_capture_cancels_session_without_affecting_other_target() {
        let clients = ClientManager::default();
        let handle = clients.add_client();
        let other = clients.add_client();
        clients.activate_client(handle);
        clients.activate_client(other);
        let old_token = clients.target_token(handle).unwrap();
        let other_token = clients.target_token(other).unwrap();
        let connection = LanMouseConnection::new(
            Certificate::generate_self_signed(vec![]).unwrap(),
            clients.clone(),
        );
        let own = Arc::new(RefusedConnection::default());
        let peer = Arc::new(RefusedConnection::default());
        let addr = "127.0.0.1:2".parse().unwrap();
        clients.set_active_addr(handle, Some(addr));
        clients.set_alive(handle, true);
        connection.sender.conns.lock().await.extend([
            (handle, (addr, own.clone() as Connection)),
            (other, (addr, peer.clone() as Connection)),
        ]);
        connection.abort_capture(handle).await;
        assert!(old_token.is_cancelled());
        assert!(!clients.target_token(handle).unwrap().is_cancelled());
        assert!(!other_token.is_cancelled());
        assert!(clients.active_addr(handle).is_none());
        assert!(own.closed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!peer.closed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!connection.sender.conns.lock().await.contains_key(&handle));
        assert!(connection.sender.conns.lock().await.contains_key(&other));
        connection.abort_capture(handle).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn old_disconnect_preserves_replacement_and_other_device() {
        let clients = ClientManager::default();
        let handle = clients.add_client();
        let other = clients.add_client();
        let addr = "127.0.0.1:2".parse().unwrap();
        let old: Connection = Arc::new(RefusedConnection::default());
        let new: Connection = Arc::new(RefusedConnection::default());
        let conns = Mutex::new(HashMap::from([
            (handle, (addr, new.clone())),
            (other, (addr, old.clone())),
        ]));
        clients.set_active_addr(handle, Some(addr));
        clients.set_alive(handle, true);
        disconnect(&clients, handle, addr, &old, &conns).await;
        assert_eq!(clients.active_addr(handle), Some(addr));
        assert!(clients.alive(handle));
        assert_eq!(conns.lock().await.len(), 2);
        disconnect(&clients, handle, addr, &new, &conns).await;
        assert_eq!(clients.active_addr(handle), None);
        assert!(!clients.alive(handle));
        assert!(conns.lock().await.contains_key(&other));
    }

    #[test]
    fn old_attempt_cleanup_preserves_replacement_even_at_same_revision() {
        let old = Rc::new(());
        let new = Rc::new(());
        let mut attempts = HashMap::from([(1, (7, new.clone()))]);
        finish_attempt(&mut attempts, 1, &old);
        assert!(attempts.contains_key(&1));
        finish_attempt(&mut attempts, 1, &new);
        assert!(attempts.is_empty());
    }

    #[tokio::test]
    async fn changed_target_starts_new_real_handshake_without_waiting_for_old_timeout() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let old_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let new_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let clients = ClientManager::default();
                let handle = clients.add_client();
                clients.set_fix_ips(handle, vec!["127.0.0.1".parse().unwrap()]);
                clients.set_port(handle, old_peer.local_addr().unwrap().port());
                clients.activate_client(handle);
                let owner = LanMouseConnection::new(
                    Certificate::generate_self_signed(vec![]).unwrap(),
                    clients.clone(),
                );
                let sender = owner.sender();
                let old_token = clients.target_token(handle).unwrap();
                assert!(sender.send(ProtoEvent::Ping, handle).await.is_err());
                let mut packet = [0u8; 2048];
                tokio::time::timeout(Duration::from_secs(2), old_peer.recv_from(&mut packet))
                    .await
                    .unwrap()
                    .unwrap();
                clients.set_port(handle, new_peer.local_addr().unwrap().port());
                assert!(old_token.is_cancelled());
                assert!(sender.send(ProtoEvent::Ping, handle).await.is_err());
                tokio::time::timeout(Duration::from_secs(2), new_peer.recv_from(&mut packet))
                    .await
                    .unwrap()
                    .unwrap();
                clients.remove_client(handle);
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if sender.connecting.lock().await.is_empty() {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert!(clients.active_addr(handle).is_none());
                while old_peer.try_recv_from(&mut packet).is_ok() {}
                // Neither loopback peer replies: no old DTLS handshake should keep
                // retransmitting after the cancellation has unwound its tasks.
                assert!(
                    tokio::time::timeout(
                        Duration::from_millis(1300),
                        old_peer.recv_from(&mut packet)
                    )
                    .await
                    .is_err()
                );
            })
            .await;
    }

    #[tokio::test]
    async fn cancellation_wakes_idle_receiver_and_closes_its_connection() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let clients = ClientManager::default();
                let handle = clients.add_client();
                clients.activate_client(handle);
                let owner = LanMouseConnection::new(
                    Certificate::generate_self_signed(vec![]).unwrap(),
                    clients.clone(),
                );
                let sender = owner.sender();
                let target = sender.target(handle).unwrap();
                let mock = Arc::new(RefusedConnection::default());
                let conn: Connection = mock.clone();
                let addr = "127.0.0.1:2".parse().unwrap();
                sender
                    .conns
                    .lock()
                    .await
                    .insert(handle, (addr, conn.clone()));
                clients.set_active_addr(handle, Some(addr));
                clients.set_alive(handle, true);
                let task = spawn_local(receive_loop(
                    clients.clone(),
                    target,
                    addr,
                    conn,
                    sender.conns.clone(),
                    sender.recv_tx.clone(),
                    Rc::new(Cell::new(false)),
                ));
                tokio::task::yield_now().await;
                clients.set_port(handle, 1234);
                tokio::time::timeout(Duration::from_secs(1), task)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(mock.closed.load(std::sync::atomic::Ordering::SeqCst));
                assert!(sender.conns.lock().await.is_empty());
            })
            .await;
    }

    #[test]
    fn bind_addr_matches_target_family() {
        let v4: SocketAddr = "192.168.1.1:4242".parse().unwrap();
        assert!(bind_addr_for(v4).is_ipv4());
        let v6: SocketAddr = "[fe80::1]:4242".parse().unwrap();
        assert!(bind_addr_for(v6).is_ipv6());
    }
}
