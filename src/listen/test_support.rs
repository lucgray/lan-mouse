use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Notify, Semaphore};

/// A controllable clipboard transport; control datagrams remain writable.
pub(crate) struct PausedClipboardConn {
    pub(crate) permits: Semaphore,
    pub(crate) started: Notify,
    pub(crate) starts: AtomicUsize,
    pub(crate) fail: AtomicBool,
    pub(crate) closed: AtomicBool,
    pub(crate) sent: Mutex<Vec<Vec<u8>>>,
}

impl Default for PausedClipboardConn {
    fn default() -> Self {
        Self {
            permits: Semaphore::new(0),
            started: Notify::new(),
            starts: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            sent: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl Conn for PausedClipboardConn {
    async fn connect(&self, _: SocketAddr) -> webrtc_util::Result<()> {
        Ok(())
    }
    async fn recv(&self, _: &mut [u8]) -> webrtc_util::Result<usize> {
        futures::future::pending().await
    }
    async fn recv_from(&self, _: &mut [u8]) -> webrtc_util::Result<(usize, SocketAddr)> {
        futures::future::pending().await
    }
    async fn send(&self, buf: &[u8]) -> webrtc_util::Result<usize> {
        if lan_mouse_proto::is_clipboard_event_type(buf[0])
            || lan_mouse_proto::is_clipboard_fragment_type(buf[0])
        {
            self.starts.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.permits.acquire().await.unwrap().forget();
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::ErrUseClosedNetworkConn);
            }
        }
        self.sent.lock().unwrap().push(buf.to_vec());
        Ok(buf.len())
    }
    async fn send_to(&self, buf: &[u8], _: SocketAddr) -> webrtc_util::Result<usize> {
        self.send(buf).await
    }
    fn local_addr(&self) -> webrtc_util::Result<SocketAddr> {
        Ok("127.0.0.1:4242".parse().unwrap())
    }
    fn remote_addr(&self) -> Option<SocketAddr> {
        None
    }
    async fn close(&self) -> webrtc_util::Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
}

pub(crate) fn listener_with_conn(
    addr: SocketAddr,
    conn: ArcConn,
) -> (LanMouseListener, Sender<ListenEvent>) {
    let (listen_tx, listen_rx) = channel();
    let (request_port_change, _) = channel();
    let (_, port_changed) = channel();
    (
        LanMouseListener {
            listen_rx,
            listen_tx: listen_tx.clone(),
            listen_task: spawn_local(futures::future::pending()),
            conns: Rc::new(AsyncMutex::new(vec![(addr, conn)])),
            request_port_change,
            port_changed,
        },
        listen_tx,
    )
}

pub(crate) async fn datagram_pair() -> (ArcConn, tokio::net::UdpSocket) {
    let conn = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    conn.connect(peer.local_addr().unwrap()).await.unwrap();
    peer.connect(conn.local_addr().unwrap()).await.unwrap();
    (Arc::new(conn), peer)
}

/// Real loopback datagrams reach the production receive loops. Only the first
/// clipboard fragment arrives; continuous motion must not suppress its retry.
pub(crate) async fn missing_fragment_during_motion(peer: &tokio::net::UdpSocket) -> bool {
    let clipboard = ProtoEvent::Input(input_event::Event::Clipboard(
        input_event::ClipboardEvent::Text("x".repeat(4096)),
    ));
    let encoded = lan_mouse_proto::encode_clipboard_event(&clipboard).unwrap();
    let first = lan_mouse_proto::ClipboardFragmenter::new(&encoded)
        .next()
        .unwrap();
    peer.send(&first).await.unwrap();
    let expected_id = lan_mouse_proto::transfer_id(&encoded);
    let motion = ProtoEvent::Input(input_event::Event::Pointer(
        input_event::PointerEvent::Motion {
            time: 1,
            dx: 1.0,
            dy: 1.0,
        },
    ));
    let (buf, len): ([u8; MAX_EVENT_SIZE], usize) = motion.into();
    let mut motion_ticks = tokio::time::interval(Duration::from_millis(20));
    motion_ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let deadline = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(deadline);
    let mut reply = [0u8; 2048];
    loop {
        tokio::select! {
            _ = &mut deadline => return false,
            _ = motion_ticks.tick() => { peer.send(&buf[..len]).await.unwrap(); }
            result = peer.recv(&mut reply) => {
                let n = result.unwrap();
                if let Some((id, seqs)) = lan_mouse_proto::decode_fragment_request(&reply[..n]) {
                    if id == expected_id && seqs.contains(&1) { return true; }
                }
            }
        }
    }
}
