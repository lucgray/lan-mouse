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
