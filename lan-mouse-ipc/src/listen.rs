use futures::{Stream, StreamExt, stream::SelectAll};
#[cfg(unix)]
use std::path::PathBuf;
use std::{
    collections::HashMap,
    io::ErrorKind,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, WriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::LinesStream;

#[cfg(unix)]
use tokio::net::UnixListener;
#[cfg(unix)]
use tokio::net::UnixStream;

#[cfg(windows)]
use tokio::net::TcpListener;
#[cfg(windows)]
use tokio::net::TcpStream;

use crate::{FrontendEvent, FrontendRequest, IpcError, IpcListenerCreationError};

#[cfg(unix)]
type ConnWriter = WriteHalf<UnixStream>;
#[cfg(windows)]
type ConnWriter = WriteHalf<TcpStream>;

/// a message from one frontend's read stream — either a read result
/// or the stream's end-of-file marker, tagged so the matching write
/// half can be dropped at the same time
enum ConnMsg {
    Line(std::io::Result<String>),
    Closed,
}

type TaggedStream = Pin<Box<dyn Stream<Item = (u64, ConnMsg)>>>;

/// per-connection outbound queue — a frontend this far behind is
/// dropped instead of back-pressuring the service loop
const FRONTEND_QUEUE: usize = 128;

pub struct AsyncFrontendListener {
    #[cfg(windows)]
    listener: TcpListener,
    #[cfg(unix)]
    listener: UnixListener,
    #[cfg(unix)]
    socket_path: PathBuf,
    line_streams: SelectAll<TaggedStream>,
    /// outbound queues keyed by the same connection id as the tagged
    /// read stream — removed together on EOF or error so
    /// `frontend_connected` cannot stay stuck on a dead stream
    tx_streams: HashMap<u64, mpsc::Sender<String>>,
    /// per-connection kill switch — resolves the tagged read stream
    /// (via take_until) so a dropped client releases its read half
    kill: HashMap<u64, oneshot::Sender<()>>,
    /// writer task handles — a writer parked in `write_all` by
    /// backpressure never polls its queue, so it cannot notice the
    /// sender being dropped; drop_conn must abort it explicitly to
    /// release the socket's write half
    writers: HashMap<u64, tokio::task::JoinHandle<()>>,
    next_conn_id: u64,
}

impl AsyncFrontendListener {
    pub async fn new() -> Result<Self, IpcListenerCreationError> {
        #[cfg(unix)]
        return Self::new_with_path(crate::default_socket_path()?).await;

        #[cfg(windows)]
        {
            let listener = match TcpListener::bind("127.0.0.1:5252").await {
                Ok(ls) => ls,
                // some other lan-mouse instance has bound the socket in the meantime
                Err(e) if e.kind() == ErrorKind::AddrInUse => {
                    return Err(IpcListenerCreationError::AlreadyRunning);
                }
                Err(e) => return Err(IpcListenerCreationError::Bind(e)),
            };
            Ok(Self {
                listener,
                line_streams: SelectAll::new(),
                tx_streams: HashMap::new(),
                kill: HashMap::new(),
                writers: HashMap::new(),
                next_conn_id: 0,
            })
        }
    }

    /// bind a unix socket at an explicit path — `new()` uses the
    /// platform default; tests pass their own
    #[cfg(unix)]
    async fn new_with_path(socket_path: PathBuf) -> Result<Self, IpcListenerCreationError> {
        log::debug!("remove socket: {socket_path:?}");
        if socket_path.exists() {
            // try to connect to see if some other instance
            // of lan-mouse is already running
            match UnixStream::connect(&socket_path).await {
                // connected -> lan-mouse is already running
                Ok(_) => return Err(IpcListenerCreationError::AlreadyRunning),
                // lan-mouse is not running but a socket was left behind
                Err(e) => {
                    log::debug!("{socket_path:?}: {e} - removing left behind socket");
                    let _ = std::fs::remove_file(&socket_path);
                }
            }
        }
        let listener = match UnixListener::bind(&socket_path) {
            Ok(ls) => ls,
            // some other lan-mouse instance has bound the socket in the meantime
            Err(e) if e.kind() == ErrorKind::AddrInUse => {
                return Err(IpcListenerCreationError::AlreadyRunning);
            }
            Err(e) => return Err(IpcListenerCreationError::Bind(e)),
        };
        Ok(Self {
            listener,
            socket_path,
            line_streams: SelectAll::new(),
            tx_streams: HashMap::new(),
            kill: HashMap::new(),
            writers: HashMap::new(),
            next_conn_id: 0,
        })
    }

    /// whether any frontend is currently connected — used to decide
    /// whether OS-level notifications are needed as a fallback channel.
    pub fn frontend_connected(&self) -> bool {
        !self.tx_streams.is_empty()
    }

    pub async fn broadcast(&mut self, notify: FrontendEvent) {
        // encode event
        let mut json = match serde_json::to_string(&notify) {
            Ok(json) => json,
            Err(e) => {
                log::error!("failed to encode frontend event: {e}");
                return;
            }
        };
        json.push('\n');

        // non-blocking enqueue per connection — a slow or dead
        // frontend fills its bounded queue and gets dropped rather
        // than stalling every other frontend and the service loop
        let mut failed = vec![];
        for (id, tx) in self.tx_streams.iter() {
            match tx.try_send(json.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    log::warn!("frontend {id} not keeping up, disconnecting");
                    failed.push(*id);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => failed.push(*id),
            }
        }
        for id in failed {
            self.drop_conn(id);
        }
    }

    /// release every resource of one connection: the outbound queue
    /// sender (so a queued writer ends), the writer task itself (it
    /// may be parked in `write_all` on a non-reading client — abort
    /// drops the socket's write half), and the read stream (kill
    /// resolves take_until, dropping the read half) — the client's
    /// socket fully closes
    fn drop_conn(&mut self, id: u64) {
        self.tx_streams.remove(&id);
        if let Some(writer) = self.writers.remove(&id) {
            writer.abort();
        }
        if let Some(kill) = self.kill.remove(&id) {
            let _ = kill.send(());
        }
    }
}

#[cfg(unix)]
impl Drop for AsyncFrontendListener {
    fn drop(&mut self) {
        log::debug!("remove socket: {:?}", self.socket_path);
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

impl Stream for AsyncFrontendListener {
    type Item = Result<FrontendRequest, IpcError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // drain read events first: a stream that hit EOF or an I/O
        // error drops its write half so broadcasts stop addressing it
        while let Poll::Ready(Some((id, msg))) = self.line_streams.poll_next_unpin(cx) {
            match msg {
                ConnMsg::Closed => {
                    self.drop_conn(id);
                }
                ConnMsg::Line(Err(e)) => {
                    log::warn!("frontend connection lost: {e}");
                    self.drop_conn(id);
                }
                ConnMsg::Line(Ok(l)) => {
                    let request = serde_json::from_str(l.as_str()).map_err(|e| e.into());
                    return Poll::Ready(Some(request));
                }
            }
        }
        let mut sync = false;
        while let Poll::Ready(Ok((stream, _))) = self.listener.poll_accept(cx) {
            let (rx, tx) = tokio::io::split(stream);
            let buf_reader = BufReader::new(rx);
            let lines = buf_reader.lines();
            let lines = LinesStream::new(lines);
            let id = self.next_conn_id;
            self.next_conn_id += 1;
            let (kill_tx, kill_rx) = oneshot::channel::<()>();
            self.kill.insert(id, kill_tx);
            let tagged: TaggedStream = Box::pin(
                lines
                    .map(ConnMsg::Line)
                    .chain(futures::stream::once(async { ConnMsg::Closed }))
                    .take_until(kill_rx)
                    .map(move |m| (id, m)),
            );
            self.line_streams.push(tagged);
            // a dedicated writer task per connection: broadcasts just
            // enqueue, and write_all (not write) guarantees the whole
            // frame lands; a wedged client only blocks its own task
            let (msg_tx, mut msg_rx) = mpsc::channel::<String>(FRONTEND_QUEUE);
            self.tx_streams.insert(id, msg_tx);
            let writer = tokio::spawn(async move {
                let mut tx: ConnWriter = tx;
                while let Some(msg) = msg_rx.recv().await {
                    if tx.write_all(msg.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
            self.writers.insert(id, writer);
            sync = true;
        }
        if sync {
            Poll::Ready(Some(Ok(FrontendRequest::Sync)))
        } else {
            Poll::Pending
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    /// unique socket path per test so parallel tests don't collide
    fn test_socket_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("lan-mouse-test-{name}-{}.sock", std::process::id()))
    }

    /// drive one poll cycle: a kill only takes effect once the
    /// SelectAll drains the ended stream — in production the service
    /// loop polls continuously, tests must do it explicitly
    async fn pump(listener: &mut AsyncFrontendListener) {
        futures::future::poll_fn(|cx| {
            let _ = listener.poll_next_unpin(cx);
            std::task::Poll::Ready(())
        })
        .await;
    }

    /// a dropped frontend must release reader, writer, queue and the
    /// socket itself — the client observes a full close (EOF), not a
    /// half-dead connection that blocks nothing but also delivers
    /// nothing
    #[tokio::test(flavor = "current_thread")]
    async fn drop_conn_releases_all_resources() {
        let path = test_socket_path("drop-conn");
        let mut listener = AsyncFrontendListener::new_with_path(path.clone())
            .await
            .expect("listener");
        let mut client = UnixStream::connect(&path).await.expect("connect");

        // accept emits the Sync request for the new connection
        let item = listener.next().await.expect("stream ended");
        assert!(matches!(item, Ok(FrontendRequest::Sync)));
        assert!(listener.frontend_connected());

        listener.drop_conn(0);
        assert!(!listener.frontend_connected());
        assert!(listener.tx_streams.is_empty());
        assert!(listener.kill.is_empty());
        pump(&mut listener).await;

        // once both halves drop the client sees EOF
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("client read timed out — socket not fully closed")
            .expect("client read");
        assert_eq!(n, 0, "expected EOF after disconnect");
    }

    /// a queue overflow drops only that client — others still receive
    /// events and the loop is not blocked
    #[tokio::test(flavor = "current_thread")]
    async fn slow_client_dropped_others_unaffected() {
        let path = test_socket_path("slow-client");
        let mut listener = AsyncFrontendListener::new_with_path(path.clone())
            .await
            .expect("listener");
        // client 1 reads; client 2 never does — one next() drains all
        // pending accepts, so a second call would pend forever
        let mut good = UnixStream::connect(&path).await.expect("connect good");
        let _stuck = UnixStream::connect(&path).await.expect("connect stuck");
        let _ = listener.next().await;
        assert_eq!(listener.tx_streams.len(), 2);

        // flood: messages bigger than any reasonable socket buffer so
        // the stuck client's writer blocks, its queue fills, and it
        // gets dropped — the healthy client must keep receiving.
        // try_read keeps good's own queue draining so only the truly
        // stuck client hits the cap
        let big = FrontendEvent::Error("x".repeat(64 * 1024));
        let mut buf = vec![0u8; 256 * 1024];
        let mut dropped_at = None;
        for i in 0..512 {
            listener.broadcast(big.clone()).await;
            tokio::task::yield_now().await;
            while good.try_read(&mut buf).is_ok() {}
            if listener.tx_streams.len() == 1 {
                dropped_at = Some(i);
                break;
            }
        }
        assert!(
            dropped_at.is_some(),
            "stuck client was never dropped (queue or socket buffers too large?)"
        );
        assert!(listener.frontend_connected());

        // the surviving connection still gets events end to end —
        // send one last event since the loop drained everything prior
        listener
            .broadcast(FrontendEvent::Error("still alive".to_string()))
            .await;
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), good.read(&mut buf))
            .await
            .expect("good client read timed out")
            .expect("read");
        assert!(n > 0);
    }

    /// a writer parked in `write_all` by a non-reading client never
    /// polls its queue, so removing the sender alone cannot end it —
    /// drop_conn must abort the task for the socket's write half to
    /// be released. Before that fix this test timed out on read: the
    /// stuck client's socket stayed half-open forever.
    #[tokio::test(flavor = "current_thread")]
    async fn blocked_writer_aborted_releases_socket() {
        let path = test_socket_path("blocked-writer");
        let mut listener = AsyncFrontendListener::new_with_path(path.clone())
            .await
            .expect("listener");
        // a client that never reads: its writer ends up parked in
        // write_all once the socket buffer fills
        let mut stuck = UnixStream::connect(&path).await.expect("connect stuck");
        let _ = listener.next().await;
        assert!(listener.frontend_connected());

        // flood until the queue overflows and the conn is dropped —
        // at that point the writer task is blocked in write_all, not
        // in recv(), so only abort() can stop it
        let big = FrontendEvent::Error("x".repeat(64 * 1024));
        for _ in 0..512 {
            listener.broadcast(big.clone()).await;
            tokio::task::yield_now().await;
            if !listener.frontend_connected() {
                break;
            }
        }
        assert!(
            !listener.frontend_connected(),
            "stuck client was never dropped (socket buffers too large?)"
        );
        assert!(listener.writers.is_empty());
        pump(&mut listener).await;

        // both halves released → after draining whatever the writer
        // pushed before the abort, the client observes EOF promptly
        let mut buf = vec![0u8; 64 * 1024];
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let n = stuck.read(&mut buf).await.expect("client read");
                if n == 0 {
                    break;
                }
            }
        })
        .await
        .expect("client read timed out — blocked writer still holds the socket");
    }
}
