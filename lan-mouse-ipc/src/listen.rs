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

pub struct AsyncFrontendListener {
    #[cfg(windows)]
    listener: TcpListener,
    #[cfg(unix)]
    listener: UnixListener,
    #[cfg(unix)]
    socket_path: PathBuf,
    line_streams: SelectAll<TaggedStream>,
    /// write halves keyed by the same connection id as the tagged
    /// read stream — removed together on EOF or error so
    /// `frontend_connected` cannot stay stuck on a dead stream
    tx_streams: HashMap<u64, ConnWriter>,
    next_conn_id: u64,
}

impl AsyncFrontendListener {
    pub async fn new() -> Result<Self, IpcListenerCreationError> {
        #[cfg(unix)]
        let (socket_path, listener) = {
            let socket_path = crate::default_socket_path()?;

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
            (socket_path, listener)
        };

        #[cfg(windows)]
        let listener = match TcpListener::bind("127.0.0.1:5252").await {
            Ok(ls) => ls,
            // some other lan-mouse instance has bound the socket in the meantime
            Err(e) if e.kind() == ErrorKind::AddrInUse => {
                return Err(IpcListenerCreationError::AlreadyRunning);
            }
            Err(e) => return Err(IpcListenerCreationError::Bind(e)),
        };

        let adapter = Self {
            listener,
            #[cfg(unix)]
            socket_path,
            line_streams: SelectAll::new(),
            tx_streams: HashMap::new(),
            next_conn_id: 0,
        };

        Ok(adapter)
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

        // TODO do simultaneously
        let mut failed = vec![];
        for (id, tx) in self.tx_streams.iter_mut() {
            if tx.write(json.as_bytes()).await.is_err() {
                failed.push(*id);
            }
        }
        for id in failed {
            self.tx_streams.remove(&id);
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
                    self.tx_streams.remove(&id);
                }
                ConnMsg::Line(Err(e)) => {
                    log::warn!("frontend connection lost: {e}");
                    self.tx_streams.remove(&id);
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
            let tagged: TaggedStream = Box::pin(
                lines
                    .map(ConnMsg::Line)
                    .chain(futures::stream::once(async { ConnMsg::Closed }))
                    .map(move |m| (id, m)),
            );
            self.line_streams.push(tagged);
            self.tx_streams.insert(id, tx);
            sync = true;
        }
        if sync {
            Poll::Ready(Some(Ok(FrontendRequest::Sync)))
        } else {
            Poll::Pending
        }
    }
}
