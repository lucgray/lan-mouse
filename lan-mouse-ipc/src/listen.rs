use futures::{Stream, StreamExt, stream::SelectAll};
#[cfg(unix)]
use std::path::PathBuf;
use std::{
    io::ErrorKind,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf};
use tokio::sync::mpsc;
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

pub struct AsyncFrontendListener {
    #[cfg(windows)]
    listener: TcpListener,
    #[cfg(unix)]
    listener: UnixListener,
    #[cfg(unix)]
    socket_path: PathBuf,
    #[cfg(unix)]
    line_streams: SelectAll<LinesStream<BufReader<ReadHalf<UnixStream>>>>,
    #[cfg(windows)]
    line_streams: SelectAll<LinesStream<BufReader<ReadHalf<TcpStream>>>>,
    tx_streams: Vec<mpsc::Sender<Arc<str>>>,
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
            tx_streams: vec![],
        };

        Ok(adapter)
    }

    pub async fn broadcast(&mut self, notify: FrontendEvent) {
        // encode event
        let mut json = serde_json::to_string(&notify).unwrap();
        json.push('\n');

        let json: Arc<str> = json.into();
        // Disconnect a slow frontend rather than blocking input routing.
        self.tx_streams
            .retain(|tx| tx.try_send(json.clone()).is_ok());
    }
}

fn frontend_writer<W>(mut writer: W, deadline: Duration) -> mpsc::Sender<Arc<str>>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Arc<str>>(64);
    tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            match tokio::time::timeout(deadline, writer.write_all(frame.as_bytes())).await {
                Ok(Ok(())) => {}
                _ => {
                    log::warn!("disconnecting frontend: notification write failed or timed out");
                    break;
                }
            }
        }
        let _ = tokio::time::timeout(deadline, writer.shutdown()).await;
    });
    tx
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
        if let Poll::Ready(Some(Ok(l))) = self.line_streams.poll_next_unpin(cx) {
            let request = serde_json::from_str(l.as_str()).map_err(|e| e.into());
            return Poll::Ready(Some(request));
        }
        let mut sync = false;
        while let Poll::Ready(Ok((stream, _))) = self.listener.poll_accept(cx) {
            let (rx, tx) = tokio::io::split(stream);
            let buf_reader = BufReader::new(rx);
            let lines = buf_reader.lines();
            let lines = LinesStream::new(lines);
            self.line_streams.push(lines);
            self.tx_streams
                .push(frontend_writer(tx, Duration::from_secs(2)));
            sync = true;
        }
        if sync {
            Poll::Ready(Some(Ok(FrontendRequest::Sync)))
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test(flavor = "current_thread")]
    async fn tiny_transport_preserves_complete_ordered_frames() {
        let (writer, mut reader) = tokio::io::duplex(3);
        let tx = frontend_writer(writer, Duration::from_secs(1));
        tx.try_send(Arc::from("{\"one\":1}\n")).unwrap();
        tx.try_send(Arc::from("{\"two\":2}\n")).unwrap();
        drop(tx);
        let mut frames = String::new();
        tokio::time::timeout(Duration::from_secs(1), reader.read_to_string(&mut frames))
            .await
            .unwrap()
            .unwrap();
        let values: Vec<serde_json::Value> = frames
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            values,
            vec![serde_json::json!({"one": 1}), serde_json::json!({"two": 2})]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_frontend_does_not_delay_healthy_writer() {
        let (stalled, _reader) = tokio::io::duplex(1);
        let slow = frontend_writer(stalled, Duration::from_millis(20));
        slow.try_send(Arc::from("blocked\n")).unwrap();
        let (healthy, mut reader) = tokio::io::duplex(1);
        let fast = frontend_writer(healthy, Duration::from_secs(1));
        fast.try_send(Arc::from("ok\n")).unwrap();
        drop(fast);
        let mut frame = String::new();
        tokio::time::timeout(Duration::from_secs(1), reader.read_to_string(&mut frame))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame, "ok\n");
        tokio::time::timeout(Duration::from_secs(1), slow.closed())
            .await
            .unwrap();
    }
}
