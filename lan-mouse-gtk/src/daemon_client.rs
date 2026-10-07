//! One cancellable IPC worker; GTK never waits for service I/O.
use futures::{Stream, StreamExt};
use lan_mouse_ipc::{
    AsyncFrontendRequestWriter, ConnectionError, FrontendEvent, FrontendRequest, IpcError,
};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(crate) enum Notice {
    Connected(u64),
    Event(u64, Box<FrontendEvent>),
    Disconnected(String),
}

pub(crate) struct DaemonClient {
    requests: Option<mpsc::Sender<(u64, FrontendRequest)>>,
    current: Arc<AtomicU64>,
    cancel: CancellationToken,
    shutdown: CancellationToken,
    thread: Option<thread::JoinHandle<()>>,
}

impl DaemonClient {
    pub(crate) fn new() -> (Self, async_channel::Receiver<Notice>) {
        let (requests, rx) = mpsc::channel(64);
        let (events, receiver) = async_channel::bounded(64);
        let current = Arc::new(AtomicU64::new(0));
        let cancel = CancellationToken::new();
        let worker_current = current.clone();
        let worker_cancel = cancel.clone();
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let thread = thread::spawn(move || {
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(run(
                    rx,
                    events,
                    worker_current,
                    worker_cancel,
                    worker_shutdown,
                    || lan_mouse_ipc::connect_async(Some(Duration::from_secs(1))),
                )),
                Err(error) => {
                    let _ = events
                        .send_blocking(Notice::Disconnected(format!("IPC worker failed: {error}")));
                }
            }
        });
        (
            Self {
                requests: Some(requests),
                current,
                cancel,
                shutdown,
                thread: Some(thread),
            },
            receiver,
        )
    }

    pub(crate) fn request(
        &self,
        generation: u64,
        request: FrontendRequest,
    ) -> Result<(), &'static str> {
        if generation == 0 || self.current.load(Ordering::Acquire) != generation {
            return Err("Service disconnected. Please wait for reconnection.");
        }
        self.requests
            .as_ref()
            .ok_or("Service connection stopped")?
            .try_send((generation, request))
            .map_err(|_| "Service request queue is busy. Please try again.")
    }
}

impl Drop for DaemonClient {
    fn drop(&mut self) {
        // Closing the sender allows accepted requests (including close-time
        // edits) to drain. The worker enforces a two-second total exit limit.
        self.requests.take();
        self.shutdown.cancel();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.cancel.cancel();
    }
}

trait RequestSink {
    fn send(&mut self, request: FrontendRequest) -> impl Future<Output = Result<(), IpcError>>;
}
impl RequestSink for AsyncFrontendRequestWriter {
    async fn send(&mut self, request: FrontendRequest) -> Result<(), IpcError> {
        self.request(request).await
    }
}

async fn run<R, W, C, F>(
    mut requests: mpsc::Receiver<(u64, FrontendRequest)>,
    events: async_channel::Sender<Notice>,
    current: Arc<AtomicU64>,
    cancel: CancellationToken,
    shutdown: CancellationToken,
    mut connect: C,
) where
    R: Stream<Item = Result<FrontendEvent, IpcError>> + Unpin,
    W: RequestSink,
    C: FnMut() -> F,
    F: Future<Output = Result<(R, W), ConnectionError>>,
{
    let mut generation = 0u64;
    let mut retry = Duration::from_millis(250);
    let mut last_error = None;
    loop {
        let connection = tokio::select! {
            _ = cancel.cancelled() => break,
            _ = shutdown.cancelled() => break,
            result = connect() => result,
        };
        let result: Result<(), String> = match connection {
            Ok((mut reader, mut writer)) => {
                generation = generation.checked_add(1).expect("IPC generation exhausted");
                // Drain requests from the old service before publishing this one.
                while requests.try_recv().is_ok() {}
                let session = async {
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        writer.send(FrontendRequest::Sync),
                    )
                    .await
                    .map_err(|_| "Service sync request timed out".to_owned())?
                    .map_err(|error| error.to_string())?;
                    current.store(generation, Ordering::Release);
                    events
                        .send(Notice::Connected(generation))
                        .await
                        .map_err(|_| "Window closed".to_owned())?;

                    loop {
                        tokio::select! {
                            event = reader.next() => match event {
                                Some(Ok(event)) => {
                                    retry = Duration::from_millis(250);
                                    last_error = None;
                                    tokio::select! {
                                        _ = shutdown.cancelled() => {},
                                        sent = events.send(Notice::Event(generation, Box::new(event))) => { sent.map_err(|_| "Window closed".to_owned())?; }
                                    } },
                                Some(Err(error)) => return Err(error.to_string()),
                                None => return Err("Service connection closed".to_owned()),
                            },
                            request = requests.recv() => {
                                let Some((epoch, request)) = request else { return Err("Window closed".to_owned()); };
                                if epoch != generation { continue; }
                                tokio::time::timeout(Duration::from_secs(2), writer.send(request)).await
                                    .map_err(|_| "Service request timed out".to_owned())?
                                    .map_err(|error| error.to_string())?;
                            },
                        }
                    }
                };
                tokio::pin!(session);
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = shutdown.cancelled() => {
                        if tokio::time::timeout(Duration::from_secs(2), &mut session).await.is_err() {
                            log::warn!("IPC shutdown timed out; pending requests may not have been applied");
                        }
                        break;
                    },
                    result = &mut session => result,
                }
            }
            Err(error) => Err(error.to_string()),
        };
        let was_connected = current.swap(0, Ordering::AcqRel) != 0;
        if events.is_closed() {
            break;
        }
        if let Err(error) = result {
            if was_connected || last_error.as_ref() != Some(&error) {
                log::warn!("GTK IPC disconnected: {error}");
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = shutdown.cancelled() => break,
                    sent = events.send(Notice::Disconnected(error.clone())) => { if sent.is_err() { break; } }
                }
                last_error = Some(error);
            }
        }
        tokio::select! { _ = cancel.cancelled() => break, _ = shutdown.cancelled() => break, _ = tokio::time::sleep(retry) => {} }
        retry = retry.saturating_mul(2).min(Duration::from_secs(5));
    }
    current.store(0, Ordering::Release);
}

#[cfg(test)]
pub(crate) fn test_client() -> (
    DaemonClient,
    Arc<AtomicU64>,
    mpsc::Receiver<(u64, FrontendRequest)>,
) {
    let (requests, rx) = mpsc::channel(64);
    let current = Arc::new(AtomicU64::new(1));
    (
        DaemonClient {
            requests: Some(requests),
            current: current.clone(),
            cancel: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            thread: None,
        },
        current,
        rx,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
    use std::collections::VecDeque;

    type Reader = UnboundedReceiver<Result<FrontendEvent, IpcError>>;
    struct Sink {
        sent: UnboundedSender<FrontendRequest>,
        stall: bool,
        stall_after_sync: bool,
    }
    impl RequestSink for Sink {
        async fn send(&mut self, request: FrontendRequest) -> Result<(), IpcError> {
            if self.stall || (self.stall_after_sync && !matches!(request, FrontendRequest::Sync)) {
                std::future::pending::<()>().await;
            }
            self.sent.unbounded_send(request).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "fixture closed")
            })?;
            Ok(())
        }
    }
    fn fixture(
        stall: bool,
    ) -> (
        UnboundedSender<Result<FrontendEvent, IpcError>>,
        Reader,
        Sink,
        UnboundedReceiver<FrontendRequest>,
    ) {
        let (tx, rx) = unbounded();
        let (sent, requests) = unbounded();
        (
            tx,
            rx,
            Sink {
                sent,
                stall,
                stall_after_sync: false,
            },
            requests,
        )
    }
    struct Harness {
        tx: mpsc::Sender<(u64, FrontendRequest)>,
        rx: mpsc::Receiver<(u64, FrontendRequest)>,
        events: async_channel::Sender<Notice>,
        notices: async_channel::Receiver<Notice>,
        current: Arc<AtomicU64>,
        cancel: CancellationToken,
    }
    fn channels() -> Harness {
        let (tx, rx) = mpsc::channel(64);
        let (events, notices) = async_channel::bounded(64);
        Harness {
            tx,
            rx,
            events,
            notices,
            current: Arc::new(AtomicU64::new(0)),
            cancel: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn eof_reconnects_syncs_and_rejects_old_generation_requests() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let (first, reader1, sink1, mut writes1) = fixture(false);
                drop(first);
                let (_second, reader2, sink2, mut writes2) = fixture(false);
                let mut sessions = VecDeque::from([(reader1, sink1), (reader2, sink2)]);
                let Harness {
                    tx,
                    rx,
                    events,
                    notices,
                    current,
                    cancel,
                } = channels();
                let worker = tokio::task::spawn_local(run(
                    rx,
                    events,
                    current.clone(),
                    cancel.clone(),
                    CancellationToken::new(),
                    move || {
                        std::future::ready(sessions.pop_front().ok_or(ConnectionError::Timeout))
                    },
                ));
                assert!(matches!(
                    notices.recv().await.unwrap(),
                    Notice::Connected(1)
                ));
                assert!(matches!(
                    writes1.next().await.unwrap(),
                    FrontendRequest::Sync
                ));
                assert!(matches!(
                    notices.recv().await.unwrap(),
                    Notice::Disconnected(_)
                ));
                assert_eq!(current.load(Ordering::Acquire), 0);
                tx.send((1, FrontendRequest::Delete(7))).await.unwrap();
                assert!(matches!(
                    notices.recv().await.unwrap(),
                    Notice::Connected(2)
                ));
                assert!(matches!(
                    writes2.next().await.unwrap(),
                    FrontendRequest::Sync
                ));
                tx.send((1, FrontendRequest::Delete(7))).await.unwrap();
                tx.send((2, FrontendRequest::Create)).await.unwrap();
                assert!(matches!(
                    writes2.next().await.unwrap(),
                    FrontendRequest::Create
                ));
                cancel.cancel();
                worker.await.unwrap();
                assert_eq!(current.load(Ordering::Acquire), 0);
            })
            .await;
    }

    #[tokio::test]
    async fn malformed_message_reports_reason_and_clears_connection_state() {
        tokio::task::LocalSet::new().run_until(async {
            let (tx, reader, sink, _writes) = fixture(false);
            tx.unbounded_send(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed fixture").into())).unwrap();
            let mut session = Some((reader, sink));
            let Harness { tx: _tx, rx, events, notices, current, cancel } = channels();
            let worker = tokio::task::spawn_local(run(rx, events, current.clone(), cancel.clone(), CancellationToken::new(), move || std::future::ready(session.take().ok_or(ConnectionError::Timeout))));
            assert!(matches!(notices.recv().await.unwrap(), Notice::Connected(1)));
            assert!(matches!(notices.recv().await.unwrap(), Notice::Disconnected(error) if error.contains("malformed")));
            assert_eq!(current.load(Ordering::Acquire), 0);
            cancel.cancel();
            worker.await.unwrap();
        }).await;
    }

    #[tokio::test]
    async fn cancellation_stops_connect_and_blocked_event_delivery() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let Harness {
                    tx: _tx,
                    rx,
                    events,
                    notices: _notices,
                    current,
                    cancel,
                } = channels();
                let worker = tokio::task::spawn_local(run(
                    rx,
                    events,
                    current,
                    cancel.clone(),
                    CancellationToken::new(),
                    std::future::pending::<Result<(Reader, Sink), ConnectionError>>,
                ));
                tokio::task::yield_now().await;
                cancel.cancel();
                tokio::time::timeout(Duration::from_secs(1), worker)
                    .await
                    .unwrap()
                    .unwrap();

                let (tx, reader, sink, _writes) = fixture(false);
                for _ in 0..100 {
                    tx.unbounded_send(Ok(FrontendEvent::CaptureStatus(
                        lan_mouse_ipc::Status::Enabled,
                    )))
                    .unwrap();
                }
                let mut session = Some((reader, sink));
                let Harness {
                    tx: _tx,
                    rx,
                    events,
                    notices: _notices,
                    current,
                    cancel,
                } = channels();
                let worker = tokio::task::spawn_local(run(
                    rx,
                    events,
                    current.clone(),
                    cancel.clone(),
                    CancellationToken::new(),
                    move || std::future::ready(session.take().ok_or(ConnectionError::Timeout)),
                ));
                tokio::task::yield_now().await;
                assert_eq!(current.load(Ordering::Acquire), 1);
                cancel.cancel();
                tokio::time::timeout(Duration::from_secs(1), worker)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(current.load(Ordering::Acquire), 0);
            })
            .await;
    }

    #[tokio::test]
    async fn stalled_sync_has_deadline_and_can_be_cancelled() {
        tokio::task::LocalSet::new().run_until(async {
            let (_tx, reader, sink, _writes) = fixture(true);
            let mut session = Some((reader, sink));
            let Harness { tx: _tx, rx, events, notices, current, cancel } = channels();
            let worker = tokio::task::spawn_local(run(rx, events, current.clone(), cancel.clone(), CancellationToken::new(), move || std::future::ready(session.take().ok_or(ConnectionError::Timeout))));
            assert!(matches!(tokio::time::timeout(Duration::from_secs(3), notices.recv()).await.unwrap().unwrap(), Notice::Disconnected(error) if error.contains("timed out")));
            assert_eq!(current.load(Ordering::Acquire), 0);
            cancel.cancel();
            worker.await.unwrap();
        }).await;
    }

    #[tokio::test]
    async fn stalled_user_request_reports_timeout_and_ends_session() {
        tokio::task::LocalSet::new().run_until(async {
            let (_peer, reader, mut sink, _writes) = fixture(false);
            sink.stall_after_sync = true;
            let mut session = Some((reader, sink));
            let Harness { tx, rx, events, notices, current, cancel } = channels();
            let worker = tokio::task::spawn_local(run(rx, events, current.clone(), cancel.clone(), CancellationToken::new(), move || std::future::ready(session.take().ok_or(ConnectionError::Timeout))));
            assert!(matches!(notices.recv().await.unwrap(), Notice::Connected(1)));
            tx.try_send((1, FrontendRequest::Create)).unwrap();
            assert!(matches!(tokio::time::timeout(Duration::from_secs(3), notices.recv()).await.unwrap().unwrap(), Notice::Disconnected(error) if error.contains("request timed out")));
            assert_eq!(current.load(Ordering::Acquire), 0);
            cancel.cancel();
            worker.await.unwrap();
        }).await;
    }

    #[tokio::test]
    async fn graceful_shutdown_has_total_deadline_for_a_blocked_request() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let (_peer, reader, mut sink, _writes) = fixture(false);
                sink.stall_after_sync = true;
                let mut session = Some((reader, sink));
                let Harness {
                    tx,
                    rx,
                    events,
                    notices,
                    current,
                    cancel,
                } = channels();
                let shutdown = CancellationToken::new();
                let worker = tokio::task::spawn_local(run(
                    rx,
                    events,
                    current.clone(),
                    cancel,
                    shutdown.clone(),
                    move || std::future::ready(session.take().ok_or(ConnectionError::Timeout)),
                ));
                assert!(matches!(
                    notices.recv().await.unwrap(),
                    Notice::Connected(1)
                ));
                tx.try_send((1, FrontendRequest::Create)).unwrap();
                drop(tx);
                shutdown.cancel();
                tokio::time::timeout(Duration::from_secs(3), worker)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(current.load(Ordering::Acquire), 0);
            })
            .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "requires an isolated XDG_RUNTIME_DIR and local socket permission"]
    async fn actual_ipc_reconnects_after_bad_json_and_closes_on_client_drop() {
        use std::{
            io::{BufRead, BufReader, Write},
            os::unix::net::UnixListener,
        };
        let isolated = std::env::var_os("LAN_MOUSE_IPC_TEST_RUNTIME_DIR")
            .expect("set isolated IPC test runtime dir");
        let path = lan_mouse_ipc::default_socket_path().unwrap();
        assert_eq!(path.parent().unwrap(), std::path::Path::new(&isolated));
        assert!(
            path.starts_with("/tmp"),
            "fixture must not connect to a production daemon"
        );
        let listener = UnixListener::bind(&path).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            assert_eq!(request.trim(), "\"Sync\"");
            stream
                .write_all(b"{\"CaptureStatus\":\"Enabled\"}\nnot-json\n")
                .unwrap();
            drop(reader);
            drop(stream);

            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            assert_eq!(request.trim(), "\"Sync\"");
            stream.write_all(b"{\"Settings\":{\"clipboard_enabled\":false,\"invert_scroll\":true,\"mouse_sensitivity\":0.5}}\n").unwrap();
            for _ in 0..16 {
                request.clear();
                reader.read_line(&mut request).unwrap();
                assert_eq!(request.trim(), "\"Create\"");
            }
            request.clear();
            assert_eq!(
                reader.read_line(&mut request).unwrap(),
                0,
                "dropping the client closes its socket"
            );
        });
        let (client, events) = DaemonClient::new();
        let next = || async {
            tokio::time::timeout(Duration::from_secs(3), events.recv())
                .await
                .unwrap()
                .unwrap()
        };
        assert!(matches!(next().await, Notice::Connected(1)));
        assert!(
            matches!(next().await, Notice::Event(1, event) if matches!(*event, FrontendEvent::CaptureStatus(lan_mouse_ipc::Status::Enabled)))
        );
        assert!(
            matches!(next().await, Notice::Disconnected(error) if error.contains("invalid json"))
        );
        assert!(client.request(1, FrontendRequest::Delete(7)).is_err());
        assert!(matches!(next().await, Notice::Connected(2)));
        assert!(
            matches!(next().await, Notice::Event(2, event) if matches!(*event, FrontendEvent::Settings { clipboard_enabled: false, invert_scroll: true, mouse_sensitivity: 0.5 }))
        );
        for _ in 0..16 {
            client.request(2, FrontendRequest::Create).unwrap();
        }
        drop(client);
        server.join().unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn gtk_request_path_is_bounded_and_does_not_replay_disconnected_edits() {
        let (requests, _rx) = mpsc::channel(64);
        let current = Arc::new(AtomicU64::new(1));
        let client = DaemonClient {
            requests: Some(requests),
            current: current.clone(),
            cancel: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            thread: None,
        };
        assert!(client.request(0, FrontendRequest::Create).is_err());
        for _ in 0..64 {
            client.request(1, FrontendRequest::Create).unwrap();
        }
        assert!(client.request(1, FrontendRequest::Delete(7)).is_err());
        current.store(2, Ordering::Release);
        assert!(client.request(1, FrontendRequest::Delete(7)).is_err());
    }
}
