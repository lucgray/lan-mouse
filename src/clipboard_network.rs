use crate::listen::{ArcConn, ClipboardSendError, send_clipboard_reply};
use futures::{FutureExt, StreamExt, future::LocalBoxFuture, stream::FuturesUnordered};
use input_event::{ClipboardContentKind, ClipboardEvent, Event};
use lan_mouse_proto::ProtoEvent;
use std::{collections::HashMap, net::SocketAddr, time::Duration};
use tokio_util::sync::CancellationToken;

const MAX_ACTIVE: usize = 4;
const MAX_PENDING: usize = 32;
const SEND_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct ClipboardRequest {
    pub addr: SocketAddr,
    pub conn: Option<ArcConn>,
    pub event: ClipboardEvent,
    pub generation: u64,
    pub cancellation: CancellationToken,
    pub session_cancellation: Option<CancellationToken>,
    pub outgoing: Option<(u64, u64)>,
}

pub(crate) struct ClipboardCompletion {
    pub addr: SocketAddr,
    pub generation: u64,
    pub kind: ClipboardContentKind,
    pub bytes: usize,
    pub result: Result<(), ClipboardSendError>,
    pub outgoing: Option<(u64, u64)>,
    pub conn: Option<ArcConn>,
}

/// Polled alongside input, never awaited inside input dispatch. One active send
/// per address; bounded concurrency and latest-value pending slots.
#[derive(Default)]
pub(crate) struct ClipboardJobs {
    pending: HashMap<SocketAddr, ClipboardRequest>,
    active: HashMap<SocketAddr, (CancellationToken, Option<ArcConn>, tokio::task::AbortHandle)>,
    jobs: FuturesUnordered<LocalBoxFuture<'static, ClipboardCompletion>>,
}

impl ClipboardJobs {
    pub fn submit(&mut self, request: ClipboardRequest) -> Result<(), Box<ClipboardRequest>> {
        if self.pending.len() == MAX_PENDING && !self.pending.contains_key(&request.addr) {
            return Err(Box::new(request));
        }
        self.pending.insert(request.addr, request);
        self.pump();
        Ok(())
    }

    fn pump(&mut self) {
        while self.active.len() < MAX_ACTIVE {
            let Some(addr) = self
                .pending
                .keys()
                .find(|addr| !self.active.contains_key(addr))
                .copied()
            else {
                break;
            };
            let request = self.pending.remove(&addr).expect("pending request");
            let generation = request.generation;
            let kind = request.event.kind();
            let bytes = request.event.content_len();
            let outgoing = request.outgoing;
            let token = request.cancellation.clone();
            let conn = request.conn.clone();
            let completion_conn = conn.clone();
            // Run independently so a different dispatch path awaiting I/O
            // cannot stop this send's timeout or cancellation from being polled.
            let task = tokio::spawn(async move {
                let mut completed = ClipboardCompletion {
                    addr,
                    generation,
                    kind,
                    bytes,
                    result: Ok(()),
                    outgoing,
                    conn: request.conn.clone(),
                };
                completed.result = tokio::select! {
                    biased;
                    _ = request.cancellation.cancelled() => Err(ClipboardSendError::Canceled),
                    _ = async {
                        match &request.session_cancellation {
                            Some(token) => token.cancelled().await,
                            None => std::future::pending().await,
                        }
                    } => Err(ClipboardSendError::Canceled),
                    result = tokio::time::timeout(SEND_TIMEOUT, send_clipboard_reply(
                        request.conn, ProtoEvent::Input(Event::Clipboard(request.event)),
                    )) => match result {
                        Ok(result) => result,
                        Err(_) => Err(ClipboardSendError::Timeout),
                    },
                };
                completed
            });
            self.active.insert(addr, (token, conn, task.abort_handle()));
            self.jobs.push(
                async move {
                    match task.await {
                        Ok(completed) => completed,
                        Err(error) => ClipboardCompletion {
                            addr,
                            generation,
                            kind,
                            bytes,
                            outgoing,
                            conn: completion_conn,
                            result: Err(ClipboardSendError::Transport(
                                std::io::Error::other(error).into(),
                            )),
                        },
                    }
                }
                .boxed_local(),
            );
        }
    }

    pub async fn completed(&mut self) -> ClipboardCompletion {
        if self.jobs.is_empty() {
            std::future::pending::<()>().await;
        }
        let result = self.jobs.next().await.expect("active job");
        self.active.remove(&result.addr);
        self.pump();
        result
    }

    pub fn cancel_stale(&mut self, addr: SocketAddr, current: Option<&ArcConn>) {
        let matches = |conn: Option<&ArcConn>| match (conn, current) {
            (Some(conn), Some(current)) => std::sync::Arc::ptr_eq(conn, current),
            _ => false,
        };
        if self
            .pending
            .get(&addr)
            .is_some_and(|request| !matches(request.conn.as_ref()))
        {
            self.pending.remove(&addr);
        }
        if let Some((token, conn, _)) = self.active.get(&addr) {
            if !matches(conn.as_ref()) {
                token.cancel();
            }
        }
    }

    #[cfg(test)]
    pub fn sizes(&self) -> (usize, usize) {
        (self.active.len(), self.pending.len())
    }
}

impl Drop for ClipboardJobs {
    fn drop(&mut self) {
        for (token, _, abort) in self.active.values() {
            token.cancel();
            abort.abort();
        }
        // Dropping futures cancels sends immediately; input connections remain.
    }
}
