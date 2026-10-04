use crate::listen::ArcConn;
use futures::{FutureExt, StreamExt, future::LocalBoxFuture, stream::FuturesUnordered};
use lan_mouse_proto::{MAX_EVENT_SIZE, ProtoEvent};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const MAX_ACTIVE: usize = 4;
const MAX_PENDING: usize = 128;
const MAX_PENDING_PER_PEER: usize = 32;
const SEND_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
pub(crate) enum ControlSendError {
    #[error("control reply queue is full")]
    Busy,
    #[error("control reply canceled")]
    Canceled,
    #[error("control reply timed out")]
    Timeout,
    #[error(transparent)]
    Transport(#[from] webrtc_util::Error),
    #[error("control reply wrote {sent} of {expected} bytes")]
    Incomplete { sent: usize, expected: usize },
}

pub(crate) struct ControlCompletion {
    pub addr: SocketAddr,
    pub conn: ArcConn,
    pub result: Result<(), ControlSendError>,
}

struct ControlRequest {
    addr: SocketAddr,
    conn: ArcConn,
    // Fixed-size encoded control packet; never retains a clipboard payload.
    buf: [u8; MAX_EVENT_SIZE],
    len: usize,
    deadline: tokio::time::Instant,
}

/// Independent sends, FIFO per peer, bounded globally. Unlike clipboard values,
/// Ack/Leave messages cannot be replaced by the newest pending message.
#[derive(Default)]
pub(crate) struct ControlJobs {
    pending: VecDeque<ControlRequest>,
    active: HashMap<SocketAddr, (ArcConn, CancellationToken, tokio::task::AbortHandle)>,
    jobs: FuturesUnordered<LocalBoxFuture<'static, ControlCompletion>>,
}

impl ControlJobs {
    pub fn submit(
        &mut self,
        addr: SocketAddr,
        conn: ArcConn,
        event: ProtoEvent,
    ) -> Result<(), ControlSendError> {
        if self.pending.len() >= MAX_PENDING
            || self.pending.iter().filter(|r| r.addr == addr).count() >= MAX_PENDING_PER_PEER
        {
            return Err(ControlSendError::Busy);
        }
        let (buf, len) = event.into();
        self.pending.push_back(ControlRequest {
            addr,
            conn,
            buf,
            len,
            deadline: tokio::time::Instant::now() + SEND_TIMEOUT,
        });
        self.pump();
        Ok(())
    }

    fn pump(&mut self) {
        while self.active.len() < MAX_ACTIVE {
            let Some(index) = self
                .pending
                .iter()
                .position(|r| !self.active.contains_key(&r.addr))
            else {
                break;
            };
            let request = self.pending.remove(index).expect("pending control reply");
            let addr = request.addr;
            let conn = request.conn.clone();
            let cancellation = CancellationToken::new();
            let task_cancel = cancellation.clone();
            let task = tokio::spawn(async move {
                let result = tokio::select! {
                    biased;
                    _ = task_cancel.cancelled() => Err(ControlSendError::Canceled),
                    result = async {
                        // The deadline includes queue residence. Do not send a
                        // stale Leave/Ack after a long backlog or dispatcher stall.
                        if tokio::time::Instant::now() >= request.deadline {
                            return Err(ControlSendError::Timeout);
                        }
                        match tokio::time::timeout_at(request.deadline, request.conn.send(&request.buf[..request.len])).await {
                        Err(_) => Err(ControlSendError::Timeout),
                        Ok(Err(error)) => Err(ControlSendError::Transport(error)),
                        Ok(Ok(sent)) if sent != request.len => Err(ControlSendError::Incomplete { sent, expected: request.len }),
                        Ok(Ok(_)) => Ok(()),
                        }
                    } => result,
                };
                ControlCompletion {
                    addr,
                    conn: request.conn,
                    result,
                }
            });
            self.active
                .insert(addr, (conn.clone(), cancellation, task.abort_handle()));
            self.jobs.push(
                async move {
                    task.await.unwrap_or_else(|error| ControlCompletion {
                        addr,
                        conn,
                        result: Err(ControlSendError::Transport(
                            std::io::Error::other(error).into(),
                        )),
                    })
                }
                .boxed_local(),
            );
        }
    }

    pub async fn completed(&mut self) -> ControlCompletion {
        if self.jobs.is_empty() {
            std::future::pending::<()>().await;
        }
        let completed = self.jobs.next().await.expect("active control reply");
        self.active.remove(&completed.addr);
        // Do not start the next packet after failure. The caller handles failed
        // current sessions before polling again; queued packets are discarded.
        if completed.result.is_err() {
            self.pending
                .retain(|r| r.addr != completed.addr || !Arc::ptr_eq(&r.conn, &completed.conn));
        }
        self.pump();
        completed
    }

    pub fn cancel_stale(&mut self, addr: SocketAddr, current: Option<&ArcConn>) {
        let matches = |conn: &ArcConn| current.is_some_and(|c| Arc::ptr_eq(conn, c));
        self.pending.retain(|r| r.addr != addr || matches(&r.conn));
        if let Some((conn, token, _)) = self.active.get(&addr) {
            if !matches(conn) {
                token.cancel();
            }
        }
    }

    #[cfg(test)]
    pub fn sizes(&self) -> (usize, usize) {
        (self.active.len(), self.pending.len())
    }
}

impl Drop for ControlJobs {
    fn drop(&mut self) {
        for (_, cancellation, abort) in self.active.values() {
            cancellation.cancel();
            abort.abort();
        }
    }
}
