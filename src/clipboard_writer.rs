//! Serial clipboard application outside the input service loop. Clipboard updates
//! are snapshots: while a write is busy, retain only the latest pending snapshot.
use input_capture::clipboard::ClipboardFeedback;
use input_emulation::clipboard::{ClipboardEmulation, ClipboardError};
use input_event::ClipboardEvent;
use std::future::Future;
use tokio::sync::{mpsc, watch};

type Applied = (ClipboardEvent, Result<(), ClipboardError>);

pub(crate) struct ClipboardWriter {
    pending: watch::Sender<Option<ClipboardEvent>>,
    completed: mpsc::Receiver<Applied>,
}

impl ClipboardWriter {
    pub(crate) fn new(emulation: ClipboardEmulation, feedback: Option<ClipboardFeedback>) -> Self {
        let (pending, requests) = watch::channel(None);
        let (completed, results) = mpsc::channel(1);
        tokio::task::spawn_local(run_writer(requests, completed, move |event| {
            let emulation = emulation.clone();
            let feedback = feedback.clone();
            async move {
                let guard = feedback.map(|feedback| feedback.begin_write());
                let result = emulation.set(event.clone()).await;
                if let Some(guard) = guard {
                    guard.finish(result.is_ok().then_some(event));
                }
                result
            }
        }));
        Self {
            pending,
            completed: results,
        }
    }

    pub(crate) fn submit(&self, event: ClipboardEvent) {
        self.pending.send_replace(Some(event));
    }

    pub(crate) fn clear_pending(&self) {
        self.pending.send_replace(None);
    }

    pub(crate) async fn completed(&mut self) -> Option<Applied> {
        self.completed.recv().await
    }
}

async fn run_writer<F, Fut>(
    mut pending: watch::Receiver<Option<ClipboardEvent>>,
    completed: mpsc::Sender<Applied>,
    mut apply: F,
) where
    F: FnMut(ClipboardEvent) -> Fut,
    Fut: Future<Output = Result<(), ClipboardError>>,
{
    while pending.changed().await.is_ok() {
        let event = pending.borrow_and_update().clone();
        if let Some(event) = event {
            let result = apply(event.clone()).await;
            if completed.send((event, result)).await.is_err() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> ClipboardEvent {
        ClipboardEvent::Text(value.into())
    }

    #[tokio::test]
    async fn busy_writer_retains_latest_and_reports_only_finished_writes() {
        let (pending, requests) = watch::channel(None);
        let (completed, mut results) = mpsc::channel(1);
        let (started, mut starts) = mpsc::channel(1);
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let gate = release.clone();
        let task = tokio::spawn(run_writer(requests, completed, move |event| {
            let started = started.clone();
            let gate = gate.clone();
            async move {
                started.send(event.clone()).await.unwrap();
                if event == text("first") {
                    gate.notified().await;
                }
                Ok(())
            }
        }));
        pending.send_replace(Some(text("first")));
        assert_eq!(starts.recv().await, Some(text("first")));
        assert!(results.try_recv().is_err());
        pending.send_replace(Some(text("obsolete")));
        pending.send_replace(Some(text("latest")));
        release.notify_one();
        assert_eq!(results.recv().await.unwrap().0, text("first"));
        assert_eq!(starts.recv().await, Some(text("latest")));
        assert_eq!(results.recv().await.unwrap().0, text("latest"));
        drop(pending);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn failure_is_reported_and_cleared_pending_is_not_written() {
        let (pending, requests) = watch::channel(None);
        let (completed, mut results) = mpsc::channel(1);
        pending.send_replace(Some(text("discard")));
        pending.send_replace(None);
        let task = tokio::spawn(run_writer(requests, completed, |_| async {
            Err(ClipboardError::Set("refused".into()))
        }));
        tokio::task::yield_now().await;
        assert!(results.try_recv().is_err());
        pending.send_replace(Some(text("fail")));
        let (event, result) = results.recv().await.unwrap();
        assert_eq!(event, text("fail"));
        assert!(result.is_err());
        drop(pending);
        task.await.unwrap();
    }
}
