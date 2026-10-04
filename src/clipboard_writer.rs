//! Serial clipboard application outside the input service loop. Clipboard updates
//! are snapshots: while a write is busy, retain only the latest pending snapshot.
use input_capture::clipboard::{ClipboardFeedback, ClipboardWriteGuard};
use input_emulation::clipboard::{ClipboardEmulation, ClipboardError};
use input_event::ClipboardEvent;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, watch};

type Applied = (ClipboardEvent, Result<(), ClipboardError>);

struct PendingWrite {
    event: ClipboardEvent,
    // The receiver takes the lease before applying. The watch value may remain
    // stored afterwards, but must not retain a finished write's suppression.
    guard: Mutex<Option<ClipboardWriteGuard>>,
}

pub(crate) struct ClipboardWriter {
    pending: watch::Sender<Option<Arc<PendingWrite>>>,
    feedback: Option<ClipboardFeedback>,
    completed: mpsc::Receiver<Applied>,
}

impl ClipboardWriter {
    pub(crate) fn new(emulation: ClipboardEmulation, feedback: Option<ClipboardFeedback>) -> Self {
        Self::with_apply(feedback, move |event| {
            let emulation = emulation.clone();
            async move { emulation.set(event).await }
        })
    }

    fn with_apply<F, Fut>(feedback: Option<ClipboardFeedback>, apply: F) -> Self
    where
        F: FnMut(ClipboardEvent) -> Fut + 'static,
        Fut: Future<Output = Result<(), ClipboardError>> + 'static,
    {
        let (pending, requests) = watch::channel(None);
        let (completed, results) = mpsc::channel(1);
        tokio::task::spawn_local(run_writer(requests, completed, apply));
        Self {
            pending,
            feedback,
            completed: results,
        }
    }

    pub(crate) fn submit(&self, event: ClipboardEvent) {
        // Reserve suppression synchronously, before scheduling the serial worker.
        // Replacement drops an unstarted request's lease; the new one remains.
        let guard = self.feedback.clone().map(ClipboardFeedback::begin_write);
        self.pending.send_replace(Some(Arc::new(PendingWrite {
            event,
            guard: Mutex::new(guard),
        })));
    }

    pub(crate) fn clear_pending(&self) {
        self.pending.send_replace(None);
    }

    pub(crate) async fn completed(&mut self) -> Option<Applied> {
        self.completed.recv().await
    }
}

async fn run_writer<F, Fut>(
    mut pending: watch::Receiver<Option<Arc<PendingWrite>>>,
    completed: mpsc::Sender<Applied>,
    mut apply: F,
) where
    F: FnMut(ClipboardEvent) -> Fut,
    Fut: Future<Output = Result<(), ClipboardError>>,
{
    while pending.changed().await.is_ok() {
        // A closed watch can still yield its last unseen value. Do not start
        // that OS write after the owning service drops the result receiver.
        if completed.is_closed() {
            break;
        }
        let request = pending.borrow_and_update().clone();
        if let Some(request) = request {
            let event = request.event.clone();
            let guard = request.guard.lock().unwrap().take();
            let result = apply(event.clone()).await;
            if let Some(guard) = guard {
                guard.finish(result.is_ok().then(|| event.clone()));
            }
            if completed.send((event, result)).await.is_err() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use input_capture::clipboard::ClipboardMonitor;

    fn text(value: &str) -> ClipboardEvent {
        ClipboardEvent::Text(value.into())
    }
    fn paused_monitor() -> ClipboardMonitor {
        // Constructor schedules sampling; disabling before the first await on
        // this current-thread LocalSet prevents any system clipboard access.
        let monitor = ClipboardMonitor::new().unwrap();
        monitor.disable();
        monitor
    }

    #[tokio::test]
    async fn busy_writer_retains_latest_and_suppresses_from_submit_through_all_writes() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let monitor = paused_monitor();
                let feedback = monitor.feedback();
                let (started, mut starts) = mpsc::channel(1);
                let release = Arc::new(tokio::sync::Notify::new());
                let gate = release.clone();
                let mut writer =
                    ClipboardWriter::with_apply(Some(feedback.clone()), move |event| {
                        let started = started.clone();
                        let gate = gate.clone();
                        async move {
                            started.send(event.clone()).await.unwrap();
                            if event == text("first") || event == text("latest-999") {
                                gate.notified().await;
                            }
                            Ok(())
                        }
                    });
                writer.submit(text("first"));
                assert!(feedback.is_writing()); // Worker has not been polled yet.
                assert_eq!(starts.recv().await, Some(text("first")));
                assert!(writer.completed.try_recv().is_err());
                for index in 0..1000 {
                    writer.submit(text(&format!("latest-{index}")));
                }
                assert!(feedback.is_writing());
                release.notify_one();
                assert_eq!(writer.completed().await.unwrap().0, text("first"));
                assert!(feedback.is_writing()); // The pending or active latest owns its lease.
                assert_eq!(starts.recv().await, Some(text("latest-999")));
                release.notify_one();
                assert_eq!(writer.completed().await.unwrap().0, text("latest-999"));
                assert!(!feedback.is_writing());
            })
            .await;
    }

    #[tokio::test]
    async fn failure_is_reported_and_cleared_pending_is_not_written() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let monitor = paused_monitor();
                let feedback = monitor.feedback();
                let mut writer = ClipboardWriter::with_apply(Some(feedback.clone()), |_| async {
                    Err(ClipboardError::Set("refused".into()))
                });
                writer.submit(text("discard"));
                assert!(feedback.is_writing());
                writer.clear_pending();
                assert!(!feedback.is_writing());
                tokio::task::yield_now().await;
                assert!(writer.completed.try_recv().is_err());
                writer.submit(text("fail"));
                let (event, result) = writer.completed().await.unwrap();
                assert_eq!(event, text("fail"));
                assert!(result.is_err());
                assert!(!feedback.is_writing());
            })
            .await;
    }

    #[tokio::test]
    async fn completion_backpressure_preserves_suppression_of_the_latest_pending_write() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let monitor = paused_monitor();
                let feedback = monitor.feedback();
                let (started, mut starts) = mpsc::channel(4);
                let release = Arc::new(tokio::sync::Notify::new());
                let gate = release.clone();
                let mut writer =
                    ClipboardWriter::with_apply(Some(feedback.clone()), move |event| {
                        let started = started.clone();
                        let gate = gate.clone();
                        async move {
                            started.send(event.clone()).await.unwrap();
                            if event == text("latest") {
                                gate.notified().await;
                            }
                            Ok(())
                        }
                    });
                writer.submit(text("first"));
                assert_eq!(starts.recv().await, Some(text("first")));
                tokio::task::yield_now().await;
                writer.submit(text("second"));
                assert_eq!(starts.recv().await, Some(text("second")));
                tokio::task::yield_now().await; // second completion is blocked by the full result slot.
                writer.submit(text("obsolete"));
                writer.submit(text("latest"));
                assert!(feedback.is_writing());
                assert!(starts.try_recv().is_err());
                assert_eq!(writer.completed().await.unwrap().0, text("first"));
                assert_eq!(writer.completed().await.unwrap().0, text("second"));
                assert_eq!(starts.recv().await, Some(text("latest")));
                assert!(feedback.is_writing());
                release.notify_one();
                assert_eq!(writer.completed().await.unwrap().0, text("latest"));
                assert!(!feedback.is_writing());
            })
            .await;
    }

    #[tokio::test]
    async fn clearing_pending_does_not_release_an_active_write_and_drop_releases_unstarted_leases()
    {
        tokio::task::LocalSet::new()
            .run_until(async {
                let monitor = paused_monitor();
                let feedback = monitor.feedback();
                let (started, mut starts) = mpsc::channel(1);
                let release = Arc::new(tokio::sync::Notify::new());
                let gate = release.clone();
                let mut writer =
                    ClipboardWriter::with_apply(Some(feedback.clone()), move |event| {
                        let started = started.clone();
                        let gate = gate.clone();
                        async move {
                            started.send(event).await.unwrap();
                            gate.notified().await;
                            Ok(())
                        }
                    });
                writer.submit(text("active"));
                starts.recv().await.unwrap();
                writer.submit(text("pending"));
                writer.clear_pending();
                assert!(feedback.is_writing());
                release.notify_one();
                writer.completed().await.unwrap().1.unwrap();
                assert!(!feedback.is_writing());
                writer.submit(text("never applied"));
                assert!(feedback.is_writing());
                drop(writer);
                tokio::time::timeout(std::time::Duration::from_millis(100), async {
                    while feedback.is_writing() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert!(starts.recv().await.is_none());
            })
            .await;
    }
}
