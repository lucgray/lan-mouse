use arboard::{Clipboard, ImageData};
use input_event::{ClipboardEvent, Event, encode_image_rgba};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::spawn_blocking;
use tokio::time::{MissedTickBehavior, interval};

use crate::{CaptureError, CaptureEvent};

#[derive(Debug)]
struct QueuedClipboard {
    event: CaptureEvent,
    revision: u64,
}

/// Clipboard monitor that watches for clipboard changes
pub struct ClipboardMonitor {
    event_rx: Receiver<QueuedClipboard>,
    _event_tx: Sender<QueuedClipboard>,
    task: tokio::task::JoinHandle<()>,
    last_content: Arc<Mutex<Option<ClipboardEvent>>>,
    last_change: Arc<Mutex<Option<Instant>>>,
    enabled: Arc<Mutex<bool>>,
    remote_write: Arc<AtomicBool>,
    write_revision: Arc<AtomicU64>,
    refresh_local: Arc<AtomicU64>,
}

/// Shared suppression state for one serial remote clipboard writer.
#[derive(Clone)]
pub struct ClipboardFeedback {
    last_content: Arc<Mutex<Option<ClipboardEvent>>>,
    last_change: Arc<Mutex<Option<Instant>>>,
    remote_write: Arc<AtomicBool>,
    write_revision: Arc<AtomicU64>,
    refresh_local: Arc<AtomicU64>,
}

pub struct ClipboardWriteGuard(ClipboardFeedback, bool);

impl ClipboardFeedback {
    pub fn begin_write(self) -> ClipboardWriteGuard {
        self.remote_write.store(true, Ordering::SeqCst);
        self.write_revision.fetch_add(1, Ordering::SeqCst);
        ClipboardWriteGuard(self, false)
    }
    fn publish_sample(
        &self,
        enabled: &Mutex<bool>,
        reader: &mut ClipboardReader,
        read_revision: u64,
        current_content: ClipboardEvent,
    ) -> Option<QueuedClipboard> {
        // Keep enabled -> content -> change lock order. Toggle must
        // not deadlock against cache publication, nor hold a lock
        // while the event queue waits for its consumer.
        let enabled = enabled.lock().unwrap();
        if !*enabled {
            reader.images.0 = None;
            return None;
        }
        let mut last_content = self.last_content.lock().unwrap();
        let mut last_change = self.last_change.lock().unwrap();
        if self.remote_write.load(Ordering::SeqCst)
            || self.write_revision.load(Ordering::SeqCst) != read_revision
        {
            reader.images.0 = None;
            return None;
        }

        let content_changed = (read_revision != 0
            && self.refresh_local.load(Ordering::SeqCst) == read_revision)
            || match last_content.as_ref() {
                None => true,
                Some(last) => last != &current_content,
            };

        if content_changed {
            // Debounce: ignore changes within 200ms of last change
            // This prevents infinite loops when both sides update clipboard
            let should_emit = match *last_change {
                None => true,
                Some(instant) => instant.elapsed() > Duration::from_millis(200),
            };

            if should_emit {
                log::info!(
                    "Clipboard changed: {:?} ({} bytes)",
                    current_content.kind(),
                    current_content.content_len()
                );
                *last_content = Some(current_content.clone());
                *last_change = Some(Instant::now());
                // An old sample must not clear a newer failed-write refresh.
                let _ = self.refresh_local.compare_exchange(
                    read_revision,
                    0,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );

                return Some(QueuedClipboard {
                    event: CaptureEvent::Input(Event::Clipboard(current_content)),
                    revision: read_revision,
                });
            } else {
                reader.images.0 = None;
                log::trace!("Clipboard changed but debounced (too recent)");
            }
        }
        None
    }
}

impl ClipboardWriteGuard {
    pub fn finish(mut self, content: Option<ClipboardEvent>) {
        self.1 = content.is_some();
        if let Some(content) = content {
            *self.0.last_content.lock().unwrap() = Some(content);
            *self.0.last_change.lock().unwrap() = Some(Instant::now());
            self.0.refresh_local.store(0, Ordering::SeqCst);
        }
    }
}

impl Drop for ClipboardWriteGuard {
    fn drop(&mut self) {
        let revision = self
            .0
            .write_revision
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1);
        if !self.1 {
            self.0.refresh_local.store(revision, Ordering::SeqCst);
        }
        self.0.remote_write.store(false, Ordering::SeqCst);
    }
}

/// Keep at most one raw image, bounded to 64 MiB. Oversized images still
/// encode normally, but are not retained. No duplicate PNG is cached.
#[derive(Default)]
struct ImageCache(Option<(usize, usize, Vec<u8>)>);

impl ImageCache {
    fn encode_with<F>(&mut self, image: ImageData<'_>, encode: F) -> Option<ClipboardEvent>
    where
        F: FnOnce(u32, u32, &[u8]) -> Option<Vec<u8>>,
    {
        let width = u32::try_from(image.width).ok()?;
        let height = u32::try_from(image.height).ok()?;
        let expected = image.width.checked_mul(image.height)?.checked_mul(4)?;
        if expected != image.bytes.len() {
            self.0 = None;
            return None;
        }
        if self.0.as_ref().is_some_and(|(width, height, bytes)| {
            *width == image.width
                && *height == image.height
                && bytes.as_slice() == image.bytes.as_ref()
        }) {
            return None;
        }
        let png = encode(width, height, &image.bytes)?;
        self.0 = if image.bytes.len() <= 64 * 1024 * 1024 {
            Some((image.width, image.height, image.bytes.into_owned()))
        } else {
            None
        };
        Some(ClipboardEvent::Image(png))
    }
}

#[derive(Default)]
struct ClipboardReader {
    clipboard: Option<Clipboard>,
    images: ImageCache,
    revision: Option<u64>,
}

impl ClipboardReader {
    fn begin_sample(&mut self, revision: u64, refresh: bool) {
        if self.revision != Some(revision) {
            if refresh {
                self.images.0 = None;
            }
            self.revision = Some(revision);
        }
    }

    /// Read text first, then images. An unchanged image skips PNG encoding.
    fn read(&mut self) -> Option<ClipboardEvent> {
        if self.clipboard.is_none() {
            self.clipboard = Clipboard::new()
                .map_err(|e| {
                    log::debug!("Failed to create clipboard: {e}");
                })
                .ok();
        }
        let clipboard = self.clipboard.as_mut()?;
        match clipboard.get_text() {
            Ok(text) => {
                self.images.0 = None;
                return Some(ClipboardEvent::Text(text));
            }
            Err(e) => log::trace!("No clipboard text: {e}"),
        }
        match clipboard.get_image() {
            Ok(image) => self.images.encode_with(image, encode_image_rgba),
            Err(e) => {
                log::trace!("No clipboard image: {e}");
                self.images.0 = None;
                // Retry platform access on the next poll after a failed read.
                self.clipboard = None;
                None
            }
        }
    }
}

impl ClipboardMonitor {
    pub fn new() -> Result<Self, CaptureError> {
        let (event_tx, event_rx) = mpsc::channel(16);
        let last_content: Arc<Mutex<Option<ClipboardEvent>>> = Arc::new(Mutex::new(None));
        let last_change: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
        let enabled = Arc::new(Mutex::new(true));
        let remote_write = Arc::new(AtomicBool::new(false));
        let writing = remote_write.clone();
        let write_revision = Arc::new(AtomicU64::new(0));
        let revisions = write_revision.clone();
        let refresh_local = Arc::new(AtomicU64::new(0));

        let feedback = ClipboardFeedback {
            last_content: last_content.clone(),
            last_change: last_change.clone(),
            remote_write: remote_write.clone(),
            write_revision: write_revision.clone(),
            refresh_local: refresh_local.clone(),
        };
        let enabled_clone = enabled.clone();
        let event_tx_clone = event_tx.clone();

        let reader = Arc::new(Mutex::new(ClipboardReader::default()));
        // Spawn monitoring task
        let task = tokio::spawn(async move {
            let mut check_interval = interval(Duration::from_millis(500));
            check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                check_interval.tick().await;

                // Check if enabled
                let is_enabled = {
                    let enabled = enabled_clone.lock().unwrap();
                    *enabled
                };

                if !is_enabled || writing.load(Ordering::SeqCst) {
                    continue;
                }

                // Read clipboard in blocking task
                let feedback = feedback.clone();
                let event_tx_clone2 = event_tx_clone.clone();
                let read_revision = revisions.load(Ordering::SeqCst);
                let reader = reader.clone();
                let enabled = enabled_clone.clone();

                let _ = spawn_blocking(move || {
                    let mut reader = reader.lock().unwrap();
                    let refresh = feedback.last_content.lock().unwrap().is_none()
                        || (read_revision != 0
                            && feedback.refresh_local.load(Ordering::SeqCst) == read_revision);
                    reader.begin_sample(read_revision, refresh);
                    let Some(current_content) = reader.read() else {
                        return;
                    };

                    let queued = feedback.publish_sample(
                        &enabled,
                        &mut reader,
                        read_revision,
                        current_content,
                    );
                    drop(reader);
                    if let Some(queued) = queued {
                        let _ = event_tx_clone2.blocking_send(queued);
                    }
                })
                .await;
            }
        });

        Ok(Self {
            event_rx,
            _event_tx: event_tx,
            task,
            last_content,
            last_change,
            enabled,
            remote_write,
            write_revision,
            refresh_local,
        })
    }

    pub fn feedback(&self) -> ClipboardFeedback {
        ClipboardFeedback {
            last_content: self.last_content.clone(),
            last_change: self.last_change.clone(),
            remote_write: self.remote_write.clone(),
            write_revision: self.write_revision.clone(),
            refresh_local: self.refresh_local.clone(),
        }
    }

    /// Receive the next clipboard event
    pub async fn recv(&mut self) -> Option<CaptureEvent> {
        while let Some(queued) = self.event_rx.recv().await {
            if *self.enabled.lock().unwrap()
                && !self.remote_write.load(Ordering::SeqCst)
                && queued.revision == self.write_revision.load(Ordering::SeqCst)
            {
                return Some(queued.event);
            }
        }
        None
    }

    /// Enable clipboard monitoring
    pub fn enable(&self) {
        let mut enabled = self.enabled.lock().unwrap();
        if !*enabled {
            self.write_revision.fetch_add(1, Ordering::SeqCst);
            *self.last_content.lock().unwrap() = None;
            *self.last_change.lock().unwrap() = None;
            self.refresh_local.store(0, Ordering::SeqCst);
            *enabled = true;
        }
        log::info!("Clipboard monitoring enabled");
    }

    /// Disable clipboard monitoring
    pub fn disable(&self) {
        let mut enabled = self.enabled.lock().unwrap();
        if *enabled {
            *enabled = false;
            self.write_revision.fetch_add(1, Ordering::SeqCst);
        }
        log::info!("Clipboard monitoring disabled");
    }

    /// Update the last known clipboard content (called when we set the clipboard)
    /// This prevents detecting our own clipboard changes as external changes
    pub fn update_last_content(&self, content: ClipboardEvent) {
        self.write_revision.fetch_add(1, Ordering::SeqCst);
        let mut last_content = self.last_content.lock().unwrap();
        let mut last_change = self.last_change.lock().unwrap();
        *last_content = Some(content);
        *last_change = Some(Instant::now());
        self.refresh_local.store(0, Ordering::SeqCst);
    }
}

impl Drop for ClipboardMonitor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn image(width: usize, height: usize, bytes: &[u8]) -> ImageData<'_> {
        ImageData {
            width,
            height,
            bytes: std::borrow::Cow::Borrowed(bytes),
        }
    }

    #[test]
    fn unchanged_image_encodes_once_and_changed_pixels_or_dimensions_reencode() {
        let mut cache = ImageCache::default();
        let mut calls = 0;
        let pixels = [1u8; 16];
        for _ in 0..120 {
            cache.encode_with(image(2, 2, &pixels), |_, _, _| {
                calls += 1;
                Some(vec![1])
            });
        }
        assert_eq!(calls, 1);
        for data in [image(1, 4, &pixels), image(2, 2, &[2u8; 16])] {
            cache.encode_with(data, |_, _, _| {
                calls += 1;
                Some(vec![2])
            });
        }
        assert_eq!(calls, 3);
        cache.0 = None; // discarded samples / text / unsupported content invalidate it
        assert!(
            cache
                .encode_with(image(2, 2, &[2u8; 16]), |_, _, _| Some(vec![3]))
                .is_some()
        );
    }

    #[test]
    fn invalid_or_failed_encoding_is_not_cached() {
        let mut cache = ImageCache::default();
        assert!(
            cache
                .encode_with(image(2, 2, &[1]), |_, _, _| panic!("invalid buffer"))
                .is_none()
        );
        assert!(
            cache
                .encode_with(image(1, 1, &[1; 4]), |_, _, _| None)
                .is_none()
        );
        assert!(
            cache
                .encode_with(image(1, 1, &[1; 4]), |_, _, _| Some(vec![1]))
                .is_some()
        );
    }

    #[test]
    #[ignore = "manual release performance measurement"]
    fn measure_static_4k_image_processing() {
        // Synthetic screenshot-like RGBA; does not measure platform read cost.
        let (width, height) = (3840, 2160);
        let mut pixels = vec![0u8; width * height * 4];
        for (index, pixel) in pixels.as_chunks_mut::<4>().0.iter_mut().enumerate() {
            let x = index % width;
            let y = index / width;
            pixel.copy_from_slice(&[(x / 16) as u8, (y / 16) as u8, ((x + y) / 32) as u8, 255]);
        }
        let baseline = Instant::now();
        for _ in 0..3 {
            std::hint::black_box(
                encode_image_rgba(width as u32, height as u32, std::hint::black_box(&pixels))
                    .unwrap(),
            );
        }
        let baseline_ns = baseline.elapsed().as_nanos() / 3;
        let mut cache = ImageCache::default();
        cache
            .encode_with(image(width, height, &pixels), encode_image_rgba)
            .unwrap();
        let optimized = Instant::now();
        for _ in 0..120 {
            assert!(
                std::hint::black_box(&mut cache)
                    .encode_with(
                        image(width, height, std::hint::black_box(&pixels)),
                        |_, _, _| panic!("unchanged image encoded")
                    )
                    .is_none()
            );
        }
        let optimized_ns = optimized.elapsed().as_nanos() / 120;
        println!(
            "4K synthetic baseline PNG ns/poll={baseline_ns}; cached pixel comparison ns/poll={optimized_ns}; unchanged polls=120; repeat encodes=0"
        );
    }

    #[tokio::test]
    async fn dropping_monitor_stops_poll_task() {
        let monitor = ClipboardMonitor::new().unwrap();
        let task = monitor.task.abort_handle();
        drop(monitor);
        tokio::task::yield_now().await;
        assert!(task.is_finished());
    }

    fn feedback() -> ClipboardFeedback {
        ClipboardFeedback {
            last_content: Arc::new(Mutex::new(Some(ClipboardEvent::Text("local".into())))),
            last_change: Arc::new(Mutex::new(None)),
            remote_write: Arc::new(AtomicBool::new(false)),
            write_revision: Arc::new(AtomicU64::new(0)),
            refresh_local: Arc::new(AtomicU64::new(0)),
        }
    }

    fn queued(text: &str, revision: u64) -> QueuedClipboard {
        QueuedClipboard {
            event: CaptureEvent::Input(Event::Clipboard(ClipboardEvent::Text(text.into()))),
            revision,
        }
    }

    fn monitor_fixture() -> ClipboardMonitor {
        let feedback = feedback();
        let (tx, rx) = mpsc::channel(16);
        ClipboardMonitor {
            event_rx: rx,
            _event_tx: tx,
            task: tokio::spawn(std::future::pending()),
            last_content: feedback.last_content,
            last_change: feedback.last_change,
            remote_write: feedback.remote_write,
            write_revision: feedback.write_revision,
            refresh_local: feedback.refresh_local,
            enabled: Arc::new(Mutex::new(true)),
        }
    }

    #[tokio::test]
    async fn queued_local_samples_do_not_survive_remote_writes_or_cache_updates() {
        let mut monitor = monitor_fixture();
        monitor
            ._event_tx
            .send(queued("old local", 0))
            .await
            .unwrap();
        monitor
            .feedback()
            .begin_write()
            .finish(Some(ClipboardEvent::Text("remote".into())));
        let current = monitor.write_revision.load(Ordering::SeqCst);
        monitor
            ._event_tx
            .send(queued("new local", current))
            .await
            .unwrap();
        assert!(
            matches!(monitor.recv().await, Some(CaptureEvent::Input(Event::Clipboard(ClipboardEvent::Text(text)))) if text == "new local")
        );
        monitor
            ._event_tx
            .send(queued("before cache update", current))
            .await
            .unwrap();
        monitor.update_last_content(ClipboardEvent::Text("updated remote".into()));
        monitor
            ._event_tx
            .send(queued(
                "after cache update",
                monitor.write_revision.load(Ordering::SeqCst),
            ))
            .await
            .unwrap();
        assert!(
            matches!(monitor.recv().await, Some(CaptureEvent::Input(Event::Clipboard(ClipboardEvent::Text(text)))) if text == "after cache update")
        );
    }

    #[tokio::test]
    async fn disable_and_reenable_discard_queued_samples_and_allow_a_fresh_snapshot() {
        let mut monitor = monitor_fixture();
        monitor
            ._event_tx
            .send(queued("before disable", 0))
            .await
            .unwrap();
        monitor.disable();
        monitor
            ._event_tx
            .send(queued(
                "while disabled",
                monitor.write_revision.load(Ordering::SeqCst),
            ))
            .await
            .unwrap();
        monitor.enable();
        assert!(monitor.last_content.lock().unwrap().is_none());
        assert!(monitor.last_change.lock().unwrap().is_none());
        let current = monitor.write_revision.load(Ordering::SeqCst);
        monitor
            ._event_tx
            .send(queued("fresh", current))
            .await
            .unwrap();
        assert!(
            matches!(monitor.recv().await, Some(CaptureEvent::Input(Event::Clipboard(ClipboardEvent::Text(text)))) if text == "fresh")
        );
    }

    #[tokio::test]
    async fn a_blocked_producer_keeps_its_original_revision_when_queue_space_returns() {
        let mut monitor = monitor_fixture();
        for _ in 0..16 {
            monitor._event_tx.try_send(queued("old", 0)).unwrap();
        }
        let tx = monitor._event_tx.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let feedback = monitor.feedback();
        let enabled = monitor.enabled.clone();
        let blocked = spawn_blocking(move || {
            let mut reader = ClipboardReader::default();
            reader.begin_sample(0, false);
            let event = feedback
                .publish_sample(
                    &enabled,
                    &mut reader,
                    0,
                    ClipboardEvent::Text("late old".into()),
                )
                .unwrap();
            started.send(()).unwrap();
            tx.blocking_send(event).unwrap();
        });
        ready.await.unwrap();
        monitor
            .feedback()
            .begin_write()
            .finish(Some(ClipboardEvent::Text("remote".into())));
        let tx = monitor._event_tx.clone();
        let current = monitor.write_revision.load(Ordering::SeqCst);
        let event = tokio::time::timeout(Duration::from_secs(1), async {
            let (event, sent) = tokio::join!(monitor.recv(), tx.send(queued("fresh", current)));
            sent.unwrap();
            event
        })
        .await
        .unwrap();
        assert!(
            matches!(event, Some(CaptureEvent::Input(Event::Clipboard(ClipboardEvent::Text(text)))) if text == "fresh")
        );
        blocked.await.unwrap();
        // Whether the old blocked producer acquired capacity before or after
        // the fresh event, it can never be delivered as the next clipboard.
        assert!(
            tokio::time::timeout(Duration::from_millis(10), monitor.recv())
                .await
                .is_err()
        );
    }

    #[test]
    fn image_cache_survives_successful_writes_but_reencodes_after_refresh() {
        let mut reader = ClipboardReader::default();
        let pixels = [1u8; 16];
        let mut calls = 0;
        for _ in 0..120 {
            reader.begin_sample(0, false);
            reader.images.encode_with(image(2, 2, &pixels), |_, _, _| {
                calls += 1;
                Some(vec![1])
            });
        }
        assert_eq!(calls, 1);
        // Successful remote writes retain raw pixels: identical images must
        // not incur a new PNG encode just because the queue revision changed.
        reader.begin_sample(2, false);
        assert!(
            reader
                .images
                .encode_with(image(2, 2, &pixels), |_, _, _| panic!(
                    "unchanged successful write encoded"
                ))
                .is_none()
        );
        reader.begin_sample(4, true);
        assert!(
            reader
                .images
                .encode_with(image(2, 2, &pixels), |_, _, _| {
                    calls += 1;
                    Some(vec![1])
                })
                .is_some()
        );
        assert_eq!(calls, 2);
    }

    #[test]
    fn failed_write_resamples_unchanged_local_content_without_losing_refresh_to_old_sample() {
        let feedback = feedback();
        let enabled = Mutex::new(true);
        let mut reader = ClipboardReader::default();
        let old_revision = feedback.write_revision.load(Ordering::SeqCst);
        feedback.clone().begin_write().finish(None);
        let current = feedback.write_revision.load(Ordering::SeqCst);
        assert!(
            feedback
                .publish_sample(
                    &enabled,
                    &mut reader,
                    old_revision,
                    ClipboardEvent::Text("local".into())
                )
                .is_none()
        );
        assert_eq!(feedback.refresh_local.load(Ordering::SeqCst), current);
        assert!(
            feedback
                .publish_sample(
                    &enabled,
                    &mut reader,
                    current,
                    ClipboardEvent::Text("local".into())
                )
                .is_some()
        );
        assert_eq!(feedback.refresh_local.load(Ordering::SeqCst), 0);
        assert!(
            feedback
                .publish_sample(
                    &enabled,
                    &mut reader,
                    current,
                    ClipboardEvent::Text("local".into())
                )
                .is_none()
        );
    }

    #[test]
    fn successful_remote_write_commits_cache_before_resuming_monitor() {
        let feedback = feedback();
        let guard = feedback.clone().begin_write();
        assert!(feedback.remote_write.load(Ordering::SeqCst));
        let read_revision = feedback.write_revision.load(Ordering::SeqCst);
        guard.finish(Some(ClipboardEvent::Text("remote".into())));
        assert!(!feedback.remote_write.load(Ordering::SeqCst));
        assert_eq!(
            *feedback.last_content.lock().unwrap(),
            Some(ClipboardEvent::Text("remote".into()))
        );
        assert!(feedback.last_change.lock().unwrap().is_some());
        assert_eq!(feedback.refresh_local.load(Ordering::SeqCst), 0);
        assert_ne!(
            read_revision,
            feedback.write_revision.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn failed_or_dropped_write_preserves_cache_and_resumes_monitor() {
        let feedback = feedback();
        feedback.clone().begin_write().finish(None);
        assert_eq!(
            *feedback.last_content.lock().unwrap(),
            Some(ClipboardEvent::Text("local".into()))
        );
        assert!(!feedback.remote_write.load(Ordering::SeqCst));
        let refresh = feedback.refresh_local.load(Ordering::SeqCst);
        assert_ne!(refresh, 0);
        assert_eq!(refresh, feedback.write_revision.load(Ordering::SeqCst));
        drop(feedback.clone().begin_write());
        assert!(!feedback.remote_write.load(Ordering::SeqCst));
        assert!(feedback.last_change.lock().unwrap().is_none());
        assert_ne!(feedback.refresh_local.load(Ordering::SeqCst), refresh);
    }
}
