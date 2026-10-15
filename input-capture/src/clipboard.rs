use arboard::Clipboard;
use input_event::{ClipboardEvent, ClipboardFile, Event, decode_image_rgba, encode_image_rgba};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{CaptureError, CaptureEvent};

/// Clipboard monitor that watches for clipboard changes
pub struct ClipboardMonitor {
    event_rx: Receiver<CaptureEvent>,
    _event_tx: Sender<CaptureEvent>,
    recorder: ClipboardSigRecorder,
    /// signature whose last share attempt failed, with the failure
    /// time — suppresses automatic re-emits for [`FAILED_RETRY`] so an
    /// unreachable peer does not produce a send (and a failure toast)
    /// on every poll tick
    failed_sig: Arc<Mutex<Option<(ContentSig, Instant)>>>,
    enabled: Arc<Mutex<bool>>,
    /// the polling thread — kept so the service can supervise it with
    /// [`Self::is_alive`] instead of waiting on channel state alone
    task: std::thread::JoinHandle<()>,
}

/// records which clipboard signature this host placed itself, so a
/// received transfer written to the local clipboard is not read back
/// as a fresh local change and echoed to the peer. Cloneable and
/// `Send`, so the detached write task that applies a received
/// clipboard can record the signature it actually produced.
#[derive(Clone)]
pub struct ClipboardSigRecorder {
    last_sig: Arc<Mutex<Option<ContentSig>>>,
    last_change: Arc<Mutex<Option<Instant>>>,
}

impl ClipboardSigRecorder {
    /// Update the last known clipboard content (called when we set the
    /// clipboard). This prevents detecting our own clipboard changes
    /// as external changes.
    ///
    /// Must be called with the content as it actually landed — e.g. a
    /// received file renamed on disk to `name (N)` must record the
    /// renamed signature or the monitor reads it back as a change.
    pub fn update(&self, content: &ClipboardEvent) {
        let mut last_sig = self.last_sig.lock().unwrap();
        let mut last_change = self.last_change.lock().unwrap();
        *last_sig = Some(sig_of(content));
        *last_change = Some(Instant::now());
    }
    /// Inspect and publish one observation. Closures keep native clipboard access
    /// on its owning thread and let tests exercise backpressure without OS writes.
    fn capture_change(
        &self,
        sig: ContentSig,
        read_content: impl FnOnce(&ContentSig) -> Option<ClipboardEvent>,
        send_event: impl FnOnce(CaptureEvent),
    ) {
        // Check if content changed
        let mut last_sig = self.last_sig.lock().unwrap();
        let mut last_change = self.last_change.lock().unwrap();

        let content_changed = match last_sig.as_ref() {
            None => true,
            Some(last) => last != &sig,
        };

        if content_changed {
            // Debounce: ignore changes within 200ms of last change
            // This prevents infinite loops when both sides update clipboard
            let should_emit = match *last_change {
                None => true,
                Some(instant) => instant.elapsed() > Duration::from_millis(200),
            };

            if should_emit {
                let Some(current_content) = read_content(&sig) else {
                    return;
                };
                log::info!(
                    "Clipboard changed: {} ({} bytes)",
                    current_content,
                    current_content.content_len()
                );
                *last_sig = Some(sig);
                *last_change = Some(Instant::now());

                // The consumer may update this state before it can drain the
                // queue. Never hold either guard across a blocking send.
                drop(last_change);
                drop(last_sig);
                send_event(CaptureEvent::Input(Event::Clipboard(current_content)));
            } else {
                log::trace!("Clipboard changed but debounced (too recent)");
            }
        }
    }
}

/// minimum gap between automatic re-emits of a signature whose share
/// attempt failed — bounds resend churn while still letting "copy the
/// same content again" work as a retry
const FAILED_RETRY: Duration = Duration::from_secs(5);

/// identity of the current clipboard payload. Files compare by the
/// set of file names only — contents are read just once per change,
/// and a file list this host put on the clipboard after a received
/// transfer (same names, different directory) still matches. Images
/// compare by a hash of the raw pixels — PNG-encoding a screenshot
/// every poll tick just for change detection costs real CPU, and the
/// PNG is only needed once an actual change is detected.
#[derive(Debug, PartialEq)]
enum ContentSig {
    Text(String),
    /// hash of width, height and the raw RGBA bytes
    Image(u64),
    Files(Vec<String>),
}

fn image_sig(width: usize, height: usize, rgba: &[u8]) -> ContentSig {
    use std::hash::Hasher;
    // Compare identical pixels consistently across polls and received transfers.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write_usize(width);
    hasher.write_usize(height);
    hasher.write(rgba);
    ContentSig::Image(hasher.finish())
}

fn file_name(path: &std::path::Path) -> Option<String> {
    path.file_name()?.to_str().map(|s| s.to_string())
}

/// read the clipboard's signature: file list first (a copied file may
/// also offer its path as text), then images, then text. An image wins
/// over text because applications that copy an image commonly also offer
/// alt text or a URL for it — the image is the more specific payload.
/// Returns `None` for empty or unsupported content.
fn read_clipboard_sig(clipboard: &mut Clipboard) -> Option<ContentSig> {
    match clipboard.get().file_list() {
        Ok(paths) if !paths.is_empty() => {
            return Some(ContentSig::Files(
                paths.iter().filter_map(|p| file_name(p)).collect(),
            ));
        }
        _ => {}
    }
    match clipboard.get_image() {
        Ok(image) => {
            log::trace!(
                "Clipboard image read: {}x{}, {} bytes RGBA",
                image.width,
                image.height,
                image.bytes.len()
            );
            return Some(image_sig(image.width, image.height, &image.bytes));
        }
        Err(e) => log::trace!("No clipboard image: {}", e),
    }
    match clipboard.get_text() {
        Ok(text) => {
            log::trace!("Clipboard text read: {} bytes", text.len());
            return Some(ContentSig::Text(text));
        }
        Err(e) => log::trace!("No clipboard text: {}", e),
    }
    None
}

/// load the payload a signature refers to; for files this is when the
/// file contents are actually read (regular files only)
fn event_from_sig(clipboard: &mut Clipboard, sig: &ContentSig) -> Option<ClipboardEvent> {
    match sig {
        ContentSig::Text(t) => Some(ClipboardEvent::Text(t.clone())),
        ContentSig::Image(_) => {
            // the sig only stored the pixel hash — re-read the image
            // and PNG-encode it now that a real change was detected
            match clipboard.get_image() {
                Ok(image) => {
                    encode_image_rgba(image.width as u32, image.height as u32, &image.bytes)
                        .map(ClipboardEvent::Image)
                }
                Err(e) => {
                    log::warn!("clipboard image vanished between sig and read: {e}");
                    None
                }
            }
        }
        ContentSig::Files(_) => match clipboard.get().file_list() {
            Ok(paths) => return read_clipboard_files(paths),
            Err(error) => {
                log::warn!("cannot read clipboard file list: {error}");
                None
            }
        },
    }
}

/// Capture metadata only. File bytes are read after switching to the peer.
fn read_clipboard_files(paths: Vec<std::path::PathBuf>) -> Option<ClipboardEvent> {
    let mut files = Vec::new();
    for path in paths {
        let Some(name) = file_name(&path) else {
            continue;
        };
        match path.metadata() {
            Ok(metadata) if metadata.is_file() => files.push(ClipboardFile {
                name,
                path,
                size: metadata.len(),
            }),
            Ok(_) => log::info!("skipping non-file clipboard entry"),
            Err(error) => log::warn!("cannot inspect clipboard file: {error}"),
        }
    }
    (!files.is_empty()).then_some(ClipboardEvent::Files(files))
}

fn sig_of(event: &ClipboardEvent) -> ContentSig {
    match event {
        ClipboardEvent::Text(t) => ContentSig::Text(t.clone()),
        // the wire payload is PNG but the monitor compares raw pixels,
        // so decode back — same pixels produce the same hash and the
        // applied transfer is not read back as a local change
        ClipboardEvent::Image(png) => match decode_image_rgba(png) {
            Some((w, h, rgba)) => image_sig(w as usize, h as usize, &rgba),
            None => ContentSig::Image(0),
        },
        ClipboardEvent::Files(files) => {
            ContentSig::Files(files.iter().map(|f| f.name.clone()).collect())
        }
    }
}

impl ClipboardMonitor {
    pub fn new() -> Result<Self, CaptureError> {
        let (event_tx, event_rx) = mpsc::channel(16);
        let recorder = ClipboardSigRecorder {
            last_sig: Arc::new(Mutex::new(None)),
            last_change: Arc::new(Mutex::new(None)),
        };
        let failed_sig: Arc<Mutex<Option<(ContentSig, Instant)>>> = Arc::new(Mutex::new(None));
        let enabled = Arc::new(Mutex::new(true));

        let recorder_clone = recorder.clone();
        let failed_sig_clone = failed_sig.clone();
        let enabled_clone = enabled.clone();
        let event_tx_clone = event_tx.clone();

        // dedicated worker thread owning ONE persistent clipboard
        // handle — arboard's Linux backends hold an X connection and a
        // listener thread per instance, so creating a fresh Clipboard
        // per poll tick is real churn (it is also !Send, which is why
        // the polling used to bounce through spawn_blocking)
        let task = std::thread::spawn(move || {
            let mut clipboard: Option<Clipboard> = None;
            loop {
                std::thread::sleep(Duration::from_millis(500));

                if !*enabled_clone.lock().unwrap() {
                    continue;
                }

                if clipboard.is_none() {
                    match Clipboard::new() {
                        Ok(c) => clipboard = Some(c),
                        Err(e) => {
                            log::debug!("Failed to create clipboard: {}", e);
                            continue;
                        }
                    }
                }
                let cb = clipboard.as_mut().unwrap();

                #[cfg(windows)]
                let clipboard_access = input_event::clipboard_access();

                // Get current clipboard signature (files compare by
                // name — contents are read only on real changes)
                let Some(sig) = read_clipboard_sig(cb) else {
                    // Clipboard might be empty or contain non-shareable data
                    continue;
                };

                // a signature whose share just failed may only
                // re-emit after FAILED_RETRY — an unreachable peer
                // would otherwise retry (and error) every tick
                let retry_suppressed = failed_sig_clone
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|(fs, at)| *fs == sig && at.elapsed() < FAILED_RETRY);
                if retry_suppressed {
                    continue;
                }

                let mut channel_closed = false;
                recorder_clone.capture_change(
                    sig,
                    |sig| event_from_sig(cb, sig),
                    |event| {
                        #[cfg(windows)]
                        drop(clipboard_access);
                        if event_tx_clone.blocking_send(event).is_err() {
                            log::error!("clipboard event receiver closed, stopping monitor");
                            channel_closed = true;
                        }
                    },
                );
                if channel_closed {
                    break;
                }
            }
        });

        Ok(Self {
            event_rx,
            _event_tx: event_tx,
            recorder,
            failed_sig,
            enabled,
            task,
        })
    }

    /// whether the polling task is still running — polled by the
    /// service liveness tick; `recv()` returning `None` already
    /// detects death, this exists so supervision does not depend on
    /// which channel arm happens to fire first
    pub fn is_alive(&self) -> bool {
        !self.task.is_finished()
    }

    /// handle recording signatures of clipboard content this host
    /// applied itself — pass to the detached task that writes a
    /// received clipboard so the write is not read back as a change
    pub fn recorder(&self) -> ClipboardSigRecorder {
        self.recorder.clone()
    }

    /// Receive the next clipboard event
    pub async fn recv(&mut self) -> Option<CaptureEvent> {
        self.event_rx.recv().await
    }

    /// Enable clipboard monitoring
    pub fn enable(&self) {
        let mut enabled = self.enabled.lock().unwrap();
        *enabled = true;
        log::info!("Clipboard monitoring enabled");
    }

    /// Disable clipboard monitoring
    pub fn disable(&self) {
        let mut enabled = self.enabled.lock().unwrap();
        *enabled = false;
        log::info!("Clipboard monitoring disabled");
    }

    /// record that sharing the current signature failed: forget it so
    /// a re-copy of the same content can retry, but suppress automatic
    /// re-emits for [`FAILED_RETRY`] — without that an unreachable peer
    /// produces a send attempt and a failure toast on every poll tick.
    pub fn mark_share_failed(&self) {
        let sig = self.recorder.last_sig.lock().unwrap().take();
        *self.failed_sig.lock().unwrap() = sig.map(|s| (s, Instant::now()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_capture_keeps_large_files_as_paths() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.zip");
        let size = u32::MAX as u64 + 123;
        std::fs::File::create(&path).unwrap().set_len(size).unwrap();
        let Some(ClipboardEvent::Files(files)) = read_clipboard_files(vec![path.clone()]) else {
            panic!("expected files")
        };
        assert_eq!(
            files,
            vec![ClipboardFile {
                name: "large.zip".into(),
                path,
                size
            }]
        );
    }

    #[test]
    fn regression_full_queue_does_not_hold_clipboard_state() {
        let recorder = ClipboardSigRecorder {
            last_sig: Arc::new(Mutex::new(None)),
            last_change: Arc::new(Mutex::new(None)),
        };
        // A full queue reproduces the consumer falling behind. The test drains
        // it before asserting, so even the broken version cannot hang the suite.
        let (tx, mut rx) = mpsc::channel(1);
        tx.blocking_send(CaptureEvent::Input(Event::Clipboard(ClipboardEvent::Text(
            "queued".into(),
        ))))
        .unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let worker_recorder = recorder.clone();
        let worker = std::thread::spawn(move || {
            worker_recorder.capture_change(
                ContentSig::Text("next".into()),
                |_| Some(ClipboardEvent::Text("next".into())),
                |event| {
                    entered_tx.send(()).unwrap();
                    tx.blocking_send(event).unwrap();
                },
            );
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let signature_available = recorder.last_sig.try_lock().is_ok();
        let timestamp_available = recorder.last_change.try_lock().is_ok();
        rx.blocking_recv().unwrap();
        worker.join().unwrap();
        rx.blocking_recv().unwrap();
        assert!(
            signature_available && timestamp_available,
            "a full clipboard queue holds the state needed by remote clipboard updates"
        );
    }

    #[test]
    fn unchanged_image_has_stable_signature() {
        let pixels = [20, 40, 60, 255, 80, 100, 120, 255];
        let expected = image_sig(2, 1, &pixels);
        for _ in 0..32 {
            assert_eq!(image_sig(2, 1, &pixels), expected);
        }
        assert_ne!(image_sig(1, 2, &pixels), expected);
        let mut changed = pixels;
        changed[0] += 1;
        assert_ne!(image_sig(2, 1, &changed), expected);
    }

    #[test]
    fn received_image_matches_monitor_signature() {
        let pixels = [20, 40, 60, 255, 80, 100, 120, 255];
        let png = encode_image_rgba(2, 1, &pixels).expect("valid test image");
        let event = ClipboardEvent::Image(png);
        assert_eq!(sig_of(&event), image_sig(2, 1, &pixels));
        let recorder = ClipboardSigRecorder {
            last_sig: Arc::new(Mutex::new(None)),
            last_change: Arc::new(Mutex::new(None)),
        };
        recorder.update(&event);
        assert_eq!(
            *recorder.last_sig.lock().unwrap(),
            Some(image_sig(2, 1, &pixels))
        );
    }

    /// Regression: a failed share used to clear the signature every
    /// time, so the very next poll re-emitted the same content —
    /// an unreachable peer produced a send + an error toast per 500ms
    /// tick forever. mark_share_failed must both forget the signature
    /// (retry on re-copy works) and record it for suppression.
    #[tokio::test(flavor = "current_thread")]
    async fn mark_share_failed_records_signature_for_suppression() {
        let monitor = ClipboardMonitor {
            event_rx: mpsc::channel(1).1,
            _event_tx: mpsc::channel(1).0,
            recorder: ClipboardSigRecorder {
                last_sig: Arc::new(Mutex::new(Some(ContentSig::Text("x".into())))),
                last_change: Arc::new(Mutex::new(None)),
            },
            failed_sig: Arc::new(Mutex::new(None)),
            enabled: Arc::new(Mutex::new(true)),
            task: std::thread::spawn(|| {}),
        };
        monitor.mark_share_failed();
        assert!(monitor.recorder.last_sig.lock().unwrap().is_none());
        let failed = monitor.failed_sig.lock().unwrap();
        assert!(
            matches!(failed.as_ref(), Some((ContentSig::Text(t), _)) if t == "x"),
            "failed signature must be recorded for retry suppression"
        );
        // a second failure refreshes the suppression window
        drop(failed);
        *monitor.recorder.last_sig.lock().unwrap() = Some(ContentSig::Text("y".into()));
        monitor.mark_share_failed();
        let failed = monitor.failed_sig.lock().unwrap();
        assert!(matches!(failed.as_ref(), Some((ContentSig::Text(t), _)) if t == "y"));
    }
}
