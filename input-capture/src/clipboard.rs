use arboard::Clipboard;
use input_event::{ClipboardEvent, ClipboardFile, Event, encode_image_rgba};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::spawn_blocking;
use tokio::time::interval;

use crate::{CaptureError, CaptureEvent};

/// Clipboard monitor that watches for clipboard changes
pub struct ClipboardMonitor {
    event_rx: Receiver<CaptureEvent>,
    _event_tx: Sender<CaptureEvent>,
    last_sig: Arc<Mutex<Option<ContentSig>>>,
    last_change: Arc<Mutex<Option<Instant>>>,
    enabled: Arc<Mutex<bool>>,
}

/// identity of the current clipboard payload. Files compare by the
/// set of file names only — contents are read just once per change,
/// and a file list this host put on the clipboard after a received
/// transfer (same names, different directory) still matches.
#[derive(Debug, PartialEq)]
enum ContentSig {
    Text(String),
    Image(Vec<u8>),
    Files(Vec<String>),
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
            let width = image.width as u32;
            let height = image.height as u32;
            match encode_image_rgba(width, height, &image.bytes) {
                Some(png) => {
                    log::trace!(
                        "Clipboard image read: {}x{}, {} bytes PNG",
                        width,
                        height,
                        png.len()
                    );
                    return Some(ContentSig::Image(png));
                }
                None => log::warn!("Failed to PNG-encode clipboard image"),
            }
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
        ContentSig::Image(png) => Some(ClipboardEvent::Image(png.clone())),
        ContentSig::Files(names) => {
            let paths = clipboard.get().file_list().ok()?;
            let mut files = Vec::with_capacity(names.len());
            for path in paths {
                let Some(name) = file_name(&path) else {
                    continue;
                };
                if !path.is_file() {
                    log::info!("skipping non-file clipboard entry {}", path.display());
                    continue;
                }
                match std::fs::read(&path) {
                    Ok(data) => files.push(ClipboardFile { name, data }),
                    Err(e) => log::warn!("cannot read clipboard file {}: {e}", path.display()),
                }
            }
            if files.is_empty() {
                None
            } else {
                Some(ClipboardEvent::Files(files))
            }
        }
    }
}

fn sig_of(event: &ClipboardEvent) -> ContentSig {
    match event {
        ClipboardEvent::Text(t) => ContentSig::Text(t.clone()),
        ClipboardEvent::Image(png) => ContentSig::Image(png.clone()),
        ClipboardEvent::Files(files) => {
            ContentSig::Files(files.iter().map(|f| f.name.clone()).collect())
        }
    }
}

impl ClipboardMonitor {
    pub fn new() -> Result<Self, CaptureError> {
        let (event_tx, event_rx) = mpsc::channel(16);
        let last_sig: Arc<Mutex<Option<ContentSig>>> = Arc::new(Mutex::new(None));
        let last_change: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
        let enabled = Arc::new(Mutex::new(true));

        let last_sig_clone = last_sig.clone();
        let last_change_clone = last_change.clone();
        let enabled_clone = enabled.clone();
        let event_tx_clone = event_tx.clone();

        // Spawn monitoring task
        tokio::spawn(async move {
            let mut check_interval = interval(Duration::from_millis(500));

            loop {
                check_interval.tick().await;

                // Check if enabled
                let is_enabled = {
                    let enabled = enabled_clone.lock().unwrap();
                    *enabled
                };

                if !is_enabled {
                    continue;
                }

                // Read clipboard in blocking task
                let last_sig_clone2 = last_sig_clone.clone();
                let last_change_clone2 = last_change_clone.clone();
                let event_tx_clone2 = event_tx_clone.clone();

                let _ = spawn_blocking(move || {
                    // Create clipboard instance
                    let mut clipboard = match Clipboard::new() {
                        Ok(c) => c,
                        Err(e) => {
                            log::debug!("Failed to create clipboard: {}", e);
                            return;
                        }
                    };

                    // Get current clipboard signature (files compare by
                    // name — contents are read only on real changes)
                    let Some(sig) = read_clipboard_sig(&mut clipboard) else {
                        // Clipboard might be empty or contain non-shareable data
                        return;
                    };

                    // Check if content changed
                    let mut last_sig = last_sig_clone2.lock().unwrap();
                    let mut last_change = last_change_clone2.lock().unwrap();

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
                            let Some(current_content) = event_from_sig(&mut clipboard, &sig) else {
                                return;
                            };
                            log::info!(
                                "Clipboard changed: {} ({} bytes)",
                                current_content,
                                current_content.content_len()
                            );
                            *last_sig = Some(sig);
                            *last_change = Some(Instant::now());

                            // Send event
                            let event = CaptureEvent::Input(Event::Clipboard(current_content));
                            let _ = event_tx_clone2.blocking_send(event);
                        } else {
                            log::trace!("Clipboard changed but debounced (too recent)");
                        }
                    }
                })
                .await;
            }
        });

        Ok(Self {
            event_rx,
            _event_tx: event_tx,
            last_sig,
            last_change,
            enabled,
        })
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

    /// Update the last known clipboard content (called when we set the clipboard)
    /// forget the recorded signature so the next identical clipboard
    /// content is treated as a fresh change — used when a share attempt
    /// failed and the user may copy the same content again to retry.
    pub fn clear_last_sig(&self) {
        let mut last_sig = self.last_sig.lock().unwrap();
        *last_sig = None;
    }

    /// This prevents detecting our own clipboard changes as external changes
    pub fn update_last_content(&self, content: ClipboardEvent) {
        let mut last_sig = self.last_sig.lock().unwrap();
        let mut last_change = self.last_change.lock().unwrap();
        *last_sig = Some(sig_of(&content));
        *last_change = Some(Instant::now());
    }
}
