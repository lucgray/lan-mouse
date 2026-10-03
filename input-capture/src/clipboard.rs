use arboard::Clipboard;
use input_event::{ClipboardEvent, Event, encode_image_rgba};
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
    last_content: Arc<Mutex<Option<ClipboardEvent>>>,
    last_change: Arc<Mutex<Option<Instant>>>,
    enabled: Arc<Mutex<bool>>,
}

/// Read the current clipboard content: text first, then images.
/// Returns `None` for empty or unsupported content.
fn read_clipboard_content(clipboard: &mut Clipboard) -> Option<ClipboardEvent> {
    match clipboard.get_text() {
        Ok(text) => {
            log::trace!("Clipboard text read: {} bytes", text.len());
            return Some(ClipboardEvent::Text(text));
        }
        Err(e) => log::trace!("No clipboard text: {}", e),
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
                    Some(ClipboardEvent::Image(png))
                }
                None => {
                    log::warn!("Failed to PNG-encode clipboard image");
                    None
                }
            }
        }
        Err(e) => {
            log::trace!("No clipboard image: {}", e);
            None
        }
    }
}

impl ClipboardMonitor {
    pub fn new() -> Result<Self, CaptureError> {
        let (event_tx, event_rx) = mpsc::channel(16);
        let last_content: Arc<Mutex<Option<ClipboardEvent>>> = Arc::new(Mutex::new(None));
        let last_change: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
        let enabled = Arc::new(Mutex::new(true));

        let last_content_clone = last_content.clone();
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
                let last_content_clone2 = last_content_clone.clone();
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

                    // Get current clipboard content (text or image)
                    let Some(current_content) = read_clipboard_content(&mut clipboard) else {
                        // Clipboard might be empty or contain non-shareable data
                        return;
                    };

                    // Check if content changed
                    let mut last_content = last_content_clone2.lock().unwrap();
                    let mut last_change = last_change_clone2.lock().unwrap();

                    let content_changed = match last_content.as_ref() {
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

                            // Never hold the cache locks while waiting for the consumer:
                            // it may be applying remote content through update_last_content.
                            drop(last_change);
                            drop(last_content);
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
            last_content,
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
    /// This prevents detecting our own clipboard changes as external changes
    pub fn update_last_content(&self, content: ClipboardEvent) {
        let mut last_content = self.last_content.lock().unwrap();
        let mut last_change = self.last_change.lock().unwrap();
        *last_content = Some(content);
        *last_change = Some(Instant::now());
    }
}
