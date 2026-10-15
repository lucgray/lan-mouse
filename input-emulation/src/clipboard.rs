use arboard::{Clipboard, ImageData};
use input_event::{ClipboardEvent, decode_image_rgba};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::task::spawn_blocking;

#[derive(Debug, Error)]
pub enum ClipboardError {
    #[error("Failed to access clipboard: {0}")]
    Access(String),
    #[error("Failed to set clipboard: {0}")]
    Set(String),
}

#[cfg(windows)]
fn clipboard_signature(_clipboard: &mut Clipboard) -> Result<u64, ClipboardError> {
    Ok(unsafe { windows::Win32::System::DataExchange::GetClipboardSequenceNumber() } as u64)
}

#[cfg(not(windows))]
fn clipboard_signature(clipboard: &mut Clipboard) -> Result<u64, ClipboardError> {
    use std::hash::{Hash, Hasher};
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    if let Ok(paths) = clipboard.get().file_list() {
        1u8.hash(&mut hash);
        paths.hash(&mut hash);
    } else if let Ok(image) = clipboard.get_image() {
        2u8.hash(&mut hash);
        image.width.hash(&mut hash);
        image.height.hash(&mut hash);
        image.bytes.hash(&mut hash);
    } else {
        match clipboard.get_text() {
            Ok(text) => {
                3u8.hash(&mut hash);
                text.hash(&mut hash);
            }
            Err(arboard::Error::ContentNotAvailable) => {
                0u8.hash(&mut hash);
            }
            Err(error) => return Err(ClipboardError::Access(error.to_string())),
        }
    }
    Ok(hash.finish())
}

/// Clipboard emulation that sets clipboard content
#[derive(Clone)]
pub struct ClipboardEmulation {
    // Use Arc<Mutex<>> to share clipboard across threads
    clipboard: Arc<Mutex<Option<Clipboard>>>,
    /// user-configured directory for received files (`None` = system
    /// downloads directory)
    download_dir: Arc<Mutex<Option<PathBuf>>>,
}

/// `dir/name`, falling back to `dir/name (N).ext` on conflicts
pub fn unique_download_path(dir: &Path, name: &str) -> PathBuf {
    let candidate = dir.join(name);
    if !candidate.exists() {
        return candidate;
    }
    let (stem, ext) = match name.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() => (s.to_string(), format!(".{e}")),
        _ => (name.to_string(), String::new()),
    };
    for i in 1..1000u32 {
        let candidate = dir.join(format!("{stem} ({i}){ext}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    dir.join(format!("{stem}-{}{ext}", std::process::id()))
}

/// directory received clipboard files are written to
pub fn download_dir() -> Option<PathBuf> {
    dirs::download_dir().or_else(dirs::home_dir)
}

impl ClipboardEmulation {
    pub async fn snapshot(&self) -> Result<u64, ClipboardError> {
        let clipboard = self.clipboard.clone();
        spawn_blocking(move || {
            #[cfg(windows)]
            let _access = input_event::clipboard_access();
            let mut guard = clipboard.lock().unwrap();
            if guard.is_none() {
                *guard = Some(
                    Clipboard::new().map_err(|error| ClipboardError::Access(error.to_string()))?,
                );
            }
            clipboard_signature(guard.as_mut().unwrap())
        })
        .await
        .map_err(|error| ClipboardError::Access(error.to_string()))?
    }

    pub async fn set_files_if_unchanged(
        &self,
        event: ClipboardEvent,
        expected: u64,
    ) -> Result<bool, ClipboardError> {
        let ClipboardEvent::Files(files) = event else {
            return Err(ClipboardError::Set("expected completed files".into()));
        };
        let clipboard = self.clipboard.clone();
        spawn_blocking(move || {
            #[cfg(windows)]
            let _access = input_event::clipboard_access();
            let mut guard = clipboard.lock().unwrap();
            let clipboard = guard
                .as_mut()
                .ok_or_else(|| ClipboardError::Access("clipboard unavailable".into()))?;
            if clipboard_signature(clipboard)? != expected {
                return Ok(false);
            }
            let paths: Vec<_> = files.into_iter().map(|file| file.path).collect();
            clipboard
                .set()
                .file_list(&paths)
                .map_err(|error| ClipboardError::Set(error.to_string()))?;
            Ok(true)
        })
        .await
        .map_err(|error| ClipboardError::Access(error.to_string()))?
    }
    pub fn new() -> Result<Self, ClipboardError> {
        // Try to create initial clipboard instance
        let clipboard = match Clipboard::new() {
            Ok(c) => Some(c),
            Err(e) => {
                log::warn!("Failed to create clipboard instance: {}", e);
                None
            }
        };

        Ok(Self {
            clipboard: Arc::new(Mutex::new(clipboard)),
            download_dir: Arc::new(Mutex::new(None)),
        })
    }

    /// directory received files are written to (`None` = the system
    /// downloads directory)
    pub fn set_download_dir(&self, dir: Option<PathBuf>) {
        *self.download_dir.lock().unwrap() = dir;
    }

    /// Set clipboard content from a clipboard event.
    ///
    /// Returns the event as it actually landed on the clipboard: for
    /// `Files` the advertised file names may differ from the wire
    /// payload (conflicting names get a `(N)` suffix on disk and the
    /// clipboard advertises the written names). File entries in the
    /// returned event carry no data — the bytes live on disk.
    pub async fn set(&self, event: ClipboardEvent) -> Result<ClipboardEvent, ClipboardError> {
        match event {
            ClipboardEvent::Text(text) => {
                let clipboard_arc = self.clipboard.clone();

                spawn_blocking(move || {
                    #[cfg(windows)]
                    let _clipboard_access = input_event::clipboard_access();
                    let mut clipboard_guard = clipboard_arc.lock().unwrap();

                    // Try to get or create clipboard
                    let clipboard = match clipboard_guard.as_mut() {
                        Some(c) => c,
                        None => {
                            // Try to create a new clipboard instance
                            match Clipboard::new() {
                                Ok(c) => {
                                    *clipboard_guard = Some(c);
                                    clipboard_guard.as_mut().unwrap()
                                }
                                Err(e) => {
                                    return Err(ClipboardError::Access(format!("{}", e)));
                                }
                            }
                        }
                    };

                    // Set clipboard text
                    clipboard
                        .set_text(text.clone())
                        .map_err(|e| ClipboardError::Set(format!("{}", e)))?;

                    log::debug!("Clipboard set, length: {} bytes", text.len());
                    Ok(ClipboardEvent::Text(text))
                })
                .await
                .map_err(|e| ClipboardError::Access(format!("Task join error: {}", e)))?
            }
            ClipboardEvent::Image(png) => {
                let clipboard_arc = self.clipboard.clone();

                spawn_blocking(move || {
                    let (width, height, rgba) = decode_image_rgba(&png)
                        .ok_or_else(|| ClipboardError::Set("invalid PNG data".into()))?;

                    #[cfg(windows)]
                    let _clipboard_access = input_event::clipboard_access();

                    let mut clipboard_guard = clipboard_arc.lock().unwrap();

                    let clipboard = match clipboard_guard.as_mut() {
                        Some(c) => c,
                        None => match Clipboard::new() {
                            Ok(c) => {
                                *clipboard_guard = Some(c);
                                clipboard_guard.as_mut().unwrap()
                            }
                            Err(e) => {
                                return Err(ClipboardError::Access(format!("{}", e)));
                            }
                        },
                    };

                    clipboard
                        .set_image(ImageData {
                            width: width as usize,
                            height: height as usize,
                            bytes: std::borrow::Cow::Owned(rgba),
                        })
                        .map_err(|e| ClipboardError::Set(format!("{}", e)))?;

                    log::debug!("Clipboard image set: {}x{}", width, height);
                    Ok(ClipboardEvent::Image(png))
                })
                .await
                .map_err(|e| ClipboardError::Access(format!("Task join error: {}", e)))?
            }
            ClipboardEvent::Files(files) => {
                let clipboard_arc = self.clipboard.clone();
                spawn_blocking(move || {
                    let written: Vec<_> = files.iter().map(|file| file.path.clone()).collect();
                    if written.is_empty() {
                        return Err(ClipboardError::Set("no received files".into()));
                    }
                    // advertise the written files on our own clipboard
                    // so they can be pasted straight into a file manager.
                    // Best-effort: on a headless host there is no clipboard,
                    // but the files landed on disk either way
                    #[cfg(windows)]
                    let _clipboard_access = input_event::clipboard_access();
                    let mut clipboard_guard = clipboard_arc.lock().unwrap();
                    if let Some(clipboard) = match clipboard_guard.as_mut() {
                        Some(c) => Some(c),
                        None => match Clipboard::new() {
                            Ok(c) => Some(clipboard_guard.insert(c)),
                            Err(e) => {
                                log::warn!("no clipboard available, files not advertised: {e}");
                                None
                            }
                        },
                    } {
                        if let Err(e) = clipboard.set().file_list(&written) {
                            log::warn!("could not set clipboard file list: {e}");
                        }
                    }
                    let applied = files;
                    Ok(ClipboardEvent::Files(applied))
                })
                .await
                .map_err(|e| ClipboardError::Access(format!("Task join error: {}", e)))?
            }
        }
    }

    pub fn destination_directory(&self) -> Result<PathBuf, ClipboardError> {
        self.download_dir
            .lock()
            .unwrap()
            .clone()
            .or_else(download_dir)
            .ok_or_else(|| ClipboardError::Set("no downloads directory".into()))
    }

    /// Get current clipboard content (for testing/verification)
    pub async fn get(&self) -> Result<String, ClipboardError> {
        let clipboard_arc = self.clipboard.clone();

        spawn_blocking(move || {
            #[cfg(windows)]
            let _clipboard_access = input_event::clipboard_access();
            let mut clipboard_guard = clipboard_arc.lock().unwrap();

            let clipboard = match clipboard_guard.as_mut() {
                Some(c) => c,
                None => match Clipboard::new() {
                    Ok(c) => {
                        *clipboard_guard = Some(c);
                        clipboard_guard.as_mut().unwrap()
                    }
                    Err(e) => {
                        return Err(ClipboardError::Access(format!("{}", e)));
                    }
                },
            };

            clipboard
                .get_text()
                .map_err(|e| ClipboardError::Access(format!("{}", e)))
        })
        .await
        .map_err(|e| ClipboardError::Access(format!("Task join error: {}", e)))?
    }
}
