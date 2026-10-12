use arboard::{Clipboard, ImageData};
use input_event::{ClipboardEvent, ClipboardFile, decode_image_rgba};
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

/// Clipboard emulation that sets clipboard content
#[derive(Clone)]
pub struct ClipboardEmulation {
    // Use Arc<Mutex<>> to share clipboard across threads
    clipboard: Arc<Mutex<Option<Clipboard>>>,
    /// user-configured directory for received files (`None` = system
    /// downloads directory)
    download_dir: Arc<Mutex<Option<PathBuf>>>,
}

/// strip any directory components / weirdness from a wire-supplied
/// file name so it can only ever land inside the downloads dir
fn safe_file_name(name: &str) -> String {
    let base = name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(name)
        .trim_matches('.');
    if base.is_empty() {
        "lan-mouse-file".to_string()
    } else {
        base.to_string()
    }
}

/// `dir/name`, falling back to `dir/name (N).ext` on conflicts
fn unique_download_path(dir: &Path, name: &str) -> PathBuf {
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
                let dir_override = self.download_dir.lock().unwrap().clone();
                spawn_blocking(move || {
                    let dir = match dir_override {
                        Some(dir) => {
                            // the configured directory may not exist yet
                            std::fs::create_dir_all(&dir).map_err(|e| {
                                ClipboardError::Set(format!("cannot create {}: {e}", dir.display()))
                            })?;
                            dir
                        }
                        None => download_dir()
                            .ok_or_else(|| ClipboardError::Set("no downloads directory".into()))?,
                    };
                    let mut written = Vec::with_capacity(files.len());
                    for ClipboardFile { name, data } in &files {
                        let path = unique_download_path(&dir, &safe_file_name(name));
                        std::fs::write(&path, data).map_err(|e| {
                            ClipboardError::Set(format!("cannot write {}: {e}", path.display()))
                        })?;
                        log::info!("wrote clipboard file {}", path.display());
                        written.push(path);
                    }
                    if written.is_empty() {
                        return Err(ClipboardError::Set("no files to write".into()));
                    }
                    // advertise the written files on our own clipboard
                    // so they can be pasted straight into a file manager.
                    // Best-effort: on a headless host there is no clipboard,
                    // but the files landed on disk either way
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
                    log::debug!(
                        "Clipboard file list set: {} file(s) in {}",
                        written.len(),
                        dir.display()
                    );
                    // report the names actually advertised — a
                    // (N)-renamed file shows on the clipboard under
                    // its new name, and the clipboard monitor must
                    // compare against that or it echoes the event
                    // back to the peer
                    let applied = written
                        .iter()
                        .filter_map(|p| {
                            p.file_name()
                                .and_then(|n| n.to_str())
                                .map(|n| ClipboardFile {
                                    name: n.to_string(),
                                    data: Vec::new(),
                                })
                        })
                        .collect();
                    Ok(ClipboardEvent::Files(applied))
                })
                .await
                .map_err(|e| ClipboardError::Access(format!("Task join error: {}", e)))?
            }
        }
    }

    /// Get current clipboard content (for testing/verification)
    pub async fn get(&self) -> Result<String, ClipboardError> {
        let clipboard_arc = self.clipboard.clone();

        spawn_blocking(move || {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: the receiver used to record the *wire* file names
    /// as the local clipboard signature while `set` wrote conflicting
    /// files under `name (N)` — the clipboard monitor then read the
    /// renamed list back as a fresh local change and echoed the whole
    /// transfer back to the peer. `set` must report the names it
    /// actually advertised, so the monitor records the right sig.
    #[tokio::test(flavor = "current_thread")]
    async fn files_set_reports_the_names_it_wrote() {
        let dir = std::env::temp_dir().join(format!("lan-mouse-clip-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // same-name file already on disk -> the write lands renamed
        std::fs::write(dir.join("app.exe"), b"old").unwrap();

        let emu = ClipboardEmulation::new().unwrap();
        emu.set_download_dir(Some(dir.clone()));
        let applied = emu
            .set(ClipboardEvent::Files(vec![ClipboardFile {
                name: "app.exe".into(),
                data: b"new".to_vec(),
            }]))
            .await
            .unwrap();

        let ClipboardEvent::Files(files) = applied else {
            panic!("expected a Files event");
        };
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "app (1).exe");
        assert!(files[0].data.is_empty());
        assert_eq!(std::fs::read(dir.join("app (1).exe")).unwrap(), b"new");
        // untouched names pass through unchanged
        let applied = emu
            .set(ClipboardEvent::Files(vec![ClipboardFile {
                name: "other.txt".into(),
                data: b"x".to_vec(),
            }]))
            .await
            .unwrap();
        let ClipboardEvent::Files(files) = applied else {
            panic!("expected a Files event");
        };
        assert_eq!(files[0].name, "other.txt");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
