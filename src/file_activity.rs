//! File history and desktop notices belong to the daemon, not the GTK window.
use lan_mouse_ipc::{FilePhase, FileTransfer};
use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
};

#[derive(Default)]
struct Pending {
    records: Option<Vec<FileTransfer>>,
    stopping: bool,
}

pub struct FileActivity {
    pending: Arc<(Mutex<Pending>, Condvar)>,
    worker: Option<JoinHandle<()>>,
}

pub fn load(path: &Path) -> io::Result<Vec<FileTransfer>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut records: Vec<FileTransfer> = serde_json::from_slice(&bytes)?;
    for record in &mut records {
        if !finished(record.phase) {
            record.phase = FilePhase::Failed;
            record.error = Some("Transfer interrupted by application restart".into());
            record.advertised = false;
        }
    }
    Ok(records)
}

fn finished(phase: FilePhase) -> bool {
    matches!(
        phase,
        FilePhase::Complete | FilePhase::Failed | FilePhase::Cancelled
    )
}

fn save(path: &Path, records: &[FileTransfer]) -> io::Result<()> {
    use std::io::Write;
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("file history directory missing"))?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(&mut file, records)?;
    file.flush()?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|error| error.error)?;
    Ok(())
}

impl FileActivity {
    pub fn new(path: PathBuf, initial: Vec<FileTransfer>, chinese: bool) -> io::Result<Self> {
        let pending = Arc::new((Mutex::new(Pending::default()), Condvar::new()));
        let worker_pending = pending.clone();
        let worker = std::thread::Builder::new()
            .name("file-activity".into())
            .spawn(move || {
                let mut previous: HashMap<_, _> = initial
                    .into_iter()
                    .map(|record| (record.id.clone(), record))
                    .collect();
                let mut desktop = Desktop::default();
                loop {
                    let (lock, wake) = &*worker_pending;
                    let mut state = lock.lock().unwrap_or_else(|error| error.into_inner());
                    while state.records.is_none() && !state.stopping {
                        state = wake.wait(state).unwrap_or_else(|error| error.into_inner());
                    }
                    let Some(records) = state.records.take() else {
                        break;
                    };
                    drop(state);
                    if let Err(error) = save(&path, &records) {
                        log::error!("file history save failed: {error}");
                    }
                    for removed in previous
                        .keys()
                        .filter(|id| !records.iter().any(|record| &record.id == *id))
                    {
                        if let Err(error) = desktop.remove(removed) {
                            log::warn!("file notification removal failed: {error}");
                        }
                    }
                    for record in &records {
                        let old = previous.get(&record.id);
                        if old == Some(record) {
                            continue;
                        }
                        let popup = old.is_none_or(|old| old.phase != record.phase)
                            && matches!(
                                record.phase,
                                FilePhase::Connecting | FilePhase::Complete | FilePhase::Failed
                            );
                        if let Err(error) = desktop.show(record, popup, chinese) {
                            log::warn!("file notification failed: {error}");
                        }
                    }
                    previous = records
                        .into_iter()
                        .map(|record| (record.id.clone(), record))
                        .collect();
                }
            })?;
        Ok(Self {
            pending,
            worker: Some(worker),
        })
    }

    /// A slow notification server can retain only the newest snapshot.
    pub fn update(&self, records: Vec<FileTransfer>) {
        let (lock, wake) = &*self.pending;
        lock.lock()
            .unwrap_or_else(|error| error.into_inner())
            .records = Some(records);
        wake.notify_one();
    }
}

impl Drop for FileActivity {
    fn drop(&mut self) {
        let (lock, wake) = &*self.pending;
        lock.lock()
            .unwrap_or_else(|error| error.into_inner())
            .stopping = true;
        wake.notify_one();
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                log::error!("file activity worker panicked");
            }
        }
    }
}

fn text(record: &FileTransfer, chinese: bool) -> (String, String) {
    let state = match record.phase {
        FilePhase::Waiting => ("Waiting for device switch", "等待切换电脑"),
        FilePhase::Queued => ("Queued", "排队中"),
        FilePhase::Connecting => ("Checking existing files", "正在检查已有文件"),
        FilePhase::Transferring if record.incoming => ("Receiving", "正在接收"),
        FilePhase::Transferring => ("Sending", "正在发送"),
        FilePhase::Verifying => ("Verifying and preparing clipboard", "正在校验并准备剪贴板"),
        FilePhase::Complete if record.advertised && record.reused => (
            "Existing files reused · ready to paste",
            "已复用原文件 · 可以粘贴",
        ),
        FilePhase::Complete if record.advertised => ("Ready to paste", "可以粘贴"),
        FilePhase::Complete => (
            "Files saved · newer clipboard preserved",
            "文件已保存 · 保留较新的剪贴板",
        ),
        FilePhase::Failed => ("Transfer failed", "传输失败"),
        FilePhase::Cancelled => ("Cancelled", "已取消"),
    };
    let title = if chinese { state.1 } else { state.0 }.to_string();
    let name = if record.names.is_empty() {
        format!(
            "{} {}",
            record.count,
            if chinese { "个文件" } else { "files" }
        )
    } else {
        record.names.join(", ")
    };
    let body = if let Some(error) = &record.error {
        format!("{name}\n{error}")
    } else {
        format!(
            "{name}\n{:.1} / {:.1} MiB · {}%",
            record.done as f64 / 1048576.0,
            record.total as f64 / 1048576.0,
            percent(record)
        )
    };
    (title, body)
}
fn percent(record: &FileTransfer) -> u64 {
    if record.total == 0 {
        0
    } else {
        (u128::from(record.done) * 100 / u128::from(record.total)) as u64
    }
}

#[derive(Default)]
struct Desktop {
    #[cfg(all(unix, not(target_os = "macos")))]
    ids: HashMap<String, u32>,
    #[cfg(windows)]
    registered: bool,
}

impl Desktop {
    fn show(
        &mut self,
        record: &FileTransfer,
        popup: bool,
        chinese: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (title, body) = text(record, chinese);
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            let mut notice = notify_rust::Notification::new();
            notice
                .appname("Lan Mouse")
                .summary(&title)
                .body(&body)
                .hint(notify_rust::Hint::CustomInt(
                    "value".into(),
                    percent(record) as i32,
                ))
                .hint(notify_rust::Hint::Transient(false))
                .hint(notify_rust::Hint::SuppressSound(!popup));
            if let Some(id) = self.ids.get(&record.id) {
                notice.id(*id);
            }
            let handle = notice.show()?;
            self.ids.insert(record.id.clone(), handle.id());
        }
        #[cfg(windows)]
        {
            use windows::{
                Data::Xml::Dom::XmlDocument,
                UI::Notifications::{ToastNotification, ToastNotificationManager},
                core::HSTRING,
            };
            if !self.registered {
                register_windows()?;
                self.registered = true;
            }
            let xml = XmlDocument::new()?;
            let progress = if finished(record.phase) {
                String::new()
            } else {
                format!(
                    "<progress title=\"{}\" value=\"{}\" valueStringOverride=\"{}%\" status=\"{}\"/>",
                    escape(&title),
                    percent(record) as f64 / 100.0,
                    percent(record),
                    escape(&title)
                )
            };
            xml.LoadXml(&HSTRING::from(format!("<toast><visual><binding template=\"ToastGeneric\"><text>Lan Mouse · {}</text><text>{}</text>{progress}</binding></visual><audio silent=\"true\"/></toast>", escape(&title), escape(&body))))?;
            let toast = ToastNotification::CreateToastNotification(&xml)?;
            toast.SetTag(&HSTRING::from(tag(&record.id)))?;
            toast.SetGroup(&HSTRING::from("files"))?;
            toast.SetSuppressPopup(!popup)?;
            ToastNotificationManager::CreateToastNotifierWithId(&HSTRING::from(APP_ID))?
                .Show(&toast)?;
        }
        #[cfg(target_os = "macos")]
        {
            let _ = popup;
            mac_notification_sys::Notification::default()
                .title(&title)
                .message(&body)
                .send()?;
        }
        Ok(())
    }
    fn remove(&mut self, id: &str) -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(windows)]
        {
            use windows::{UI::Notifications::ToastNotificationManager, core::HSTRING};
            ToastNotificationManager::History()?.RemoveGroupedTagWithId(
                &HSTRING::from(tag(id)),
                &HSTRING::from("files"),
                &HSTRING::from(APP_ID),
            )?;
        }
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            self.ids.remove(id);
        }
        #[cfg(target_os = "macos")]
        {
            let _ = id;
        }
        Ok(())
    }
}

#[cfg(windows)]
const APP_ID: &str = "LanMouse.FileTransfers";
#[cfg(windows)]
fn tag(id: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(id.as_bytes())[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
#[cfg(windows)]
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
#[cfg(windows)]
fn register_windows() -> windows::core::Result<()> {
    use windows::{
        Win32::System::Registry::*,
        core::{HSTRING, PCWSTR, w},
    };
    let mut key = HKEY::default();
    // Per-user notification identity: no elevation or dependency on the GUI process.
    unsafe {
        RegCreateKeyExW(
            HKEY_CURRENT_USER,
            &HSTRING::from(format!("Software\\Classes\\AppUserModelId\\{APP_ID}")),
            None,
            PCWSTR::null(),
            REG_OPTION_NON_VOLATILE,
            KEY_SET_VALUE,
            None,
            &mut key,
            None,
        )
        .ok()?;
        let name: Vec<u8> = "Lan Mouse\0"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        let result = RegSetValueExW(key, w!("DisplayName"), None, REG_SZ, Some(&name)).ok();
        let close = RegCloseKey(key).ok();
        result?;
        close?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(phase: FilePhase) -> FileTransfer {
        FileTransfer {
            id: "test".into(),
            incoming: false,
            names: vec!["test.bin".into()],
            count: 1,
            total: 10,
            done: 4,
            phase,
            order: 1,
            reused: false,
            advertised: false,
            error: None,
        }
    }
    #[test]
    fn streaming_history_survives_restart_and_marks_unfinished() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("history.json");
        save(&path, &[record(FilePhase::Transferring)]).unwrap();
        let restored = load(&path).unwrap();
        assert_eq!(restored[0].phase, FilePhase::Failed);
        assert_eq!(restored[0].done, 4);
        save(&path, &[record(FilePhase::Complete)]).unwrap();
        assert_eq!(load(&path).unwrap()[0].phase, FilePhase::Complete);
        save(&path, &[]).unwrap();
        assert!(load(&path).unwrap().is_empty());
    }
    #[test]
    fn streaming_history_coalesces_pending_updates() {
        let activity = FileActivity {
            pending: Arc::new((Mutex::new(Pending::default()), Condvar::new())),
            worker: None,
        };
        for count in 0..100 {
            let mut value = record(FilePhase::Transferring);
            value.order = count;
            activity.update(vec![value]);
        }
        let state = activity.pending.0.lock().unwrap();
        let records = state.records.as_ref().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].order, 99);
    }
}
