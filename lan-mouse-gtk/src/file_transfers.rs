//! System notifications show transfer cards; the main window never renders history.
use crate::i18n::{gettext, tr};
use lan_mouse_ipc::{FilePhase, FileTransfer};
use std::collections::HashMap;

#[derive(Default)]
pub struct FileTransfers {
    records: HashMap<String, FileTransfer>,
}
impl FileTransfers {
    pub fn reset(&mut self, records: Vec<FileTransfer>) {
        self.records = records
            .into_iter()
            .filter(|record| !finished(record.phase))
            .map(|record| (record.id.clone(), record))
            .collect();
        self.refresh_status();
    }
    pub fn update(&mut self, record: FileTransfer) {
        if finished(record.phase) {
            self.records.remove(&record.id);
        } else {
            self.records.insert(record.id.clone(), record);
        }
        self.refresh_status();
    }
    fn refresh_status(&self) {
        let active = self.records.values().find(|record| {
            matches!(
                record.phase,
                FilePhase::Connecting | FilePhase::Transferring | FilePhase::Verifying
            )
        });
        let queued = self
            .records
            .values()
            .filter(|record| matches!(record.phase, FilePhase::Waiting | FilePhase::Queued))
            .count();
        let tip = match active {
            Some(record) => format!("Lan Mouse — {}", status(record)),
            None if queued > 0 => format!(
                "Lan Mouse — {}",
                tr(
                    "File transfers · waiting {count}",
                    &[("count", &queued.to_string())]
                )
            ),
            None => "Lan Mouse".to_string(),
        };
        #[cfg(windows)]
        crate::windows_status_item::set_status(&tip);
        #[cfg(target_os = "linux")]
        crate::linux_status_item::set_status(&tip);
    }
}
fn finished(phase: FilePhase) -> bool {
    matches!(
        phase,
        FilePhase::Complete | FilePhase::Failed | FilePhase::Cancelled
    )
}
fn bytes(value: u64) -> String {
    if value >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", value as f64 / (1024.0 * 1024.0 * 1024.0))
    } else {
        format!("{:.1} MiB", value as f64 / (1024.0 * 1024.0))
    }
}
fn status(record: &FileTransfer) -> String {
    match record.phase {
        FilePhase::Waiting => gettext("Waiting for device switch"),
        FilePhase::Queued => gettext("Queued · current transfer continues"),
        FilePhase::Connecting => gettext("Connecting · checking existing files"),
        FilePhase::Transferring => format!(
            "{} · {} / {} · {:.0}%",
            gettext(if record.incoming {
                "Receiving"
            } else {
                "Sending"
            }),
            bytes(record.done),
            bytes(record.total),
            if record.total == 0 {
                0.0
            } else {
                record.done as f64 / record.total as f64 * 100.0
            }
        ),
        FilePhase::Verifying => gettext("Verifying files and preparing clipboard"),
        FilePhase::Complete => gettext(match (record.reused, record.advertised) {
            (true, true) => "Existing files reused · ready to paste",
            (false, true) => "Complete · ready to paste",
            (_, false) => "Files saved · newer clipboard preserved",
        }),
        FilePhase::Failed => format!(
            "{}: {}",
            gettext("Transfer failed"),
            record.error.as_deref().unwrap_or_default()
        ),
        FilePhase::Cancelled => gettext("Cancelled"),
    }
}
