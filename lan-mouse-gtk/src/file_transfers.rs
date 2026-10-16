use crate::{
    i18n::{gettext, tr},
    window::Window,
};
use adw::prelude::*;
use lan_mouse_ipc::{FilePhase, FileTransfer, FrontendRequest};
use std::collections::HashMap;

struct Row {
    container: gtk::Box,
    title: gtk::Label,
    status: gtk::Label,
    progress: gtk::ProgressBar,
    cancel: gtk::Button,
    record: FileTransfer,
}
pub struct FileTransfers {
    pub widget: gtk::Box,
    list: gtk::Box,
    summary: gtk::Label,
    rows: HashMap<String, Row>,
}
impl FileTransfers {
    pub fn new(window: &Window) -> Self {
        let widget = gtk::Box::new(gtk::Orientation::Vertical, 6);
        widget.set_margin_start(18);
        widget.set_margin_end(18);
        widget.set_margin_top(6);
        widget.set_margin_bottom(6);
        let heading = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        let summary = gtk::Label::new(None);
        summary.set_xalign(0.0);
        summary.set_hexpand(true);
        let clear = gtk::Button::with_label(&gettext("Clear finished"));
        let weak = window.downgrade();
        clear.connect_clicked(move |_| {
            if let Some(window) = weak.upgrade() {
                window.request(FrontendRequest::ClearFileTransfers);
            }
        });
        heading.append(&summary);
        heading.append(&clear);
        widget.append(&heading);
        let list = gtk::Box::new(gtk::Orientation::Vertical, 8);
        let scroll = gtk::ScrolledWindow::builder()
            .max_content_height(230)
            .propagate_natural_height(true)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .child(&list)
            .build();
        widget.append(&scroll);
        widget.set_visible(false);
        Self {
            widget,
            list,
            summary,
            rows: HashMap::new(),
        }
    }
    pub fn reset(&mut self, records: Vec<FileTransfer>, window: &Window) {
        while let Some(child) = self.list.first_child() {
            self.list.remove(&child);
        }
        self.rows.clear();
        for record in records.into_iter().rev() {
            self.update(record, window);
        }
        self.refresh_summary();
    }
    pub fn update(&mut self, record: FileTransfer, window: &Window) {
        if !self.rows.contains_key(&record.id) {
            let container = gtk::Box::new(gtk::Orientation::Vertical, 3);
            let line = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            let title = gtk::Label::new(None);
            title.set_xalign(0.0);
            title.set_hexpand(true);
            title.set_ellipsize(gtk::pango::EllipsizeMode::Middle);
            let cancel = gtk::Button::with_label(&gettext("Cancel"));
            let weak = window.downgrade();
            let id = record.id.clone();
            cancel.connect_clicked(move |_| {
                if let Some(window) = weak.upgrade() {
                    window.request(FrontendRequest::CancelFileTransfer(id.clone()));
                }
            });
            line.append(&title);
            line.append(&cancel);
            container.append(&line);
            let status = gtk::Label::new(None);
            status.set_xalign(0.0);
            status.set_wrap(true);
            container.append(&status);
            let progress = gtk::ProgressBar::new();
            container.append(&progress);
            self.list.prepend(&container);
            self.rows.insert(
                record.id.clone(),
                Row {
                    container,
                    title,
                    status,
                    progress,
                    cancel,
                    record: record.clone(),
                },
            );
        }
        let row = self.rows.get_mut(&record.id).unwrap();
        if row.record.order != record.order {
            self.list.remove(&row.container);
            self.list.prepend(&row.container);
        }
        let name = if record.names.is_empty() {
            tr("{count} files", &[("count", &record.count.to_string())])
        } else {
            record.names.join(", ")
        };
        row.title.set_label(&name);
        row.title.set_tooltip_text(Some(&name));
        let status = status(&record);
        row.status.set_label(&status);
        row.progress.set_fraction(if record.total == 0 {
            0.0
        } else {
            record.done as f64 / record.total as f64
        });
        row.progress.set_visible(matches!(
            record.phase,
            FilePhase::Connecting | FilePhase::Transferring | FilePhase::Verifying
        ));
        row.cancel.set_visible(matches!(
            record.phase,
            FilePhase::Waiting
                | FilePhase::Queued
                | FilePhase::Connecting
                | FilePhase::Transferring
                | FilePhase::Verifying
        ));
        row.record = record;
        self.refresh_summary();
    }
    fn refresh_summary(&self) {
        let queued = self
            .rows
            .values()
            .filter(|r| matches!(r.record.phase, FilePhase::Waiting | FilePhase::Queued))
            .count();
        let active = self.rows.values().find(|r| {
            matches!(
                r.record.phase,
                FilePhase::Connecting | FilePhase::Transferring | FilePhase::Verifying
            )
        });
        let summary = tr(
            "File transfers · waiting {count}",
            &[("count", &queued.to_string())],
        );
        self.summary.set_label(&summary);
        self.widget.set_visible(!self.rows.is_empty());
        let tip = active
            .map(|r| format!("Lan Mouse — {}", status(&r.record)))
            .unwrap_or_else(|| format!("Lan Mouse — {summary}"));
        #[cfg(windows)]
        crate::windows_status_item::set_status(&tip);
        #[cfg(target_os = "linux")]
        crate::linux_status_item::set_status(&tip);
    }
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
