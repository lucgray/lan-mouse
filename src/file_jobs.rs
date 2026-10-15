use input_event::ClipboardFile;
use lan_mouse_ipc::{ClientHandle, FilePhase, FileTransfer};
use std::collections::{HashMap, VecDeque};

pub struct SendJob {
    pub id: String,
    pub handle: ClientHandle,
    pub files: Vec<ClipboardFile>,
}
#[derive(Default)]
pub struct FileJobs {
    pub records: HashMap<String, FileTransfer>,
    jobs: HashMap<String, (ClientHandle, Vec<ClipboardFile>)>,
    queue: VecDeque<String>,
    pub active: Option<String>,
    order: u64,
}
impl FileJobs {
    pub fn restore(records: Vec<FileTransfer>) -> Self {
        Self {
            order: records.iter().map(|record| record.order).max().unwrap_or(0),
            records: records
                .into_iter()
                .map(|record| (record.id.clone(), record))
                .collect(),
            ..Default::default()
        }
    }
    pub fn id(handle: ClientHandle, files: &[ClipboardFile]) -> String {
        format!(
            "send/{handle}/{}",
            crate::file_transfer::selection_label(files)
        )
    }
    pub fn touch(&mut self, id: &str) {
        self.order += 1;
        if let Some(record) = self.records.get_mut(id) {
            record.order = self.order;
        }
    }
    pub fn stage(&mut self, handle: ClientHandle, files: Vec<ClipboardFile>) -> String {
        let id = Self::id(handle, &files);
        for (previous, (peer, _)) in &self.jobs {
            if *peer == handle && previous != &id {
                if let Some(record) = self.records.get_mut(previous) {
                    if record.phase == FilePhase::Waiting {
                        record.phase = FilePhase::Cancelled;
                    }
                }
            }
        }
        self.records
            .entry(id.clone())
            .or_insert_with(|| FileTransfer {
                id: id.clone(),
                incoming: false,
                names: files.iter().map(|f| f.name.clone()).collect(),
                count: files.len() as u32,
                total: files.iter().map(|f| f.size).sum(),
                done: 0,
                phase: FilePhase::Waiting,
                order: 0,
                reused: false,
                advertised: false,
                error: None,
            });
        if let Some(record) = self.records.get_mut(&id) {
            if matches!(
                record.phase,
                FilePhase::Complete | FilePhase::Failed | FilePhase::Cancelled
            ) {
                record.reused = record.phase == FilePhase::Complete || record.reused;
                record.phase = FilePhase::Waiting;
                record.advertised = false;
                record.error = None;
            }
        }
        self.jobs.insert(id.clone(), (handle, files));
        self.touch(&id);
        id
    }
    pub fn enqueue(&mut self, handle: ClientHandle, files: Vec<ClipboardFile>) -> String {
        let id = self.stage(handle, files);
        if self.active.as_ref() != Some(&id) && !self.queue.contains(&id) {
            let record = self.records.get_mut(&id).unwrap();
            record.phase = FilePhase::Queued;
            record.done = 0;
            record.error = None;
            self.queue.push_back(id.clone());
        }
        id
    }
    pub fn next(&mut self) -> Option<SendJob> {
        if self.active.is_some() {
            return None;
        }
        let id = self.queue.pop_front()?;
        let (handle, files) = self.jobs.get(&id)?.clone();
        self.active = Some(id.clone());
        self.records.get_mut(&id)?.phase = FilePhase::Connecting;
        Some(SendJob { id, handle, files })
    }
    pub fn progress(&mut self, id: &str, done: u64, total: u64) {
        if let Some(record) = self.records.get_mut(id) {
            if matches!(
                record.phase,
                FilePhase::Complete | FilePhase::Failed | FilePhase::Cancelled
            ) {
                return;
            }
            record.done = done;
            record.total = total;
            record.phase = if done >= total {
                FilePhase::Verifying
            } else {
                FilePhase::Transferring
            };
        }
    }
    pub fn finish(&mut self, id: &str, result: Result<(bool, bool), String>) {
        if self.active.as_deref() == Some(id) {
            self.active = None;
        }
        if let Some(record) = self.records.get_mut(id) {
            match result {
                Ok((reused, advertised)) => {
                    record.phase = FilePhase::Complete;
                    record.done = record.total;
                    record.reused = reused;
                    record.advertised = advertised;
                    record.error = None;
                }
                Err(error) => {
                    record.phase = FilePhase::Failed;
                    record.error = Some(error);
                }
            }
        }
    }
    pub fn cancel(&mut self, id: &str) -> bool {
        if self.records.get(id).is_none_or(|r| {
            matches!(
                r.phase,
                FilePhase::Complete | FilePhase::Failed | FilePhase::Cancelled
            )
        }) {
            return false;
        }
        self.queue.retain(|queued| queued != id);
        let active = self.active.as_deref() == Some(id);
        if active {
            self.active = None;
        }
        if let Some(record) = self.records.get_mut(id) {
            record.phase = FilePhase::Cancelled;
        }
        active
    }
    pub fn incoming(&mut self, id: String, count: u32, total: u64) {
        self.records.insert(
            id.clone(),
            FileTransfer {
                id: id.clone(),
                incoming: true,
                names: Vec::new(),
                count,
                total,
                done: 0,
                phase: FilePhase::Connecting,
                order: 0,
                reused: false,
                advertised: false,
                error: None,
            },
        );
        self.touch(&id);
    }
    pub fn snapshot(&self) -> Vec<FileTransfer> {
        let mut records: Vec<_> = self.records.values().cloned().collect();
        records.sort_by_key(|r| std::cmp::Reverse(r.order));
        records
    }
    pub fn clear_finished(&mut self) {
        self.records.retain(|_, r| {
            !matches!(
                r.phase,
                FilePhase::Complete | FilePhase::Failed | FilePhase::Cancelled
            )
        });
        self.jobs.retain(|id, _| self.records.contains_key(id));
    }

    pub fn clear_waiting(&mut self) -> bool {
        let mut changed = false;
        for record in self.records.values_mut() {
            if record.phase == FilePhase::Waiting {
                record.phase = FilePhase::Cancelled;
                changed = true;
            }
        }
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn files(name: &str) -> Vec<ClipboardFile> {
        vec![ClipboardFile {
            name: name.into(),
            path: name.into(),
            size: 7,
            modified_ns: 1,
        }]
    }
    #[test]
    fn streaming_duplicate_does_not_restart_active_or_grow_queue() {
        let mut jobs = FileJobs::default();
        let id = jobs.enqueue(0, files("a"));
        let first = jobs.next().unwrap();
        jobs.progress(&id, 3, 7);
        for _ in 0..100 {
            assert_eq!(jobs.enqueue(0, files("a")), id);
        }
        assert!(jobs.next().is_none());
        assert_eq!(jobs.records.len(), 1);
        assert_eq!(jobs.records[&id].done, 3);
        jobs.finish(&first.id, Ok((false, true)));
        assert!(jobs.next().is_none());
        jobs.enqueue(0, files("a"));
        assert_eq!(jobs.next().unwrap().id, id);
        assert_eq!(jobs.records.len(), 1);
    }
    #[test]
    fn streaming_distinct_jobs_wait_and_cancel_without_replacing_active() {
        let mut jobs = FileJobs::default();
        let a = jobs.enqueue(0, files("a"));
        jobs.next().unwrap();
        let b = jobs.enqueue(0, files("b"));
        let c = jobs.enqueue(0, files("c"));
        assert!(!jobs.cancel(&b));
        assert_eq!(jobs.active.as_deref(), Some(a.as_str()));
        jobs.finish(&a, Ok((false, true)));
        assert_eq!(jobs.next().unwrap().id, c);
    }
}
