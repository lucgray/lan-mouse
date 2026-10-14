//! Opt-in, bounded input timing records. Never serialize event payloads or key codes.
use crate::{Event, KeyboardEvent, PointerEvent};
use std::{
    collections::hash_map::RandomState,
    fs::{File, OpenOptions},
    hash::{BuildHasher, Hash},
    io::{self, BufWriter, Write},
    path::PathBuf,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(serde::Deserialize)]
pub struct Config {
    pub path: PathBuf,
    pub max_file_bytes: u64,
    pub queue_capacity: usize,
    pub sample_interval_ms: u64,
}

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum Stage {
    Native,
    Capture,
    Send,
    Sent,
    Receive,
    Apply,
    Applied,
    Suppressed,
}
const STAGES: [Stage; 8] = [
    Stage::Native,
    Stage::Capture,
    Stage::Send,
    Stage::Sent,
    Stage::Receive,
    Stage::Apply,
    Stage::Applied,
    Stage::Suppressed,
];

#[derive(Clone, Copy, Debug)]
pub enum Control {
    Start,
    Edge,
    Release,
    Enter,
    Leave,
    ForwardingStopped,
}

#[derive(Debug)]
enum Detail {
    Key {
        token: u64,
        state: u8,
        source_time: u32,
    },
    Control {
        kind: Control,
        value: f64,
    },
}
struct Record {
    unix_us: u128,
    elapsed_us: u128,
    stage: Stage,
    peer: u64,
    detail: Detail,
}
struct Recorder {
    tx: SyncSender<Record>,
    salt: RandomState,
    started: Instant,
    dropped: Arc<AtomicU64>,
    motion: Arc<[AtomicU64; 8]>,
}
static RECORDER: OnceLock<Recorder> = OnceLock::new();

pub fn init(config: Config) -> io::Result<()> {
    if config.max_file_bytes == 0 || config.queue_capacity == 0 || config.sample_interval_ms == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "diagnostic limits must be positive",
        ));
    }
    let writer = RollingWriter::new(config.path, config.max_file_bytes)?;
    let (tx, rx) = mpsc::sync_channel(config.queue_capacity);
    let dropped = Arc::new(AtomicU64::new(0));
    let motion = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
    let worker_dropped = dropped.clone();
    let worker_motion = motion.clone();
    thread::Builder::new()
        .name("input-diagnostics".into())
        .spawn(move || {
            if let Err(error) = write_records(
                writer,
                rx,
                worker_dropped,
                worker_motion,
                Duration::from_millis(config.sample_interval_ms),
            ) {
                log::error!("input diagnostics stopped: {error}");
            }
        })?;
    RECORDER
        .set(Recorder {
            tx,
            salt: RandomState::new(),
            started: Instant::now(),
            dropped,
            motion,
        })
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "input diagnostics already initialized",
            )
        })?;
    control(
        Stage::Native,
        0u8,
        Control::Start,
        std::process::id() as f64,
    );
    log::info!("input timing diagnostics enabled; no key codes or clipboard contents recorded");
    Ok(())
}

/// Only keyboard transitions and motion counts are observed. Clipboard and button payloads are ignored.
pub fn event(stage: Stage, peer: impl Hash, event: &Event) {
    let Some(recorder) = RECORDER.get() else {
        return;
    };
    match event {
        Event::Keyboard(KeyboardEvent::Key { time, key, state }) => {
            recorder.send(
                stage,
                peer,
                Detail::Key {
                    token: recorder.salt.hash_one(key),
                    state: *state,
                    source_time: *time,
                },
            );
        }
        Event::Pointer(PointerEvent::Motion { .. }) => {
            recorder.motion[stage as usize].fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

pub fn control(stage: Stage, peer: impl Hash, kind: Control, value: f64) {
    if let Some(recorder) = RECORDER.get() {
        recorder.send(stage, peer, Detail::Control { kind, value });
    }
}

impl Recorder {
    fn send(&self, stage: Stage, peer: impl Hash, detail: Detail) {
        let record = Record {
            unix_us: unix_us(),
            elapsed_us: self.started.elapsed().as_micros(),
            stage,
            peer: self.salt.hash_one(peer),
            detail,
        };
        // Recording must never wait for disk or a full diagnostic queue.
        if self.tx.try_send(record).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn unix_us() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros(),
        Err(error) => {
            log::error!("input diagnostic clock is before epoch: {error}");
            0
        }
    }
}

fn format_record(record: &Record) -> String {
    let detail = match record.detail {
        Detail::Key {
            token,
            state,
            source_time,
        } => format!("key token={token:016x} state={state} source_time={source_time}"),
        Detail::Control { kind, value } => format!("control kind={kind:?} value={value}"),
    };
    format!(
        "utc_us={} elapsed_us={} stage={:?} peer={:016x} {}\n",
        record.unix_us, record.elapsed_us, record.stage, record.peer, detail
    )
}

fn write_records(
    mut writer: RollingWriter,
    rx: Receiver<Record>,
    dropped: Arc<AtomicU64>,
    motion: Arc<[AtomicU64; 8]>,
    interval: Duration,
) -> io::Result<()> {
    let mut last_sample = Instant::now();
    loop {
        match rx.recv_timeout(interval.saturating_sub(last_sample.elapsed())) {
            Ok(record) => writer.line(&format_record(&record))?,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return writer.flush(),
        }
        if last_sample.elapsed() >= interval {
            let elapsed_ms = last_sample.elapsed().as_millis();
            let dropped = dropped.swap(0, Ordering::Relaxed);
            writer.line(&format!(
                "utc_us={} sample_ms={elapsed_ms} dropped={dropped} motion={:?}\n",
                unix_us(),
                STAGES.map(|stage| motion[stage as usize].swap(0, Ordering::Relaxed))
            ))?;
            if dropped > 0 {
                log::warn!(
                    "input diagnostic queue dropped {dropped} records; input forwarding was not blocked"
                );
            }
            writer.flush()?;
            last_sample = Instant::now();
        }
    }
}

struct RollingWriter {
    path: PathBuf,
    previous: PathBuf,
    file: Option<BufWriter<File>>,
    bytes: u64,
    limit: u64,
}
impl RollingWriter {
    fn new(path: PathBuf, limit: u64) -> io::Result<Self> {
        let previous = PathBuf::from(format!("{}.1", path.display()));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let bytes = file.metadata()?.len();
        Ok(Self {
            path,
            previous,
            file: Some(BufWriter::new(file)),
            bytes,
            limit,
        })
    }
    fn line(&mut self, line: &str) -> io::Result<()> {
        let size = line.len() as u64;
        if size > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "diagnostic record exceeds configured file size",
            ));
        }
        if self.bytes + size > self.limit {
            self.flush()?;
            self.file.take();
            if self.previous.exists() {
                std::fs::remove_file(&self.previous)?;
            }
            std::fs::rename(&self.path, &self.previous)?;
            self.file = Some(BufWriter::new(File::create(&self.path)?));
            self.bytes = 0;
        }
        self.file
            .as_mut()
            .expect("writer exists outside rotation")
            .write_all(line.as_bytes())?;
        self.bytes += size;
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file
            .as_mut()
            .expect("writer exists outside rotation")
            .flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostics_rotation_is_bounded_and_preserves_recent_records() {
        let dir = std::env::temp_dir().join(format!(
            "lan-mouse-diagnostics-{}-{}",
            std::process::id(),
            unix_us()
        ));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("timing.log");
        let mut writer = RollingWriter::new(path.clone(), 8).unwrap();
        for line in ["111\n", "222\n", "333\n", "444\n", "555\n"] {
            writer.line(line).unwrap();
        }
        writer.flush().unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "555\n");
        assert_eq!(
            std::fs::read_to_string(&writer.previous).unwrap(),
            "333\n444\n"
        );
        drop(writer);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn diagnostics_full_queue_does_not_block_and_reports_loss() {
        let (tx, rx) = mpsc::sync_channel(1);
        let recorder = Recorder {
            tx,
            salt: RandomState::new(),
            started: Instant::now(),
            dropped: Arc::new(AtomicU64::new(0)),
            motion: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
        };
        recorder.send(
            Stage::Send,
            0u8,
            Detail::Key {
                token: 123,
                state: 1,
                source_time: 0,
            },
        );
        recorder.send(
            Stage::Send,
            0u8,
            Detail::Key {
                token: 123,
                state: 0,
                source_time: 0,
            },
        );
        assert_eq!(recorder.dropped.load(Ordering::Relaxed), 1);
        assert!(rx.try_recv().is_ok());
    }
    #[test]
    fn diagnostics_key_record_contains_only_timing_and_opaque_token() {
        let record = Record {
            unix_us: 1,
            elapsed_us: 2,
            stage: Stage::Send,
            peer: 3,
            detail: Detail::Key {
                token: 4,
                state: 0,
                source_time: 5,
            },
        };
        assert_eq!(
            format_record(&record),
            "utc_us=1 elapsed_us=2 stage=Send peer=0000000000000003 key token=0000000000000004 state=0 source_time=5\n"
        );
    }
}
