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
    InputQueueFull,
    InputQueueClosed,
    ReleaseBind,
    BackendStopped,
    RemoteReturn,
    SendFailed,
    WaitingForAck,
}

#[derive(Debug)]
enum Detail {
    Key {
        token: u64,
        state: u8,
        source_time: u32,
        modifier: bool,
    },
    NativeKey {
        token: u64,
        state: u8,
        message_ms: u32,
        observed_ms: u32,
        injected: bool,
        modifier: bool,
    },
    Button {
        token: u64,
        state: u32,
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
    metrics: Arc<Metrics>,
}
#[derive(Default)]
struct Metrics {
    last_motion_us: [AtomicU64; 8],
    max_motion_gap_us: [AtomicU64; 8],
    max_operation_us: [AtomicU64; 8],
    native_pointer_wait_ms: AtomicU64,
}
static RECORDER: OnceLock<Recorder> = OnceLock::new();

pub fn init(config: Config, executable_sha256: &str) -> io::Result<()> {
    if config.max_file_bytes == 0 || config.queue_capacity == 0 || config.sample_interval_ms == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "diagnostic limits must be positive",
        ));
    }
    let header = format!(
        "schema=2 utc_us={} process={} os={} executable_sha256={} sample_interval_ms={} max_file_bytes={} queue_capacity={}\n",
        unix_us(),
        std::process::id(),
        std::env::consts::OS,
        executable_sha256,
        config.sample_interval_ms,
        config.max_file_bytes,
        config.queue_capacity
    );
    let mut writer = RollingWriter::new(config.path, config.max_file_bytes, header)?;
    writer.flush()?;
    let (tx, rx) = mpsc::sync_channel(config.queue_capacity);
    let dropped = Arc::new(AtomicU64::new(0));
    let motion = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
    let worker_dropped = dropped.clone();
    let worker_motion = motion.clone();
    let metrics = Arc::new(Metrics::default());
    let worker_metrics = metrics.clone();
    let started = Instant::now();
    thread::Builder::new()
        .name("input-diagnostics".into())
        .spawn(move || {
            if let Err(error) = write_records(
                writer,
                rx,
                worker_dropped,
                worker_motion,
                worker_metrics,
                started,
                Duration::from_millis(config.sample_interval_ms),
            ) {
                log::error!("input diagnostics stopped: {error}");
            }
        })?;
    RECORDER
        .set(Recorder {
            tx,
            salt: RandomState::new(),
            started,
            dropped,
            motion,
            metrics,
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

/// No character, raw key/button code, clipboard content or motion coordinate is serialized.
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
                    modifier: is_modifier(*key),
                },
            );
        }
        Event::Pointer(PointerEvent::Motion { .. }) => {
            recorder.motion[stage as usize].fetch_add(1, Ordering::Relaxed);
            observe_motion(
                &recorder.metrics,
                stage,
                recorder.started.elapsed().as_micros() as u64,
            );
        }
        Event::Pointer(PointerEvent::Button { button, state, .. }) => {
            recorder.send(
                stage,
                peer,
                Detail::Button {
                    token: recorder.salt.hash_one(button),
                    state: *state,
                },
            );
        }
        _ => {}
    }
}

pub fn enabled() -> bool {
    RECORDER.get().is_some()
}

fn is_modifier(key: u32) -> bool {
    use crate::scancode::Linux::*;
    matches!(
        crate::scancode::Linux::try_from(key),
        Ok(KeyLeftCtrl
            | KeyRightCtrl
            | KeyLeftAlt
            | KeyRightalt
            | KeyLeftShift
            | KeyRightShift
            | KeyLeftMeta
            | KeyRightmeta)
    )
}

/// Original Windows message timestamps stay local: the transmitted event is unchanged.
pub fn native_key(
    peer: impl Hash,
    event: &KeyboardEvent,
    message_ms: u32,
    observed_ms: u32,
    injected: bool,
) {
    if let (Some(recorder), KeyboardEvent::Key { key, state, .. }) = (RECORDER.get(), event) {
        recorder.send(
            Stage::Native,
            peer,
            Detail::NativeKey {
                token: recorder.salt.hash_one(key),
                state: *state,
                message_ms,
                observed_ms,
                injected,
                modifier: is_modifier(*key),
            },
        );
    }
}

pub fn native_pointer(message_ms: u32, observed_ms: u32) {
    if let Some(recorder) = RECORDER.get() {
        recorder.metrics.native_pointer_wait_ms.fetch_max(
            u64::from(observed_ms.wrapping_sub(message_ms)),
            Ordering::Relaxed,
        );
    }
}

pub fn duration(stage: Stage, elapsed: Duration) {
    if let Some(recorder) = RECORDER.get() {
        recorder.metrics.max_operation_us[stage as usize]
            .fetch_max(elapsed.as_micros() as u64, Ordering::Relaxed);
    }
}

fn observe_motion(metrics: &Metrics, stage: Stage, now_us: u64) {
    let last = metrics.last_motion_us[stage as usize].swap(now_us, Ordering::Relaxed);
    if last != 0 {
        metrics.max_motion_gap_us[stage as usize]
            .fetch_max(now_us.saturating_sub(last), Ordering::Relaxed);
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
            modifier,
        } => format!(
            "key token={token:016x} state={state} source_time={source_time} modifier={modifier}"
        ),
        Detail::NativeKey {
            token,
            state,
            message_ms,
            observed_ms,
            injected,
            modifier,
        } => format!(
            "native_key token={token:016x} state={state} message_ms={message_ms} observed_ms={observed_ms} wait_ms={} injected={injected} modifier={modifier}",
            observed_ms.wrapping_sub(message_ms)
        ),
        Detail::Button { token, state } => format!("button token={token:016x} state={state}"),
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
    metrics: Arc<Metrics>,
    started: Instant,
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
            let now_us = started.elapsed().as_micros() as u64;
            let last_motion_age_us = STAGES.map(|stage| {
                let last = metrics.last_motion_us[stage as usize].load(Ordering::Relaxed);
                (last != 0).then(|| now_us.saturating_sub(last))
            });
            writer.line(&format!(
                "utc_us={} sample_ms={elapsed_ms} dropped={dropped} motion={:?} motion_gap_max_us={:?} motion_age_us={last_motion_age_us:?} operation_max_us={:?} native_pointer_wait_max_ms={}\n",
                unix_us(),
                STAGES.map(|stage| motion[stage as usize].swap(0, Ordering::Relaxed)),
                STAGES.map(|stage| metrics.max_motion_gap_us[stage as usize].swap(0, Ordering::Relaxed)),
                STAGES.map(|stage| metrics.max_operation_us[stage as usize].swap(0, Ordering::Relaxed)),
                metrics.native_pointer_wait_ms.swap(0, Ordering::Relaxed),
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
    header: String,
}
impl RollingWriter {
    fn new(path: PathBuf, limit: u64, header: String) -> io::Result<Self> {
        let previous = PathBuf::from(format!("{}.1", path.display()));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let bytes = file.metadata()?.len();
        let mut writer = Self {
            path,
            previous,
            file: Some(BufWriter::new(file)),
            bytes,
            limit,
            header: String::new(),
        };
        writer.line(&header)?;
        writer.header = header;
        Ok(writer)
    }
    fn line(&mut self, line: &str) -> io::Result<()> {
        let size = line.len() as u64;
        if size + self.header.len() as u64 > self.limit {
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
            self.file
                .as_mut()
                .expect("writer exists after rotation")
                .write_all(self.header.as_bytes())?;
            self.bytes = self.header.len() as u64;
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
        let mut writer = RollingWriter::new(path.clone(), 8, String::new()).unwrap();
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
    fn diagnostics_rotation_keeps_process_header_within_limit() {
        let dir = std::env::temp_dir().join(format!(
            "lan-mouse-header-{}-{}",
            std::process::id(),
            unix_us()
        ));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("timing.log");
        let mut writer = RollingWriter::new(path.clone(), 12, "hdr\n".into()).unwrap();
        for line in ["111\n", "222\n", "333\n", "444\n", "555\n"] {
            writer.line(line).unwrap();
        }
        writer.flush().unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hdr\n555\n");
        assert_eq!(
            std::fs::read_to_string(&writer.previous).unwrap(),
            "hdr\n333\n444\n"
        );
        assert!(writer.line("oversized\n").is_err());
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
            metrics: Arc::new(Metrics::default()),
        };
        recorder.send(
            Stage::Send,
            0u8,
            Detail::Key {
                token: 123,
                state: 1,
                source_time: 0,
                modifier: false,
            },
        );
        recorder.send(
            Stage::Send,
            0u8,
            Detail::Key {
                token: 123,
                state: 0,
                source_time: 0,
                modifier: false,
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
                modifier: false,
            },
        };
        assert_eq!(
            format_record(&record),
            "utc_us=1 elapsed_us=2 stage=Send peer=0000000000000003 key token=0000000000000004 state=0 source_time=5 modifier=false\n"
        );
    }
    #[test]
    fn diagnostics_native_clock_handles_tick_wrap_without_changing_event() {
        let record = Record {
            unix_us: 1,
            elapsed_us: 2,
            stage: Stage::Native,
            peer: 3,
            detail: Detail::NativeKey {
                token: 4,
                state: 0,
                message_ms: u32::MAX - 4,
                observed_ms: 5,
                injected: false,
                modifier: true,
            },
        };
        let line = format_record(&record);
        assert!(line.contains("wait_ms=10 "));
        assert!(line.contains("modifier=true"));
        assert!(!line.contains("scan"));
    }
    #[test]
    fn diagnostics_modifiers_are_not_mistaken_for_typing() {
        use crate::scancode::Linux::*;
        assert!(is_modifier(KeyLeftCtrl as u32));
        assert!(is_modifier(KeyRightShift as u32));
        assert!(!is_modifier(KeyA as u32));
        assert!(!is_modifier(KeyF3 as u32));
    }
    #[test]
    fn diagnostics_motion_gap_preserves_silence_between_samples() {
        let metrics = Metrics::default();
        observe_motion(&metrics, Stage::Receive, 100);
        observe_motion(&metrics, Stage::Receive, 108);
        assert_eq!(
            metrics.max_motion_gap_us[Stage::Receive as usize].swap(0, Ordering::Relaxed),
            8
        );
        observe_motion(&metrics, Stage::Receive, 1_000_108);
        assert_eq!(
            metrics.max_motion_gap_us[Stage::Receive as usize].load(Ordering::Relaxed),
            1_000_000
        );
        assert_eq!(
            metrics.last_motion_us[Stage::Send as usize].load(Ordering::Relaxed),
            0
        );
    }
}
