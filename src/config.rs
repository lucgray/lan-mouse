use crate::capture_test::TestCaptureArgs;
use crate::emulation_test::TestEmulationArgs;
use clap::{Parser, Subcommand, ValueEnum};
use notify::event::ModifyKind;
use notify::{EventKind, RecommendedWatcher, Watcher};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env::{self, VarError};
use std::fmt::Display;
use std::fs::{self, File};
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use std::{collections::HashSet, io};
use thiserror::Error;
use toml;
use toml_edit::{self, DocumentMut};

use lan_mouse_cli::CliArgs;
use lan_mouse_ipc::{DEFAULT_PORT, Position};

use input_event::scancode::{
    self,
    Linux::{KeyLeftAlt, KeyLeftCtrl, KeyLeftMeta, KeyLeftShift, KeyScrollLock},
};

use shadow_rs::shadow;

shadow!(build);

/// Local build's 8-byte ASCII short commit hash, suitable for use
/// in [`lan_mouse_proto::ProtoEvent::Hello`]. Pads with `'?'` if
/// shadow_rs returns an unexpected length so the field is always
/// well-formed on the wire.
pub fn local_commit() -> [u8; 8] {
    let bytes = build::SHORT_COMMIT.as_bytes();
    let mut out = [b'?'; 8];
    let n = bytes.len().min(8);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}

const CONFIG_FILE_NAME: &str = "config.toml";
const CERT_FILE_NAME: &str = "lan-mouse.pem";

fn default_path() -> Result<PathBuf, VarError> {
    #[cfg(unix)]
    let default_path = {
        let xdg_config_home =
            env::var("XDG_CONFIG_HOME").unwrap_or(format!("{}/.config", env::var("HOME")?));
        format!("{xdg_config_home}/lan-mouse/")
    };

    #[cfg(not(unix))]
    let default_path = {
        #[cfg(windows)]
        if crate::is_windows_service() {
            "C:\\ProgramData\\lan-mouse\\".to_string()
        } else {
            let app_data =
                env::var("LOCALAPPDATA").unwrap_or(format!("{}/.config", env::var("USERPROFILE")?));
            format!("{app_data}\\lan-mouse\\")
        }
        #[cfg(not(windows))]
        {
            let app_data =
                env::var("LOCALAPPDATA").unwrap_or(format!("{}/.config", env::var("USERPROFILE")?));
            format!("{app_data}\\lan-mouse\\")
        }
    };
    Ok(PathBuf::from(default_path))
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
struct ConfigToml {
    jail_bind: Option<Vec<scancode::Linux>>,
    capture_backend: Option<CaptureBackend>,
    emulation_backend: Option<EmulationBackend>,
    port: Option<u16>,
    release_bind: Option<Vec<scancode::Linux>>,
    key_repeat_delay: Option<u64>,
    key_repeat_interval: Option<u64>,
    /// key binds that enter the client(s) at a position without the
    /// pointer having to cross the corresponding screen edge
    ///
    /// Keyed by position rather than by client because entering is
    /// position-based: crossing an edge enters every client at that
    /// edge, and a bind is deliberately no different.
    enter_binds: Option<HashMap<Position, Vec<scancode::Linux>>>,
    /// transformations applied to input events on their way to other
    /// devices (the counterpart of receive-side post-processing)
    input_pre_processing: Option<InputPreProcessing>,
    cert_path: Option<PathBuf>,
    clients: Option<Vec<TomlClient>>,
    authorized_fingerprints: Option<HashMap<String, String>>,
    input_post_processing: Option<InputConfig>,
    /// enable clipboard sharing between machines (default: true)
    enable_clipboard: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
struct InputConfig {
    // TODO: implement scroll_sensitivity and mouse_acceleration
    invert_scroll: Option<bool>,
    mouse_sensitivity: Option<f64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
struct InputPreProcessing {
    /// keys to send as a different key, e.g. to reconcile modifier
    /// layouts between operating systems
    remap_keys: Option<HashMap<scancode::Linux, scancode::Linux>>,
    /// invert the direction of vertical scroll events, e.g. to
    /// reconcile "natural" macOS scrolling with a Windows or Linux peer
    invert_scroll_vertical: Option<bool>,
    /// invert the direction of horizontal scroll events
    invert_scroll_horizontal: Option<bool>,
    /// chord-specific key overrides, e.g. sending Command as Alt only
    /// when Tab is pressed while it's held (Cmd+Tab → Alt+Tab), without
    /// disturbing what Command sends as on its own or with anything else
    remap_chords: Option<Vec<crate::remap::ChordRemap>>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
struct TomlClient {
    hostname: Option<String>,
    host_name: Option<String>,
    ips: Option<Vec<IpAddr>>,
    port: Option<u16>,
    position: Option<Position>,
    activate_on_startup: Option<bool>,
    enter_hook: Option<String>,
    leave_hook: Option<String>,
}

#[derive(Parser, Debug)]
#[command(author, version=build::CLAP_LONG_VERSION, about, long_about = None)]
struct Args {
    /// the listen port for lan-mouse
    #[arg(short, long)]
    port: Option<u16>,

    /// non-default config file location
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// capture backend override
    #[arg(long)]
    capture_backend: Option<CaptureBackend>,

    /// emulation backend override
    #[arg(long)]
    emulation_backend: Option<EmulationBackend>,

    /// path to non-default certificate location
    #[arg(long)]
    cert_path: Option<PathBuf>,

    /// subcommands
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Clone, Debug, PartialEq)]
pub enum Command {
    /// test input emulation
    TestEmulation(TestEmulationArgs),
    /// test input capture
    TestCapture(TestCaptureArgs),
    /// Lan Mouse commandline interface
    Cli(CliArgs),
    /// run in daemon mode
    Daemon,
    /// Install as system service (Windows: SCM service, Linux: systemd, macOS: launchd)
    #[cfg(windows)]
    Install,
    /// Uninstall system service
    #[cfg(windows)]
    Uninstall,
    /// Query service status
    #[cfg(windows)]
    Status,
    /// Run as Windows service (internal - spawns session daemons)
    #[cfg(windows)]
    #[command(hide = true)]
    WinSvc,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
pub enum CaptureBackend {
    #[cfg(libei_capture)]
    #[serde(rename = "input-capture-portal")]
    InputCapturePortal,
    #[cfg(layer_shell_capture)]
    #[serde(rename = "layer-shell")]
    LayerShell,
    #[cfg(x11_capture)]
    #[serde(rename = "x11")]
    X11,
    #[cfg(windows)]
    #[serde(rename = "windows")]
    Windows,
    #[cfg(target_os = "macos")]
    #[serde(rename = "macos")]
    MacOs,
    #[serde(rename = "dummy")]
    Dummy,
}

impl Display for CaptureBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(libei_capture)]
            CaptureBackend::InputCapturePortal => write!(f, "input-capture-portal"),
            #[cfg(layer_shell_capture)]
            CaptureBackend::LayerShell => write!(f, "layer-shell"),
            #[cfg(x11_capture)]
            CaptureBackend::X11 => write!(f, "X11"),
            #[cfg(windows)]
            CaptureBackend::Windows => write!(f, "windows"),
            #[cfg(target_os = "macos")]
            CaptureBackend::MacOs => write!(f, "MacOS"),
            CaptureBackend::Dummy => write!(f, "dummy"),
        }
    }
}

impl From<CaptureBackend> for input_capture::Backend {
    fn from(backend: CaptureBackend) -> Self {
        match backend {
            #[cfg(libei_capture)]
            CaptureBackend::InputCapturePortal => Self::InputCapturePortal,
            #[cfg(layer_shell_capture)]
            CaptureBackend::LayerShell => Self::LayerShell,
            #[cfg(x11_capture)]
            CaptureBackend::X11 => Self::X11,
            #[cfg(windows)]
            CaptureBackend::Windows => Self::Windows,
            #[cfg(target_os = "macos")]
            CaptureBackend::MacOs => Self::MacOs,
            CaptureBackend::Dummy => Self::Dummy,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
pub enum EmulationBackend {
    #[cfg(evdev_emulation)]
    #[serde(rename = "evdev")]
    Evdev,
    #[cfg(wlroots_emulation)]
    #[serde(rename = "wlroots")]
    Wlroots,
    #[cfg(libei_emulation)]
    #[serde(rename = "libei")]
    Libei,
    #[cfg(rdp_emulation)]
    #[serde(rename = "xdp")]
    Xdp,
    #[cfg(x11_emulation)]
    #[serde(rename = "x11")]
    X11,
    #[cfg(windows)]
    #[serde(rename = "windows")]
    Windows,
    #[cfg(target_os = "macos")]
    #[serde(rename = "macos")]
    MacOs,
    #[serde(rename = "dummy")]
    Dummy,
}

impl From<EmulationBackend> for input_emulation::Backend {
    fn from(backend: EmulationBackend) -> Self {
        match backend {
            #[cfg(evdev_emulation)]
            EmulationBackend::Evdev => Self::Evdev,
            #[cfg(wlroots_emulation)]
            EmulationBackend::Wlroots => Self::Wlroots,
            #[cfg(libei_emulation)]
            EmulationBackend::Libei => Self::Libei,
            #[cfg(rdp_emulation)]
            EmulationBackend::Xdp => Self::Xdp,
            #[cfg(x11_emulation)]
            EmulationBackend::X11 => Self::X11,
            #[cfg(windows)]
            EmulationBackend::Windows => Self::Windows,
            #[cfg(target_os = "macos")]
            EmulationBackend::MacOs => Self::MacOs,
            EmulationBackend::Dummy => Self::Dummy,
        }
    }
}

impl Display for EmulationBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(evdev_emulation)]
            EmulationBackend::Evdev => write!(f, "evdev"),
            #[cfg(wlroots_emulation)]
            EmulationBackend::Wlroots => write!(f, "wlroots"),
            #[cfg(libei_emulation)]
            EmulationBackend::Libei => write!(f, "libei"),
            #[cfg(rdp_emulation)]
            EmulationBackend::Xdp => write!(f, "xdg-desktop-portal"),
            #[cfg(x11_emulation)]
            EmulationBackend::X11 => write!(f, "X11"),
            #[cfg(windows)]
            EmulationBackend::Windows => write!(f, "windows"),
            #[cfg(target_os = "macos")]
            EmulationBackend::MacOs => write!(f, "macos"),
            EmulationBackend::Dummy => write!(f, "dummy"),
        }
    }
}

#[derive(Debug)]
pub struct Config {
    /// command line arguments
    args: Args,
    /// path to the certificate file used
    cert_path: PathBuf,
    /// path to the config file used
    config_path: PathBuf,
    /// path to config directory (parent of above)
    config_dir: PathBuf,
    watch_target: PathBuf,
    watched_dirs: HashSet<PathBuf>,
    /// the (optional) toml config and it's path
    config_toml: Option<ConfigToml>,
    /// Bytes last successfully loaded or saved; protects external edits.
    disk_baseline: Option<String>,
    pending_save: Option<ConfigToml>,
    save_task: Option<tokio::task::JoinHandle<io::Result<String>>>,
    read_task: Option<tokio::task::JoinHandle<ConfigReadResult>>,
    reload_conflict: bool,
    // filesystem watcher
    watcher: notify::RecommendedWatcher,
    // channel for filesystem events
    watch_rx: tokio::sync::mpsc::Receiver<Result<notify::Event, notify::Error>>,
    watch_overflow: Arc<WatchOverflow>,
}

type ConfigReadResult = io::Result<ReadSnapshot>;

#[derive(Debug)]
struct ReadSnapshot {
    target: PathBuf,
    config: io::Result<Option<(String, ConfigToml)>>,
}

#[derive(Debug, Default)]
struct WatchOverflow {
    rescan: AtomicBool,
    error: Mutex<Option<notify::Error>>,
}

fn deliver_watch_event(
    tx: &tokio::sync::mpsc::Sender<Result<notify::Event, notify::Error>>,
    overflow: &WatchOverflow,
    event: Result<notify::Event, notify::Error>,
) {
    if let Err(tokio::sync::mpsc::error::TrySendError::Full(event)) = tx.try_send(event) {
        overflow.rescan.store(true, Ordering::Release);
        if let Err(error) = event {
            *overflow.error.lock().expect("watch error lock") = Some(error);
        }
    }
}

pub struct ConfigClient {
    pub ips: HashSet<IpAddr>,
    pub hostname: Option<String>,
    pub port: u16,
    pub pos: Position,
    pub active: bool,
    pub enter_hook: Option<String>,
    pub leave_hook: Option<String>,
}

impl From<TomlClient> for ConfigClient {
    fn from(toml: TomlClient) -> Self {
        let active = toml.activate_on_startup.unwrap_or(false);
        let enter_hook = toml.enter_hook;
        let leave_hook = toml.leave_hook;
        let hostname = toml.hostname;
        let ips = HashSet::from_iter(toml.ips.into_iter().flatten());
        let port = toml.port.unwrap_or(DEFAULT_PORT);
        let pos = toml.position.unwrap_or_default();
        Self {
            ips,
            hostname,
            port,
            pos,
            active,
            enter_hook,
            leave_hook,
        }
    }
}

impl From<ConfigClient> for TomlClient {
    fn from(client: ConfigClient) -> Self {
        let hostname = client.hostname;
        let host_name = None;
        let mut ips = client.ips.into_iter().collect::<Vec<_>>();
        ips.sort();
        let ips = Some(ips);
        let port = if client.port == DEFAULT_PORT {
            None
        } else {
            Some(client.port)
        };
        let position = Some(client.pos);
        let activate_on_startup = if client.active { Some(true) } else { None };
        let enter_hook = client.enter_hook;
        let leave_hook = client.leave_hook;
        Self {
            hostname,
            host_name,
            ips,
            port,
            position,
            activate_on_startup,
            enter_hook,
            leave_hook,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Var(#[from] VarError),
    #[error(transparent)]
    Watcher(#[from] notify::Error),
}

const DEFAULT_RELEASE_KEYS: [scancode::Linux; 4] =
    [KeyLeftCtrl, KeyLeftShift, KeyLeftMeta, KeyLeftAlt];

const DEFAULT_JAIL_KEY: scancode::Linux = KeyScrollLock;

impl Config {
    pub fn new() -> Result<Self, ConfigError> {
        let args = Args::parse();
        Self::from_args(args)
    }

    pub fn new_with_args<I, T>(args_iter: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let args = Args::parse_from(args_iter);
        Self::from_args(args)
    }

    fn from_args(args: Args) -> Result<Self, ConfigError> {
        #[cfg(windows)]
        if matches!(args.command, Some(Command::WinSvc)) {
            crate::set_is_windows_service(true);
        }

        // --config <file> overrules default location
        let config_path = args
            .config
            .clone()
            .unwrap_or(default_path()?.join(CONFIG_FILE_NAME));
        let config_path = if config_path.is_absolute() {
            config_path
        } else {
            env::current_dir()?.join(config_path)
        };
        let config_dir = config_path
            .parent()
            .expect("config directory")
            .to_path_buf();

        // Ensure the config directory exists and write a default config file
        // if none is present. Runs on every Config::new(), regardless of which
        // entry path (GUI main, spawned daemon, CLI, test commands) we're on,
        // so a fresh Mac never hits "No such file or directory" on config.toml
        // and notify::Watcher (which requires the dir to exist on macOS
        // FSEvents and some Linux backends) has a concrete path to watch.
        fs::create_dir_all(&config_dir)?;
        let config_dir = fs::canonicalize(config_dir)?;
        let config_path = config_dir.join(config_path.file_name().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "config path must name a file")
        })?);
        if fs::symlink_metadata(&config_path)
            .is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
        {
            let default_toml = toml::to_string_pretty(&ConfigToml::default())
                .expect("default ConfigToml serialization cannot fail");
            fs::write(&config_path, default_toml)?;
        }

        let disk_baseline = fs::read_to_string(&config_path).ok();
        let config_toml =
            disk_baseline
                .as_deref()
                .and_then(|text| match toml::from_str::<ConfigToml>(text) {
                    Ok(config) => Some(config),
                    Err(error) => {
                        log::warn!("{config_path:?}: {error}; continuing without parsed config");
                        None
                    }
                });

        // --cert-path <file> overrules default location
        let cert_path = args
            .cert_path
            .clone()
            .or(config_toml.as_ref().and_then(|c| c.cert_path.clone()))
            .unwrap_or(default_path()?.join(CERT_FILE_NAME));

        let (tx, watch_rx) = tokio::sync::mpsc::channel(16);
        let watch_overflow = Arc::new(WatchOverflow::default());
        let overflow = watch_overflow.clone();
        let watcher = RecommendedWatcher::new(
            move |res| deliver_watch_event(&tx, &overflow, res),
            notify::Config::default(),
        )?;
        let mut config = Config {
            args,
            cert_path,
            watch_target: resolve_config_target(&config_path)?,
            config_path,
            config_dir,
            watched_dirs: HashSet::new(),
            config_toml,
            disk_baseline,
            pending_save: None,
            save_task: None,
            read_task: None,
            reload_conflict: false,
            watcher,
            watch_rx,
            watch_overflow,
        };
        config.watch()?;
        Ok(config)
    }

    fn watch(&mut self) -> Result<(), notify::Error> {
        self.refresh_watch_target(self.watch_target.clone())
    }

    fn refresh_watch_target(&mut self, target: PathBuf) -> Result<(), notify::Error> {
        let mut needed = HashSet::from([self.config_dir.clone()]);
        if let Some(parent) = target.parent() {
            needed.insert(parent.to_owned());
        }
        // Subscribe to the new target before retiring the previous directory.
        let additions: Vec<_> = needed.difference(&self.watched_dirs).cloned().collect();
        for directory in additions {
            self.watcher
                .watch(&directory, notify::RecursiveMode::NonRecursive)?;
            self.watched_dirs.insert(directory);
        }
        let obsolete: Vec<_> = self.watched_dirs.difference(&needed).cloned().collect();
        self.watched_dirs.extend(needed);
        self.watch_target = target;
        for directory in obsolete {
            match self.watcher.unwatch(&directory) {
                Ok(()) => {
                    self.watched_dirs.remove(&directory);
                }
                Err(error) => log::warn!("could not retire config watch {directory:?}: {error}"),
            }
        }
        Ok(())
    }

    fn unwatch(&mut self) -> Result<(), notify::Error> {
        let directories: Vec<_> = self.watched_dirs.iter().cloned().collect();
        for directory in directories {
            self.watcher.unwatch(&directory)?;
            self.watched_dirs.remove(&directory);
        }
        Ok(())
    }

    pub async fn changed(&mut self) -> Result<bool, notify::Error> {
        loop {
            if self.save_task.is_some() {
                // The handle stays in Config across cancellation by select!, so
                // a completed save cannot lose its baseline or error report.
                self.finish_save().await?;
                return Ok(false);
            }
            if self.read_task.is_some() {
                return self.finish_read().await.map_err(Into::into);
            }
            let overflow_error = self
                .watch_overflow
                .error
                .lock()
                .expect("watch error lock")
                .take();
            if let Some(error) = overflow_error {
                return Err(error);
            }
            if self.watch_overflow.rescan.swap(false, Ordering::AcqRel) {
                self.start_read();
                continue;
            }
            let event = self.watch_rx.recv().await.expect("channel closed");
            let event = event?;
            if (event.paths.contains(&self.config_path) || event.paths.contains(&self.watch_target))
                && matches!(
                    event.kind,
                    EventKind::Create(_)
                        | EventKind::Modify(ModifyKind::Data(_))
                        | EventKind::Modify(ModifyKind::Name(_))
                        | EventKind::Remove(_)
                )
            {
                self.start_read();
            }
        }
    }

    /// Queue a snapshot without serializing or waiting for filesystem I/O.
    /// At most one save runs and one newer snapshot is retained.
    pub fn queue_write_back(&mut self) {
        self.pending_save = Some(self.config_toml.clone().unwrap_or_default());
        self.start_save();
    }

    fn start_save(&mut self) {
        if self.save_task.is_some() || self.read_task.is_some() {
            return;
        }
        if let Some(snapshot) = self.pending_save.take() {
            let path = self.config_path.clone();
            let baseline = self.disk_baseline.clone();
            self.save_task = Some(tokio::task::spawn_blocking(move || {
                save_snapshot(&path, baseline.as_deref(), snapshot)
            }));
        }
    }

    async fn finish_save(&mut self) -> io::Result<()> {
        let result = self.save_task.as_mut().expect("save task").await;
        self.save_task = None;
        match result.map_err(io::Error::other).and_then(|result| result) {
            Ok(bytes) => {
                self.disk_baseline = Some(bytes);
                self.start_save();
                Ok(())
            }
            Err(error) => {
                // Do not replay newer snapshots derived from an obsolete file.
                // A subsequent external reload or explicit edit can try again.
                self.pending_save = None;
                Err(error)
            }
        }
    }

    fn start_read(&mut self) {
        let path = self.config_path.clone();
        let baseline = self.disk_baseline.clone();
        self.read_task = Some(tokio::task::spawn_blocking(move || {
            read_snapshot(&path, baseline.as_deref())
        }));
    }

    async fn finish_read(&mut self) -> io::Result<bool> {
        let result = self.read_task.as_mut().expect("read task").await;
        self.read_task = None;
        let applied = result
            .map_err(io::Error::other)
            .and_then(|result| result)
            .and_then(|snapshot| self.apply_read(snapshot));
        match applied {
            Ok(changed) => {
                self.start_save();
                Ok(changed)
            }
            Err(error) => {
                if self.pending_save.take().is_some() {
                    return Err(io::Error::new(
                        error.kind(),
                        format!(
                            "configuration reload failed; pending settings were not saved: {error}"
                        ),
                    ));
                }
                Err(error)
            }
        }
    }

    fn apply_read(&mut self, snapshot: ReadSnapshot) -> io::Result<bool> {
        self.refresh_watch_target(snapshot.target)
            .map_err(io::Error::other)?;
        let Some((bytes, config)) = snapshot.config? else {
            return Ok(false);
        };
        let changed = self
            .config_toml
            .as_ref()
            .is_none_or(|current| current != &config);
        // The external file wins over snapshots derived from the old file.
        // The service reports this explicitly while applying the reload.
        if self.pending_save.take().is_some() && changed {
            self.reload_conflict = true;
        }
        self.config_toml = Some(config);
        self.disk_baseline = Some(bytes);
        Ok(changed)
    }

    pub fn take_reload_conflict(&mut self) -> bool {
        std::mem::take(&mut self.reload_conflict)
    }

    /// Wait until all accepted snapshots have been saved, or report failure.
    pub async fn flush(&mut self) -> io::Result<()> {
        while self.save_task.is_some() || self.read_task.is_some() {
            if self.read_task.is_some() {
                self.finish_read().await?;
                if self.take_reload_conflict() {
                    return Err(io::Error::other(
                        "external configuration replaced pending settings",
                    ));
                }
            } else {
                self.finish_save().await?;
            }
        }
        Ok(())
    }

    /// the command to run
    pub fn command(&self) -> Option<Command> {
        self.args.command.clone()
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    /// public key fingerprints authorized for connection
    pub fn authorized_fingerprints(&self) -> HashMap<String, String> {
        self.config_toml
            .as_ref()
            .and_then(|c| c.authorized_fingerprints.clone())
            .unwrap_or_default()
    }

    /// path to certificate
    pub fn cert_path(&self) -> &Path {
        &self.cert_path
    }

    /// optional input-capture backend override
    pub fn capture_backend(&self) -> Option<CaptureBackend> {
        self.args
            .capture_backend
            .or(self.config_toml.as_ref().and_then(|c| c.capture_backend))
    }

    /// optional input-emulation backend override
    pub fn emulation_backend(&self) -> Option<EmulationBackend> {
        self.args
            .emulation_backend
            .or(self.config_toml.as_ref().and_then(|c| c.emulation_backend))
    }

    /// the port to use (initially)
    pub fn port(&self) -> u16 {
        self.args
            .port
            .or(self.config_toml.as_ref().and_then(|c| c.port))
            .unwrap_or(DEFAULT_PORT)
    }

    pub fn invert_scroll(&self) -> bool {
        self.config_toml
            .as_ref()
            .and_then(|c| c.input_post_processing.as_ref())
            .and_then(|i| i.invert_scroll)
            .unwrap_or(false)
    }

    pub fn mouse_sensitivity(&self) -> f64 {
        self.config_toml
            .as_ref()
            .and_then(|c| c.input_post_processing.as_ref())
            .and_then(|i| i.mouse_sensitivity)
            .unwrap_or(1.0)
    }

    /// list of configured clients
    pub fn clients(&self) -> Vec<ConfigClient> {
        self.config_toml
            .as_ref()
            .map(|c| c.clients.clone())
            .unwrap_or_default()
            .into_iter()
            .flatten()
            .map(From::<TomlClient>::from)
            .collect()
    }

    /// release bind for returning control to the host
    pub fn release_bind(&self) -> Vec<scancode::Linux> {
        self.config_toml
            .as_ref()
            .and_then(|c| c.release_bind.clone())
            .unwrap_or(Vec::from_iter(DEFAULT_RELEASE_KEYS.iter().cloned()))
    }

    pub fn jail_bind(&self) -> Vec<scancode::Linux> {
        self.config_toml
            .as_ref()
            .and_then(|c| c.jail_bind.clone())
            .unwrap_or(vec![DEFAULT_JAIL_KEY])
    }

    /// key-repeat timing for emulation backends that regenerate repeats
    /// themselves (macOS and Windows). Values are read in milliseconds; unset
    /// fields fall back to the [`input_emulation::EmulationOptions`] defaults.
    pub fn emulation_options(&self) -> input_emulation::EmulationOptions {
        let defaults = input_emulation::EmulationOptions::default();
        let config_toml = self.config_toml.as_ref();
        input_emulation::EmulationOptions {
            key_repeat_delay: config_toml
                .and_then(|c| c.key_repeat_delay)
                .map(Duration::from_millis)
                .unwrap_or(defaults.key_repeat_delay),
            key_repeat_interval: config_toml
                .and_then(|c| c.key_repeat_interval)
                .map(Duration::from_millis)
                .unwrap_or(defaults.key_repeat_interval),
        }
    }

    /// key binds that enter a client without an edge crossing,
    /// binds that enter a position without an edge crossing
    pub fn enter_binds(&self) -> HashMap<Position, Vec<scancode::Linux>> {
        self.config_toml
            .as_ref()
            .and_then(|c| c.enter_binds.clone())
            .unwrap_or_default()
            .into_iter()
            .filter(|(_, bind)| !bind.is_empty())
            .collect()
    }

    /// keys rewritten on their way to other devices
    pub fn remap_keys(&self) -> HashMap<scancode::Linux, scancode::Linux> {
        self.config_toml
            .as_ref()
            .and_then(|c| c.input_pre_processing.as_ref())
            .and_then(|p| p.remap_keys.clone())
            .unwrap_or_default()
    }

    /// chord-specific key overrides applied on their way to other devices
    pub fn remap_chords(&self) -> Vec<crate::ChordRemap> {
        self.config_toml
            .as_ref()
            .and_then(|c| c.input_pre_processing.as_ref())
            .and_then(|p| p.remap_chords.clone())
            .unwrap_or_default()
    }

    /// whether vertical scroll events are inverted on their way to other devices
    pub fn invert_scroll_vertical(&self) -> bool {
        self.config_toml
            .as_ref()
            .and_then(|c| c.input_pre_processing.as_ref())
            .and_then(|p| p.invert_scroll_vertical)
            .unwrap_or(false)
    }

    /// whether horizontal scroll events are inverted on their way to other devices
    pub fn invert_scroll_horizontal(&self) -> bool {
        self.config_toml
            .as_ref()
            .and_then(|c| c.input_pre_processing.as_ref())
            .and_then(|p| p.invert_scroll_horizontal)
            .unwrap_or(false)
    }

    /// set configured clients
    pub fn set_clients(&mut self, clients: Vec<ConfigClient>) {
        self.toml_mut().clients = Some(clients.into_iter().map(|c| c.into()).collect::<Vec<_>>());
    }

    /// set authorized keys
    pub fn set_authorized_keys(&mut self, fingerprints: HashMap<String, String>) {
        if self.config_toml.is_none() {
            self.config_toml = Some(Default::default());
        }
        self.config_toml
            .as_mut()
            .expect("config")
            .authorized_fingerprints = Some(fingerprints);
    }

    fn toml_mut(&mut self) -> &mut ConfigToml {
        if self.config_toml.is_none() {
            self.config_toml = Some(Default::default());
        }
        self.config_toml.as_mut().expect("config")
    }

    /// persist the clipboard sharing toggle
    pub fn set_clipboard_enabled(&mut self, enabled: bool) {
        self.toml_mut().enable_clipboard = Some(enabled);
    }

    /// persist the scroll-inversion toggle
    pub fn set_invert_scroll(&mut self, invert: bool) {
        self.toml_mut()
            .input_post_processing
            .get_or_insert_with(Default::default)
            .invert_scroll = Some(invert);
    }

    /// persist the mouse sensitivity multiplier
    pub fn set_mouse_sensitivity(&mut self, sensitivity: f64) {
        self.toml_mut()
            .input_post_processing
            .get_or_insert_with(Default::default)
            .mouse_sensitivity = Some(sensitivity);
    }

    /// Blocking utility; service reloads use the retained background read task.
    pub fn read_from_disk(&mut self) -> Result<bool, io::Error> {
        if self.save_task.is_some() || self.read_task.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "configuration I/O is running",
            ));
        }
        let snapshot = read_snapshot(&self.config_path, self.disk_baseline.as_deref())?;
        self.apply_read(snapshot)
    }

    /// Blocking utility retained for callers outside the service input loop.
    pub fn write_back(&mut self) -> Result<(), io::Error> {
        if self.save_task.is_some() || self.read_task.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "configuration I/O is running",
            ));
        }
        log::info!("writing config to {:?}", self.config_path);
        if let Err(e) = self.unwatch() {
            log::warn!("could not suspend config watcher: {e}");
        }
        let saved = save_snapshot(
            self.config_path(),
            self.disk_baseline.as_deref(),
            self.config_toml.clone().unwrap_or_default(),
        );
        if let Ok(bytes) = &saved {
            self.disk_baseline = Some(bytes.clone());
        }
        // Always re-arm the watcher, including after write/sync/rename failure.
        let watched = self.watch().map_err(io::Error::other);
        if let Err(e) = &watched {
            log::warn!("could not restore config watcher: {e}");
        }
        saved.map(|_| ()).and(watched)
    }

    /// whether clipboard sharing is enabled (default: true)
    pub fn clipboard_enabled(&self) -> bool {
        self.config_toml
            .as_ref()
            .and_then(|c| c.enable_clipboard)
            .unwrap_or(true)
    }
}

fn resolve_config_target(path: &Path) -> io::Result<PathBuf> {
    match fs::canonicalize(path) {
        Ok(target) => Ok(target),
        Err(error) => {
            // A dangling final link still has a watchable target directory.
            // Preserve it and detect creation instead of writing defaults over it.
            if !fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
                return Err(error);
            }
            let target = fs::read_link(path)?;
            let target = if target.is_absolute() {
                target
            } else {
                path.parent().unwrap_or(Path::new(".")).join(target)
            };
            let parent = fs::canonicalize(target.parent().unwrap_or(Path::new(".")))?;
            let name = target.file_name().ok_or(error)?;
            Ok(parent.join(name))
        }
    }
}

fn read_snapshot(path: &Path, baseline: Option<&str>) -> ConfigReadResult {
    let target = resolve_config_target(path)?;
    let config = (|| {
        let bytes = fs::read_to_string(&target)?;
        if baseline == Some(bytes.as_str()) {
            return Ok(None);
        }
        let document = bytes
            .parse::<DocumentMut>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let config = toml_edit::de::from_document::<ConfigToml>(document)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        Ok(Some((bytes, config)))
    })();
    Ok(ReadSnapshot { target, config })
}

fn save_snapshot(path: &Path, baseline: Option<&str>, snapshot: ConfigToml) -> io::Result<String> {
    let baseline = baseline.ok_or_else(|| {
        io::Error::other("configuration was not readable; refusing to overwrite it")
    })?;
    let bytes = toml_edit::ser::to_string_pretty(&snapshot).map_err(io::Error::other)?;
    let verify = |target: &Path| -> io::Result<()> {
        if fs::read_to_string(target)? != baseline {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "configuration changed externally; reload before saving",
            ));
        }
        Ok(())
    };
    verify(path)?;
    atomic_write_config_checked(path, |file| file.write_all(bytes.as_bytes()), verify)?;
    Ok(bytes)
}

#[cfg(test)]
fn atomic_write_config(
    path: &Path,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    atomic_write_config_checked(path, write, |_| Ok(()))
}

fn atomic_write_config_checked(
    path: &Path,
    write: impl FnOnce(&mut File) -> io::Result<()>,
    before_commit: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    // Keep user-managed symlinks intact and replace their target instead.
    let was_symlink =
        fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink());
    let target = if was_symlink {
        fs::canonicalize(path)?
    } else {
        path.to_owned()
    };
    let parent = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    if let Ok(metadata) = fs::metadata(&target) {
        temporary
            .as_file()
            .set_permissions(metadata.permissions())?;
    }
    write(temporary.as_file_mut())?;
    temporary.as_file().sync_all()?;
    if was_symlink && fs::canonicalize(path)? != target {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "configuration symlink changed while preparing save",
        ));
    }
    before_commit(&target)?;
    temporary.persist(&target).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use input_event::scancode::Linux::*;

    #[test]
    fn failed_atomic_save_keeps_original_and_removes_temporary_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "enable_clipboard = false\n").unwrap();
        let error = atomic_write_config(&path, |file| {
            file.write_all(b"partial")?;
            Err(io::Error::other("injected write failure"))
        });
        assert!(error.is_err());
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "enable_clipboard = false\n"
        );
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
        atomic_write_config(&path, |file| file.write_all(b"enable_clipboard = true\n")).unwrap();
        assert_eq!(
            parse(&fs::read_to_string(&path).unwrap()).enable_clipboard,
            Some(true)
        );
    }

    #[cfg(unix)]
    #[test]
    fn atomic_save_preserves_symlink_and_target_permissions() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target.toml");
        let link = directory.path().join("config.toml");
        fs::write(&target, "old").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o640)).unwrap();
        symlink(&target, &link).unwrap();
        atomic_write_config(&link, |file| file.write_all(b"new")).unwrap();
        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_to_string(&target).unwrap(), "new");
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn watcher_is_restored_after_save_failure() {
        let directory = tempfile::tempdir().unwrap();
        let directory_path = directory.path().canonicalize().unwrap();
        let blocked = directory_path.join("blocked");
        fs::create_dir(&blocked).unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let watcher = RecommendedWatcher::new(
            move |event| {
                let _ = tx.blocking_send(event);
            },
            notify::Config::default(),
        )
        .unwrap();
        let mut config = Config {
            args: Args::parse_from(["lan-mouse"]),
            cert_path: directory.path().join("cert"),
            watch_target: blocked.clone(),
            watched_dirs: HashSet::new(),
            config_path: blocked,
            config_dir: directory_path.clone(),
            config_toml: Some(parse("enable_clipboard = false")),
            disk_baseline: None,
            pending_save: None,
            save_task: None,
            read_task: None,
            reload_conflict: false,
            watcher,
            watch_rx: rx,
            watch_overflow: Arc::new(WatchOverflow::default()),
        };
        config.watch().unwrap();
        assert!(config.write_back().is_err());
        config.config_path = directory_path.join("config.toml");
        atomic_write_config(config.config_path(), |file| {
            file.write_all(b"enable_clipboard = true")
        })
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), config.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(config.clipboard_enabled());
    }

    #[test]
    fn deleting_last_client_persists_empty_list_and_other_settings() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(
            &path,
            r#"
            enable_clipboard = false
            [authorized_fingerprints]
            trusted = "peer"
            [[clients]]
            hostname = "old-peer"
            activate_on_startup = true
        "#,
        )
        .unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        assert_eq!(config.clients().len(), 1);
        config.set_clients(Vec::new());
        config.write_back().unwrap();
        let reloaded =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        assert!(reloaded.clients().is_empty());
        assert!(!reloaded.clipboard_enabled());
        assert_eq!(
            reloaded.authorized_fingerprints().get("trusted"),
            Some(&"peer".into())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn watcher_error_is_reported_and_next_valid_edit_can_reload() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "enable_clipboard = false").unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        config.watch_rx = rx;
        tx.send(Err(notify::Error::generic("injected watcher error")))
            .await
            .unwrap();
        assert!(config.changed().await.is_err());
        assert!(!config.clipboard_enabled());
        fs::write(&path, "enable_clipboard = true").unwrap();
        tx.send(Ok(notify::Event::new(EventKind::Modify(ModifyKind::Data(
            notify::event::DataChange::Content,
        )))
        .add_path(path)))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), config.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(config.clipboard_enabled());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_save_coalesces_and_survives_cancelled_wait_without_blocking_runtime() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "enable_clipboard = false").unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        let snapshot = config.config_toml.clone().unwrap();
        let baseline = config.disk_baseline.clone();
        let worker_path = path.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        config.save_task = Some(tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            save_snapshot(&worker_path, baseline.as_deref(), snapshot)
        }));
        started_rx.await.unwrap();
        for index in 1..=1000 {
            config.set_mouse_sensitivity(index as f64 / 100.0);
            config.set_clipboard_enabled(true);
            config.queue_write_back();
        }
        assert_eq!(
            config
                .pending_save
                .as_ref()
                .unwrap()
                .input_post_processing
                .as_ref()
                .unwrap()
                .mouse_sensitivity,
            Some(10.0)
        );
        // A main-runtime timer fires while the actual disk worker is held.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), config.changed())
                .await
                .is_err()
        );
        assert!(config.save_task.is_some());
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), config.flush())
            .await
            .unwrap()
            .unwrap();
        assert!(config.save_task.is_none());
        assert!(config.pending_save.is_none());
        let persisted = parse(&fs::read_to_string(&path).unwrap());
        assert_eq!(persisted.enable_clipboard, Some(true));
        assert_eq!(
            persisted.input_post_processing.unwrap().mouse_sensitivity,
            Some(10.0)
        );
        // Watcher notifications from either saved snapshot cannot roll back
        // newer runtime state. The latest bytes equal the committed baseline.
        assert!(!config.read_from_disk().unwrap());
        assert!(config.clipboard_enabled());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn background_save_rejects_external_edit_then_can_save_after_reload() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "enable_clipboard = false").unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        config.set_clipboard_enabled(true);
        let external =
            "# editor change\nenable_clipboard = false\n[authorized_fingerprints]\nnew = 'peer'\n";
        fs::write(&path, external).unwrap();
        config.queue_write_back();
        config.set_mouse_sensitivity(1.5);
        config.queue_write_back();
        let error = config.flush().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read_to_string(&path).unwrap(), external);
        assert!(config.pending_save.is_none());
        assert!(config.read_from_disk().unwrap());
        config.set_clipboard_enabled(true);
        config.queue_write_back();
        config.flush().await.unwrap();
        let reloaded =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        assert!(reloaded.clipboard_enabled());
        assert_eq!(
            reloaded.authorized_fingerprints().get("new"),
            Some(&"peer".into())
        );
        assert_eq!(reloaded.mouse_sensitivity(), 1.0);
    }

    #[test]
    fn final_commit_guard_preserves_edit_made_while_preparing_save() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "original").unwrap();
        let result = atomic_write_config_checked(
            &path,
            |temporary| {
                temporary.write_all(b"queued save")?;
                fs::write(&path, "external edit")
            },
            |target| {
                if fs::read_to_string(target)? != "original" {
                    Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "changed externally",
                    ))
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read_to_string(&path).unwrap(), "external edit");
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn watcher_overflow_does_not_block_and_preserves_error_and_final_edit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "enable_clipboard = false").unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        config.watch_rx = rx;
        deliver_watch_event(
            &tx,
            &config.watch_overflow,
            Ok(notify::Event::new(EventKind::Any)),
        );
        for _ in 0..1000 {
            deliver_watch_event(
                &tx,
                &config.watch_overflow,
                Ok(notify::Event::new(EventKind::Any)),
            );
        }
        deliver_watch_event(
            &tx,
            &config.watch_overflow,
            Err(notify::Error::generic("overflow error")),
        );
        fs::write(&path, "enable_clipboard = true").unwrap();
        deliver_watch_event(
            &tx,
            &config.watch_overflow,
            Ok(notify::Event::new(EventKind::Modify(ModifyKind::Data(
                notify::event::DataChange::Content,
            )))
            .add_path(path)),
        );
        assert!(config.changed().await.is_err());
        assert!(
            tokio::time::timeout(Duration::from_secs(1), config.changed())
                .await
                .unwrap()
                .unwrap()
        );
        assert!(config.clipboard_enabled());
        drop(config);
        // Closed receiver is also nonblocking.
        deliver_watch_event(
            &tx,
            &WatchOverflow::default(),
            Ok(notify::Event::new(EventKind::Any)),
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_external_read_survives_cancelled_wait_and_reports_replaced_edits() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "enable_clipboard = false").unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        let external =
            "# external edit\nenable_clipboard = true\n[authorized_fingerprints]\nnew = 'peer'\n";
        fs::write(&path, external).unwrap();
        let worker_path = path.clone();
        let baseline = config.disk_baseline.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        config.read_task = Some(tokio::task::spawn_blocking(move || {
            let snapshot = read_snapshot(&worker_path, baseline.as_deref());
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            snapshot
        }));
        started_rx.await.unwrap();
        config.set_mouse_sensitivity(2.5);
        config.queue_write_back();
        assert!(config.save_task.is_none());
        assert!(config.pending_save.is_some());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), config.changed())
                .await
                .is_err()
        );
        assert!(config.read_task.is_some());
        assert_eq!(config.mouse_sensitivity(), 2.5);
        release_tx.send(()).unwrap();
        assert!(config.changed().await.unwrap());
        assert!(config.take_reload_conflict());
        assert!(!config.take_reload_conflict());
        assert_eq!(config.mouse_sensitivity(), 1.0);
        assert!(config.clipboard_enabled());
        assert!(config.pending_save.is_none());
        assert!(config.save_task.is_none());
        assert_eq!(fs::read_to_string(&path).unwrap(), external);
        // A subsequent explicit edit uses the newly loaded authorization.
        config.set_mouse_sensitivity(1.75);
        config.queue_write_back();
        config.flush().await.unwrap();
        assert_eq!(
            parse(&fs::read_to_string(&path).unwrap())
                .authorized_fingerprints
                .unwrap()
                .get("new"),
            Some(&"peer".into())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn own_file_read_keeps_newer_edits_and_flush_waits_for_their_save() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "enable_clipboard = false").unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        // Start the same production read path used for watcher notifications.
        config.start_read();
        config.set_clipboard_enabled(true);
        config.set_mouse_sensitivity(2.0);
        config.queue_write_back();
        assert!(config.save_task.is_none());
        config.flush().await.unwrap();
        assert!(!config.take_reload_conflict());
        assert!(config.clipboard_enabled());
        let disk = parse(&fs::read_to_string(&path).unwrap());
        assert_eq!(disk.enable_clipboard, Some(true));
        assert_eq!(
            disk.input_post_processing.unwrap().mouse_sensitivity,
            Some(2.0)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalid_async_reload_preserves_state_and_can_recover_after_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "enable_clipboard = false").unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        let initial_baseline = config.disk_baseline.clone();
        fs::write(&path, "enable_clipboard = [broken").unwrap();
        config.start_read();
        config.set_mouse_sensitivity(2.5);
        config.queue_write_back();
        let error = config.finish_read().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("pending settings were not saved")
        );
        assert!(!config.clipboard_enabled());
        assert_eq!(config.mouse_sensitivity(), 2.5);
        assert_eq!(config.disk_baseline, initial_baseline);
        assert!(config.pending_save.is_none());
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "enable_clipboard = [broken"
        );
        fs::write(&path, "enable_clipboard = true").unwrap();
        config.start_read();
        assert!(config.finish_read().await.unwrap());
        assert!(config.clipboard_enabled());
        assert_eq!(config.mouse_sensitivity(), 1.0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_flush_reports_external_read_displacing_pending_save() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "enable_clipboard = false").unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", path.to_str().unwrap()]).unwrap();
        let external = "# keep on exit\nenable_clipboard = true";
        fs::write(&path, external).unwrap();
        config.start_read();
        config.set_mouse_sensitivity(2.0);
        config.queue_write_back();
        assert!(
            config
                .flush()
                .await
                .unwrap_err()
                .to_string()
                .contains("replaced pending settings")
        );
        assert!(config.pending_save.is_none());
        assert!(config.save_task.is_none());
        assert_eq!(fs::read_to_string(&path).unwrap(), external);
    }

    async fn await_semantic_reload(config: &mut Config) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !config.changed().await.unwrap() {}
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn relative_single_filename_receives_absolute_watcher_events() {
        let cwd = env::current_dir().unwrap();
        let file = tempfile::NamedTempFile::new_in(&cwd).unwrap();
        fs::write(file.path(), "enable_clipboard = false").unwrap();
        let relative = file.path().strip_prefix(&cwd).unwrap();
        assert!(!relative.is_absolute());
        assert_eq!(relative.components().count(), 1);
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", relative.to_str().unwrap()]).unwrap();
        assert_eq!(config.config_path(), fs::canonicalize(file.path()).unwrap());
        fs::write(file.path(), "enable_clipboard = true").unwrap();
        await_semantic_reload(&mut config).await;
        assert!(config.clipboard_enabled());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn symlink_external_atomic_edit_and_retarget_to_invalid_file_stay_watched() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let first_dir = directory.path().join("first");
        let second_dir = directory.path().join("second");
        fs::create_dir(&first_dir).unwrap();
        fs::create_dir(&second_dir).unwrap();
        let first = first_dir.join("config.toml");
        let second = second_dir.join("config.toml");
        let link = directory.path().join("config.toml");
        fs::write(&first, "enable_clipboard = false").unwrap();
        fs::write(&second, "enable_clipboard = [broken").unwrap();
        symlink(&first, &link).unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", link.to_str().unwrap()]).unwrap();
        atomic_write_config(&first, |file| file.write_all(b"enable_clipboard = true")).unwrap();
        await_semantic_reload(&mut config).await;
        assert!(config.clipboard_enabled());
        let replacement = directory.path().join("replacement-link");
        symlink(&second, &replacement).unwrap();
        fs::rename(replacement, &link).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if config.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(config.clipboard_enabled());
        assert_eq!(config.watch_target, fs::canonicalize(&second).unwrap());
        assert!(
            config
                .watched_dirs
                .contains(&second_dir.canonicalize().unwrap())
        );
        assert!(
            !config
                .watched_dirs
                .contains(&first_dir.canonicalize().unwrap())
        );
        atomic_write_config(&second, |file| file.write_all(b"enable_clipboard = false")).unwrap();
        await_semantic_reload(&mut config).await;
        assert!(!config.clipboard_enabled());
        config.set_mouse_sensitivity(1.5);
        config.queue_write_back();
        config.flush().await.unwrap();
        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            parse(&fs::read_to_string(&second).unwrap())
                .input_post_processing
                .unwrap()
                .mouse_sensitivity,
            Some(1.5)
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn dangling_final_symlink_is_preserved_and_target_creation_reloads() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let target_dir = directory.path().join("target");
        fs::create_dir(&target_dir).unwrap();
        let target = target_dir.join("missing.toml");
        let link = directory.path().join("config.toml");
        symlink("target/missing.toml", &link).unwrap();
        let mut config =
            Config::new_with_args(["lan-mouse", "--config", link.to_str().unwrap()]).unwrap();
        assert!(!target.exists());
        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        fs::write(&target, "enable_clipboard = false").unwrap();
        await_semantic_reload(&mut config).await;
        assert!(!config.clipboard_enabled());
    }

    #[cfg(unix)]
    #[test]
    fn save_rejects_symlink_retarget_during_preparation_even_with_identical_contents() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("first.toml");
        let second = directory.path().join("second.toml");
        let link = directory.path().join("config.toml");
        fs::write(&first, "same bytes").unwrap();
        fs::write(&second, "same bytes").unwrap();
        symlink(&first, &link).unwrap();
        let result = atomic_write_config(&link, |file| {
            file.write_all(b"queued changes")?;
            fs::remove_file(&link)?;
            symlink(&second, &link)
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            fs::canonicalize(&link).unwrap(),
            fs::canonicalize(&second).unwrap()
        );
        assert_eq!(fs::read_to_string(&first).unwrap(), "same bytes");
        assert_eq!(fs::read_to_string(&second).unwrap(), "same bytes");
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 3);
    }

    fn parse(toml: &str) -> ConfigToml {
        toml::from_str(toml).expect("valid toml")
    }

    #[test]
    fn empty_config_parses_with_all_defaults() {
        let c = parse("");
        assert_eq!(c.enable_clipboard, None);
        assert_eq!(c.jail_bind, None);
        assert_eq!(c.enter_binds, None);
        assert_eq!(c.input_pre_processing, None);
        assert_eq!(c.input_post_processing, None);
    }

    #[test]
    fn parses_clipboard_toggle() {
        assert_eq!(
            parse("enable_clipboard = false").enable_clipboard,
            Some(false)
        );
        assert_eq!(
            parse("enable_clipboard = true").enable_clipboard,
            Some(true)
        );
    }

    #[test]
    fn parses_jail_bind() {
        let c = parse(r#"jail_bind = ["KeyScrollLock"]"#);
        assert_eq!(c.jail_bind, Some(vec![KeyScrollLock]));
    }

    #[test]
    fn parses_enter_binds_per_position() {
        let c = parse(
            r#"
            [enter_binds]
            right = ["KeyRightmeta", "KeyRightalt"]
            left = ["KeyLeftMeta"]
            "#,
        );
        let binds = c.enter_binds.expect("enter_binds");
        assert_eq!(binds[&Position::Right], vec![KeyRightmeta, KeyRightalt]);
        assert_eq!(binds[&Position::Left], vec![KeyLeftMeta]);
    }

    #[test]
    fn parses_input_pre_processing() {
        let c = parse(
            r#"
            [input_pre_processing]
            invert_scroll_vertical = true
            invert_scroll_horizontal = false

            [input_pre_processing.remap_keys]
            KeyLeftMeta = "KeyLeftCtrl"

            [[input_pre_processing.remap_chords]]
            modifier = "KeyLeftMeta"
            trigger = "KeyTab"
            to = "KeyLeftAlt"
            "#,
        );
        let pre = c.input_pre_processing.expect("pre_processing");
        assert_eq!(pre.invert_scroll_vertical, Some(true));
        assert_eq!(pre.invert_scroll_horizontal, Some(false));
        assert_eq!(pre.remap_keys.as_ref().unwrap()[&KeyLeftMeta], KeyLeftCtrl);
        let chord = &pre.remap_chords.as_ref().unwrap()[0];
        assert_eq!(chord.modifier, KeyLeftMeta);
        assert_eq!(chord.trigger, KeyTab);
        assert_eq!(chord.to, KeyLeftAlt);
    }

    #[test]
    fn parses_input_post_processing() {
        let c = parse(
            r#"
            [input_post_processing]
            invert_scroll = true
            mouse_sensitivity = 1.5
            "#,
        );
        let post = c.input_post_processing.expect("post_processing");
        assert_eq!(post.invert_scroll, Some(true));
        assert_eq!(post.mouse_sensitivity, Some(1.5));
    }

    #[test]
    fn parses_key_repeat_timing() {
        let c = parse(
            r#"
            key_repeat_delay = 400
            key_repeat_interval = 25
            "#,
        );
        assert_eq!(c.key_repeat_delay, Some(400));
        assert_eq!(c.key_repeat_interval, Some(25));
    }

    #[test]
    fn capitalized_scancode_spellings_are_accepted() {
        // #501: config keys must accept the spellings users actually type
        let c = parse(r#"release_bind = ["KeyA", "KeyS", "KeyD", "KeyF"]"#);
        assert_eq!(c.release_bind, Some(vec![KeyA, KeyS, KeyD, KeyF]));
    }
}
