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
    /// directory files received via the clipboard are written to
    /// (default: the system downloads directory)
    download_dir: Option<PathBuf>,
    /// UI language override, e.g. "zh_CN" — unset means "follow the
    /// system locale". Consumed by the GTK frontend (gettext `LANGUAGE`).
    language: Option<String>,
    /// where user-facing hints are shown: "app" (in-window banner,
    /// default), "system" (OS notifications) or "both"
    notification_mode: Option<String>,
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
    /// only send input to this client — input it sends us is ignored
    send_only: Option<bool>,
    /// only receive input from this client — never activated and
    /// never connected to
    receive_only: Option<bool>,
}

impl ConfigToml {
    fn new(path: &Path) -> Result<ConfigToml, ConfigError> {
        let config = fs::read_to_string(path)?;
        Ok(toml::from_str::<_>(&config)?)
    }
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
    /// the (optional) toml config and it's path
    config_toml: Option<ConfigToml>,
    // filesystem watcher
    watcher: notify::RecommendedWatcher,
    // channel for filesystem events
    watch_rx: tokio::sync::mpsc::Receiver<Result<notify::Event, notify::Error>>,
    /// set once the watcher channel dies or reports an error —
    /// `changed()` pends forever afterwards instead of spinning
    watch_failed: bool,
}

pub struct ConfigClient {
    pub ips: HashSet<IpAddr>,
    pub hostname: Option<String>,
    pub port: u16,
    pub pos: Position,
    pub active: bool,
    pub enter_hook: Option<String>,
    pub leave_hook: Option<String>,
    pub send_only: bool,
    pub receive_only: bool,
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
            send_only: toml.send_only.unwrap_or(false),
            receive_only: toml.receive_only.unwrap_or(false),
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
            send_only: client.send_only.then_some(true),
            receive_only: client.receive_only.then_some(true),
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
        let config_dir = config_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();

        // Ensure the config directory exists and write a default config file
        // if none is present. Runs on every Config::new(), regardless of which
        // entry path (GUI main, spawned daemon, CLI, test commands) we're on,
        // so a fresh Mac never hits "No such file or directory" on config.toml
        // and notify::Watcher (which requires the dir to exist on macOS
        // FSEvents and some Linux backends) has a concrete path to watch.
        fs::create_dir_all(&config_dir)?;
        if !config_path.exists() {
            let default_toml = toml::to_string_pretty(&ConfigToml::default())
                .expect("default ConfigToml serialization cannot fail");
            fs::write(&config_path, default_toml)?;
        }

        let config_toml = match ConfigToml::new(&config_path) {
            Err(e) => {
                log::warn!("{config_path:?}: {e}");
                log::warn!("Continuing without config file ...");
                None
            }
            Ok(c) => Some(c),
        };

        // --cert-path <file> overrules default location
        let cert_path = args
            .cert_path
            .clone()
            .or(config_toml.as_ref().and_then(|c| c.cert_path.clone()))
            .unwrap_or(default_path()?.join(CERT_FILE_NAME));

        let (tx, watch_rx) = tokio::sync::mpsc::channel(16);
        let watcher = RecommendedWatcher::new(
            move |res| {
                if tx.blocking_send(res).is_err() {
                    log::warn!("config watch event dropped: channel full or closed");
                }
            },
            notify::Config::default(),
        )?;
        let mut config = Config {
            args,
            cert_path,
            config_path,
            config_dir,
            config_toml,
            watcher,
            watch_rx,
            watch_failed: false,
        };
        config.watch()?;
        Ok(config)
    }

    fn watch(&mut self) -> Result<(), notify::Error> {
        self.watcher
            .watch(&self.config_dir, notify::RecursiveMode::NonRecursive)?;
        Ok(())
    }

    fn unwatch(&mut self) -> Result<(), notify::Error> {
        self.watcher.unwatch(&self.config_dir)?;
        Ok(())
    }

    /// Resolves once a watched change to the config file has been
    /// reloaded, or with an error when the watcher is gone or reports
    /// errors. After a watcher failure this pends forever — a dead
    /// watcher must not spin or crash the service loop.
    pub async fn changed(&mut self) -> Result<(), notify::Error> {
        if self.watch_failed {
            return std::future::pending().await;
        }
        loop {
            let Some(event) = self.watch_rx.recv().await else {
                self.watch_failed = true;
                return Err(notify::Error::generic("config watcher channel closed"));
            };
            let Ok(event) = event else {
                self.watch_failed = true;
                return Err(event.unwrap_err());
            };
            if event.paths.contains(&self.config_path)
                && matches!(
                    event.kind,
                    EventKind::Create(_)
                        | EventKind::Modify(ModifyKind::Data(_))
                        | EventKind::Remove(_)
                )
                && self.read_from_disk()?
            {
                return Ok(());
            }
        }
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
        if clients.is_empty() {
            return;
        }
        if self.config_toml.is_none() {
            self.config_toml = Some(Default::default());
        }
        self.config_toml.as_mut().expect("config").clients =
            Some(clients.into_iter().map(|c| c.into()).collect::<Vec<_>>());
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

    /// persist the clipboard file download directory (`None` = default)
    pub fn set_download_dir(&mut self, dir: Option<PathBuf>) {
        self.toml_mut().download_dir = dir;
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

    pub fn read_from_disk(&mut self) -> Result<bool, io::Error> {
        log::info!("reading config from {:?}", self.config_path);

        let current_config = fs::read_to_string(&self.config_path)?;
        let current_config = match current_config.parse::<DocumentMut>() {
            Ok(c) => c,
            Err(e) => {
                log::warn!("{:?} {e}", self.config_path());
                return Ok(false);
            }
        };
        let mut changed = false;
        match toml_edit::de::from_document::<ConfigToml>(current_config) {
            Ok(current_config) => {
                changed = self
                    .config_toml
                    .as_ref()
                    .is_none_or(|c| c != &current_config);
                self.config_toml.replace(current_config);
            }
            Err(e) => log::warn!("{:?} {e}", self.config_path()),
        };
        if changed {
            log::info!("config changed");
        } else {
            log::info!("config unchanged");
        }
        Ok(changed)
    }

    pub fn write_back(&mut self) -> Result<(), io::Error> {
        log::info!("writing config to {:?}", self.config_path);
        /* the new config */
        let new_config = self.config_toml.clone().unwrap_or_default();
        let new_config = toml_edit::ser::to_string_pretty(&new_config).expect("config");

        /*
         * TODO merge with current config file to preserve comments
         * => eventually we might want to split this up into clients configured
         * via the config file and clients managed through the GUI / frontend.
         * The latter should be saved to $XDG_DATA_HOME instead of $XDG_CONFIG_HOME,
         * and clients configured through .config could be made permanent.
         * For now we just override the config file.
         */

        if let Err(e) = self.unwatch() {
            log::warn!("failed to unwatch config directory: {e}");
        }
        /* write new config to file */
        if let Some(p) = self.config_path().parent() {
            fs::create_dir_all(p)?;
        }
        {
            let mut f = File::create(self.config_path())?;
            f.write_all(new_config.as_bytes())?;
            f.sync_all()?;
        }

        if let Err(e) = self.watch() {
            // losing the watcher silently disables config hot-reload
            self.watch_failed = true;
            log::error!("failed to re-watch config directory: {e}");
        }

        Ok(())
    }

    /// whether clipboard sharing is enabled (default: true)
    pub fn clipboard_enabled(&self) -> bool {
        self.config_toml
            .as_ref()
            .and_then(|c| c.enable_clipboard)
            .unwrap_or(true)
    }

    /// configured directory for received clipboard files
    /// (`None` = system downloads directory)
    pub fn download_dir(&self) -> Option<PathBuf> {
        self.config_toml
            .as_ref()
            .and_then(|c| c.download_dir.clone())
    }

    /// UI language override ("en", "zh_CN", ...) — None = system locale.
    pub fn language(&self) -> Option<String> {
        self.config_toml.as_ref().and_then(|c| c.language.clone())
    }

    pub fn set_language(&mut self, language: Option<String>) {
        self.toml_mut().language = language;
    }

    /// "app" | "system" | "both" — None keeps the built-in default.
    pub fn notification_mode(&self) -> Option<String> {
        self.config_toml
            .as_ref()
            .and_then(|c| c.notification_mode.clone())
    }

    pub fn set_notification_mode(&mut self, mode: Option<String>) {
        self.toml_mut().notification_mode = mode;
    }

    /// Key-repeat timing in milliseconds — None restores backend defaults.
    pub fn set_key_repeat(&mut self, delay: Option<u64>, interval: Option<u64>) {
        self.toml_mut().key_repeat_delay = delay;
        self.toml_mut().key_repeat_interval = interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use input_event::scancode::Linux::*;

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
    fn parses_download_dir() {
        let c = parse("download_dir = \"/tmp/lm-downloads\"");
        assert_eq!(c.download_dir, Some(PathBuf::from("/tmp/lm-downloads")));
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

    #[test]
    fn client_direction_flags_parse_and_survive_save() {
        let c = parse(
            r#"
            [[clients]]
            position = "right"
            receive_only = true

            [[clients]]
            position = "left"
            send_only = true

            [[clients]]
            position = "top"
            "#,
        );
        let clients = c.clients.expect("clients");
        assert_eq!(clients[0].receive_only, Some(true));
        assert_eq!(clients[0].send_only, None);
        assert_eq!(clients[1].send_only, Some(true));
        assert_eq!(clients[2].send_only, None);
        assert_eq!(clients[2].receive_only, None);

        // ConfigClient round-trips the flags back to toml so a
        // save_config() rewrite does not silently drop them
        let cfg = ConfigClient::from(clients[0].clone());
        assert!(cfg.receive_only && !cfg.send_only);
        let back = TomlClient::from(cfg);
        assert_eq!(back.receive_only, Some(true));
        assert_eq!(back.send_only, None);

        // unset flags stay absent in the written toml — a `false`
        // serialization would flip a send_only into a documented default
        let cfg = ConfigClient::from(clients[2].clone());
        let back = TomlClient::from(cfg);
        assert_eq!(back.send_only, None);
        assert_eq!(back.receive_only, None);
    }
}
