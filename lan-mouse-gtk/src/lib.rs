mod authorization_window;
mod client_object;
mod client_row;
mod fingerprint_window;
mod key_object;
mod key_row;
#[cfg(target_os = "linux")]
mod linux_status_item;
#[cfg(target_os = "macos")]
mod macos_privacy;
#[cfg(target_os = "macos")]
mod macos_status_item;
mod settings_window;
mod window;
#[cfg(windows)]
mod windows_status_item;

use std::{env, process, str, sync::OnceLock};

use gtk::CssProvider;
use window::Window;

use input_event::ClipboardContentKind;
use lan_mouse_ipc::FrontendEvent;
#[cfg(all(unix, feature = "wayland_window_identifier", not(target_os = "macos")))]
use lan_mouse_ipc::{FrontendRequest, WindowIdentifier};

/// Local build's commit hash, set once by [`run`] before the GTK
/// main loop starts. Read by per-row UI to compare against each
/// peer's [`lan_mouse_ipc::ClientState::peer_commit`] for the
/// soft-warn version-mismatch indicator.
pub(crate) static LOCAL_COMMIT: OnceLock<[u8; 8]> = OnceLock::new();

/// Whether the lan-mouse service is a child of this process, set once
/// by [`run_with_options`] before the GTK main loop starts. When `false`
/// the GUI is a pure client of an external daemon (e.g. a systemd
/// service): it then runs as a plain window — no tray icon, closing the
/// window quits the frontend and leaves the daemon untouched.
pub(crate) static OWNS_SERVICE: OnceLock<bool> = OnceLock::new();

/// Options controlling how the GTK frontend integrates with its service.
#[derive(Clone, Copy, Debug)]
pub struct RunOptions {
    owns_service: bool,
}

impl RunOptions {
    /// Marks whether the service is owned by the process running the frontend.
    ///
    /// On Linux, externally managed services use plain-window semantics rather
    /// than creating a tray icon. This option has no effect on other platforms.
    pub fn with_service_ownership(mut self, owns_service: bool) -> Self {
        self.owns_service = owns_service;
        self
    }
}

impl Default for RunOptions {
    fn default() -> Self {
        Self { owns_service: true }
    }
}

/// Convenience: returns the local commit as an 8-char ASCII string,
/// or a placeholder if unset (which would indicate a programmer
/// error since [`run`] always sets it).
pub(crate) fn local_commit_str() -> String {
    LOCAL_COMMIT
        .get()
        .and_then(|c| std::str::from_utf8(c).ok())
        .unwrap_or("????????")
        .to_string()
}

/// human-readable byte size for toast hints (e.g. `512 B`, `4 KB`)
fn human_bytes(bytes: usize) -> String {
    if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

use adw::Application;
use gtk::{IconTheme, gdk::Display, glib::clone, prelude::*};
use gtk::{gio, glib, prelude::ApplicationExt};

use self::client_object::ClientObject;
use self::key_object::KeyObject;

#[cfg(all(unix, feature = "wayland_window_identifier", not(target_os = "macos")))]
use gdk4_wayland::WaylandToplevel;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GtkError {
    #[error("gtk frontend exited with non zero exit code: {0}")]
    NonZeroExitCode(i32),
}

/// Runs the GTK frontend assuming it owns the service it connects to.
pub fn run(local_commit: [u8; 8]) -> Result<(), GtkError> {
    run_with_options(local_commit, RunOptions::default())
}

/// Runs the GTK frontend with explicit lifecycle integration options.
pub fn run_with_options(local_commit: [u8; 8], options: RunOptions) -> Result<(), GtkError> {
    log::debug!("running gtk frontend");
    LOCAL_COMMIT
        .set(local_commit)
        .expect("local_commit set once");
    OWNS_SERVICE
        .set(options.owns_service)
        .expect("owns_service set once");

    #[cfg(windows)]
    let ret = std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024) // https://gitlab.gnome.org/GNOME/gtk/-/commit/52dbb3f372b2c3ea339e879689c1de535ba2c2c3 -> caused crash on windows
        .name("gtk".into())
        .spawn(gtk_main)
        .unwrap()
        .join()
        .unwrap();
    #[cfg(not(windows))]
    let ret = gtk_main();

    match ret {
        glib::ExitCode::SUCCESS => Ok(()),
        e => Err(GtkError::NonZeroExitCode(e.value())),
    }
}

fn gtk_main() -> glib::ExitCode {
    #[cfg(target_os = "macos")]
    {
        configure_macos_bundle_environment();
        install_macos_gtk_log_filter();
    }

    gio::resources_register_include!("lan-mouse.gresource").expect("Failed to register resources.");

    let app = Application::builder()
        .application_id("de.feschber.LanMouse")
        .build();

    app.connect_startup(|app| {
        load_css();
        load_icons();
        setup_actions(app);
        setup_menu(app);
    });
    app.connect_activate(build_ui);

    let args: Vec<&'static str> = vec![];
    app.run_with_args(&args)
}

#[cfg(target_os = "macos")]
fn install_macos_gtk_log_filter() {
    glib::log_set_writer_func(|level, fields| {
        if level == glib::LogLevel::Warning && is_gtk_theme_parser_warning(fields) {
            return glib::LogWriterOutput::Handled;
        }

        glib::log_writer_default(level, fields)
    });
}

#[cfg(target_os = "macos")]
fn is_gtk_theme_parser_warning(fields: &[glib::LogField<'_>]) -> bool {
    let mut domain = None;
    let mut message = None;

    for field in fields {
        match field.key() {
            "GLIB_DOMAIN" => domain = field.value_str(),
            "MESSAGE" => message = field.value_str(),
            _ => {}
        }
    }

    domain == Some("Gtk")
        && message.is_some_and(|message| message.starts_with("Theme parser warning: gtk.css:"))
}

#[cfg(target_os = "macos")]
fn configure_macos_bundle_environment() {
    let Ok(exe) = env::current_exe() else {
        return;
    };
    let Some(contents) = exe
        .parent()
        .and_then(|dir| dir.parent())
        .map(std::path::Path::to_owned)
    else {
        return;
    };

    let share = contents.join("Resources").join("share");
    if !share.exists() {
        return;
    }

    let schemas = share.join("glib-2.0").join("schemas");
    if schemas.exists() {
        env::set_var("GSETTINGS_SCHEMA_DIR", schemas);
    }

    env::set_var("XDG_DATA_DIRS", &share);
    env::set_var(
        "GTK_DATA_PREFIX",
        contents.join("Resources").to_string_lossy().as_ref(),
    );
}

fn load_css() {
    let provider = CssProvider::default();
    provider.load_from_resource("de/feschber/LanMouse/style.css");
    gtk::style_context_add_provider_for_display(
        &Display::default().expect("Could not connect to a display"),
        &provider,
        gtk::STYLE_PROVIDER_PRIORITY_APPLICATION,
    );
}

fn load_icons() {
    let display = &Display::default().expect("Could not connect to a display.");
    let icon_theme = IconTheme::for_display(display);
    icon_theme.add_resource_path("/de/feschber/LanMouse/icons");
}

// Add application actions
fn setup_actions(app: &adw::Application) {
    // Quit action
    // This is important on macOS, where users expect a File->Quit action with a Cmd+Q shortcut.
    let quit_action = gio::SimpleAction::new("quit", None);
    quit_action.connect_activate({
        let app = app.clone();
        move |_, _| {
            app.quit();
        }
    });
    app.add_action(&quit_action);

    // Preferences action (hamburger menu in the main window)
    let preferences_action = gio::SimpleAction::new("preferences", None);
    preferences_action.connect_activate({
        let app = app.clone();
        move |_, _| {
            if let Some(window) = app
                .active_window()
                .and_then(|w| w.downcast::<Window>().ok())
            {
                window.open_settings();
            }
        }
    });
    app.add_action(&preferences_action);
}

// Set up a global menu
//
// Currently this is used only on macOS
fn setup_menu(app: &adw::Application) {
    let menu = gio::Menu::new();

    let file_menu = gio::Menu::new();
    file_menu.append(Some("Quit"), Some("app.quit"));
    menu.append_submenu(Some("_File"), &file_menu);

    app.set_menubar(Some(&menu))
}

fn build_ui(app: &Application) {
    // GApplication is single-instance: launching the binary again
    // activates the primary instance instead. Re-present the existing
    // window rather than building a second UI — this is also the "reopen"
    // path while the window is hidden in the tray.
    if let Some(window) = app.windows().first() {
        window.present();
        return;
    }

    log::debug!("connecting to lan-mouse-socket");
    let (mut frontend_rx, frontend_tx) = match lan_mouse_ipc::try_connect() {
        Ok(conn) => conn,
        Err(e) => {
            log::warn!("could not connect to daemon ({e}), spawning a new one");
            if let Err(spawn_err) = process::Command::new(
                env::current_exe().expect("could not determine executable path"),
            )
            .args(env::args().skip(1))
            .arg("daemon")
            .spawn()
            {
                log::error!("failed to spawn daemon: {spawn_err}");
                process::exit(1);
            }
            match lan_mouse_ipc::connect() {
                Ok(conn) => conn,
                Err(e) => {
                    log::error!("{e}");
                    process::exit(1);
                }
            }
        }
    };
    log::debug!("connected to lan-mouse-socket");

    let (sender, receiver) = async_channel::bounded(10);

    gio::spawn_blocking(move || {
        while let Some(e) = frontend_rx.next_event() {
            match e {
                Ok(e) => sender.send_blocking(e).unwrap(),
                Err(e) => {
                    log::error!("{e}");
                    break;
                }
            }
        }
    });

    let window = Window::new(app, frontend_tx);
    #[cfg(target_os = "macos")]
    {
        window.connect_close_request(|window| {
            window.set_visible(false);
            glib::Propagation::Stop
        });
        macos_status_item::setup(app, &window);
        // First-launch TCC prompts. No-op when already granted.
        macos_privacy::fire_initial_prompts();
        // Watch the Accessibility grant continuously for the lifetime
        // of the process. On a grant, swap the warning row into its
        // "relaunch required" state (the daemon subprocess already
        // bailed and can't recover without a restart). On a REVOKE,
        // quit immediately — an active CGEventTap at
        // HeadInsertEventTap can wedge system input if the process
        // lingers after losing AX, and forcing the process to exit is
        // the only bulletproof way to guarantee the kernel tears the
        // tap down.
        let window_weak = window.downgrade();
        let app_weak = app.downgrade();
        macos_privacy::watch_accessibility_state(move |change| match change {
            macos_privacy::AccessibilityChange::Granted => {
                if let Some(window) = window_weak.upgrade() {
                    window.present();
                    window.refresh_capture_emulation_status();
                }
            }
            macos_privacy::AccessibilityChange::Revoked => {
                log::warn!("Accessibility revoked — quitting to avoid wedging system input");
                if let Some(app) = app_weak.upgrade() {
                    app.quit();
                }
            }
        });
    }

    // export TopLevel handle and send it to the service so that it can put the InpuCapture / RemoteDesktop
    // windows on top of it using xdg-foreign.
    #[cfg(all(unix, feature = "wayland_window_identifier", not(target_os = "macos")))]
    window.connect_show(|window| {
        // needs the surface so we have to present first!
        if let Some(surface) = window.surface() {
            if surface.display().backend().is_wayland() {
                // let surface = surface.downcast::<WaylandSurface>();
                let toplevel = surface.downcast::<WaylandToplevel>().expect("xdg-toplevel");
                let window = window.clone();
                toplevel.export_handle(move |_toplevel, handle| {
                    if let Ok(handle) = handle {
                        let handle = handle.to_string();
                        window.request(FrontendRequest::WindowIdentifier(
                            WindowIdentifier::Wayland(handle),
                        ));
                    }
                });
            }
        }
    });

    // Mirror the macOS menu bar behavior: when a StatusNotifierItem
    // host is available, closing the window only hides it and the
    // tray menu is used to re-open or quit. Without a tray host the
    // default close-means-quit behavior is kept, since a hidden
    // window would otherwise be unreachable. When the service belongs
    // to an external daemon the GUI is a pure client and gets no tray
    // at all — quitting it must not suggest stopping the service.
    #[cfg(target_os = "linux")]
    let tray_active = {
        let owns_service = OWNS_SERVICE.get().copied().unwrap_or(true);
        if !owns_service {
            log::debug!("client of an external lan-mouse daemon, not creating a tray icon");
        }
        let tray_active = owns_service && linux_status_item::setup(app, &window);
        if tray_active {
            window.connect_close_request(|window| {
                window.set_visible(false);
                glib::Propagation::Stop
            });
        }
        tray_active
    };

    // Mirror macOS: the daemon is always a child of this process on
    // Windows (no external-daemon detection), so the tray is set up
    // unconditionally. Only if tray creation actually fails does the
    // window keep the default close-means-quit behavior, since a
    // hidden window would otherwise be unreachable.
    #[cfg(windows)]
    let tray_active = {
        let tray_active = windows_status_item::setup(app, &window);
        if tray_active {
            window.connect_close_request(|window| {
                window.set_visible(false);
                glib::Propagation::Stop
            });
        }
        tray_active
    };

    glib::spawn_future_local(clone!(
        #[weak]
        window,
        async move {
            loop {
                let notify = receiver.recv().await.unwrap_or_else(|_| process::exit(1));
                match notify {
                    FrontendEvent::Created(handle, client, state) => {
                        window.new_client(handle, client, state)
                    }
                    FrontendEvent::Deleted(client) => window.delete_client(client),
                    FrontendEvent::State(handle, config, state) => {
                        window.update_client_config(handle, config);
                        window.update_client_state(handle, state);
                    }
                    FrontendEvent::NoSuchClient(_) => {}
                    FrontendEvent::Error(e) => window.show_toast(e.as_str()),
                    FrontendEvent::Enumerate(clients) => window.update_client_list(clients),
                    FrontendEvent::PortChanged(port, msg) => window.update_port(port, msg),
                    FrontendEvent::CaptureStatus(s) => window.set_capture(s.into()),
                    FrontendEvent::EmulationStatus(s) => window.set_emulation(s.into()),
                    FrontendEvent::AuthorizedUpdated(keys) => window.set_authorized_keys(keys),
                    FrontendEvent::PublicKeyFingerprint(fp) => window.set_pk_fp(&fp),
                    FrontendEvent::ConnectionAttempt { fingerprint } => {
                        window.request_authorization(&fingerprint);
                    }
                    FrontendEvent::DeviceConnected {
                        fingerprint: _,
                        addr,
                    } => {
                        window.show_toast(format!("device connected: {addr}").as_str());
                    }
                    FrontendEvent::DeviceEntered {
                        fingerprint: _,
                        addr,
                        pos,
                    } => {
                        window.show_toast(format!("device entered: {addr} ({pos})").as_str());
                    }
                    FrontendEvent::IncomingDisconnected(addr) => {
                        window.show_toast(format!("{addr} disconnected").as_str());
                    }
                    FrontendEvent::Settings {
                        clipboard_enabled,
                        invert_scroll,
                        mouse_sensitivity,
                    } => {
                        window.update_settings(clipboard_enabled, invert_scroll, mouse_sensitivity);
                    }
                    FrontendEvent::ClipboardShared {
                        received,
                        kind,
                        bytes,
                    } => {
                        let kind = match kind {
                            ClipboardContentKind::Text => "text",
                            ClipboardContentKind::Image => "image",
                            ClipboardContentKind::Files => "files (saved to Downloads)",
                        };
                        let direction = if received { "received" } else { "shared" };
                        window.show_toast(
                            format!("clipboard {kind} {direction} ({})", human_bytes(bytes))
                                .as_str(),
                        );
                    }
                    FrontendEvent::ClipboardProgress {
                        incoming,
                        received,
                        total,
                    } => {
                        window.update_clipboard_progress(incoming, received, total);
                    }
                    FrontendEvent::ClipboardTooLarge { bytes, limit } => {
                        window.show_toast(
                            format!(
                                "clipboard too large to share: {} ({} limit)",
                                human_bytes(bytes),
                                human_bytes(limit)
                            )
                            .as_str(),
                        );
                    }
                }
            }
        }
    ));

    // Present on every launch unless
    // `LAN_MOUSE_HIDDEN=1` requests a quiet start into the tray.
    // Without a tray the window is always presented.
    #[cfg(windows)]
    if !tray_active || env::var_os("LAN_MOUSE_HIDDEN").is_none() {
        window.present();
    }

    #[cfg(all(not(windows), not(target_os = "linux"), not(target_os = "macos")))]
    window.present();

    // Like on macOS below: present on every launch unless
    // `LAN_MOUSE_HIDDEN=1` requests a quiet start into the tray.
    // Without a tray the window is always presented.
    #[cfg(target_os = "linux")]
    if !tray_active || env::var_os("LAN_MOUSE_HIDDEN").is_none() {
        window.present();
    }

    // On macOS, default to presenting the main window on every launch
    // so the user gets a visible confirmation that the app is running
    // — including the post-grant relaunch and normal Dock/Finder/`open`
    // launches. Opt out by setting `LAN_MOUSE_HIDDEN=1` in the
    // environment (useful for a LaunchAgent / login-item configuration
    // where the user wants the app to come up quietly into the menu
    // bar only, with no window on boot).
    #[cfg(target_os = "macos")]
    if env::var_os("LAN_MOUSE_HIDDEN").is_none() {
        window.present();
    }
}
