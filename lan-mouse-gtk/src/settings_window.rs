mod imp;

use std::rc::Rc;

use adw::prelude::*;
use glib::Object;
use gtk::{gio, glib, subclass::prelude::ObjectSubclassIsExt};

use crate::i18n::gettext;

glib::wrapper! {
    pub struct SettingsWindow(ObjectSubclass<imp::SettingsWindow>)
    @extends adw::PreferencesWindow, adw::Window, gtk::Window, gtk::Widget,
    @implements gio::ActionGroup, gio::ActionMap, gtk::Accessible, gtk::Buildable,
                gtk::ConstraintTarget, gtk::Native, gtk::Root, gtk::ShortcutManager;
}

/// a snapshot of the daemon settings applied to the widgets
#[derive(Clone, Default)]
pub struct SettingsValues {
    pub clipboard_enabled: bool,
    pub invert_scroll: bool,
    pub mouse_sensitivity: f64,
    pub download_dir: String,
    /// the daemon's listen port
    pub port: u16,
    /// configured UI language — `""` (i.e. index 0 in `LANGUAGE_CODES`)
    /// means "follow the system locale"
    pub language: String,
    /// key-repeat timing in ms
    pub key_repeat_delay: u64,
    pub key_repeat_interval: u64,
    /// "app" | "system" | "both" — where hints are shown
    pub notification_mode: String,
}

/// Language choices offered in the combo row, in dropdown order;
/// index 0 is the "system default" sentinel stored as an empty string.
pub(crate) const LANGUAGE_CODES: [&str; 3] = ["", "en", "zh_CN"];

/// notification-mode values in dropdown order — must match
/// `notification_model` in settings_window.ui
pub(crate) const NOTIFICATION_MODES: [&str; 3] = ["app", "system", "both"];

impl SettingsWindow {
    pub(crate) fn new() -> Self {
        let this: Self = Object::builder().build();
        this.imp()
            .version_row
            .set_subtitle(env!("CARGO_PKG_VERSION"));
        this
    }

    /// apply daemon-side settings to the widgets without triggering the
    /// change callbacks
    pub(crate) fn update_values(&self, values: &SettingsValues) {
        let updating = self.imp().updating.clone();
        updating.set(true);
        self.imp()
            .clipboard_switch
            .set_active(values.clipboard_enabled);
        self.imp()
            .invert_scroll_switch
            .set_active(values.invert_scroll);
        self.imp()
            .sensitivity_spin
            .set_value(values.mouse_sensitivity);
        self.imp()
            .download_dir_row
            .set_subtitle(&values.download_dir);
        self.imp().port_spin.set_value(values.port as f64);
        self.imp()
            .key_delay_spin
            .set_value(values.key_repeat_delay as f64);
        self.imp()
            .key_interval_spin
            .set_value(values.key_repeat_interval as f64);
        let index = LANGUAGE_CODES
            .iter()
            .position(|c| *c == values.language)
            .unwrap_or(0);
        self.imp().language_row.set_selected(index as u32);
        let index = NOTIFICATION_MODES
            .iter()
            .position(|m| *m == values.notification_mode)
            .unwrap_or(0);
        self.imp().notification_row.set_selected(index as u32);
        updating.set(false);
    }

    /// folder picker for the directory received clipboard files are
    /// written to; `f` is called with the chosen path
    pub(crate) fn connect_download_dir_activated(&self, f: impl Fn(String) + 'static) {
        let f = Rc::new(f);
        let this = self.clone();
        self.imp().download_dir_button.connect_clicked(move |_| {
            let dialog = gtk::FileChooserDialog::new(
                Some(&gettext("Choose download folder")),
                Some(&this),
                gtk::FileChooserAction::SelectFolder,
                &[
                    (&gettext("Cancel"), gtk::ResponseType::Cancel),
                    (&gettext("Select"), gtk::ResponseType::Accept),
                ],
            );
            if let Some(current) = this.imp().download_dir_row.subtitle() {
                if !current.is_empty() {
                    let _ = dialog.set_current_folder(Some(&gio::File::for_path(current.as_str())));
                }
            }
            let f = f.clone();
            dialog.connect_response(move |dialog, response| {
                if response == gtk::ResponseType::Accept {
                    if let Some(path) = dialog.current_folder().and_then(|folder| folder.path()) {
                        f(path.display().to_string());
                    }
                }
                dialog.close();
            });
            dialog.show();
        });
    }

    /// `f` is called with the chosen language code (`""` = system default)
    pub(crate) fn connect_language_changed(&self, f: impl Fn(&'static str) + 'static) {
        let updating = self.imp().updating.clone();
        self.imp().language_row.connect_selected_notify(move |row| {
            if updating.get() {
                return;
            }
            if let Some(code) = LANGUAGE_CODES.get(row.selected() as usize) {
                f(code);
            }
        });
    }

    /// `f` is called with the chosen mode ("app" | "system" | "both")
    pub(crate) fn connect_notification_mode_changed(&self, f: impl Fn(&'static str) + 'static) {
        let updating = self.imp().updating.clone();
        self.imp()
            .notification_row
            .connect_selected_notify(move |row| {
                if updating.get() {
                    return;
                }
                if let Some(mode) = NOTIFICATION_MODES.get(row.selected() as usize) {
                    f(mode);
                }
            });
    }

    /// `f` is called with (delay_ms, interval_ms) whenever either
    /// key-repeat row changes
    pub(crate) fn connect_key_repeat_changed(&self, f: impl Fn(u64, u64) + 'static) {
        let updating = self.imp().updating.clone();
        let f = Rc::new(f);
        let on_change = {
            let this = self.clone();
            move || {
                if updating.get() {
                    return;
                }
                let delay = this.imp().key_delay_spin.value() as u64;
                let interval = this.imp().key_interval_spin.value() as u64;
                f(delay, interval);
            }
        };
        let on_change = Rc::new(on_change);
        let cb = on_change.clone();
        self.imp()
            .key_delay_spin
            .connect_value_changed(move |_| cb());
        self.imp()
            .key_interval_spin
            .connect_value_changed(move |_| on_change());
    }

    /// `f` is called with the listen port as typed/spun by the user
    pub(crate) fn connect_port_changed(&self, f: impl Fn(u16) + 'static) {
        let updating = self.imp().updating.clone();
        self.imp().port_spin.connect_value_changed(move |spin| {
            if !updating.get() {
                f(spin.value() as u16);
            }
        });
    }

    pub(crate) fn connect_clipboard_toggled(&self, f: impl Fn(bool) + 'static) {
        let updating = self.imp().updating.clone();
        self.imp()
            .clipboard_switch
            .connect_active_notify(move |switch| {
                if !updating.get() {
                    f(switch.is_active());
                }
            });
    }

    pub(crate) fn connect_invert_scroll_toggled(&self, f: impl Fn(bool) + 'static) {
        let updating = self.imp().updating.clone();
        self.imp()
            .invert_scroll_switch
            .connect_active_notify(move |switch| {
                if !updating.get() {
                    f(switch.is_active());
                }
            });
    }

    pub(crate) fn connect_sensitivity_changed(&self, f: impl Fn(f64) + 'static) {
        let updating = self.imp().updating.clone();
        self.imp()
            .sensitivity_spin
            .connect_value_changed(move |spin| {
                if !updating.get() {
                    f(spin.value());
                }
            });
    }
}
