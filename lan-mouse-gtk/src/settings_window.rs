mod imp;

use glib::Object;
use gtk::{gio, glib, prelude::*, subclass::prelude::ObjectSubclassIsExt};

glib::wrapper! {
    pub struct SettingsWindow(ObjectSubclass<imp::SettingsWindow>)
    @extends adw::PreferencesWindow, adw::Window, gtk::Window, gtk::Widget,
    @implements gio::ActionGroup, gio::ActionMap, gtk::Accessible, gtk::Buildable,
                gtk::ConstraintTarget, gtk::Native, gtk::Root, gtk::ShortcutManager;
}

impl SettingsWindow {
    pub(crate) fn new() -> Self {
        Object::builder().build()
    }

    pub(crate) fn set_daemon_available(&self, available: bool) {
        self.imp().clipboard_switch.set_sensitive(available);
        self.imp().invert_scroll_switch.set_sensitive(available);
        self.imp().sensitivity_spin.set_sensitive(available);
    }

    /// apply daemon-side settings to the widgets without triggering the
    /// change callbacks
    pub(crate) fn update_values(
        &self,
        clipboard_enabled: bool,
        invert_scroll: bool,
        mouse_sensitivity: f64,
    ) {
        let updating = self.imp().updating.clone();
        updating.set(true);
        self.imp().clipboard_switch.set_active(clipboard_enabled);
        self.imp().invert_scroll_switch.set_active(invert_scroll);
        self.imp().sensitivity_spin.set_value(mouse_sensitivity);
        updating.set(false);
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
