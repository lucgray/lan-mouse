mod imp;

use std::rc::Rc;

use adw::prelude::*;
use glib::Object;
use gtk::{gio, glib, subclass::prelude::ObjectSubclassIsExt};

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

    /// apply daemon-side settings to the widgets without triggering the
    /// change callbacks
    pub(crate) fn update_values(
        &self,
        clipboard_enabled: bool,
        invert_scroll: bool,
        mouse_sensitivity: f64,
        download_dir: &str,
    ) {
        let updating = self.imp().updating.clone();
        updating.set(true);
        self.imp().clipboard_switch.set_active(clipboard_enabled);
        self.imp().invert_scroll_switch.set_active(invert_scroll);
        self.imp().sensitivity_spin.set_value(mouse_sensitivity);
        self.imp().download_dir_row.set_subtitle(download_dir);
        updating.set(false);
    }

    /// folder picker for the directory received clipboard files are
    /// written to; `f` is called with the chosen path
    pub(crate) fn connect_download_dir_activated(&self, f: impl Fn(String) + 'static) {
        let f = Rc::new(f);
        let this = self.clone();
        self.imp().download_dir_button.connect_clicked(move |_| {
            let dialog = gtk::FileChooserDialog::new(
                Some("Choose download folder"),
                Some(&this),
                gtk::FileChooserAction::SelectFolder,
                &[
                    ("Cancel", gtk::ResponseType::Cancel),
                    ("Select", gtk::ResponseType::Accept),
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
