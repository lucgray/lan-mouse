use std::cell::Cell;
use std::rc::Rc;

use adw::subclass::prelude::*;
use adw::{ActionRow, ComboRow};
use glib::subclass::InitializingObject;
use gtk::{Button, CompositeTemplate, SpinButton, Switch, glib};

#[derive(CompositeTemplate, Default)]
#[template(resource = "/de/feschber/LanMouse/settings_window.ui")]
pub struct SettingsWindow {
    #[template_child]
    pub clipboard_switch: TemplateChild<Switch>,
    #[template_child]
    pub invert_scroll_switch: TemplateChild<Switch>,
    #[template_child]
    pub sensitivity_spin: TemplateChild<SpinButton>,
    #[template_child]
    pub download_dir_row: TemplateChild<ActionRow>,
    #[template_child]
    pub download_dir_button: TemplateChild<Button>,
    #[template_child]
    pub language_row: TemplateChild<ComboRow>,
    #[template_child]
    pub port_spin: TemplateChild<SpinButton>,
    #[template_child]
    pub key_delay_spin: TemplateChild<SpinButton>,
    #[template_child]
    pub key_interval_spin: TemplateChild<SpinButton>,
    #[template_child]
    pub version_row: TemplateChild<ActionRow>,
    /// suppresses change callbacks while daemon state is applied to the
    /// widgets, so a settings sync doesn't echo back as a request
    pub updating: Rc<Cell<bool>>,
}

#[glib::object_subclass]
impl ObjectSubclass for SettingsWindow {
    const NAME: &'static str = "SettingsWindow";
    const ABSTRACT: bool = false;

    type Type = super::SettingsWindow;
    type ParentType = adw::PreferencesWindow;

    fn class_init(klass: &mut Self::Class) {
        klass.bind_template();
    }

    fn instance_init(obj: &InitializingObject<Self>) {
        obj.init_template();
    }
}

impl ObjectImpl for SettingsWindow {}
impl WidgetImpl for SettingsWindow {}
impl WindowImpl for SettingsWindow {}
impl AdwWindowImpl for SettingsWindow {}
impl PreferencesWindowImpl for SettingsWindow {}
