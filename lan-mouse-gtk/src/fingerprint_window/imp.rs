use std::sync::OnceLock;

use adw::prelude::*;
use adw::subclass::prelude::*;
use glib::subclass::InitializingObject;
use gtk::{
    Button, CompositeTemplate, Label, Text,
    glib::{self, subclass::Signal},
    template_callbacks,
};

#[derive(CompositeTemplate, Default)]
#[template(resource = "/de/feschber/LanMouse/fingerprint_window.ui")]
pub struct FingerprintWindow {
    #[template_child]
    pub description: TemplateChild<Text>,
    #[template_child]
    pub fingerprint: TemplateChild<Text>,
    #[template_child]
    pub confirm_button: TemplateChild<Button>,
    #[template_child]
    pub validation_error: TemplateChild<Label>,
}

#[glib::object_subclass]
impl ObjectSubclass for FingerprintWindow {
    const NAME: &'static str = "FingerprintWindow";
    const ABSTRACT: bool = false;

    type Type = super::FingerprintWindow;
    type ParentType = adw::Window;

    fn class_init(klass: &mut Self::Class) {
        klass.bind_template();
        klass.bind_template_callbacks();
    }

    fn instance_init(obj: &InitializingObject<Self>) {
        obj.init_template();
    }
}

#[template_callbacks]
impl FingerprintWindow {
    #[template_callback]
    fn handle_fingerprint_changed(&self, _text: &Text) {
        self.validation_error.set_visible(false);
        self.fingerprint.remove_css_class("error");
    }

    #[template_callback]
    fn handle_confirm(&self, _button: Button) {
        let desc = self.description.text().as_str().trim().to_owned();
        let fp = match lan_mouse_ipc::normalize_fingerprint(self.fingerprint.text().as_str()) {
            Ok(fp) => fp,
            Err(error) => {
                self.validation_error.set_label(&error.to_string());
                self.validation_error.set_visible(true);
                self.fingerprint.add_css_class("error");
                self.fingerprint.grab_focus();
                return;
            }
        };
        self.fingerprint.set_text(&fp);
        self.handle_fingerprint_changed(&self.fingerprint);
        self.obj().emit_by_name("confirm-clicked", &[&desc, &fp])
    }
}

impl ObjectImpl for FingerprintWindow {
    fn signals() -> &'static [Signal] {
        static SIGNALS: OnceLock<Vec<Signal>> = OnceLock::new();
        SIGNALS.get_or_init(|| {
            vec![
                Signal::builder("confirm-clicked")
                    .param_types([String::static_type(), String::static_type()])
                    .build(),
            ]
        })
    }
}

impl WidgetImpl for FingerprintWindow {}
impl WindowImpl for FingerprintWindow {}
impl ApplicationWindowImpl for FingerprintWindow {}
impl AdwWindowImpl for FingerprintWindow {}
