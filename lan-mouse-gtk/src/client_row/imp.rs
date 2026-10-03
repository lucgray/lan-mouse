use super::draft::Draft;
use std::{
    cell::{Cell, RefCell},
    time::Duration,
};

use adw::subclass::prelude::*;
use adw::{ActionRow, ComboRow, prelude::*};
use glib::{Binding, subclass::InitializingObject};
use gtk::glib::subclass::Signal;
use gtk::glib::{SignalHandlerId, clone};
use gtk::{Button, CompositeTemplate, Entry, Switch, glib};
use lan_mouse_ipc::{DEFAULT_PORT, Position};
use std::sync::OnceLock;

use crate::client_object::ClientObject;

#[derive(CompositeTemplate, Default)]
#[template(resource = "/de/feschber/LanMouse/client_row.ui")]
pub struct ClientRow {
    #[template_child]
    pub enable_switch: TemplateChild<gtk::Switch>,
    #[template_child]
    pub dns_button: TemplateChild<gtk::Button>,
    #[template_child]
    pub hostname: TemplateChild<gtk::Entry>,
    #[template_child]
    pub port: TemplateChild<gtk::Entry>,
    #[template_child]
    pub position: TemplateChild<ComboRow>,
    #[template_child]
    pub delete_row: TemplateChild<ActionRow>,
    #[template_child]
    pub delete_button: TemplateChild<gtk::Button>,
    #[template_child]
    pub dns_loading_indicator: TemplateChild<gtk::Spinner>,
    pub bindings: RefCell<Vec<Binding>>,
    hostname_draft: RefCell<Draft<String>>,
    hostname_timeout: RefCell<Option<glib::SourceId>>,
    port_draft: RefCell<Draft<u16>>,
    port_invalid: Cell<bool>,
    port_timeout: RefCell<Option<glib::SourceId>>,
    hostname_change_handler: RefCell<Option<SignalHandlerId>>,
    port_change_handler: RefCell<Option<SignalHandlerId>>,
    position_change_handler: RefCell<Option<SignalHandlerId>>,
    set_state_handler: RefCell<Option<SignalHandlerId>>,
    pub client_object: RefCell<Option<ClientObject>>,
}

#[glib::object_subclass]
impl ObjectSubclass for ClientRow {
    // `NAME` needs to match `class` attribute of template
    const NAME: &'static str = "ClientRow";
    const ABSTRACT: bool = false;

    type Type = super::ClientRow;
    type ParentType = adw::ExpanderRow;

    fn class_init(klass: &mut Self::Class) {
        klass.bind_template();
        klass.bind_template_callbacks();
    }

    fn instance_init(obj: &InitializingObject<Self>) {
        obj.init_template();
    }
}

impl ObjectImpl for ClientRow {
    fn constructed(&self) {
        self.parent_constructed();
        self.delete_button.connect_clicked(clone!(
            #[weak(rename_to = row)]
            self,
            move |button| {
                row.handle_client_delete(button);
            }
        ));
        let handler = self.hostname.connect_changed(clone!(
            #[weak(rename_to = row)]
            self,
            move |entry| {
                row.handle_hostname_changed(entry);
            }
        ));
        self.hostname_change_handler.replace(Some(handler));
        let handler = self.port.connect_changed(clone!(
            #[weak(rename_to = row)]
            self,
            move |entry| {
                row.handle_port_changed(entry);
            }
        ));
        self.port_change_handler.replace(Some(handler));
        self.hostname.connect_activate(clone!(
            #[weak(rename_to = row)]
            self,
            move |_| row.flush_hostname()
        ));
        self.port.connect_activate(clone!(
            #[weak(rename_to = row)]
            self,
            move |_| row.flush_port()
        ));
        let hostname_focus = gtk::EventControllerFocus::new();
        hostname_focus.connect_leave(clone!(
            #[weak(rename_to = row)]
            self,
            move |_| row.flush_hostname()
        ));
        self.hostname.add_controller(hostname_focus);
        let port_focus = gtk::EventControllerFocus::new();
        port_focus.connect_leave(clone!(
            #[weak(rename_to = row)]
            self,
            move |_| row.flush_port()
        ));
        self.port.add_controller(port_focus);
        let handler = self.position.connect_selected_notify(clone!(
            #[weak(rename_to = row)]
            self,
            move |position| {
                row.handle_position_changed(position);
            }
        ));
        self.position_change_handler.replace(Some(handler));
        let handler = self.enable_switch.connect_state_set(clone!(
            #[weak(rename_to = row)]
            self,
            #[upgrade_or]
            glib::Propagation::Proceed,
            move |switch, state| {
                row.handle_activate_switch(state, switch);
                glib::Propagation::Proceed
            }
        ));
        self.set_state_handler.replace(Some(handler));
    }

    fn dispose(&self) {
        self.cancel_pending_edits();
    }

    fn signals() -> &'static [glib::subclass::Signal] {
        static SIGNALS: OnceLock<Vec<Signal>> = OnceLock::new();
        SIGNALS.get_or_init(|| {
            vec![
                Signal::builder("request-activate")
                    .param_types([bool::static_type()])
                    .build(),
                Signal::builder("request-delete").build(),
                Signal::builder("request-dns").build(),
                Signal::builder("request-hostname-change")
                    .param_types([String::static_type()])
                    .build(),
                Signal::builder("request-port-change")
                    .param_types([u32::static_type()])
                    .build(),
                Signal::builder("request-position-change")
                    .param_types([u32::static_type()])
                    .build(),
            ]
        })
    }
}

#[gtk::template_callbacks]
impl ClientRow {
    #[template_callback]
    fn handle_activate_switch(&self, state: bool, _switch: &Switch) -> bool {
        self.obj().emit_by_name::<()>("request-activate", &[&state]);
        true // dont run default handler
    }

    #[template_callback]
    fn handle_request_dns(&self, _: &Button) {
        self.flush_hostname();
        self.flush_port();
        self.obj().emit_by_name::<()>("request-dns", &[]);
    }

    #[template_callback]
    fn handle_client_delete(&self, _button: &Button) {
        self.cancel_pending_edits();
        self.obj().emit_by_name::<()>("request-delete", &[]);
    }

    fn handle_port_changed(&self, port_entry: &Entry) {
        if let Some(timeout) = self.port_timeout.borrow_mut().take() {
            timeout.remove();
        }
        let text = port_entry.text();
        let port = if text.is_empty() {
            Ok(DEFAULT_PORT)
        } else {
            text.parse::<u16>()
        };
        match port {
            Ok(port) => {
                self.port_invalid.set(false);
                port_entry.remove_css_class("error");
                self.port_draft.borrow_mut().stage(port);
                let timeout = glib::timeout_add_local_once(
                    Duration::from_millis(400),
                    clone!(
                        #[weak(rename_to = row)]
                        self,
                        move || {
                            row.port_timeout.borrow_mut().take();
                            row.flush_port();
                        }
                    ),
                );
                self.port_timeout.replace(Some(timeout));
            }
            Err(_) => {
                self.port_invalid.set(true);
                self.port_draft.borrow_mut().clear();
                port_entry.add_css_class("error");
            }
        }
    }

    fn handle_hostname_changed(&self, hostname_entry: &Entry) {
        if let Some(timeout) = self.hostname_timeout.borrow_mut().take() {
            timeout.remove();
        }
        self.hostname_draft
            .borrow_mut()
            .stage(hostname_entry.text().to_string());
        let timeout = glib::timeout_add_local_once(
            Duration::from_millis(400),
            clone!(
                #[weak(rename_to = row)]
                self,
                move || {
                    row.hostname_timeout.borrow_mut().take();
                    row.flush_hostname();
                }
            ),
        );
        self.hostname_timeout.replace(Some(timeout));
    }

    pub(super) fn flush_hostname(&self) {
        if let Some(timeout) = self.hostname_timeout.borrow_mut().take() {
            timeout.remove();
        }
        let confirmed = self
            .client_object
            .borrow()
            .as_ref()
            .map(|client| client.get_data().hostname.unwrap_or_default());
        if let Some(confirmed) = confirmed {
            let value = self.hostname_draft.borrow_mut().submit(&confirmed);
            if let Some(value) = value {
                self.obj()
                    .emit_by_name::<()>("request-hostname-change", &[&value]);
            }
        }
    }

    pub(super) fn flush_port(&self) {
        if let Some(timeout) = self.port_timeout.borrow_mut().take() {
            timeout.remove();
        }
        let confirmed = self
            .client_object
            .borrow()
            .as_ref()
            .map(|client| client.get_data().port as u16);
        if let Some(confirmed) = confirmed {
            let value = self.port_draft.borrow_mut().submit(&confirmed);
            if let Some(value) = value {
                self.obj()
                    .emit_by_name::<()>("request-port-change", &[&(value as u32)]);
            }
        }
    }

    pub(super) fn cancel_pending_edits(&self) {
        if let Some(timeout) = self.hostname_timeout.borrow_mut().take() {
            timeout.remove();
        }
        if let Some(timeout) = self.port_timeout.borrow_mut().take() {
            timeout.remove();
        }
        self.hostname_draft.borrow_mut().clear();
        self.port_draft.borrow_mut().clear();
        self.port_invalid.set(false);
    }

    fn handle_position_changed(&self, position: &ComboRow) {
        self.obj()
            .emit_by_name("request-position-change", &[&position.selected()])
    }

    pub(super) fn set_hostname(&self, hostname: Option<String>) {
        if !self
            .hostname_draft
            .borrow_mut()
            .accept(&hostname.clone().unwrap_or_default())
        {
            return;
        }
        let position = self.hostname.position();
        let handler = self.hostname_change_handler.borrow();
        let handler = handler.as_ref().expect("signal handler");
        self.hostname.block_signal(handler);
        self.client_object
            .borrow_mut()
            .as_mut()
            .expect("client object")
            .set_property("hostname", hostname);
        self.hostname.unblock_signal(handler);
        self.hostname.set_position(position);
    }

    pub(super) fn set_port(&self, port: u16) {
        if self.port_invalid.get() || !self.port_draft.borrow_mut().accept(&port) {
            return;
        }
        let position = self.port.position();
        let handler = self.port_change_handler.borrow();
        let handler = handler.as_ref().expect("signal handler");
        self.port.block_signal(handler);
        self.client_object
            .borrow_mut()
            .as_mut()
            .expect("client object")
            .set_port(port as u32);
        self.port.unblock_signal(handler);
        self.port.set_position(position);
    }

    pub(super) fn set_pos(&self, pos: Position) {
        let handler = self.position_change_handler.borrow();
        let handler = handler.as_ref().expect("signal handler");
        self.position.block_signal(handler);
        self.client_object
            .borrow_mut()
            .as_mut()
            .expect("client object")
            .set_position(pos.to_string());
        self.position.unblock_signal(handler);
    }

    pub(super) fn set_active(&self, active: bool) {
        let handler = self.set_state_handler.borrow();
        let handler = handler.as_ref().expect("signal handler");
        self.enable_switch.block_signal(handler);
        self.client_object
            .borrow_mut()
            .as_mut()
            .expect("client object")
            .set_active(active);
        self.enable_switch.unblock_signal(handler);
    }

    pub(super) fn set_dns_state(&self, resolved: bool) {
        if resolved {
            self.dns_button.set_css_classes(&["success"])
        } else {
            self.dns_button.set_css_classes(&["warning"])
        }
    }
}

impl WidgetImpl for ClientRow {}
impl BoxImpl for ClientRow {}
impl ListBoxRowImpl for ClientRow {}
impl PreferencesRowImpl for ClientRow {}
impl ExpanderRowImpl for ClientRow {}
