mod authorization;
mod imp;

use std::{collections::HashMap, time::Instant};

use adw::prelude::*;
use adw::subclass::prelude::*;
use glib::{Object, clone};
use gtk::{
    NoSelection, gio,
    glib::{self, closure_local},
};

use lan_mouse_ipc::{
    ClientConfig, ClientHandle, ClientState, DEFAULT_PORT, FrontendRequest, Position,
};

use crate::{
    authorization_window::AuthorizationWindow, fingerprint_window::FingerprintWindow,
    key_object::KeyObject, key_row::KeyRow, settings_window::SettingsWindow,
};

use super::{client_object::ClientObject, client_row::ClientRow};

#[cfg(target_os = "macos")]
fn set_button_content_label(button: &gtk::Button, label: &str) {
    // The Reenable/Grant/Relaunch button wraps its icon+label in an
    // AdwButtonContent (see window.ui). Walk into it and swap the label
    // rather than GtkButton::set_label, which would replace the content
    // widget and drop the icon.
    if let Some(content) = button.child().and_downcast::<adw::ButtonContent>() {
        content.set_label(label);
    }
}

glib::wrapper! {
    pub struct Window(ObjectSubclass<imp::Window>)
        @extends adw::ApplicationWindow, gtk::Window, gtk::Widget,
        @implements gio::ActionGroup, gio::ActionMap, gtk::Accessible, gtk::Buildable,
                    gtk::ConstraintTarget, gtk::Native, gtk::Root, gtk::ShortcutManager;
}

impl Window {
    pub(super) fn new(app: &adw::Application, conn: crate::daemon_client::DaemonClient) -> Self {
        let window: Self = Object::builder().property("application", app).build();
        window.imp().daemon_client.borrow_mut().replace(conn);
        window.connect_close_request(|window| {
            window.clear_authorization_dialogs();
            for index in 0..window.clients().n_items() {
                if let Some(row) = window.row_by_idx(index as i32) {
                    row.flush_pending_edits();
                }
            }
            glib::Propagation::Proceed
        });
        window
    }

    fn clients(&self) -> gio::ListStore {
        self.imp()
            .clients
            .borrow()
            .clone()
            .expect("Could not get clients")
    }

    fn authorized(&self) -> gio::ListStore {
        self.imp()
            .authorized
            .borrow()
            .clone()
            .expect("Could not get authorized")
    }

    fn client_by_idx(&self, idx: u32) -> Option<ClientObject> {
        self.clients().item(idx).map(|o| o.downcast().unwrap())
    }

    fn authorized_by_idx(&self, idx: u32) -> Option<KeyObject> {
        self.authorized().item(idx).map(|o| o.downcast().unwrap())
    }

    fn row_by_idx(&self, idx: i32) -> Option<ClientRow> {
        self.imp()
            .client_list
            .get()
            .row_at_index(idx)
            .map(|o| o.downcast().expect("expected ClientRow"))
    }

    fn setup_authorized(&self) {
        let store = gio::ListStore::new::<KeyObject>();
        self.imp().authorized.replace(Some(store));
        let selection_model = NoSelection::new(Some(self.authorized()));
        self.imp().authorized_list.bind_model(
            Some(&selection_model),
            clone!(
                #[weak(rename_to = window)]
                self,
                #[upgrade_or_panic]
                move |obj| {
                    let key_obj = obj.downcast_ref().expect("object of type `KeyObject`");
                    let row = window.create_key_row(key_obj);
                    row.connect_closure(
                        "request-delete",
                        false,
                        closure_local!(
                            #[strong]
                            window,
                            move |row: KeyRow| {
                                if let Some(key_obj) = window.authorized_by_idx(row.index() as u32)
                                {
                                    window.request_fingerprint_remove(key_obj.get_fingerprint());
                                }
                            }
                        ),
                    );
                    row.upcast()
                }
            ),
        )
    }

    fn setup_clients(&self) {
        let model = gio::ListStore::new::<ClientObject>();
        self.imp().clients.replace(Some(model));

        let selection_model = NoSelection::new(Some(self.clients()));
        self.imp().client_list.bind_model(
            Some(&selection_model),
            clone!(
                #[weak(rename_to = window)]
                self,
                #[upgrade_or_panic]
                move |obj| {
                    let client_object = obj
                        .downcast_ref()
                        .expect("Expected object of type `ClientObject`.");
                    let row = window.create_client_row(client_object);
                    row.connect_closure(
                        "request-hostname-change",
                        false,
                        closure_local!(
                            #[strong]
                            window,
                            move |row: ClientRow, hostname: String| {
                                log::debug!("request-hostname-change");
                                if let Some(client) = window.client_by_idx(row.index() as u32) {
                                    let hostname = Some(hostname).filter(|s| !s.is_empty());
                                    /* changed in response to FrontendEvent
                                     * -> do not request additional update */
                                    window.request(FrontendRequest::UpdateHostname(
                                        client.handle(),
                                        hostname,
                                    ));
                                }
                            }
                        ),
                    );
                    row.connect_closure(
                        "request-port-change",
                        false,
                        closure_local!(
                            #[strong]
                            window,
                            move |row: ClientRow, port: u32| {
                                if let Some(client) = window.client_by_idx(row.index() as u32) {
                                    window.request(FrontendRequest::UpdatePort(
                                        client.handle(),
                                        port as u16,
                                    ));
                                }
                            }
                        ),
                    );
                    row.connect_closure(
                        "request-activate",
                        false,
                        closure_local!(
                            #[strong]
                            window,
                            move |row: ClientRow, active: bool| {
                                if let Some(client) = window.client_by_idx(row.index() as u32) {
                                    log::debug!(
                                        "request: {} client",
                                        if active { "activating" } else { "deactivating" }
                                    );
                                    window.request(FrontendRequest::Activate(
                                        client.handle(),
                                        active,
                                    ));
                                }
                            }
                        ),
                    );
                    row.connect_closure(
                        "request-delete",
                        false,
                        closure_local!(
                            #[strong]
                            window,
                            move |row: ClientRow| {
                                if let Some(client) = window.client_by_idx(row.index() as u32) {
                                    window.request(FrontendRequest::Delete(client.handle()));
                                }
                            }
                        ),
                    );
                    row.connect_closure(
                        "request-dns",
                        false,
                        closure_local!(
                            #[strong]
                            window,
                            move |row: ClientRow| {
                                if let Some(client) = window.client_by_idx(row.index() as u32) {
                                    window.request(FrontendRequest::ResolveDns(
                                        client.get_data().handle,
                                    ));
                                }
                            }
                        ),
                    );
                    row.connect_closure(
                        "request-position-change",
                        false,
                        closure_local!(
                            #[strong]
                            window,
                            move |row: ClientRow, pos_idx: u32| {
                                if let Some(client) = window.client_by_idx(row.index() as u32) {
                                    let position = match pos_idx {
                                        0 => Position::Left,
                                        1 => Position::Right,
                                        2 => Position::Top,
                                        _ => Position::Bottom,
                                    };
                                    window.request(FrontendRequest::UpdatePosition(
                                        client.handle(),
                                        position,
                                    ));
                                }
                            }
                        ),
                    );
                    row.upcast()
                }
            ),
        );
    }

    fn setup_icon(&self) {
        self.set_icon_name(Some("de.feschber.LanMouse"));
    }

    /// workaround for a bug in libadwaita that shows an ugly line beneath
    /// the last element if a placeholder is set.
    /// https://gitlab.gnome.org/GNOME/gtk/-/merge_requests/6308
    fn update_placeholder_visibility(&self) {
        let visible = self.clients().n_items() == 0;
        let placeholder = self.imp().client_placeholder.get();
        self.imp().client_list.set_placeholder(match visible {
            true => Some(&placeholder),
            false => None,
        });
    }

    fn update_auth_placeholder_visibility(&self) {
        let visible = self.authorized().n_items() == 0;
        let placeholder = self.imp().authorized_placeholder.get();
        self.imp().authorized_list.set_placeholder(match visible {
            true => Some(&placeholder),
            false => None,
        });
    }

    fn create_client_row(&self, client_object: &ClientObject) -> ClientRow {
        let row = ClientRow::new(client_object);
        row.bind(client_object);
        row
    }

    fn create_key_row(&self, key_object: &KeyObject) -> KeyRow {
        let row = KeyRow::new();
        row.bind(key_object);
        row
    }

    pub(super) fn new_client(
        &self,
        handle: ClientHandle,
        client: ClientConfig,
        state: ClientState,
    ) {
        let client = ClientObject::new(handle, client, state.clone());
        self.clients().append(&client);
        self.update_placeholder_visibility();
        self.update_dns_state(handle, !state.ips.is_empty());
    }

    pub(super) fn update_client_list(
        &self,
        clients: Vec<(ClientHandle, ClientConfig, ClientState)>,
    ) {
        for (handle, client, state) in clients {
            if self.client_idx(handle).is_some() {
                self.update_client_config(handle, client);
                self.update_client_state(handle, state);
            } else {
                self.new_client(handle, client, state);
            }
        }
    }

    pub(super) fn update_port(&self, port: u16, msg: Option<String>) {
        if let Some(msg) = msg {
            self.show_toast(msg.as_str());
        }
        self.imp().set_port(port);
    }

    fn client_idx(&self, handle: ClientHandle) -> Option<usize> {
        self.clients()
            .iter::<ClientObject>()
            .position(|c| c.ok().map(|c| c.handle() == handle).unwrap_or_default())
    }

    pub(super) fn delete_client(&self, handle: ClientHandle) {
        let Some(idx) = self.client_idx(handle) else {
            log::warn!("could not find client with handle {handle}");
            return;
        };

        self.clients().remove(idx as u32);
        if self.clients().n_items() == 0 {
            self.update_placeholder_visibility();
        }
    }

    pub(super) fn update_client_config(&self, handle: ClientHandle, client: ClientConfig) {
        let Some(row) = self.row_for_handle(handle) else {
            log::warn!("could not find row for handle {handle}");
            return;
        };
        row.set_hostname(client.hostname);
        row.set_port(client.port);
        row.set_position(client.pos);
    }

    pub(super) fn update_client_state(&self, handle: ClientHandle, state: ClientState) {
        let Some(row) = self.row_for_handle(handle) else {
            log::warn!("could not find row for handle {handle}");
            return;
        };
        let Some(client_object) = self.client_object_for_handle(handle) else {
            log::warn!("could not find row for handle {handle}");
            return;
        };

        /* activation state */
        row.set_active(state.active);

        /* dns state */
        client_object.set_resolving(state.resolving);

        self.update_dns_state(handle, !state.ips.is_empty());
        let ips = state
            .ips
            .into_iter()
            .map(|ip| ip.to_string())
            .collect::<Vec<_>>();
        client_object.set_ips(ips);

        /* peer build version (drives the version-match indicator) */
        client_object.set_property(
            "peer-commit",
            crate::client_object::peer_commit_to_string(state.peer_commit),
        );
        row.refresh_version_status();
    }

    fn client_object_for_handle(&self, handle: ClientHandle) -> Option<ClientObject> {
        self.client_idx(handle)
            .and_then(|i| self.client_by_idx(i as u32))
    }

    fn row_for_handle(&self, handle: ClientHandle) -> Option<ClientRow> {
        self.client_idx(handle)
            .and_then(|i| self.row_by_idx(i as i32))
    }

    fn update_dns_state(&self, handle: ClientHandle, resolved: bool) {
        if let Some(client_row) = self.row_for_handle(handle) {
            client_row.set_dns_state(resolved);
        }
    }

    fn request_port_change(&self) {
        let port = self
            .imp()
            .port_entry
            .get()
            .text()
            .as_str()
            .parse::<u16>()
            .unwrap_or(DEFAULT_PORT);
        self.request(FrontendRequest::ChangePort(port));
    }

    fn request_capture(&self) {
        self.request(FrontendRequest::EnableCapture);
    }

    fn request_emulation(&self) {
        self.request(FrontendRequest::EnableEmulation);
    }

    fn request_client_create(&self) {
        self.request(FrontendRequest::Create);
    }

    fn open_fingerprint_dialog(&self, fp: Option<String>) {
        if !self.imp().daemon_ready.get() {
            return;
        }
        if let Some(editor) = self.imp().fingerprint_window.borrow().as_ref() {
            editor.present();
            return;
        }
        if fp.is_none() {
            if let Some(prompt) = self.imp().authorization_window.borrow().as_ref() {
                prompt.present();
                return;
            }
        }
        let window = FingerprintWindow::new(fp);
        window.set_transient_for(Some(self));
        window.connect_closure(
            "confirm-clicked",
            false,
            closure_local!(
                #[weak(rename_to = parent)]
                self,
                move |w: FingerprintWindow, desc: String, fp: String| {
                    if parent.imp().fingerprint_window.borrow().as_ref() != Some(&w)
                        || !parent.imp().daemon_ready.get()
                    {
                        return;
                    }
                    // Keep the description/fingerprint draft open on a full or
                    // disconnected request queue; closing is not a success signal.
                    if parent.try_request(FrontendRequest::AuthorizeKey(desc, fp)) {
                        w.close();
                    }
                }
            ),
        );
        let parent = self.downgrade();
        window.connect_close_request(move |w| {
            if let Some(parent) = parent.upgrade() {
                if parent.imp().fingerprint_window.borrow().as_ref() == Some(w) {
                    parent.imp().fingerprint_window.borrow_mut().take();
                    parent
                        .imp()
                        .authorization_queue
                        .borrow_mut()
                        .complete(Instant::now());
                    parent.schedule_authorization();
                }
            }
            glib::Propagation::Proceed
        });
        self.imp().fingerprint_window.replace(Some(window.clone()));
        window.present();
    }

    fn request_fingerprint_remove(&self, fp: String) {
        self.request(FrontendRequest::RemoveAuthorizedKey(fp));
    }

    pub(crate) fn request(&self, request: FrontendRequest) {
        self.try_request(request);
    }

    fn try_request(&self, request: FrontendRequest) -> bool {
        if let FrontendRequest::WindowIdentifier(identifier) = &request {
            self.imp()
                .window_identifier
                .replace(Some(identifier.clone()));
            if self.daemon_generation() == 0 {
                return true;
            }
        }
        let edit = match &request {
            FrontendRequest::UpdateHostname(handle, _) => Some((*handle, true)),
            FrontendRequest::UpdatePort(handle, _) => Some((*handle, false)),
            _ => None,
        };
        let result = self
            .imp()
            .daemon_client
            .borrow()
            .as_ref()
            .ok_or("Service unavailable")
            .and_then(|client| client.request(self.daemon_generation(), request));
        if let Err(error) = result {
            if let Some((handle, hostname)) = edit {
                if let Some(row) = self.row_for_handle(handle) {
                    row.reject_edit_submission(hostname);
                }
            }
            self.show_toast(error);
            return false;
        }
        true
    }

    pub(super) fn stop_daemon_client(&self) {
        self.imp().daemon_ready.set(false);
        self.clear_authorization_dialogs();
        self.imp().daemon_client.borrow_mut().take();
    }

    pub(super) fn daemon_worker_stopped(&self) {
        self.daemon_disconnected("IPC worker stopped. Relaunch the frontend to reconnect.");
        self.imp()
            .connection_row
            .set_title("Service connection stopped");
    }

    pub(super) fn daemon_generation(&self) -> u64 {
        self.imp().daemon_generation.get()
    }

    pub(super) fn daemon_connected(&self, generation: u64) {
        self.imp().daemon_ready.set(false);
        self.clear_authorization_dialogs();
        self.imp().daemon_generation.set(generation);
        self.imp()
            .connection_row
            .set_title("Synchronizing service state");
        self.imp()
            .connection_row
            .set_subtitle("Waiting for current settings");
        self.imp().connection_row.set_visible(true);
        self.imp().service_controls.set_sensitive(false);
        let identifier = self.imp().window_identifier.borrow().clone();
        if let Some(identifier) = identifier {
            self.request(FrontendRequest::WindowIdentifier(identifier));
        }
    }

    pub(super) fn daemon_synced(&self) {
        self.imp().daemon_ready.set(true);
        self.present_next_authorization();
        self.imp().connection_row.set_visible(false);
        self.imp().service_controls.set_sensitive(true);
        if let Some(settings) = self.imp().settings_window.borrow().as_ref() {
            settings.set_daemon_available(true);
        }
    }

    pub(super) fn daemon_disconnected(&self, error: &str) {
        self.imp().daemon_generation.set(0);
        self.imp().daemon_ready.set(false);
        self.imp()
            .connection_row
            .set_title("Service disconnected — reconnecting");
        self.imp().connection_row.set_subtitle(error);
        self.imp().connection_row.set_visible(true);
        self.imp().service_controls.set_sensitive(false);
        self.clients().remove_all();
        self.authorized().remove_all();
        self.update_placeholder_visibility();
        self.update_auth_placeholder_visibility();
        self.set_pk_fp("Service disconnected");
        self.set_capture(false);
        self.set_emulation(false);
        if let Some(settings) = self.imp().settings_window.borrow().as_ref() {
            settings.set_daemon_available(false);
        }
        self.clear_authorization_dialogs();
    }

    pub(super) fn show_toast(&self, msg: &str) {
        let toast = adw::Toast::new(msg);
        self.add_toast(toast);
    }

    pub(super) fn add_toast(&self, toast: adw::Toast) {
        let toast_overlay = &self.imp().toast_overlay;
        toast_overlay.add_toast(toast);
    }

    pub(super) fn set_capture(&self, active: bool) {
        self.imp().capture_active.replace(active);
        self.update_capture_emulation_status();
    }

    pub(super) fn set_emulation(&self, active: bool) {
        self.imp().emulation_active.replace(active);
        self.update_capture_emulation_status();
    }

    #[cfg(target_os = "macos")]
    pub(super) fn refresh_capture_emulation_status(&self) {
        self.update_capture_emulation_status();
    }

    fn update_capture_emulation_status(&self) {
        let capture = self.imp().capture_active.get();
        let emulation = self.imp().emulation_active.get();

        #[cfg(target_os = "macos")]
        {
            // On macOS, capture and emulation share the same TCC gate
            // (Accessibility). Collapse to a single warning row —
            // emulation_status_row stays hidden and capture_status_row
            // doubles as the shared status indicator. Its text and
            // button mutate based on whether we're waiting for AX or
            // waiting for the user to relaunch the app.
            let anything_off = !capture || !emulation;
            self.imp().emulation_status_row.set_visible(false);
            self.imp().capture_status_row.set_visible(anything_off);
            self.imp().capture_emulation_group.set_visible(anything_off);

            if anything_off {
                self.update_macos_warning_row_text();
            }
        }

        #[cfg(not(target_os = "macos"))]
        {
            self.imp().capture_status_row.set_visible(!capture);
            self.imp().emulation_status_row.set_visible(!emulation);
            self.imp()
                .capture_emulation_group
                .set_visible(!capture || !emulation);
        }
    }

    #[cfg(target_os = "macos")]
    fn update_macos_warning_row_text(&self) {
        let row = &self.imp().capture_status_row;
        let button = &self.imp().input_capture_button;

        if crate::macos_privacy::accessibility_granted() {
            // AX granted but capture/emulation still off → the daemon
            // subprocess bailed at startup and needs a fresh process to
            // re-initialize with the new grant in place.
            row.set_title("relaunch required");
            row.set_subtitle("Accessibility granted — restart to activate capture and emulation");
            set_button_content_label(button, "Relaunch");
        } else {
            // AX missing → send the user to System Settings.
            row.set_title("input capture is disabled");
            row.set_subtitle("grant Accessibility permission to enable");
            set_button_content_label(button, "Grant");
        }
    }

    pub(super) fn set_authorized_keys(&self, fingerprints: HashMap<String, String>) {
        let active_authorized = self
            .imp()
            .authorization_queue
            .borrow_mut()
            .set_authorized(fingerprints.keys().cloned().collect());
        let authorized = self.authorized();
        // clear list
        authorized.remove_all();
        // insert fingerprints
        for (fingerprint, description) in fingerprints {
            let key_obj = KeyObject::new(description, fingerprint);
            authorized.append(&key_obj);
        }
        self.update_auth_placeholder_visibility();
        if active_authorized {
            let editor = self.imp().fingerprint_window.borrow().clone();
            let prompt = self.imp().authorization_window.borrow().clone();
            if let Some(editor) = editor {
                editor.close();
            } else if let Some(prompt) = prompt {
                prompt.close();
            }
        }
    }

    pub(super) fn set_pk_fp(&self, fingerprint: &str) {
        self.imp().fingerprint_row.set_subtitle(fingerprint);
    }

    /// store the settings state pushed by the daemon and apply it to an
    /// open settings window, if any
    pub(super) fn update_settings(
        &self,
        clipboard_enabled: bool,
        invert_scroll: bool,
        mouse_sensitivity: f64,
    ) {
        self.imp()
            .settings
            .set((clipboard_enabled, invert_scroll, mouse_sensitivity));
        if let Some(w) = self.imp().settings_window.borrow().as_ref() {
            w.update_values(clipboard_enabled, invert_scroll, mouse_sensitivity);
        }
    }

    pub(crate) fn open_settings(&self) {
        if let Some(w) = self.imp().settings_window.borrow().as_ref() {
            w.present();
            return;
        }
        let settings_window = SettingsWindow::new();
        settings_window.set_transient_for(Some(self));
        settings_window.set_daemon_available(self.imp().daemon_ready.get());
        let (clipboard_enabled, invert_scroll, mouse_sensitivity) = self.imp().settings.get();
        settings_window.update_values(clipboard_enabled, invert_scroll, mouse_sensitivity);
        settings_window.connect_clipboard_toggled(clone!(
            #[weak(rename_to = window)]
            self,
            move |enabled| window.request(FrontendRequest::SetClipboardEnabled(enabled))
        ));
        settings_window.connect_invert_scroll_toggled(clone!(
            #[weak(rename_to = window)]
            self,
            move |invert| window.request(FrontendRequest::UpdateScrollingInversion(invert))
        ));
        settings_window.connect_sensitivity_changed(clone!(
            #[weak(rename_to = window)]
            self,
            move |sensitivity| {
                window.request(FrontendRequest::UpdateMouseSensitivity(sensitivity))
            }
        ));
        self.imp()
            .settings_window
            .replace(Some(settings_window.clone()));
        settings_window.present();
    }

    pub(super) fn request_authorization(&self, fingerprint: &str) {
        if self.daemon_generation() == 0 {
            return;
        }
        self.imp()
            .authorization_queue
            .borrow_mut()
            .enqueue(fingerprint, Instant::now());
        self.present_next_authorization();
    }

    fn schedule_authorization(&self) {
        if self.imp().authorization_next.borrow().is_some() {
            return;
        }
        let parent = self.downgrade();
        let source = glib::idle_add_local_once(move || {
            if let Some(parent) = parent.upgrade() {
                parent.imp().authorization_next.borrow_mut().take();
                parent.present_next_authorization();
            }
        });
        self.imp().authorization_next.replace(Some(source));
    }

    fn clear_authorization_dialogs(&self) {
        if let Some(source) = self.imp().authorization_next.borrow_mut().take() {
            source.remove();
        }
        self.imp().authorization_queue.borrow_mut().clear();
        // Detach both before closing: callbacks cannot advance old queued prompts.
        let prompt = self.imp().authorization_window.borrow_mut().take();
        let editor = self.imp().fingerprint_window.borrow_mut().take();
        if let Some(prompt) = prompt {
            prompt.close();
        }
        if let Some(editor) = editor {
            editor.close();
        }
    }

    fn present_next_authorization(&self) {
        if !self.imp().daemon_ready.get()
            || self.imp().authorization_window.borrow().is_some()
            || self.imp().fingerprint_window.borrow().is_some()
        {
            return;
        }
        let Some(fingerprint) = self.imp().authorization_queue.borrow_mut().next() else {
            return;
        };
        let window = AuthorizationWindow::new(&fingerprint);
        window.set_transient_for(Some(self));
        window.connect_closure(
            "confirm-clicked",
            false,
            closure_local!(
                #[weak(rename_to = parent)]
                self,
                move |w: AuthorizationWindow, fp: String| {
                    if parent.imp().authorization_window.borrow().as_ref() != Some(&w)
                        || !parent.imp().daemon_ready.get()
                    {
                        return;
                    }
                    // Install the editor first, so closing the prompt preserves
                    // this active identity through description editing.
                    parent.open_fingerprint_dialog(Some(fp));
                    w.close();
                }
            ),
        );
        window.connect_closure(
            "cancel-clicked",
            false,
            closure_local!(move |w: AuthorizationWindow| {
                w.close();
            }),
        );
        let parent = self.downgrade();
        window.connect_close_request(move |w| {
            if let Some(parent) = parent.upgrade() {
                if parent.imp().authorization_window.borrow().as_ref() == Some(w) {
                    parent.imp().authorization_window.borrow_mut().take();
                    if parent.imp().fingerprint_window.borrow().is_none() {
                        parent
                            .imp()
                            .authorization_queue
                            .borrow_mut()
                            .complete(Instant::now());
                        parent.schedule_authorization();
                    }
                }
            }
            glib::Propagation::Proceed
        });
        self.imp()
            .authorization_window
            .replace(Some(window.clone()));
        window.present();
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    #[ignore = "requires a GTK display; run separately"]
    fn actual_authorization_windows_preserve_interaction_drafts_and_session_identity() {
        fn advance(window: &Window) {
            let context = glib::MainContext::default();
            let deadline = Instant::now() + std::time::Duration::from_secs(2);
            while window.imp().authorization_next.borrow().is_some() {
                assert!(
                    Instant::now() < deadline,
                    "next authorization idle did not run"
                );
                context.iteration(false);
            }
        }
        fn prompt(window: &Window) -> AuthorizationWindow {
            window
                .imp()
                .authorization_window
                .borrow()
                .as_ref()
                .unwrap()
                .clone()
        }
        fn editor(window: &Window) -> FingerprintWindow {
            window
                .imp()
                .fingerprint_window
                .borrow()
                .as_ref()
                .unwrap()
                .clone()
        }
        adw::init().unwrap();
        gio::resources_register_include!("lan-mouse.gresource").unwrap();
        let app = adw::Application::new(
            Some("de.feschber.LanMouse.AuthorizationTest"),
            gio::ApplicationFlags::NON_UNIQUE,
        );
        app.register(None::<&gio::Cancellable>).unwrap();
        let (client, current, mut requests) = crate::daemon_client::test_client();
        let window = Window::new(&app, client);
        window.present();
        window.daemon_connected(1);
        window.request_authorization("before-sync");
        assert!(window.imp().authorization_window.borrow().is_none());
        window.set_authorized_keys(HashMap::from([(
            "before-sync".into(),
            "already accepted".into(),
        )]));
        window.daemon_synced();
        assert!(window.imp().authorization_window.borrow().is_none());

        window.request_authorization("peer-a");
        let original = prompt(&window);
        assert!(original.imp().message.wraps());
        assert_eq!(
            original.imp().message.wrap_mode(),
            gtk::pango::WrapMode::WordChar
        );
        for _ in 0..1000 {
            window.request_authorization("peer-a");
            window.request_authorization("peer-b");
        }
        assert_eq!(prompt(&window), original);
        assert_eq!(original.imp().fingerprint.text(), "peer-a");
        original.imp().confirm_button.emit_clicked();
        let draft = editor(&window);
        assert!(window.imp().authorization_window.borrow().is_none());
        draft.imp().description.set_text("my peer description");
        window.request_authorization("peer-c");
        window.request_authorization("peer-a");
        assert_eq!(editor(&window), draft);
        assert_eq!(draft.imp().fingerprint.text(), "peer-a");
        for _ in 0..64 {
            window.request(FrontendRequest::Create);
        }
        draft.imp().confirm_button.emit_clicked();
        assert_eq!(editor(&window), draft); // A full request queue keeps the draft open.
        assert_eq!(draft.imp().description.text(), "my peer description");
        assert!(window.imp().authorization_next.borrow().is_none());
        while requests.try_recv().is_ok() {}
        draft.imp().confirm_button.emit_clicked();
        assert!(
            matches!(requests.try_recv().unwrap(), (1, FrontendRequest::AuthorizeKey(desc, fp)) if desc == "my peer description" && fp == "peer-a")
        );
        assert!(window.imp().fingerprint_window.borrow().is_none());
        advance(&window);
        assert_eq!(prompt(&window).imp().fingerprint.text(), "peer-b");
        window.set_authorized_keys(HashMap::from([
            ("peer-b".into(), "b".into()),
            ("peer-c".into(), "c".into()),
        ]));
        advance(&window);
        assert!(window.imp().authorization_window.borrow().is_none());

        window.open_fingerprint_dialog(None);
        let manual = editor(&window);
        manual.imp().fingerprint.set_text("manual-key");
        manual.imp().description.set_text("manual description");
        window.request_authorization("peer-d");
        assert_eq!(editor(&window), manual);
        assert!(window.imp().authorization_window.borrow().is_none());
        manual.imp().confirm_button.emit_clicked();
        assert!(
            matches!(requests.try_recv().unwrap(), (1, FrontendRequest::AuthorizeKey(desc, fp)) if desc == "manual description" && fp == "manual-key")
        );
        advance(&window);
        prompt(&window).imp().cancel_button.emit_clicked();
        advance(&window);
        for _ in 0..1000 {
            window.request_authorization("peer-d");
        }
        assert!(window.imp().authorization_window.borrow().is_none());
        window.request_authorization("peer-e");
        window.request_authorization("peer-f");
        prompt(&window).close(); // Window-manager close also advances the queue.
        advance(&window);
        let stale_prompt = prompt(&window);
        assert_eq!(stale_prompt.imp().fingerprint.text(), "peer-f");
        stale_prompt.imp().confirm_button.emit_clicked();
        let stale_editor = editor(&window);
        window.request_authorization("peer-g");
        current.store(0, Ordering::Release);
        window.daemon_disconnected("fixture EOF");
        assert!(window.imp().authorization_window.borrow().is_none());
        assert!(window.imp().fingerprint_window.borrow().is_none());
        assert!(!stale_editor.is_visible());
        assert!(window.imp().authorization_next.borrow().is_none());
        stale_editor.imp().confirm_button.emit_clicked();
        stale_prompt.imp().confirm_button.emit_clicked();
        assert!(requests.try_recv().is_err());
        current.store(2, Ordering::Release);
        window.daemon_connected(2);
        window.request_authorization("peer-d"); // Previous dismissal belongs to the old session.
        assert!(window.imp().authorization_window.borrow().is_none());
        window.daemon_synced();
        assert_eq!(prompt(&window).imp().fingerprint.text(), "peer-d");
        window.request_authorization("never-replay");
        prompt(&window).close();
        assert!(window.imp().authorization_next.borrow().is_some());
        current.store(0, Ordering::Release);
        window.daemon_disconnected("before pending idle");
        assert!(window.imp().authorization_next.borrow().is_none());
        current.store(3, Ordering::Release);
        window.daemon_connected(3);
        window.daemon_synced();
        assert!(window.imp().authorization_window.borrow().is_none());
        window.set_authorized_keys(HashMap::new());
        let weak = window.downgrade();
        window.stop_daemon_client();
        window.close();
        drop(window);
        assert!(
            weak.upgrade().is_none(),
            "authorization callbacks must not retain the parent window"
        );
    }

    #[test]
    #[ignore = "requires a GTK display; run separately"]
    fn actual_window_retains_visibility_clears_stale_state_and_waits_for_sync() {
        adw::init().unwrap();
        gio::resources_register_include!("lan-mouse.gresource").unwrap();
        let app = adw::Application::new(
            Some("de.feschber.LanMouse.RecoveryTest"),
            gio::ApplicationFlags::NON_UNIQUE,
        );
        app.register(None::<&gio::Cancellable>).unwrap();
        let (client, current, mut requests) = crate::daemon_client::test_client();
        let window = Window::new(&app, client);
        window.present();
        window.daemon_connected(1);
        assert!(!window.imp().service_controls.is_sensitive());
        assert!(window.imp().connection_row.is_visible());
        window.new_client(
            7,
            ClientConfig {
                hostname: Some("old.local".into()),
                ..Default::default()
            },
            ClientState {
                alive: true,
                ..Default::default()
            },
        );
        window.set_capture(true);
        window.set_emulation(true);
        window.update_settings(true, false, 1.);
        window.daemon_synced();
        window.open_settings();
        assert!(window.imp().service_controls.is_sensitive());
        assert_eq!(window.clients().n_items(), 1);

        current.store(0, Ordering::Release);
        window.daemon_disconnected("fixture EOF");
        assert!(window.is_visible());
        assert_eq!(window.clients().n_items(), 0);
        assert!(!window.imp().capture_active.get());
        assert!(!window.imp().emulation_active.get());
        assert!(window.imp().connection_row.is_visible());
        assert!(!window.imp().service_controls.is_sensitive());
        assert!(
            !window
                .imp()
                .settings_window
                .borrow()
                .as_ref()
                .unwrap()
                .imp()
                .clipboard_switch
                .is_sensitive()
        );

        current.store(2, Ordering::Release);
        window.daemon_connected(2);
        assert!(!window.imp().service_controls.is_sensitive());
        window.new_client(
            7,
            ClientConfig {
                hostname: Some("fresh.local".into()),
                ..Default::default()
            },
            Default::default(),
        );
        window.update_settings(false, true, 0.5);
        window.daemon_synced();
        assert!(window.is_visible());
        assert!(window.imp().service_controls.is_sensitive());
        assert!(!window.imp().connection_row.is_visible());
        assert_eq!(window.clients().n_items(), 1);
        assert_eq!(
            window
                .client_by_idx(0)
                .unwrap()
                .get_data()
                .hostname
                .as_deref(),
            Some("fresh.local")
        );
        assert_eq!(window.imp().settings.get(), (false, true, 0.5));
        assert!(
            window
                .imp()
                .settings_window
                .borrow()
                .as_ref()
                .unwrap()
                .imp()
                .clipboard_switch
                .is_sensitive()
        );
        while requests.try_recv().is_ok() {}
        for _ in 0..64 {
            window.request(FrontendRequest::Create);
        }
        let row = window.row_for_handle(7).unwrap();
        row.imp().hostname.set_text("retry.local");
        row.flush_pending_edits(); // full queue rejects this submission
        assert_eq!(row.imp().hostname.text(), "retry.local");
        while requests.try_recv().is_ok() {}
        row.flush_pending_edits(); // retry once capacity is available
        assert!(
            matches!(requests.try_recv().unwrap(), (2, FrontendRequest::UpdateHostname(7, Some(host))) if host == "retry.local")
        );
        window.stop_daemon_client();
        window.close();
    }
}
