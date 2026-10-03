use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashSet},
    net::{IpAddr, SocketAddr},
    rc::Rc,
};

use lan_mouse_ipc::{ClientConfig, ClientHandle, ClientState, Position};

use crate::config::ConfigClient;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
pub struct ClientManager {
    clients: Rc<RefCell<BTreeMap<ClientHandle, (ClientConfig, ClientState)>>>,
    next_handle: Rc<Cell<ClientHandle>>,
    revisions: Rc<RefCell<BTreeMap<ClientHandle, u64>>>,
    target_tokens: Rc<RefCell<BTreeMap<ClientHandle, CancellationToken>>>,
}

impl ClientManager {
    /// get all clients
    pub fn clients(&self) -> Vec<(ClientConfig, ClientState)> {
        self.clients.borrow().values().cloned().collect::<Vec<_>>()
    }

    pub fn add_with_config(&self, config_client: ConfigClient) -> ClientHandle {
        let config = ClientConfig {
            hostname: config_client.hostname,
            fix_ips: config_client.ips.into_iter().collect(),
            port: config_client.port,
            pos: config_client.pos,
            cmd: config_client.enter_hook,
            leave_cmd: config_client.leave_hook,
        };
        let state = ClientState {
            active: config_client.active,
            ips: HashSet::from_iter(config.fix_ips.iter().cloned()),
            ..Default::default()
        };
        let handle = self.add_client();
        self.set_config(handle, config);
        self.set_state(handle, state);
        handle
    }

    /// add a new client to this manager
    pub fn add_client(&self) -> ClientHandle {
        let handle = self.next_handle.get();
        assert!(
            handle < ClientHandle::MAX / 2,
            "client handle space exhausted"
        );
        self.next_handle.set(handle + 1);
        self.clients.borrow_mut().insert(handle, Default::default());
        self.revisions.borrow_mut().insert(handle, 0);
        self.target_tokens
            .borrow_mut()
            .insert(handle, CancellationToken::new());
        handle
    }

    /// set the config of the given client
    pub fn set_config(&self, handle: ClientHandle, config: ClientConfig) {
        if let Some((c, _)) = self.clients.borrow_mut().get_mut(&handle) {
            *c = config;
        }
        self.invalidate_target(handle);
    }

    /// set the state of the given client
    pub fn set_state(&self, handle: ClientHandle, state: ClientState) {
        if let Some((_, s)) = self.clients.borrow_mut().get_mut(&handle) {
            *s = state;
        }
    }

    /// activate the given client
    /// returns, whether the client was activated
    pub fn activate_client(&self, handle: ClientHandle) -> bool {
        let mut clients = self.clients.borrow_mut();
        match clients.get_mut(&handle) {
            Some((_, s)) if !s.active => {
                s.active = true;
                true
            }
            _ => false,
        }
    }

    /// deactivate the given client
    /// returns, whether the client was deactivated
    pub fn deactivate_client(&self, handle: ClientHandle) -> bool {
        let mut clients = self.clients.borrow_mut();
        match clients.get_mut(&handle) {
            Some((_, s)) if s.active => {
                s.active = false;
                drop(clients);
                self.invalidate_target(handle);
                true
            }
            _ => false,
        }
    }

    /// find a client by its address
    pub fn get_client(&self, addr: SocketAddr) -> Option<ClientHandle> {
        // since there shouldn't be more than a handful of clients at any given
        // time this is likely faster than using a HashMap
        self.clients
            .borrow()
            .iter()
            .find_map(|(k, (_, s))| {
                if s.active && s.ips.contains(&addr.ip()) {
                    Some(k)
                } else {
                    None
                }
            })
            .copied()
    }

    /// get the client at the given position
    pub fn client_at(&self, pos: Position) -> Option<ClientHandle> {
        self.clients
            .borrow()
            .iter()
            .find_map(|(k, (c, s))| {
                if s.active && c.pos == pos {
                    Some(k)
                } else {
                    None
                }
            })
            .copied()
    }

    pub(crate) fn get_hostname(&self, handle: ClientHandle) -> Option<String> {
        self.clients
            .borrow_mut()
            .get_mut(&handle)
            .and_then(|(c, _)| c.hostname.clone())
    }

    /// get the position of the corresponding client
    pub(crate) fn get_pos(&self, handle: ClientHandle) -> Option<Position> {
        self.clients.borrow().get(&handle).map(|(c, _)| c.pos)
    }

    /// remove a client from the list
    pub fn remove_client(&self, client: ClientHandle) -> Option<(ClientConfig, ClientState)> {
        self.revisions.borrow_mut().remove(&client);
        if let Some(token) = self.target_tokens.borrow_mut().remove(&client) {
            token.cancel();
        }
        self.clients.borrow_mut().remove(&client)
    }

    /// get the config & state of the given client
    pub fn get_state(&self, handle: ClientHandle) -> Option<(ClientConfig, ClientState)> {
        self.clients.borrow().get(&handle).cloned()
    }

    /// get the current config & state of all clients
    pub fn get_client_states(&self) -> Vec<(ClientHandle, ClientConfig, ClientState)> {
        self.clients
            .borrow()
            .iter()
            .map(|(k, v)| (*k, v.0.clone(), v.1.clone()))
            .collect()
    }

    /// update the fix ips of the client
    pub fn set_fix_ips(&self, handle: ClientHandle, fix_ips: Vec<IpAddr>) {
        let changed = if let Some((c, _)) = self.clients.borrow_mut().get_mut(&handle) {
            if c.fix_ips == fix_ips {
                false
            } else {
                c.fix_ips = fix_ips;
                true
            }
        } else {
            false
        };
        if changed {
            self.invalidate_target(handle);
        }
        self.update_ips(handle);
    }

    /// update the dns-ips of the client
    pub fn set_dns_ips(&self, handle: ClientHandle, dns_ips: Vec<IpAddr>) {
        let changed = if let Some((_, s)) = self.clients.borrow_mut().get_mut(&handle) {
            let changed =
                s.dns_ips.iter().collect::<HashSet<_>>() != dns_ips.iter().collect::<HashSet<_>>();
            s.dns_ips = dns_ips;
            changed
        } else {
            false
        };
        if changed {
            self.invalidate_target(handle);
        }
        self.update_ips(handle);
    }

    fn update_ips(&self, handle: ClientHandle) {
        if let Some((c, s)) = self.clients.borrow_mut().get_mut(&handle) {
            s.ips = c
                .fix_ips
                .iter()
                .cloned()
                .chain(s.dns_ips.iter().cloned())
                .collect::<HashSet<_>>();
        }
    }

    /// update the hostname of the given client
    /// this automatically clears the active ip address and ips from dns
    pub fn set_hostname(&self, handle: ClientHandle, hostname: Option<String>) -> bool {
        let mut clients = self.clients.borrow_mut();
        let Some((c, s)) = clients.get_mut(&handle) else {
            return false;
        };

        // hostname changed
        if c.hostname != hostname {
            c.hostname = hostname;
            s.active_addr = None;
            s.dns_ips.clear();
            s.resolving = false;
            drop(clients);
            self.invalidate_target(handle);
            self.update_ips(handle);
            true
        } else {
            false
        }
    }

    /// update the port of the client
    pub(crate) fn set_port(&self, handle: ClientHandle, port: u16) {
        let changed = if let Some((c, _)) = self.clients.borrow_mut().get_mut(&handle) {
            if c.port == port {
                false
            } else {
                c.port = port;
                true
            }
        } else {
            false
        };
        if changed {
            self.invalidate_target(handle);
        }
    }

    /// update the position of the client
    /// returns true, if a change in capture position is required (pos changed & client is active)
    pub(crate) fn set_pos(&self, handle: ClientHandle, pos: Position) -> bool {
        match self.clients.borrow_mut().get_mut(&handle) {
            Some((c, s)) if c.pos != pos => {
                log::info!("update pos {handle} {} -> {}", c.pos, pos);
                c.pos = pos;
                s.active
            }
            _ => false,
        }
    }

    /// update the enter hook command of the client
    pub(crate) fn set_enter_hook(&self, handle: ClientHandle, enter_hook: Option<String>) {
        if let Some((c, _s)) = self.clients.borrow_mut().get_mut(&handle) {
            c.cmd = enter_hook;
        }
    }

    /// update the leave hook command of the client
    pub(crate) fn set_leave_hook(&self, handle: ClientHandle, leave_hook: Option<String>) {
        if let Some((c, _s)) = self.clients.borrow_mut().get_mut(&handle) {
            c.leave_cmd = leave_hook;
        }
    }

    /// set resolving status of the client
    pub(crate) fn set_resolving(&self, handle: ClientHandle, status: bool) {
        if let Some((_, s)) = self.clients.borrow_mut().get_mut(&handle) {
            s.resolving = status;
        }
    }

    /// get the enter hook command
    pub(crate) fn get_enter_cmd(&self, handle: ClientHandle) -> Option<String> {
        self.clients
            .borrow()
            .get(&handle)
            .and_then(|(c, _)| c.cmd.clone())
    }

    /// get the leave hook command
    pub(crate) fn get_leave_cmd(&self, handle: ClientHandle) -> Option<String> {
        self.clients
            .borrow()
            .get(&handle)
            .and_then(|(c, _)| c.leave_cmd.clone())
    }

    /// returns all clients that are currently registered
    pub(crate) fn registered_clients(&self) -> Vec<ClientHandle> {
        self.clients.borrow().keys().copied().collect()
    }

    /// returns all clients that are currently active
    pub(crate) fn active_clients(&self) -> Vec<ClientHandle> {
        self.clients
            .borrow()
            .iter()
            .filter(|(_, (_, s))| s.active)
            .map(|(h, _)| *h)
            .collect()
    }

    pub(crate) fn target_revision(&self, handle: ClientHandle) -> Option<u64> {
        self.revisions.borrow().get(&handle).copied()
    }

    pub(crate) fn target_is_current(&self, handle: ClientHandle, revision: u64) -> bool {
        self.target_revision(handle) == Some(revision)
            && self
                .target_tokens
                .borrow()
                .get(&handle)
                .is_some_and(|token| !token.is_cancelled())
            && self
                .clients
                .borrow()
                .get(&handle)
                .is_some_and(|(_, s)| s.active)
    }

    pub(crate) fn target_token(&self, handle: ClientHandle) -> Option<CancellationToken> {
        self.target_tokens.borrow().get(&handle).cloned()
    }

    pub(crate) fn cancel_targets(&self) {
        for token in self.target_tokens.borrow().values() {
            token.cancel();
        }
        for (_, state) in self.clients.borrow_mut().values_mut() {
            state.active_addr = None;
            state.alive = false;
            state.peer_commit = None;
        }
    }

    fn invalidate_target(&self, handle: ClientHandle) {
        if let Some(token) = self.target_tokens.borrow_mut().get_mut(&handle) {
            token.cancel();
            *token = CancellationToken::new();
        }
        if let Some(revision) = self.revisions.borrow_mut().get_mut(&handle) {
            *revision = revision
                .checked_add(1)
                .expect("client revision space exhausted");
        }
        if let Some((_, state)) = self.clients.borrow_mut().get_mut(&handle) {
            state.active_addr = None;
            state.alive = false;
            state.peer_commit = None;
        }
    }

    pub(crate) fn set_active_addr(&self, handle: ClientHandle, addr: Option<SocketAddr>) {
        if let Some((_, s)) = self.clients.borrow_mut().get_mut(&handle) {
            s.active_addr = addr;
        }
    }

    pub(crate) fn set_alive(&self, handle: ClientHandle, alive: bool) {
        if let Some((_, s)) = self.clients.borrow_mut().get_mut(&handle) {
            s.alive = alive;
        }
    }

    pub(crate) fn set_peer_commit(&self, handle: ClientHandle, commit: Option<[u8; 8]>) {
        if let Some((_, s)) = self.clients.borrow_mut().get_mut(&handle) {
            s.peer_commit = commit;
        }
    }

    pub(crate) fn active_addr(&self, handle: ClientHandle) -> Option<SocketAddr> {
        self.clients
            .borrow()
            .get(&handle)
            .and_then(|(_, s)| s.active_addr)
    }

    pub(crate) fn alive(&self, handle: ClientHandle) -> bool {
        self.clients
            .borrow()
            .get(&handle)
            .map(|(_, s)| s.alive)
            .unwrap_or(false)
    }

    pub(crate) fn get_port(&self, handle: ClientHandle) -> Option<u16> {
        self.clients.borrow().get(&handle).map(|(c, _)| c.port)
    }

    pub(crate) fn get_ips(&self, handle: ClientHandle) -> Option<HashSet<IpAddr>> {
        self.clients
            .borrow()
            .get(&handle)
            .map(|(_, s)| s.ips.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deleted_clients_cannot_pollute_new_clients() {
        let clients = ClientManager::default();
        let old = clients.add_client();
        clients.remove_client(old);
        let new = clients.add_client();
        assert_ne!(old, new);
        clients.set_dns_ips(old, vec!["192.0.2.1".parse().unwrap()]);
        clients.set_active_addr(old, Some("192.0.2.1:4242".parse().unwrap()));
        assert!(clients.active_addr(new).is_none());
        assert!(clients.get_ips(new).unwrap().is_empty());
        assert_eq!(clients.registered_clients(), vec![new]);
    }

    #[test]
    fn changing_target_invalidates_old_work_and_connection_state() {
        let clients = ClientManager::default();
        let handle = clients.add_client();
        clients.activate_client(handle);
        let revision = clients.target_revision(handle).unwrap();
        clients.set_active_addr(handle, Some("192.0.2.1:4242".parse().unwrap()));
        clients.set_alive(handle, true);
        clients.set_peer_commit(handle, Some(*b"12345678"));
        clients.set_hostname(handle, Some("other.local".into()));
        assert!(!clients.target_is_current(handle, revision));
        assert!(!clients.alive(handle));
        assert!(clients.active_addr(handle).is_none());
        assert!(clients.get_state(handle).unwrap().1.peer_commit.is_none());
        let revision = clients.target_revision(handle).unwrap();
        clients.set_hostname(handle, Some("other.local".into()));
        assert!(clients.target_is_current(handle, revision));
        clients.set_port(handle, 5000);
        assert!(!clients.target_is_current(handle, revision));
    }
}
