//! One latest valid snapshot, with explicit write order and per-peer/session delivery.
use crate::listen::ArcConn;
use input_event::ClipboardEvent;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Weak},
};

#[derive(Clone)]
pub(crate) struct Snapshot {
    pub revision: u64,
    pub event: Arc<ClipboardEvent>,
    pub origin: Option<String>,
}

#[derive(Default)]
pub(crate) struct ClipboardReplay {
    revision: u64,
    latest: Option<Snapshot>,
    remote: Option<(u64, String)>,
    sent: HashMap<String, (u64, Weak<dyn webrtc_util::Conn + Send + Sync>)>,
}

impl ClipboardReplay {
    pub fn invalidate(&mut self) -> u64 {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("clipboard revision exhausted");
        self.latest = None;
        self.remote = None;
        self.sent.clear();
        self.revision
    }
    pub fn local(&mut self, event: ClipboardEvent) {
        let revision = self.invalidate();
        self.latest = Some(Snapshot {
            revision,
            event: Arc::new(event),
            origin: None,
        });
    }
    pub fn begin_remote(&mut self, origin: String) -> u64 {
        let revision = self.invalidate();
        self.remote = Some((revision, origin));
        revision
    }
    pub fn applied(&mut self, revision: u64, event: ClipboardEvent, success: bool) -> bool {
        if self
            .remote
            .as_ref()
            .is_none_or(|(current, _)| *current != revision)
        {
            return false;
        }
        let (_, origin) = self.remote.take().unwrap();
        if success {
            self.latest = Some(Snapshot {
                revision,
                event: Arc::new(event),
                origin: Some(origin),
            });
        }
        true
    }
    pub fn snapshot(&self) -> Option<Snapshot> {
        self.latest.clone()
    }
    pub fn should_send(&self, snapshot: &Snapshot, peer: &str, conn: &ArcConn) -> bool {
        if snapshot.origin.as_deref() == Some(peer) {
            return false;
        }
        !self.sent.get(peer).is_some_and(|(revision, old)| {
            *revision == snapshot.revision
                && old.upgrade().is_some_and(|old| Arc::ptr_eq(&old, conn))
        })
    }
    pub fn accepted(&mut self, snapshot: &Snapshot, peer: String, conn: &ArcConn) {
        self.sent
            .insert(peer, (snapshot.revision, Arc::downgrade(conn)));
    }
    pub fn retain_peers(&mut self, current: &HashSet<String>) {
        self.sent.retain(|peer, _| current.contains(peer));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn text(value: &str) -> ClipboardEvent {
        ClipboardEvent::Text(value.into())
    }
    struct Peer;
    #[async_trait::async_trait]
    impl webrtc_util::Conn for Peer {
        async fn connect(&self, _: std::net::SocketAddr) -> webrtc_util::Result<()> {
            unreachable!()
        }
        async fn recv(&self, _: &mut [u8]) -> webrtc_util::Result<usize> {
            unreachable!()
        }
        async fn recv_from(
            &self,
            _: &mut [u8],
        ) -> webrtc_util::Result<(usize, std::net::SocketAddr)> {
            unreachable!()
        }
        async fn send(&self, _: &[u8]) -> webrtc_util::Result<usize> {
            unreachable!()
        }
        async fn send_to(&self, _: &[u8], _: std::net::SocketAddr) -> webrtc_util::Result<usize> {
            unreachable!()
        }
        fn local_addr(&self) -> webrtc_util::Result<std::net::SocketAddr> {
            unreachable!()
        }
        fn remote_addr(&self) -> Option<std::net::SocketAddr> {
            None
        }
        async fn close(&self) -> webrtc_util::Result<()> {
            Ok(())
        }
        fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
            self
        }
    }
    #[test]
    fn latest_replaces_offline_values_and_replays_once_per_current_session() {
        let mut replay = ClipboardReplay::default();
        replay.local(text("old"));
        for index in 0..1000 {
            replay.local(text(&format!("latest-{index}")));
        }
        let snapshot = replay.snapshot().unwrap();
        assert_eq!(*snapshot.event, text("latest-999"));
        let old: ArcConn = Arc::new(Peer);
        let replacement: ArcConn = Arc::new(Peer);
        assert!(replay.should_send(&snapshot, "peer", &old));
        replay.accepted(&snapshot, "peer".into(), &old);
        assert!(!replay.should_send(&snapshot, "peer", &old));
        assert!(replay.should_send(&snapshot, "peer", &replacement));
        replay.accepted(&snapshot, "peer".into(), &replacement);
        assert!(!replay.should_send(&snapshot, "peer", &replacement));
        let weak = Arc::downgrade(&replacement);
        drop(replacement);
        assert!(weak.upgrade().is_none()); // bookkeeping does not keep sessions alive.
        replay.retain_peers(&HashSet::new());
        assert!(replay.sent.is_empty());
    }
    #[test]
    fn remote_intent_blocks_replay_and_late_results_cannot_replace_newer_state() {
        let mut replay = ClipboardReplay::default();
        replay.local(text("local before remote"));
        let first = replay.begin_remote("first origin".into());
        assert!(replay.snapshot().is_none());
        let latest = replay.begin_remote("latest origin".into());
        assert!(!replay.applied(first, text("first remote"), true));
        assert!(replay.snapshot().is_none());
        assert!(replay.applied(latest, text("latest remote"), true));
        let snapshot = replay.snapshot().unwrap();
        let conn: ArcConn = Arc::new(Peer);
        assert!(!replay.should_send(&snapshot, "latest origin", &conn));
        assert!(replay.should_send(&snapshot, "other peer", &conn));
        let pending = replay.begin_remote("older".into());
        replay.local(text("new local after apply completed"));
        assert!(!replay.applied(pending, text("older remote"), true));
        assert_eq!(
            *replay.snapshot().unwrap().event,
            text("new local after apply completed")
        );
    }
    #[test]
    fn failure_and_disable_never_replay_a_superseded_known_value() {
        let mut replay = ClipboardReplay::default();
        replay.local(text("old local"));
        let failed = replay.begin_remote("peer".into());
        assert!(replay.applied(failed, text("not applied"), false));
        assert!(replay.snapshot().is_none());
        replay.local(text("observed local after failure"));
        let old = replay.begin_remote("peer".into());
        replay.invalidate();
        assert!(!replay.applied(old, text("late after disable"), true));
        assert!(replay.snapshot().is_none());
        replay.local(text("fresh enable observation"));
        assert!(replay.snapshot().unwrap().revision > old);
    }
}
