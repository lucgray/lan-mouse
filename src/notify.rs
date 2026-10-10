//! OS-level notifications as a fallback channel for user-facing hints.
//!
//! Every user-facing event also reaches [`notify_for_event`]; when no
//! frontend is connected the service calls it so hints like "clipboard
//! shared" still show up as real system notifications instead of only
//! living inside a window that may not exist (daemon-only, tray-hidden).

use input_event::ClipboardContentKind;
use lan_mouse_ipc::FrontendEvent;
use std::sync::OnceLock;

/// Single worker thread that owns every desktop-notification call.
///
/// notify-rust's blocking `show()` drives zbus through an internal
/// `block_on` — called from inside the daemon's tokio runtime it panics
/// ("cannot start a runtime from within a runtime"), and the release
/// profile's `panic = "abort"` turns that into a daemon crash loop on
/// any notification-worthy event. Routing the calls through a plain
/// `std::thread` keeps them outside runtime context entirely; it also
/// keeps D-Bus latency off the input-handling main loop.
fn dispatcher() -> &'static std::sync::mpsc::Sender<(String, String)> {
    static TX: OnceLock<std::sync::mpsc::Sender<(String, String)>> = OnceLock::new();
    TX.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<(String, String)>();
        std::thread::Builder::new()
            .name("desktop-notify".into())
            .spawn(move || {
                while let Ok((summary, body)) = rx.recv() {
                    send(&summary, &body);
                }
            })
            .expect("failed to spawn desktop-notification thread");
        tx
    })
}

/// Send a desktop notification. Best-effort: failures only log.
/// MUST run on the dispatcher thread — never inside the tokio runtime.
fn send(summary: &str, body: &str) {
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if let Err(e) = notify_rust::Notification::new()
            .summary(summary)
            .body(body)
            .appname("Lan Mouse")
            .show()
        {
            log::warn!("could not send desktop notification: {e}");
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Err(e) = mac_notification_sys::Notification::default()
            .title(summary)
            .message(body)
            .send()
        {
            log::warn!("could not send desktop notification: {e}");
        }
    }
    #[cfg(windows)]
    {
        // no simple headless toast on Windows without a registered app —
        // keep it in the log; the tray GUI covers the common case.
        log::info!("notification: {summary} — {body}");
    }
}

/// Map a frontend event to `(summary, body)` for a desktop notification.
/// Returns `None` for events that are not user-facing hints (progress
/// ticks, status churn, state sync) — those would be pure spam.
fn text_for_event(event: &FrontendEvent) -> Option<(String, String)> {
    use FrontendEvent::*;
    match event {
        ClipboardShared {
            received,
            kind,
            bytes,
        } => {
            let (what, dir) = (
                match kind {
                    ClipboardContentKind::Text => "Text",
                    ClipboardContentKind::Image => "Image",
                    ClipboardContentKind::Files => "Files",
                },
                if *received { "received" } else { "shared" },
            );
            Some((
                format!("Clipboard {dir}"),
                format!("{what}, {} bytes", human_bytes(*bytes)),
            ))
        }
        ClipboardTooLarge { bytes, limit } => Some((
            "Clipboard too large".to_string(),
            format!(
                "{} exceeds the {} limit",
                human_bytes(*bytes),
                human_bytes(*limit)
            ),
        )),
        DeviceConnected {
            addr,
            fingerprint: _,
        } => Some(("Device connected".to_string(), addr.to_string())),
        IncomingDisconnected(addr) => Some(("Device disconnected".to_string(), addr.to_string())),
        ConnectionAttempt { fingerprint } => Some((
            "Connection attempt".to_string(),
            format!("Device {fingerprint} needs authorization"),
        )),
        PortChanged(port, Some(e)) => Some((
            "Port change failed".to_string(),
            format!("still on {port}: {e}"),
        )),
        Error(e) => Some(("Lan Mouse".to_string(), e.clone())),
        _ => None,
    }
}

/// Minimum gap between two identical desktop notifications — identical
/// failures (e.g. a share that keeps failing on every clipboard poll)
/// would otherwise spam a notification per poll cycle.
const DEDUP_WINDOW: std::time::Duration = std::time::Duration::from_secs(10);

/// dedup gate: `true` when this (summary, body) should be shown now —
/// a repeat of an identical notification inside [`DEDUP_WINDOW`] is
/// suppressed. Extracted so tests can exercise the policy directly.
fn dedup_passes(key: &(String, String)) -> bool {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Instant;
    static RECENT: OnceLock<Mutex<HashMap<(String, String), Instant>>> = OnceLock::new();
    let mut recent = RECENT
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if recent.get(key).is_some_and(|t| t.elapsed() < DEDUP_WINDOW) {
        return false;
    }
    recent.insert(key.clone(), Instant::now());
    true
}

pub(crate) fn notify_for_event(event: &FrontendEvent) {
    let Some((summary, body)) = text_for_event(event) else {
        return;
    };
    let key = (summary.clone(), body.clone());
    if !dedup_passes(&key) {
        return;
    }
    if dispatcher().send((summary, body)).is_err() {
        log::warn!("desktop-notification thread is gone");
    }
}

fn human_bytes(b: usize) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i + 1 < UNITS.len() {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} {}", UNITS[0])
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: notify-rust's blocking `show()` panics inside a
    /// tokio runtime context (zbus drives an internal `block_on`), and
    /// the release profile turns that panic into an abort. The fix
    /// routes sends through a dedicated std thread — this test would
    /// have died before the fix.
    #[tokio::test(flavor = "current_thread")]
    async fn notify_inside_runtime_does_not_panic() {
        notify_for_event(&FrontendEvent::Error("test".to_string()));
        notify_for_event(&FrontendEvent::DeviceConnected {
            addr: "192.0.2.1:4242".parse().unwrap(),
            fingerprint: "ab:cd".to_string(),
        });
    }

    #[test]
    fn identical_notifications_are_deduped() {
        let key = ("dedup-test".to_string(), "unique-body".to_string());
        assert!(dedup_passes(&key), "first occurrence must pass");
        assert!(
            !dedup_passes(&key),
            "identical repeat inside the window must be suppressed"
        );
        let other = ("dedup-test".to_string(), "other-body".to_string());
        assert!(dedup_passes(&other), "a different body still passes");
    }

    #[test]
    fn error_maps_to_notification_text() {
        let (s, b) = text_for_event(&FrontendEvent::Error("boom".to_string())).unwrap();
        assert_eq!(s, "Lan Mouse");
        assert_eq!(b, "boom");
    }
}
