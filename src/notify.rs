//! OS-level notifications as a fallback channel for user-facing hints.
//!
//! Every user-facing event also reaches [`notify_for_event`]; when no
//! frontend is connected the service calls it so hints like "clipboard
//! shared" still show up as real system notifications instead of only
//! living inside a window that may not exist (daemon-only, tray-hidden).

use input_event::ClipboardContentKind;
use lan_mouse_ipc::FrontendEvent;

/// Send a desktop notification. Best-effort: failures only log.
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

pub(crate) fn notify_for_event(event: &FrontendEvent) {
    let Some((summary, body)) = text_for_event(event) else {
        return;
    };
    // `send` blocks on DBus via notify-rust → zbus, whose `block_on`
    // (tokio feature) panics when called from a thread inside a tokio
    // runtime context ("Cannot start a runtime from within a runtime").
    // Blocking pool threads carry no runtime context, so the nested
    // `Runtime::block_on` is legal there.
    tokio::task::spawn_blocking(move || send(&summary, &body));
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
