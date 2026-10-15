//! Exercises daemon-owned history and native notices without a GUI or clipboard.
use lan_mouse::file_activity::{FileActivity, load};
use lan_mouse_ipc::{FilePhase, FileTransfer};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let path = std::path::PathBuf::from(std::env::args().nth(1).ok_or("history path required")?);
    let activity = FileActivity::new(path.clone(), Vec::new(), true)?;
    let mut record = FileTransfer {
        id: "native-notification-probe".into(),
        incoming: false,
        names: vec!["Lan Mouse 通知测试.bin".into()],
        count: 1,
        total: 100,
        done: 0,
        phase: FilePhase::Connecting,
        order: 1,
        reused: false,
        advertised: false,
        error: None,
    };
    for (phase, done) in [
        (FilePhase::Connecting, 0),
        (FilePhase::Transferring, 50),
        (FilePhase::Complete, 100),
    ] {
        record.phase = phase;
        record.done = done;
        record.advertised = phase == FilePhase::Complete;
        activity.update(vec![record.clone()]);
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    drop(activity);
    assert_eq!(load(&path)?, vec![record]);
    #[cfg(windows)]
    {
        use windows::{UI::Notifications::ToastNotificationManager, core::HSTRING};
        let notices = ToastNotificationManager::History()?
            .GetHistoryWithId(&HSTRING::from("LanMouse.FileTransfers"))?;
        let count = (0..notices.Size()?)
            .filter(|index| {
                notices
                    .GetAt(*index)
                    .and_then(|n| n.Group())
                    .is_ok_and(|group| group == "files")
            })
            .count();
        assert!(count >= 1, "native notification history is empty");
        println!("native_notification_count={count}");
    }
    println!("persistent_history_verified=true");
    Ok(())
}
