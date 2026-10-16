//! Read-only native clipboard refresh probe. Never writes or sends clipboard data.
use input_capture::{CaptureEvent, clipboard::ClipboardMonitor};
use input_event::{ClipboardEvent, Event};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut monitor = ClipboardMonitor::new()?;
    monitor.refresh().await?;
    let mut first = 0;
    while let Some(event) = monitor.try_recv() {
        if matches!(
            event,
            CaptureEvent::Input(Event::Clipboard(ClipboardEvent::Files(_)))
        ) {
            first += 1;
        } else {
            return Err("native probe requires an existing file selection".into());
        }
    }
    assert_eq!(first, 1);
    let mut repeated = 0;
    for _ in 0..100 {
        monitor.refresh().await?;
        while monitor.try_recv().is_some() {
            repeated += 1;
        }
    }
    println!("initial_file_events={first} unchanged_refreshes=100 repeated_events={repeated}");
    assert_eq!(
        repeated, 0,
        "unchanged native clipboard refresh emitted another copy"
    );
    Ok(())
}
