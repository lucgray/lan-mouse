//! Explicit desktop test: stop Lan Mouse first and restore the cursor afterward.
//! Moves the pointer only; never presses keys/buttons or reads the clipboard.
#[cfg(all(
    target_os = "linux",
    feature = "layer_shell_capture",
    feature = "evdev_emulation"
))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use futures::StreamExt;
    use input_capture::{Backend, CaptureEvent, InputCapture, Position};
    use input_emulation::{Backend as EmulationBackend, InputEmulation};
    use input_event::{Event, PointerEvent};
    use std::time::Duration;

    if std::env::args().nth(1).as_deref() != Some("--move-pointer") {
        return Err("requires --move-pointer and a stopped Lan Mouse service".into());
    }
    env_logger::init();
    let mut capture = InputCapture::new(Some(Backend::LayerShell), Default::default()).await?;
    let mut emulation = InputEmulation::new(
        Some(EmulationBackend::Evdev),
        Default::default(),
        Default::default(),
    )
    .await?;
    emulation.create(1).await;
    capture.create_enter_only(1, Position::Left).await?;
    capture.remote_enter()?;
    // Let the compositor configure/map the edge before moving into it.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), capture.next())
            .await
            .is_err()
    );
    emulation
        .consume(
            Event::Pointer(PointerEvent::Motion {
                time: 0,
                dx: -10000.,
                dy: 0.,
            }),
            1,
        )
        .await?;
    let first = tokio::time::timeout(Duration::from_secs(2), capture.next())
        .await?
        .ok_or("capture ended")??;
    assert!(matches!(first, (1, CaptureEvent::Begin(_))));
    capture.release().await?;
    // Keep the pointer on the same surface: there will be no second native Enter.
    capture.remote_enter()?;
    emulation
        .consume(
            Event::Pointer(PointerEvent::Motion {
                time: 1,
                dx: -1.,
                dy: 0.,
            }),
            1,
        )
        .await?;
    let second = tokio::time::timeout(Duration::from_secs(2), capture.next())
        .await?
        .ok_or("capture ended")??;
    assert!(matches!(second, (1, CaptureEvent::Begin(_))));
    for time in 2..102 {
        emulation
            .consume(
                Event::Pointer(PointerEvent::Motion {
                    time,
                    dx: -1.,
                    dy: 0.,
                }),
                1,
            )
            .await?;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(300), capture.next())
            .await
            .is_err()
    );
    capture.release().await?;
    capture.destroy(1).await?;
    capture.terminate().await?;
    emulation.terminate().await;
    println!("first_return=1 same_surface_return=1 extra_outward_motions=100 duplicate_returns=0");
    Ok(())
}

#[cfg(not(all(
    target_os = "linux",
    feature = "layer_shell_capture",
    feature = "evdev_emulation"
)))]
fn main() {
    eprintln!("return_edge_probe requires Linux layer-shell capture and evdev emulation");
    std::process::exit(1);
}
