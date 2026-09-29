use super::error::{EmulationError, WindowsEmulationCreationError};
use input_event::{
    BTN_BACK, BTN_FORWARD, BTN_LEFT, BTN_MIDDLE, BTN_RIGHT, Event, KeyboardEvent, PointerEvent,
    scancode,
};

use async_trait::async_trait;
use std::io;
use std::ops::BitOrAssign;
use tokio::task::AbortHandle;
use windows::Win32::System::StationsAndDesktops::{
    CloseDesktop, DESKTOP_ACCESS_FLAGS, DESKTOP_CONTROL_FLAGS, GetThreadDesktop, OpenInputDesktop,
    SetThreadDesktop,
};
use windows::Win32::System::Threading::GetCurrentThreadId;
use windows::Win32::UI::Input::KeyboardAndMouse::{
    INPUT, INPUT_KEYBOARD, INPUT_MOUSE, KEYBDINPUT, KEYEVENTF_KEYUP, KEYEVENTF_SCANCODE,
    MOUSEEVENTF_HWHEEL, MOUSEEVENTF_LEFTDOWN, MOUSEEVENTF_LEFTUP, MOUSEEVENTF_MIDDLEDOWN,
    MOUSEEVENTF_MIDDLEUP, MOUSEEVENTF_MOVE, MOUSEEVENTF_RIGHTDOWN, MOUSEEVENTF_RIGHTUP,
    MOUSEEVENTF_WHEEL, MOUSEINPUT,
};
use windows::Win32::UI::Input::KeyboardAndMouse::{
    INPUT_0, KEYEVENTF_EXTENDEDKEY, MOUSEEVENTF_XDOWN, MOUSEEVENTF_XUP, SendInput,
};
use windows::Win32::UI::WindowsAndMessaging::{
    GetSystemMetrics, SM_CXVIRTUALSCREEN, SM_CYVIRTUALSCREEN, SM_XVIRTUALSCREEN, SM_YVIRTUALSCREEN,
    SetCursorPos, XBUTTON1, XBUTTON2,
};

use super::{Emulation, EmulationHandle, EmulationOptions, Position};

// Desktop access rights for input injection
// GENERIC_WRITE (0x40000000) + DESKTOP_CREATEWINDOW (0x0002) + DESKTOP_HOOKCONTROL (0x0008)
// DF_ALLOWOTHERACCOUNTHOOK (0x0001) allows accessing desktops owned by other accounts
const GENERIC_WRITE: u32 = 0x40000000;
const DESKTOP_CREATEWINDOW: u32 = 0x0002;
const DESKTOP_HOOKCONTROL: u32 = 0x0008;
const DF_ALLOWOTHERACCOUNTHOOK: u32 = 0x0001;
const DESKTOP_ACCESS_FOR_INPUT: u32 = DESKTOP_CREATEWINDOW | DESKTOP_HOOKCONTROL | GENERIC_WRITE;

// Linux keycodes for modifier tracking
const KEY_LEFT_META: u32 = 125;
const KEY_RIGHT_META: u32 = 126;
// Linux keycode for L
const KEY_L: u32 = 38;

pub(crate) struct WindowsEmulation {
    repeat_task: Option<AbortHandle>,
    options: EmulationOptions,
    meta_pressed: bool,
}

impl WindowsEmulation {
    pub(crate) fn new(options: EmulationOptions) -> Result<Self, WindowsEmulationCreationError> {
        Ok(Self {
            repeat_task: None,
            options,
            meta_pressed: false,
        })
    }
}

#[async_trait]
impl Emulation for WindowsEmulation {
    async fn consume(&mut self, event: Event, _: EmulationHandle) -> Result<(), EmulationError> {
        match event {
            Event::Pointer(pointer_event) => match pointer_event {
                PointerEvent::Motion { time: _, dx, dy } => {
                    rel_mouse(dx as i32, dy as i32)?;
                }
                PointerEvent::Button {
                    time: _,
                    button,
                    state,
                } => mouse_button(button, state)?,
                PointerEvent::Axis {
                    time: _,
                    axis,
                    value,
                } => scroll(axis, value as i32)?,
                PointerEvent::AxisDiscrete120 { axis, value } => scroll(axis, value)?,
            },
            Event::Keyboard(keyboard_event) => match keyboard_event {
                KeyboardEvent::Key {
                    time: _,
                    key,
                    state,
                } => {
                    // Track Meta/Super key state
                    if key == KEY_LEFT_META || key == KEY_RIGHT_META {
                        self.meta_pressed = state == 1;
                    }

                    // Intercept Win+L: LockWorkStation() cannot be triggered
                    // via SendInput because Windows blocks it as a Secure
                    // Attention Sequence. Instead we lock the session directly.
                    if key == KEY_L && state == 1 && self.meta_pressed {
                        log::info!("Win+L detected, locking workstation");
                        lock_workstation();
                        return Ok(());
                    }

                    match state {
                        // pressed
                        0 => self.kill_repeat_task(),
                        1 => self.spawn_repeat_task(key).await,
                        _ => {}
                    }
                    key_event(key, state)?;
                }
                KeyboardEvent::Modifiers { .. } => {}
            },
            Event::Clipboard(_) => {
                // Clipboard events are not emulated through this backend
                // They are handled directly by the clipboard emulation module
                log::debug!("ignoring clipboard event in windows emulation");
            }
        }
        // FIXME
        Ok(())
    }

    async fn create(&mut self, _handle: EmulationHandle) {}

    async fn destroy(&mut self, _handle: EmulationHandle) {}

    async fn terminate(&mut self) {}

    async fn warp(&mut self, _handle: EmulationHandle, pos: Position, t: f64) {
        let Some((x, y)) = warp_target(pos, t) else {
            log::warn!("could not determine virtual screen bounds for cursor warp");
            return;
        };
        // SAFETY: SetCursorPos with in-bounds screen coordinates is
        // always safe to call.
        if let Err(e) = unsafe { SetCursorPos(x, y) } {
            log::warn!("failed to warp cursor to {pos:?} @ {t:.2}: {e:?}");
        }
    }
}

impl WindowsEmulation {
    async fn spawn_repeat_task(&mut self, key: u32) {
        // there can only be one repeating key and it's
        // always the last to be pressed
        self.kill_repeat_task();
        let repeat_delay = self.options.key_repeat_delay;
        let repeat_interval = self.options.key_repeat_interval;
        let repeat_task = tokio::task::spawn_local(async move {
            tokio::time::sleep(repeat_delay).await;
            loop {
                if let Err(e) = key_event(key, 1) {
                    // Retrying a refused injection in a tight loop would freeze this task
                    // forever, so stop repeating and let the error reach the caller.
                    log::warn!("stopping key repeat for key {key}: {e}");
                    break;
                }
                tokio::time::sleep(repeat_interval).await;
            }
        });
        self.repeat_task = Some(repeat_task.abort_handle());
    }
    fn kill_repeat_task(&mut self) {
        if let Some(task) = self.repeat_task.take() {
            task.abort();
        }
    }
}

/// Number of attempts made for a single `SendInput` call before reporting a failure.
///
/// `SendInput` returns zero when the injection is refused, for example while a
/// higher-integrity window has focus. Retrying forever would block this task
/// completely, so give up after a few attempts and return an error that the caller
/// can act on.
const MAX_SEND_INPUT_ATTEMPTS: usize = 3;

/// Submits one input event, returning an error if the operating system refuses it.
fn send_input(input: INPUT) -> Result<(), EmulationError> {
    for _ in 0..MAX_SEND_INPUT_ATTEMPTS {
        // SAFETY: `input` is a fully initialized `INPUT` and the slice length matches
        // the `cbSize` argument, as required by `SendInput`.
        if unsafe { SendInput(&[input], std::mem::size_of::<INPUT>() as i32) } > 0 {
            return Ok(());
        }
    }
    Err(EmulationError::Io(io::Error::other(
        "SendInput refused the event",
    )))
}

/// Send input with desktop switching to handle UAC prompts and other secure desktops.
/// When running in a user session (spawned by the Windows service), this allows
/// input injection on the Secure Desktop (UAC prompts) by temporarily switching
/// to the current input desktop.
fn send_input_safe(input: INPUT) -> Result<(), EmulationError> {
    unsafe {
        // Try to open the current input desktop (may be Secure Desktop during UAC)
        // This only works when running in the user's session, not from Session 0
        let input_desktop = match OpenInputDesktop(
            DESKTOP_CONTROL_FLAGS(DF_ALLOWOTHERACCOUNTHOOK),
            true, // fInherit
            DESKTOP_ACCESS_FLAGS(DESKTOP_ACCESS_FOR_INPUT),
        ) {
            Ok(desktop) => desktop,
            Err(e) => {
                // Desktop switching not available - fall back to direct SendInput
                // This works for normal desktop but won't reach UAC/login screen
                log::debug!("OpenInputDesktop failed: {} - using direct SendInput", e);
                return send_input(input);
            }
        };

        // Save current desktop, switch to input desktop, send input, restore
        let old_desktop = GetThreadDesktop(GetCurrentThreadId());

        if SetThreadDesktop(input_desktop).is_err() {
            log::warn!("SetThreadDesktop failed, using direct SendInput");
            let _ = CloseDesktop(input_desktop);
            return send_input(input);
        }

        let result = send_input(input);

        // Restore original desktop
        if let Ok(desktop) = old_desktop {
            if !desktop.is_invalid() {
                let _ = SetThreadDesktop(desktop);
            }
        }
        let _ = CloseDesktop(input_desktop);
        result
    }
}

fn send_mouse_input(mi: MOUSEINPUT) -> Result<(), EmulationError> {
    send_input_safe(INPUT {
        r#type: INPUT_MOUSE,
        Anonymous: INPUT_0 { mi },
    })
}

fn send_keyboard_input(ki: KEYBDINPUT) -> Result<(), EmulationError> {
    send_input_safe(INPUT {
        r#type: INPUT_KEYBOARD,
        Anonymous: INPUT_0 { ki },
    })
}
fn rel_mouse(dx: i32, dy: i32) -> Result<(), EmulationError> {
    let mi = MOUSEINPUT {
        dx,
        dy,
        mouseData: 0,
        dwFlags: MOUSEEVENTF_MOVE,
        time: 0,
        dwExtraInfo: 0,
    };
    send_mouse_input(mi)
}

fn mouse_button(button: u32, state: u32) -> Result<(), EmulationError> {
    let dw_flags = match state {
        0 => match button {
            BTN_LEFT => MOUSEEVENTF_LEFTUP,
            BTN_RIGHT => MOUSEEVENTF_RIGHTUP,
            BTN_MIDDLE => MOUSEEVENTF_MIDDLEUP,
            BTN_BACK => MOUSEEVENTF_XUP,
            BTN_FORWARD => MOUSEEVENTF_XUP,
            _ => return Ok(()),
        },
        1 => match button {
            BTN_LEFT => MOUSEEVENTF_LEFTDOWN,
            BTN_RIGHT => MOUSEEVENTF_RIGHTDOWN,
            BTN_MIDDLE => MOUSEEVENTF_MIDDLEDOWN,
            BTN_BACK => MOUSEEVENTF_XDOWN,
            BTN_FORWARD => MOUSEEVENTF_XDOWN,
            _ => return Ok(()),
        },
        _ => return Ok(()),
    };
    let mouse_data = match button {
        BTN_BACK => XBUTTON1 as u32,
        BTN_FORWARD => XBUTTON2 as u32,
        _ => 0,
    };
    let mi = MOUSEINPUT {
        dx: 0,
        dy: 0, // no movement
        mouseData: mouse_data,
        dwFlags: dw_flags,
        time: 0,
        dwExtraInfo: 0,
    };
    send_mouse_input(mi)
}

fn scroll(axis: u8, value: i32) -> Result<(), EmulationError> {
    // WHEEL is positive up but HWHEEL positive right, like lan-mouse's horizontal axis
    let (event_type, value) = match axis {
        0 => (MOUSEEVENTF_WHEEL, value.saturating_neg()),
        1 => (MOUSEEVENTF_HWHEEL, value),
        _ => return Ok(()),
    };
    let mi = MOUSEINPUT {
        dx: 0,
        dy: 0,
        mouseData: value as u32,
        dwFlags: event_type,
        time: 0,
        dwExtraInfo: 0,
    };
    send_mouse_input(mi)
}

/// Absolute coordinate for a normalized (`0.0..=1.0`) cross-axis
/// position within `min..=max`. Inverse of the capture-side
/// normalization in `input-capture`'s macOS backend.
fn denormalize(t: f64, min: f64, max: f64) -> f64 {
    min + t.clamp(0.0, 1.0) * (max - min)
}

/// The point to warp the cursor to when a peer's cursor enters this
/// device from `pos` at normalized cross-axis position `t`, in virtual
/// screen coordinates. `None` if the virtual screen bounds can't be
/// determined.
fn warp_target(pos: Position, t: f64) -> Option<(i32, i32)> {
    // SAFETY: GetSystemMetrics with a valid SYSTEM_METRICS_INDEX is
    // always safe to call.
    let (x0, y0, cx, cy) = unsafe {
        (
            GetSystemMetrics(SM_XVIRTUALSCREEN),
            GetSystemMetrics(SM_YVIRTUALSCREEN),
            GetSystemMetrics(SM_CXVIRTUALSCREEN),
            GetSystemMetrics(SM_CYVIRTUALSCREEN),
        )
    };
    if cx <= 0 || cy <= 0 {
        return None;
    }
    let (xmin, xmax) = (x0 as f64, (x0 + cx) as f64);
    let (ymin, ymax) = (y0 as f64, (y0 + cy) as f64);
    let edge_offset = 1.0;
    let (x, y) = match pos {
        Position::Left => (xmin + edge_offset, denormalize(t, ymin, ymax)),
        Position::Right => (xmax - edge_offset, denormalize(t, ymin, ymax)),
        Position::Top => (denormalize(t, xmin, xmax), ymin + edge_offset),
        Position::Bottom => (denormalize(t, xmin, xmax), ymax - edge_offset),
    };
    Some((x as i32, y as i32))
}

fn key_event(key: u32, state: u8) -> Result<(), EmulationError> {
    let scancode = match linux_keycode_to_windows_scancode(key) {
        Some(code) => code,
        None => return Ok(()),
    };
    let extended = scancode > 0xff;
    let scancode = scancode & 0xff;
    let mut flags = KEYEVENTF_SCANCODE;
    if extended {
        flags.bitor_assign(KEYEVENTF_EXTENDEDKEY);
    }
    if state == 0 {
        flags.bitor_assign(KEYEVENTF_KEYUP);
    }
    let ki = KEYBDINPUT {
        wVk: Default::default(),
        wScan: scancode,
        dwFlags: flags,
        time: 0,
        dwExtraInfo: 0,
    };
    send_keyboard_input(ki)
}

fn linux_keycode_to_windows_scancode(linux_keycode: u32) -> Option<u16> {
    let linux_scancode = match scancode::Linux::try_from(linux_keycode) {
        Ok(s) => s,
        Err(_) => {
            log::warn!("unknown keycode: {linux_keycode}");
            return None;
        }
    };
    log::trace!("linux code: {linux_scancode:?}");
    let windows_scancode = match scancode::Windows::try_from(linux_scancode) {
        Ok(s) => s,
        Err(_) => {
            log::warn!("failed to translate linux code into windows scancode: {linux_scancode:?}");
            return None;
        }
    };
    log::trace!("windows code: {windows_scancode:?}");
    Some(windows_scancode as u16)
}

/// Lock the workstation.
///
/// `Win+L` is a Secure Attention Sequence that Windows blocks from being
/// injected via `SendInput`.  When running inside a user session (Session != 0)
/// we can call `LockWorkStation()` which is the documented public API.
///
/// When running in Session 0 (the service session) there is no interactive
/// desktop to lock, so we disconnect the console session via
/// `WTSDisconnectSession` which achieves the same visible effect (returns to
/// the lock / login screen).
fn lock_workstation() {
    // Try the simple path first — works when we are in the user's session.
    unsafe {
        use windows::Win32::System::Shutdown::LockWorkStation;
        if LockWorkStation().is_ok() {
            log::info!("LockWorkStation succeeded");
            return;
        }
        log::warn!("LockWorkStation failed, trying WTSDisconnectSession");

        // Fallback for Session 0: disconnect the active console session.
        use windows::Win32::System::RemoteDesktop::{
            WTS_CURRENT_SERVER_HANDLE, WTSDisconnectSession, WTSGetActiveConsoleSessionId,
        };
        let session_id = WTSGetActiveConsoleSessionId();
        if WTSDisconnectSession(Some(WTS_CURRENT_SERVER_HANDLE), session_id, true).is_err() {
            log::error!("WTSDisconnectSession also failed");
        } else {
            log::info!("WTSDisconnectSession succeeded (session {})", session_id);
        }
    }
}

#[cfg(test)]
mod warp_test {
    use super::denormalize;

    #[test]
    fn midpoint() {
        assert_eq!(denormalize(0.5, 0.0, 100.0), 50.0);
    }

    #[test]
    fn extremes() {
        assert_eq!(denormalize(0.0, 0.0, 100.0), 0.0);
        assert_eq!(denormalize(1.0, 0.0, 100.0), 100.0);
    }

    #[test]
    fn clamps_out_of_range_t() {
        assert_eq!(denormalize(-0.5, 0.0, 100.0), 0.0);
        assert_eq!(denormalize(1.5, 0.0, 100.0), 100.0);
    }

    #[test]
    fn offset_bounds() {
        // a virtual screen that doesn't start at the origin, e.g. a
        // monitor placed to the left of / above the primary display
        assert_eq!(denormalize(0.5, -500.0, -300.0), -400.0);
    }
}
