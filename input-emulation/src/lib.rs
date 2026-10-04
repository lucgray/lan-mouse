use async_trait::async_trait;
use std::{collections::HashMap, fmt::Display, time::Duration};

use input_event::{Event, KeyboardEvent, PointerEvent};

pub use self::error::{EmulationCreationError, EmulationError, InputEmulationError};

#[cfg(windows)]
mod windows;

#[cfg(x11)]
mod x11;

#[cfg(wlroots)]
mod wlroots;

#[cfg(rdp)]
mod xdg_desktop_portal;

#[cfg(libei)]
mod libei;

#[cfg(evdev)]
mod evdev;

#[cfg(target_os = "macos")]
mod macos;

#[cfg(any(windows, test))]
mod repeat;

pub mod clipboard;
/// fallback input emulation (logs events)
mod dummy;
mod error;
#[cfg(any(windows, x11, evdev, test))]
mod motion;
#[cfg(any(wlroots, libei, rdp, x11))]
mod scroll_accumulator;

pub type EmulationHandle = u64;

/// Upper bound applied to each cleanup step performed by
/// [`InputEmulation::destroy_bounded`] and [`InputEmulation::terminate`].
///
/// Cleanup must not block the emulation task forever, e.g. when a backend connection is
/// backed up and flushing the key/button releases keeps returning `WouldBlock`.
const DEFAULT_CLEANUP_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_TERMINATION_TIMEOUT: Duration = Duration::from_secs(1);

/// Default delay before a held key begins repeating.
pub const DEFAULT_KEY_REPEAT_DELAY: Duration = Duration::from_millis(500);

/// Default interval between repeats once a held key has started repeating.
pub const DEFAULT_KEY_REPEAT_INTERVAL: Duration = Duration::from_millis(32);

/// Tunable options for input-emulation backends.
///
/// These currently only affect the macOS and Windows backends: on those
/// platforms synthetic key events are not auto-repeated by the OS, so lan-mouse
/// regenerates key repeats itself. The other backends leave key repeat to the
/// receiving compositor / OS and ignore these values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmulationOptions {
    /// How long a key must be held before it starts repeating.
    pub key_repeat_delay: Duration,
    /// The interval between repeats once a key has started repeating.
    pub key_repeat_interval: Duration,
}

impl Default for EmulationOptions {
    fn default() -> Self {
        Self {
            key_repeat_delay: DEFAULT_KEY_REPEAT_DELAY,
            key_repeat_interval: DEFAULT_KEY_REPEAT_INTERVAL,
        }
    }
}

/// Edge a peer's cursor entered this device from, used by
/// [`InputEmulation::warp`] to place the cursor on the matching edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    Left,
    Right,
    Top,
    Bottom,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Backend {
    #[cfg(evdev)]
    Evdev,
    #[cfg(wlroots)]
    Wlroots,
    #[cfg(libei)]
    Libei,
    #[cfg(rdp)]
    Xdp,
    #[cfg(x11)]
    X11,
    #[cfg(windows)]
    Windows,
    #[cfg(target_os = "macos")]
    MacOs,
    Dummy,
}

impl Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(evdev)]
            Backend::Evdev => write!(f, "evdev"),
            #[cfg(wlroots)]
            Backend::Wlroots => write!(f, "wlroots"),
            #[cfg(libei)]
            Backend::Libei => write!(f, "libei"),
            #[cfg(rdp)]
            Backend::Xdp => write!(f, "xdg-desktop-portal"),
            #[cfg(x11)]
            Backend::X11 => write!(f, "X11"),
            #[cfg(windows)]
            Backend::Windows => write!(f, "windows"),
            #[cfg(target_os = "macos")]
            Backend::MacOs => write!(f, "macos"),
            Backend::Dummy => write!(f, "dummy"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct InputConfig {
    pub invert_scroll: bool,
    pub mouse_sensitivity: f64,
}

impl Default for InputConfig {
    fn default() -> Self {
        Self {
            invert_scroll: false,
            mouse_sensitivity: 1.0,
        }
    }
}

fn post_process_event(event: Event, config: InputConfig) -> Event {
    match event {
        Event::Pointer(PointerEvent::Motion { time, dx, dy }) => {
            Event::Pointer(PointerEvent::Motion {
                time,
                dx: (dx * config.mouse_sensitivity).clamp(-f64::MAX, f64::MAX),
                dy: (dy * config.mouse_sensitivity).clamp(-f64::MAX, f64::MAX),
            })
        }
        Event::Pointer(PointerEvent::AxisDiscrete120 { axis, value }) if config.invert_scroll => {
            Event::Pointer(PointerEvent::AxisDiscrete120 {
                axis,
                value: value.saturating_neg(),
            })
        }
        Event::Pointer(PointerEvent::Axis { time, axis, value }) if config.invert_scroll => {
            Event::Pointer(PointerEvent::Axis {
                time,
                axis,
                value: -value,
            })
        }
        _ => event,
    }
}

pub struct InputEmulation {
    emulation: Box<dyn Emulation>,
    handles: HashMap<EmulationHandle, TrackedInput>,
    input_config: InputConfig,
    /// Bound applied to each backend operation during cleanup.
    cleanup_timeout: Duration,
    termination_timeout: Duration,
    last_cleanup_handle: Option<EmulationHandle>,
}

/// Delivery state for one key or pointer-button transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TrackedTransition {
    /// A press was submitted but the backend future was cancelled before confirmation.
    PressPending,
    /// The backend confirmed the press.
    Pressed,
    /// A release was submitted but the backend future was cancelled before confirmation.
    ReleasePending,
}

/// Input that has been handed to the backend for a single [`EmulationHandle`].
///
/// Pending transitions are retained across cancellation so an essential event replayed by
/// the caller can be submitted again. Confirmed presses remain tracked until a confirmed
/// release or cleanup, so an uncertain/cancelled release is safely retried.
#[derive(Default)]
struct TrackedInput {
    keys: HashMap<u32, TrackedTransition>,
    buttons: HashMap<u32, TrackedTransition>,
}

impl InputEmulation {
    #[cfg_attr(not(any(target_os = "macos", windows)), allow(unused_variables))]
    async fn with_backend(
        backend: Backend,
        options: EmulationOptions,
        input_config: InputConfig,
    ) -> Result<InputEmulation, EmulationCreationError> {
        let emulation: Box<dyn Emulation> = match backend {
            #[cfg(evdev)]
            Backend::Evdev => Box::new(evdev::EvdevEmulation::new()?),
            #[cfg(wlroots)]
            Backend::Wlroots => Box::new(wlroots::WlrootsEmulation::new()?),
            #[cfg(libei)]
            Backend::Libei => Box::new(libei::LibeiEmulation::new().await?),
            #[cfg(x11)]
            Backend::X11 => Box::new(x11::X11Emulation::new()?),
            #[cfg(rdp)]
            Backend::Xdp => Box::new(xdg_desktop_portal::DesktopPortalEmulation::new().await?),
            #[cfg(windows)]
            Backend::Windows => Box::new(windows::WindowsEmulation::new(options)?),
            #[cfg(target_os = "macos")]
            Backend::MacOs => Box::new(macos::MacOSEmulation::new(options)?),
            Backend::Dummy => Box::new(dummy::DummyEmulation::new()),
        };
        let mut input_config = input_config;
        if !input_config.mouse_sensitivity.is_finite() {
            log::warn!("nonfinite mouse sensitivity; using 1.0");
            input_config.mouse_sensitivity = 1.0;
        }
        Ok(Self {
            emulation,
            handles: HashMap::new(),
            input_config,
            cleanup_timeout: DEFAULT_CLEANUP_TIMEOUT,
            termination_timeout: DEFAULT_TERMINATION_TIMEOUT,
            last_cleanup_handle: None,
        })
    }

    pub async fn new(
        backend: Option<Backend>,
        options: EmulationOptions,
        input_config: InputConfig,
    ) -> Result<InputEmulation, EmulationCreationError> {
        if let Some(backend) = backend {
            let b = Self::with_backend(backend, options, input_config).await;
            if b.is_ok() {
                log::info!("using emulation backend: {backend}");
            }
            return b;
        }

        for backend in [
            #[cfg(evdev)]
            Backend::Evdev,
            #[cfg(wlroots)]
            Backend::Wlroots,
            #[cfg(libei)]
            Backend::Libei,
            #[cfg(rdp)]
            Backend::Xdp,
            #[cfg(x11)]
            Backend::X11,
            #[cfg(windows)]
            Backend::Windows,
            #[cfg(target_os = "macos")]
            Backend::MacOs,
            Backend::Dummy,
        ] {
            match Self::with_backend(backend, options, input_config).await {
                Ok(b) => {
                    log::info!("using emulation backend: {backend}");
                    return Ok(b);
                }
                Err(e) if e.cancelled_by_user() => return Err(e),
                Err(e) => log::warn!("{e}"),
            }
        }

        Err(EmulationCreationError::NoAvailableBackend)
    }

    pub async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), EmulationError> {
        event.validate_input()?;
        let event = post_process_event(event, self.input_config);
        event.validate_input()?;
        match event {
            Event::Keyboard(KeyboardEvent::Key { key, state, .. }) => {
                // suppress duplicate presses and unmatched releases
                if !self.track_key(handle, key, state) {
                    return Ok(());
                }
                self.emulation.consume(event, handle).await?;
                self.complete_key_transition(handle, key, state);
                Ok(())
            }
            Event::Pointer(PointerEvent::Button { button, state, .. }) => {
                // suppress duplicate presses and unmatched releases
                if !self.track_button(handle, button, state) {
                    return Ok(());
                }
                self.emulation.consume(event, handle).await?;
                self.complete_button_transition(handle, button, state);
                Ok(())
            }
            _ => self.emulation.consume(event, handle).await,
        }
    }

    pub async fn create(&mut self, handle: EmulationHandle) -> bool {
        if self.handles.contains_key(&handle) {
            return false;
        }
        self.handles.insert(handle, TrackedInput::default());
        self.emulation.create(handle).await;
        true
    }

    /// Warp the cursor to the normalized (`0.0..=1.0`) position `t`
    /// along the given edge, so a cursor entering this device lands at
    /// the same relative spot it left the peer's opposite edge at.
    /// Backends that can't perform an absolute warp leave the cursor
    /// wherever it already was, same as before this existed.
    pub async fn warp(&mut self, handle: EmulationHandle, pos: Position, t: f64) {
        if !t.is_finite() {
            log::warn!("ignoring nonfinite cursor warp position");
            return;
        }
        self.emulation.warp(handle, pos, t).await
    }

    /// Release everything tracked for `handle` and destroy the backend state.
    ///
    /// Convenience wrapper around [`destroy_bounded`](Self::destroy_bounded) that discards
    /// the result; used for the common case where no failure handling is possible.
    pub async fn destroy(&mut self, handle: EmulationHandle) {
        let _ = self.destroy_bounded(handle).await;
    }

    /// Release all keys and pointer buttons tracked for `handle` and then destroy the
    /// backend state for it.
    ///
    /// Both steps are bounded by an internal timeout so that a stalled backend cannot block
    /// cleanup indefinitely. Returns `false` when releasing the tracked input or destroying
    /// the backend did not complete successfully (including a timeout); in that case the
    /// handle and unconfirmed input remain registered so a later
    /// [`terminate_bounded`](Self::terminate_bounded) (or another `destroy_bounded`)
    /// can retry. Confirmed individual releases leave the ledger immediately.
    pub async fn destroy_bounded(&mut self, handle: EmulationHandle) -> bool {
        let Some(tracked) = self.handles.get(&handle) else {
            return true;
        };
        let keys = tracked.keys.keys().copied().collect::<Vec<_>>();
        let buttons = tracked.buttons.keys().copied().collect::<Vec<_>>();

        let released = tokio::time::timeout(
            self.cleanup_timeout,
            self.release_tracked(handle, &keys, &buttons),
        )
        .await;

        if !matches!(released, Ok(Ok(()))) {
            log::warn!("releasing input for handle {handle} did not complete successfully");
            return false;
        }

        let destroyed =
            tokio::time::timeout(self.cleanup_timeout, self.emulation.destroy(handle)).await;
        if destroyed.is_err() {
            log::warn!("destroying emulation for handle {handle} did not complete in time");
            return false;
        }

        self.handles.remove(&handle);
        true
    }

    /// Compatibility wrapper; use `terminate_bounded` when failed cleanup must
    /// remain observable and the instance must be kept for retry.
    pub async fn terminate(&mut self) {
        let _ = self.terminate_bounded().await;
    }

    /// Try cleanup within one aggregate deadline (one second by default).
    /// Returns true only after all handles and the backend termination complete.
    /// Failed input remains tracked and the backend is not terminated while
    /// unreleased handles remain. Retain this instance to retry a false result.
    /// Cleanup rotates between handles so an early stalled peer cannot starve
    /// every later peer on each retry. Deadlines cover yielding operations only.
    pub async fn terminate_bounded(&mut self) -> bool {
        self.emulation.stop_repeating();
        tokio::time::timeout(self.termination_timeout, async {
            let mut handles = self.handles.keys().copied().collect::<Vec<_>>();
            handles.sort_unstable();
            let start = self
                .last_cleanup_handle
                .and_then(|last| handles.iter().position(|&handle| handle > last))
                .unwrap_or(0);
            for offset in 0..handles.len() {
                let handle = handles[(start + offset) % handles.len()];
                self.last_cleanup_handle = Some(handle);
                let _ = self.destroy_bounded(handle).await;
            }
            if !self.handles.is_empty() {
                return false;
            }
            tokio::time::timeout(self.cleanup_timeout, self.emulation.terminate())
                .await
                .is_ok()
        })
        .await
        .unwrap_or(false)
    }

    /// Release all keys currently tracked as pressed for `handle`.
    ///
    /// The handle stays registered and keys only leave the tracked set once the backend
    /// confirmed their release.
    pub async fn release_keys(&mut self, handle: EmulationHandle) -> Result<(), EmulationError> {
        let keys = self
            .handles
            .get(&handle)
            .map(|tracked| tracked.keys.keys().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        self.release_tracked(handle, &keys, &[]).await
    }

    pub fn has_pressed_keys(&self, handle: EmulationHandle) -> bool {
        self.handles
            .get(&handle)
            .is_some_and(|tracked| !tracked.keys.is_empty())
    }

    /// Record an incoming key transition and report whether it must be forwarded.
    ///
    /// Confirmed duplicate presses and unmatched releases are suppressed. A pending press
    /// or release is forwarded again when the caller replays it after cancellation.
    fn track_key(&mut self, handle: EmulationHandle, key: u32, state: u8) -> bool {
        let Some(tracked) = self.handles.get_mut(&handle) else {
            return false;
        };
        if state == 0 {
            let Some(transition) = tracked.keys.get_mut(&key) else {
                return false;
            };
            *transition = TrackedTransition::ReleasePending;
            true
        } else {
            match tracked.keys.entry(key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(TrackedTransition::PressPending);
                    true
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    *entry.get() == TrackedTransition::PressPending
                }
            }
        }
    }

    fn complete_key_transition(&mut self, handle: EmulationHandle, key: u32, state: u8) {
        let Some(tracked) = self.handles.get_mut(&handle) else {
            return;
        };
        if state == 0 {
            tracked.keys.remove(&key);
        } else if let Some(transition) = tracked.keys.get_mut(&key) {
            *transition = TrackedTransition::Pressed;
        }
    }

    /// Same contract as [`track_key`](Self::track_key) for pointer buttons.
    fn track_button(&mut self, handle: EmulationHandle, button: u32, state: u32) -> bool {
        let Some(tracked) = self.handles.get_mut(&handle) else {
            return false;
        };
        if state == 0 {
            let Some(transition) = tracked.buttons.get_mut(&button) else {
                return false;
            };
            *transition = TrackedTransition::ReleasePending;
            true
        } else {
            match tracked.buttons.entry(button) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(TrackedTransition::PressPending);
                    true
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    *entry.get() == TrackedTransition::PressPending
                }
            }
        }
    }

    fn complete_button_transition(&mut self, handle: EmulationHandle, button: u32, state: u32) {
        let Some(tracked) = self.handles.get_mut(&handle) else {
            return;
        };
        if state == 0 {
            tracked.buttons.remove(&button);
        } else if let Some(transition) = tracked.buttons.get_mut(&button) {
            *transition = TrackedTransition::Pressed;
        }
    }

    /// Release the given keys and buttons plus the modifier state for `handle`.
    ///
    /// Sending a release for input the backend never applied is harmless, so a superset of
    /// the actual backend state is released.
    async fn release_tracked(
        &mut self,
        handle: EmulationHandle,
        keys: &[u32],
        buttons: &[u32],
    ) -> Result<(), EmulationError> {
        for &key in keys {
            if let Ok(scancode) = input_event::scancode::Linux::try_from(key) {
                log::warn!("releasing stuck key: {scancode:?}");
            }
            let event = Event::Keyboard(KeyboardEvent::Key {
                time: 0,
                key,
                state: 0,
            });
            self.track_key(handle, key, 0);
            self.emulation.consume(event, handle).await?;
            self.complete_key_transition(handle, key, 0);
        }

        for &button in buttons {
            log::warn!("releasing stuck button: {button}");
            let event = Event::Pointer(PointerEvent::Button {
                time: 0,
                button,
                state: 0,
            });
            self.track_button(handle, button, 0);
            self.emulation.consume(event, handle).await?;
            self.complete_button_transition(handle, button, 0);
        }

        let event = Event::Keyboard(KeyboardEvent::Modifiers {
            depressed: 0,
            latched: 0,
            locked: 0,
            group: 0,
        });
        self.emulation.consume(event, handle).await
    }

    pub fn update_config(&mut self, mut input_config: InputConfig) {
        if !input_config.mouse_sensitivity.is_finite() {
            log::warn!("nonfinite mouse sensitivity; preserving previous multiplier");
            input_config.mouse_sensitivity = self.input_config.mouse_sensitivity;
        }
        self.input_config = input_config;
    }
}

#[async_trait]
trait Emulation: Send {
    /// Stop generating repeats without closing the backend needed for release.
    fn stop_repeating(&mut self) {}
    async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), EmulationError>;
    async fn create(&mut self, handle: EmulationHandle);
    async fn destroy(&mut self, handle: EmulationHandle);
    async fn terminate(&mut self);
    /// Warp the cursor to the normalized cross-axis position `t` along
    /// `pos`. Best-effort: a no-op default for backends that can't do
    /// an absolute warp, or platforms not implemented yet.
    async fn warp(&mut self, _handle: EmulationHandle, _pos: Position, _t: f64) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use input_event::{BTN_LEFT, KeyboardEvent, PointerEvent};
    use std::{
        future::pending,
        sync::{Arc, Mutex},
        time::Instant,
    };

    /// Backend used by the tests. It records everything it is asked to do and can be told
    /// to stall or fail on specific events, so cancellation and timeout paths are
    /// observable.
    #[derive(Default)]
    struct MockControl {
        consumed: Vec<(EmulationHandle, Event)>,
        destroyed: Vec<EmulationHandle>,
        warps: Vec<(EmulationHandle, Position, f64)>,
        terminated: bool,
        repeat_stopped: bool,
        /// `consume` waits forever for this event instead of recording it
        stall_on: Option<Event>,
        /// `consume` records this event and then returns an error
        fail_on: Option<Event>,
        /// `destroy`/`terminate` wait forever
        stall_destroy: bool,
        stall_terminate: bool,
    }

    struct MockEmulation {
        control: Arc<Mutex<MockControl>>,
    }

    impl MockEmulation {
        fn new() -> (Self, Arc<Mutex<MockControl>>) {
            let control = Arc::new(Mutex::new(MockControl::default()));
            (
                Self {
                    control: control.clone(),
                },
                control,
            )
        }
    }

    #[async_trait]
    impl Emulation for MockEmulation {
        fn stop_repeating(&mut self) {
            self.control.lock().unwrap().repeat_stopped = true;
        }
        async fn consume(
            &mut self,
            event: Event,
            handle: EmulationHandle,
        ) -> Result<(), EmulationError> {
            let (stall, fail) = {
                let mut control = self.control.lock().unwrap();
                if control.stall_on.as_ref() == Some(&event) {
                    (true, false)
                } else {
                    control.consumed.push((handle, event.clone()));
                    (false, control.fail_on.as_ref() == Some(&event))
                }
            };
            if stall {
                pending::<()>().await;
            }
            if fail {
                return Err(EmulationError::EndOfStream);
            }
            Ok(())
        }

        async fn warp(&mut self, handle: EmulationHandle, pos: Position, t: f64) {
            self.control.lock().unwrap().warps.push((handle, pos, t));
        }

        async fn create(&mut self, _: EmulationHandle) {}

        async fn destroy(&mut self, handle: EmulationHandle) {
            let stall = {
                let mut control = self.control.lock().unwrap();
                control.destroyed.push(handle);
                control.stall_destroy
            };
            if stall {
                pending::<()>().await;
            }
        }

        async fn terminate(&mut self) {
            let stall = {
                let mut control = self.control.lock().unwrap();
                control.terminated = true;
                control.stall_terminate
            };
            if stall {
                pending::<()>().await;
            }
        }
    }

    fn emulation_with(control: &Arc<Mutex<MockControl>>) -> InputEmulation {
        InputEmulation {
            emulation: Box::new(MockEmulation {
                control: control.clone(),
            }),
            handles: HashMap::new(),
            input_config: InputConfig::default(),
            cleanup_timeout: Duration::from_millis(20),
            termination_timeout: Duration::from_millis(60),
            last_cleanup_handle: None,
        }
    }

    fn key_event(key: u32, state: u8) -> Event {
        Event::Keyboard(KeyboardEvent::Key {
            time: 0,
            key,
            state,
        })
    }

    fn button_event(button: u32, state: u32) -> Event {
        Event::Pointer(PointerEvent::Button {
            time: 0,
            button,
            state,
        })
    }

    fn consumed(control: &Arc<Mutex<MockControl>>) -> Vec<(EmulationHandle, Event)> {
        control.lock().unwrap().consumed.clone()
    }

    #[tokio::test]
    async fn suppresses_duplicate_presses_and_unmatched_releases() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.create(1).await;

        // duplicate press is suppressed
        emulation.consume(key_event(30, 1), 0).await.unwrap();
        emulation.consume(key_event(30, 1), 0).await.unwrap();
        // release on the wrong handle is unmatched
        emulation.consume(key_event(30, 0), 1).await.unwrap();
        // release on the owning handle is forwarded
        emulation.consume(key_event(30, 0), 0).await.unwrap();
        // a second release is unmatched again
        emulation.consume(key_event(30, 0), 0).await.unwrap();

        // same for pointer buttons
        emulation
            .consume(button_event(BTN_LEFT, 1), 1)
            .await
            .unwrap();
        emulation
            .consume(button_event(BTN_LEFT, 1), 1)
            .await
            .unwrap();
        emulation
            .consume(button_event(BTN_LEFT, 0), 0)
            .await
            .unwrap();
        emulation
            .consume(button_event(BTN_LEFT, 0), 1)
            .await
            .unwrap();
        emulation
            .consume(button_event(BTN_LEFT, 0), 1)
            .await
            .unwrap();

        assert_eq!(
            consumed(&control),
            vec![
                (0, key_event(30, 1)),
                (0, key_event(30, 0)),
                (1, button_event(BTN_LEFT, 1)),
                (1, button_event(BTN_LEFT, 0)),
            ]
        );
        assert!(!emulation.has_pressed_keys(0));
        assert!(!emulation.has_pressed_keys(1));
    }

    #[tokio::test]
    async fn nonfinite_pointer_input_is_rejected_without_touching_held_state() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.consume(key_event(29, 1), 0).await.unwrap();
        for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            for event in [
                Event::Pointer(PointerEvent::Motion {
                    time: 0,
                    dx: invalid,
                    dy: 1.0,
                }),
                Event::Pointer(PointerEvent::Motion {
                    time: 0,
                    dx: 1.0,
                    dy: invalid,
                }),
                Event::Pointer(PointerEvent::Axis {
                    time: 0,
                    axis: 0,
                    value: invalid,
                }),
            ] {
                assert!(matches!(
                    emulation.consume(event, 0).await,
                    Err(EmulationError::InvalidInput(_))
                ));
            }
            emulation.warp(0, Position::Left, invalid).await;
        }
        for event in [
            Event::Pointer(PointerEvent::Axis {
                time: 0,
                axis: 2,
                value: 1.0,
            }),
            Event::Pointer(PointerEvent::AxisDiscrete120 {
                axis: u8::MAX,
                value: 120,
            }),
        ] {
            assert!(emulation.consume(event, 0).await.is_err());
        }
        assert_eq!(control.lock().unwrap().consumed.len(), 1);
        assert!(control.lock().unwrap().warps.is_empty());
        assert!(emulation.has_pressed_keys(0));
        let normal = Event::Pointer(PointerEvent::Motion {
            time: 0,
            dx: 1.0,
            dy: -1.0,
        });
        emulation.consume(normal.clone(), 0).await.unwrap();
        assert_eq!(control.lock().unwrap().consumed.last().unwrap().1, normal);
        emulation.warp(0, Position::Left, 0.5).await;
        assert_eq!(
            control.lock().unwrap().warps,
            vec![(0, Position::Left, 0.5)]
        );
        assert!(emulation.terminate_bounded().await);
    }

    #[tokio::test]
    async fn sensitivity_overflow_and_minimum_scroll_remain_finite_and_bounded() {
        let direct = InputEmulation::with_backend(
            Backend::Dummy,
            Default::default(),
            InputConfig {
                mouse_sensitivity: f64::NAN,
                invert_scroll: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(direct.input_config.mouse_sensitivity, 1.0);
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.update_config(InputConfig {
            mouse_sensitivity: 2.0,
            invert_scroll: true,
        });
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            emulation.update_config(InputConfig {
                mouse_sensitivity: value,
                invert_scroll: true,
            });
            assert_eq!(emulation.input_config.mouse_sensitivity, 2.0);
        }
        emulation
            .consume(
                Event::Pointer(PointerEvent::Motion {
                    time: 0,
                    dx: f64::MAX,
                    dy: -f64::MAX,
                }),
                0,
            )
            .await
            .unwrap();
        assert_eq!(
            control.lock().unwrap().consumed.last().unwrap().1,
            Event::Pointer(PointerEvent::Motion {
                time: 0,
                dx: f64::MAX,
                dy: -f64::MAX
            })
        );
        emulation
            .consume(
                Event::Pointer(PointerEvent::AxisDiscrete120 {
                    axis: 0,
                    value: i32::MIN,
                }),
                0,
            )
            .await
            .unwrap();
        assert_eq!(
            control.lock().unwrap().consumed.last().unwrap().1,
            Event::Pointer(PointerEvent::AxisDiscrete120 {
                axis: 0,
                value: i32::MAX
            })
        );
        assert!(emulation.terminate_bounded().await);
    }

    #[tokio::test]
    async fn invalid_transitions_cannot_grow_or_change_tracked_input() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.consume(key_event(29, 1), 0).await.unwrap();
        emulation.consume(button_event(272, 1), 0).await.unwrap();
        for code in 0x10000..0x10000 + 10000 {
            for event in [key_event(code, 1), button_event(code, 1)] {
                assert!(matches!(
                    emulation.consume(event, 0).await,
                    Err(EmulationError::InvalidInput(_))
                ));
            }
        }
        for event in [
            key_event(29, 2),
            key_event(29, u8::MAX),
            key_event(u32::MAX, 0),
            button_event(272, 2),
            button_event(272, u32::MAX),
            button_event(u32::MAX, 0),
        ] {
            assert!(matches!(
                emulation.consume(event, 0).await,
                Err(EmulationError::InvalidInput(_))
            ));
        }
        assert_eq!(emulation.handles[&0].keys.len(), 1);
        assert_eq!(emulation.handles[&0].buttons.len(), 1);
        assert_eq!(control.lock().unwrap().consumed.len(), 2);
        assert_eq!(emulation.handles[&0].keys[&29], TrackedTransition::Pressed);
        assert_eq!(
            emulation.handles[&0].buttons[&272],
            TrackedTransition::Pressed
        );
        assert!(emulation.terminate_bounded().await);
        assert!(emulation.handles.is_empty());
    }

    #[tokio::test]
    async fn complete_evdev_code_domain_remains_available_and_finite() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        for code in 0..=input_event::MAX_EVDEV_CODE {
            emulation.consume(key_event(code, 1), 0).await.unwrap();
            emulation.consume(button_event(code, 1), 0).await.unwrap();
        }
        let codes = (input_event::MAX_EVDEV_CODE + 1) as usize;
        assert_eq!(emulation.handles[&0].keys.len(), codes);
        assert_eq!(emulation.handles[&0].buttons.len(), codes);
        assert_eq!(control.lock().unwrap().consumed.len(), 2 * codes);
        assert!(
            emulation
                .consume(key_event(input_event::MAX_EVDEV_CODE + 1, 1), 0)
                .await
                .is_err()
        );
        assert!(
            emulation
                .consume(button_event(input_event::MAX_EVDEV_CODE + 1, 1), 0)
                .await
                .is_err()
        );
        assert!(emulation.terminate_bounded().await);
        assert!(emulation.handles.is_empty());
    }

    #[tokio::test]
    async fn destroy_releases_tracked_input_per_handle() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.create(1).await;

        emulation.consume(key_event(30, 1), 0).await.unwrap();
        emulation
            .consume(button_event(BTN_LEFT, 1), 0)
            .await
            .unwrap();
        emulation.consume(key_event(31, 1), 1).await.unwrap();

        assert!(emulation.destroy_bounded(0).await);

        let events = consumed(&control);
        assert!(events.contains(&(0, key_event(30, 0))));
        assert!(events.contains(&(0, button_event(BTN_LEFT, 0))));
        // the other handle must not be released or destroyed
        assert!(!events.contains(&(1, key_event(31, 0))));
        assert!(!control.lock().unwrap().destroyed.contains(&1));

        assert!(emulation.destroy_bounded(1).await);
        assert!(consumed(&control).contains(&(1, key_event(31, 0))));
        assert!(control.lock().unwrap().destroyed.contains(&1));
    }

    #[tokio::test]
    async fn cancelled_consume_retains_cleanup_state() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        let press = key_event(30, 1);
        control.lock().unwrap().stall_on = Some(press.clone());

        // cancel the consume future while the backend is still busy
        let cancelled =
            tokio::time::timeout(Duration::from_millis(5), emulation.consume(press, 0)).await;
        assert!(cancelled.is_err());
        assert!(
            emulation.has_pressed_keys(0),
            "a cancelled press must stay tracked for cleanup"
        );

        // cleanup must still release the key once the backend recovers
        control.lock().unwrap().stall_on = None;
        assert!(emulation.destroy_bounded(0).await);
        assert!(consumed(&control).contains(&(0, key_event(30, 0))));
        assert!(!emulation.has_pressed_keys(0));
    }

    #[tokio::test]
    async fn cancelled_press_is_forwarded_again_when_replayed() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        let press = key_event(30, 1);
        control.lock().unwrap().stall_on = Some(press.clone());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(5),
                emulation.consume(press.clone(), 0)
            )
            .await
            .is_err()
        );

        control.lock().unwrap().stall_on = None;
        emulation.consume(press.clone(), 0).await.unwrap();
        assert_eq!(consumed(&control), vec![(0, press.clone())]);
        assert!(emulation.has_pressed_keys(0));

        let button_press = button_event(BTN_LEFT, 1);
        control.lock().unwrap().stall_on = Some(button_press.clone());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(5),
                emulation.consume(button_press.clone(), 0)
            )
            .await
            .is_err()
        );

        control.lock().unwrap().stall_on = None;
        emulation.consume(button_press.clone(), 0).await.unwrap();
        assert_eq!(consumed(&control), vec![(0, press), (0, button_press)]);
    }

    #[tokio::test]
    async fn cancelled_button_release_is_forwarded_again_when_replayed() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        let press = button_event(BTN_LEFT, 1);
        let release = button_event(BTN_LEFT, 0);
        emulation.consume(press.clone(), 0).await.unwrap();

        control.lock().unwrap().stall_on = Some(release.clone());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(5),
                emulation.consume(release.clone(), 0)
            )
            .await
            .is_err()
        );

        control.lock().unwrap().stall_on = None;
        emulation.consume(release.clone(), 0).await.unwrap();
        assert_eq!(consumed(&control), vec![(0, press), (0, release)]);
        assert!(emulation.destroy_bounded(0).await);
    }

    #[tokio::test]
    async fn failed_consume_retains_cleanup_state() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        let press = key_event(30, 1);
        control.lock().unwrap().fail_on = Some(press.clone());
        assert!(emulation.consume(press, 0).await.is_err());
        assert!(
            emulation.has_pressed_keys(0),
            "a failed press must stay tracked for cleanup"
        );

        control.lock().unwrap().fail_on = None;
        assert!(emulation.destroy_bounded(0).await);
        assert!(consumed(&control).contains(&(0, key_event(30, 0))));
    }

    #[tokio::test]
    async fn destroy_bounded_reports_stalled_release_and_can_retry() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.consume(key_event(30, 1), 0).await.unwrap();

        // stalled release: cleanup cannot complete
        control.lock().unwrap().stall_on = Some(key_event(30, 0));
        assert!(!emulation.destroy_bounded(0).await);
        assert!(emulation.has_pressed_keys(0));
        assert!(!control.lock().unwrap().destroyed.contains(&0));

        // backend recovers, the retry succeeds
        control.lock().unwrap().stall_on = None;
        assert!(emulation.destroy_bounded(0).await);
        assert!(control.lock().unwrap().destroyed.contains(&0));
        assert!(!emulation.has_pressed_keys(0));
    }

    #[tokio::test]
    async fn failed_termination_retains_backend_until_releases_recover() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.consume(key_event(29, 1), 0).await.unwrap();
        control.lock().unwrap().stall_on = Some(key_event(29, 0));
        assert!(!emulation.terminate_bounded().await);
        assert!(emulation.has_pressed_keys(0));
        {
            let mut control = control.lock().unwrap();
            assert!(control.repeat_stopped);
            assert!(
                !control.terminated,
                "release transport must remain available"
            );
            control.stall_on = None;
        }
        assert!(emulation.terminate_bounded().await);
        assert!(emulation.handles.is_empty());
        assert!(control.lock().unwrap().terminated);
    }

    #[tokio::test]
    async fn cleanup_commits_partial_release_before_later_failure() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.consume(key_event(29, 1), 0).await.unwrap();
        emulation.consume(button_event(272, 1), 0).await.unwrap();
        control.lock().unwrap().stall_on = Some(button_event(272, 0));
        assert!(!emulation.destroy_bounded(0).await);
        assert!(!emulation.has_pressed_keys(0));
        assert!(emulation.handles[&0].buttons.contains_key(&272));
        // A confirmed key release must not suppress a later legitimate press.
        emulation.consume(key_event(29, 1), 0).await.unwrap();
        assert!(emulation.has_pressed_keys(0));
        control.lock().unwrap().stall_on = None;
        assert!(emulation.destroy_bounded(0).await);
        let control = control.lock().unwrap();
        assert_eq!(
            control
                .consumed
                .iter()
                .filter(|(_, e)| *e == key_event(29, 1))
                .count(),
            2
        );
        assert_eq!(
            control
                .consumed
                .iter()
                .filter(|(_, e)| *e == key_event(29, 0))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn aggregate_cleanup_deadline_rotates_to_later_healthy_handles() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        for handle in 0..8 {
            emulation.create(handle).await;
            emulation
                .consume(key_event(if handle == 7 { 31 } else { 30 }, 1), handle)
                .await
                .unwrap();
        }
        control.lock().unwrap().stall_on = Some(key_event(30, 0));
        let start = Instant::now();
        assert!(!emulation.terminate_bounded().await);
        assert!(start.elapsed() < Duration::from_secs(1));
        for _ in 0..4 {
            assert!(!emulation.terminate_bounded().await);
            if !emulation.handles.contains_key(&7) {
                break;
            }
        }
        assert!(
            !emulation.handles.contains_key(&7),
            "stalled early peers starved a healthy later peer"
        );
        assert!(emulation.has_pressed_keys(0));
        assert!(!control.lock().unwrap().terminated);
        control.lock().unwrap().stall_on = None;
        assert!(emulation.terminate_bounded().await);
    }

    #[tokio::test]
    async fn terminate_is_bounded_across_many_stalled_handles() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);

        const HANDLES: u64 = 32;
        for handle in 0..HANDLES {
            emulation.create(handle).await;
            emulation.consume(key_event(30, 1), handle).await.unwrap();
        }

        // One aggregate deadline bounds the attempt despite 32 stalled peers.
        control.lock().unwrap().stall_on = Some(key_event(30, 0));
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(10), emulation.terminate())
            .await
            .expect("terminate must be bounded across all handles");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "terminate scaled beyond the per-handle bound"
        );
        // The stalled input stays tracked so a later attempt can still release it.
        assert!(emulation.has_pressed_keys(0));
    }

    #[tokio::test]
    async fn stalled_destroy_is_reported_and_terminate_is_bounded() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        control.lock().unwrap().stall_destroy = true;
        assert!(!emulation.destroy_bounded(0).await);
        assert!(emulation.handles.contains_key(&0));
        control.lock().unwrap().stall_destroy = false;

        control.lock().unwrap().stall_terminate = true;
        // terminate must return even though the backend terminate never completes
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), emulation.terminate_bounded())
                .await
                .expect("terminate must be bounded")
        );
        assert!(control.lock().unwrap().terminated);
        assert!(emulation.handles.is_empty());
        control.lock().unwrap().stall_terminate = false;
        assert!(emulation.terminate_bounded().await);
    }

    #[test]
    fn post_processing_scales_pointer_motion() {
        let event = Event::Pointer(PointerEvent::Motion {
            time: 42,
            dx: 3.0,
            dy: -4.0,
        });
        let config = InputConfig {
            mouse_sensitivity: 1.5,
            ..Default::default()
        };

        assert_eq!(
            post_process_event(event, config),
            Event::Pointer(PointerEvent::Motion {
                time: 42,
                dx: 4.5,
                dy: -6.0,
            })
        );
    }

    #[test]
    fn post_processing_inverts_continuous_and_discrete_scrolling() {
        let config = InputConfig {
            invert_scroll: true,
            ..Default::default()
        };

        assert_eq!(
            post_process_event(
                Event::Pointer(PointerEvent::Axis {
                    time: 7,
                    axis: 0,
                    value: 2.5,
                }),
                config,
            ),
            Event::Pointer(PointerEvent::Axis {
                time: 7,
                axis: 0,
                value: -2.5,
            })
        );
        assert_eq!(
            post_process_event(
                Event::Pointer(PointerEvent::AxisDiscrete120 {
                    axis: 1,
                    value: 120,
                }),
                config,
            ),
            Event::Pointer(PointerEvent::AxisDiscrete120 {
                axis: 1,
                value: -120,
            })
        );
    }

    #[test]
    fn post_processing_leaves_other_events_unchanged() {
        let event = Event::Pointer(PointerEvent::Button {
            time: 5,
            button: 0x110,
            state: 1,
        });

        assert_eq!(
            post_process_event(
                event.clone(),
                InputConfig {
                    invert_scroll: true,
                    mouse_sensitivity: 2.0,
                },
            ),
            event
        );
    }
}
