use async_trait::async_trait;
use std::{collections::HashMap, ptr};
use x11::{
    xlib::{self, XCloseDisplay},
    xtest,
};

use input_event::{
    BTN_BACK, BTN_FORWARD, BTN_LEFT, BTN_MIDDLE, BTN_RIGHT, Event, KeyboardEvent, PointerEvent,
};

use crate::{
    error::EmulationError, motion::MotionRemainders, scroll_accumulator::Scroll120Accumulator,
};

use super::{Emulation, EmulationHandle, error::X11EmulationCreationError};

pub(crate) struct X11Emulation {
    display: *mut xlib::Display,
    scroll_states: HashMap<EmulationHandle, ScrollState>,
    motion_remainders: MotionRemainders,
}

unsafe impl Send for X11Emulation {}

impl X11Emulation {
    pub(crate) fn new() -> Result<Self, X11EmulationCreationError> {
        let display = unsafe {
            match xlib::XOpenDisplay(ptr::null()) {
                d if std::ptr::eq(d, ptr::null_mut::<xlib::Display>()) => {
                    Err(X11EmulationCreationError::OpenDisplay)
                }
                display => Ok(display),
            }
        }?;
        Ok(Self {
            display,
            scroll_states: HashMap::new(),
            motion_remainders: MotionRemainders::default(),
        })
    }

    fn emulate_mouse_button(&self, button: u32, state: u32) {
        let Some(x11_button) = evdev_button_to_x11(button) else {
            // Unsupported buttons must never turn into an unrelated click.
            return;
        };
        unsafe {
            xtest::XTestFakeButtonEvent(self.display, x11_button, state as i32, 0);
        };
    }

    const SCROLL_UP: u32 = 4;
    const SCROLL_DOWN: u32 = 5;
    const SCROLL_LEFT: u32 = 6;
    const SCROLL_RIGHT: u32 = 7;

    async fn emulate_scroll(&mut self, handle: EmulationHandle, axis: u8, delta: ScrollDelta) {
        let steps = self
            .scroll_states
            .entry(handle)
            .or_default()
            .accumulate(axis, delta);
        emit_scroll_steps(axis, steps, |direction| self.emit_scroll_click(direction)).await;
    }

    fn emit_scroll_click(&mut self, direction: u32) {
        // No await between press and release: cancellation cannot orphan a wheel press.
        unsafe {
            xtest::XTestFakeButtonEvent(self.display, direction, 1, 0);
            xtest::XTestFakeButtonEvent(self.display, direction, 0, 0);
            xlib::XFlush(self.display);
        }
    }

    #[allow(dead_code)]
    fn emulate_key(&self, key: u32, state: u8) {
        let key = key + 8; // xorg keycodes are shifted by 8
        unsafe {
            xtest::XTestFakeKeyEvent(self.display, key, state as i32, 0);
        }
    }
}

impl Drop for X11Emulation {
    fn drop(&mut self) {
        unsafe {
            XCloseDisplay(self.display);
        }
    }
}

#[async_trait]
impl Emulation for X11Emulation {
    async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), EmulationError> {
        match event {
            Event::Pointer(pointer_event) => match pointer_event {
                PointerEvent::Motion { time: _, dx, dy } => {
                    let display = self.display;
                    self.motion_remainders.deliver(handle, (dx, dy), |x, y| {
                        unsafe {
                            xtest::XTestFakeRelativeMotionEvent(display, x, y, 0, 0);
                        }
                        // XTest delivery errors still need separate native handling.
                        Ok::<_, EmulationError>(())
                    })?;
                }
                PointerEvent::Button {
                    time: _,
                    button,
                    state,
                } => {
                    self.emulate_mouse_button(button, state);
                }
                PointerEvent::Axis {
                    time: _,
                    axis,
                    value,
                } => {
                    self.emulate_scroll(handle, axis, ScrollDelta::Continuous(value))
                        .await;
                }
                PointerEvent::AxisDiscrete120 { axis, value } => {
                    self.emulate_scroll(handle, axis, ScrollDelta::Discrete120(value))
                        .await;
                }
            },
            Event::Keyboard(KeyboardEvent::Key {
                time: _,
                key,
                state,
            }) => {
                self.emulate_key(key, state);
            }
            _ => {}
        }
        unsafe {
            xlib::XFlush(self.display);
        }
        // FIXME
        Ok(())
    }

    async fn create(&mut self, handle: EmulationHandle) {
        self.scroll_states.entry(handle).or_default();
        self.motion_remainders.remove(handle);
    }

    async fn destroy(&mut self, handle: EmulationHandle) {
        self.scroll_states.remove(&handle);
        self.motion_remainders.remove(handle);
    }

    async fn terminate(&mut self) {
        self.scroll_states.clear();
        self.motion_remainders.clear();
    }
}

// Core X11 has only wheel clicks. Approximate smooth scroll with the historic
// Wayland convention of 10 logical units per click; this is not pixel precision.
const CONTINUOUS_SCROLL_UNITS: f64 = 10.0;
const SCROLL_BATCH_SIZE: u32 = 32;

enum ScrollDelta {
    Continuous(f64),
    Discrete120(i32),
}

#[derive(Default)]
struct ScrollState {
    discrete: Scroll120Accumulator,
    continuous: [f64; 2],
}

impl ScrollState {
    fn accumulate(&mut self, axis: u8, delta: ScrollDelta) -> i32 {
        if axis > 1 {
            return 0;
        }
        match delta {
            ScrollDelta::Discrete120(value) => self.discrete.accumulate(axis, value),
            ScrollDelta::Continuous(value) => {
                if !value.is_finite() {
                    return 0;
                }
                let residual = &mut self.continuous[usize::from(axis)];
                let total = value + *residual;
                let steps = (total / CONTINUOUS_SCROLL_UNITS).trunc();
                *residual = if steps < f64::from(i32::MIN) || steps > f64::from(i32::MAX) {
                    0.0
                } else {
                    total % CONTINUOUS_SCROLL_UNITS
                };
                steps as i32
            }
        }
    }
}

async fn emit_scroll_steps(axis: u8, steps: i32, mut emit: impl FnMut(u32)) {
    if steps == 0 {
        return;
    }
    let direction = match (axis, steps < 0) {
        (0, true) => X11Emulation::SCROLL_UP,
        (0, false) => X11Emulation::SCROLL_DOWN,
        (1, true) => X11Emulation::SCROLL_LEFT,
        (1, false) => X11Emulation::SCROLL_RIGHT,
        _ => return,
    };
    for index in 0..steps.unsigned_abs() {
        // Bounded complete pairs between yields keep the dispatcher cancelable.
        if index % SCROLL_BATCH_SIZE == 0 {
            tokio::task::yield_now().await;
        }
        emit(direction);
    }
}

fn evdev_button_to_x11(button: u32) -> Option<u32> {
    match button {
        BTN_RIGHT => Some(3),
        BTN_MIDDLE => Some(2),
        BTN_BACK => Some(8),
        BTN_FORWARD => Some(9),
        BTN_LEFT => Some(1),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scroll_preserves_zero_fraction_multiple_direction_and_axis() {
        let mut state = ScrollState::default();
        let mut emitted = Vec::new();
        for (axis, value) in [(0, 0), (0, 24), (0, 24), (0, 24), (0, 24)] {
            let steps = state.accumulate(axis, ScrollDelta::Discrete120(value));
            emit_scroll_steps(axis, steps, |button| emitted.push(button)).await;
        }
        assert!(emitted.is_empty());
        let steps = state.accumulate(0, ScrollDelta::Discrete120(24));
        emit_scroll_steps(0, steps, |button| emitted.push(button)).await;
        assert_eq!(emitted, vec![5]);
        let steps = state.accumulate(0, ScrollDelta::Discrete120(-240));
        emit_scroll_steps(0, steps, |button| emitted.push(button)).await;
        let steps = state.accumulate(1, ScrollDelta::Discrete120(240));
        emit_scroll_steps(1, steps, |button| emitted.push(button)).await;
        assert_eq!(emitted, vec![5, 4, 4, 7, 7]);
        assert_eq!(state.accumulate(0, ScrollDelta::Discrete120(60)), 0);
        assert_eq!(state.accumulate(0, ScrollDelta::Discrete120(-60)), 0);
        assert_eq!(state.accumulate(1, ScrollDelta::Discrete120(-120)), -1);
        assert_eq!(state.accumulate(0, ScrollDelta::Continuous(0.0)), 0);
        for _ in 0..3 {
            assert_eq!(state.accumulate(0, ScrollDelta::Continuous(2.5)), 0);
        }
        assert_eq!(state.accumulate(1, ScrollDelta::Continuous(10.0)), 1);
        assert_eq!(state.accumulate(0, ScrollDelta::Continuous(2.5)), 1);
        assert_eq!(state.accumulate(0, ScrollDelta::Continuous(3.0)), 0);
        assert_eq!(state.accumulate(0, ScrollDelta::Continuous(-3.0)), 0);
        assert_eq!(state.accumulate(0, ScrollDelta::Continuous(-20.0)), -2);
        assert_eq!(
            state.accumulate(0, ScrollDelta::Continuous(f64::MAX)),
            i32::MAX
        );
        assert_eq!(state.accumulate(0, ScrollDelta::Continuous(10.0)), 1);
        assert_eq!(state.accumulate(0, ScrollDelta::Continuous(f64::NAN)), 0);
        assert_eq!(state.accumulate(0, ScrollDelta::Continuous(-10.0)), -1);
    }

    #[test]
    fn scroll_remainders_do_not_cross_handle_lifetimes() {
        let mut states = HashMap::<EmulationHandle, ScrollState>::new();
        assert_eq!(
            states
                .entry(0)
                .or_default()
                .accumulate(0, ScrollDelta::Discrete120(60)),
            0
        );
        assert_eq!(
            states
                .entry(1)
                .or_default()
                .accumulate(0, ScrollDelta::Discrete120(60)),
            0
        );
        assert_eq!(
            states
                .get_mut(&0)
                .unwrap()
                .accumulate(0, ScrollDelta::Discrete120(60)),
            1
        );
        states.remove(&1);
        assert_eq!(
            states
                .entry(1)
                .or_default()
                .accumulate(0, ScrollDelta::Discrete120(60)),
            0
        );
        assert_eq!(
            states
                .get_mut(&1)
                .unwrap()
                .accumulate(0, ScrollDelta::Discrete120(60)),
            1
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn huge_scroll_delivery_yields_and_can_be_canceled_between_complete_pairs() {
        use std::{cell::Cell, rc::Rc};
        let count = Rc::new(Cell::new(0usize));
        let competitor_ran = Rc::new(Cell::new(false));
        let recorded = count.clone();
        let competitor = competitor_ran.clone();
        let delivery = emit_scroll_steps(0, i32::MAX, move |button| {
            assert_eq!(button, 5);
            recorded.set(recorded.get() + 1);
        });
        let (result, ()) = tokio::join!(
            tokio::time::timeout(std::time::Duration::from_millis(5), delivery),
            async move {
                tokio::task::yield_now().await;
                competitor.set(true);
            },
        );
        assert!(result.is_err());
        assert!(competitor_ran.get());
        assert!(count.get() > 0 && count.get() < i32::MAX as usize);
        let after_cancel = count.get();
        tokio::task::yield_now().await;
        assert_eq!(count.get(), after_cancel);
    }

    #[test]
    fn supported_buttons_keep_their_native_identity() {
        for (evdev, x11) in [
            (BTN_LEFT, 1),
            (BTN_MIDDLE, 2),
            (BTN_RIGHT, 3),
            (BTN_BACK, 8),
            (BTN_FORWARD, 9),
        ] {
            assert_eq!(evdev_button_to_x11(evdev), Some(x11));
        }
    }

    #[test]
    fn unsupported_buttons_cannot_become_left_clicks() {
        for button in [0, 0x116, 0x117, input_event::MAX_EVDEV_CODE, u32::MAX] {
            assert_eq!(evdev_button_to_x11(button), None);
        }
    }
}
