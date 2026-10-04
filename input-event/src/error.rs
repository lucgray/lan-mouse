use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum InvalidInputEvent {
    #[error("evdev code {0} exceeds KEY_MAX (0x2ff)")]
    CodeOutOfRange(u32),
    #[error("key state {0} is not release (0) or press (1)")]
    KeyState(u8),
    #[error("button state {0} is not release (0) or press (1)")]
    ButtonState(u32),
}
