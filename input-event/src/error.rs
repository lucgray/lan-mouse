use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum InvalidInputEvent {
    #[error("motion components must be finite")]
    NonFiniteMotion,
    #[error("scroll value must be finite")]
    NonFiniteScroll,
    #[error("scroll axis {0} is not vertical (0) or horizontal (1)")]
    Axis(u8),
    #[error("evdev code {0} exceeds KEY_MAX (0x2ff)")]
    CodeOutOfRange(u32),
    #[error("key state {0} is not release (0) or press (1)")]
    KeyState(u8),
    #[error("button state {0} is not release (0) or press (1)")]
    ButtonState(u32),
}
