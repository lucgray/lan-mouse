use std::fmt::{self, Display};

pub mod error;
pub mod scancode;

#[cfg(all(unix, feature = "libei", not(target_os = "macos")))]
mod libei;

// FIXME
pub const BTN_LEFT: u32 = 0x110;
pub const BTN_RIGHT: u32 = 0x111;
pub const BTN_MIDDLE: u32 = 0x112;
pub const BTN_BACK: u32 = 0x113;
pub const BTN_FORWARD: u32 = 0x114;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum PointerEvent {
    /// relative motion event
    Motion { time: u32, dx: f64, dy: f64 },
    /// mouse button event
    Button { time: u32, button: u32, state: u32 },
    /// axis event, scroll event for touchpads
    ///
    /// Positive is down/right, and the value is the direction the content
    /// moves on the host, its Natural scrolling already applied. Receivers
    /// reproduce it without applying their own.
    Axis { time: u32, axis: u8, value: f64 },
    /// discrete axis event, scroll event for mice - 120 = one scroll tick,
    /// same direction convention as [`PointerEvent::Axis`]
    AxisDiscrete120 { axis: u8, value: i32 },
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum KeyboardEvent {
    /// a key press / release event
    Key { time: u32, key: u32, state: u8 },
    /// modifiers changed state
    Modifiers {
        depressed: u32,
        latched: u32,
        locked: u32,
        group: u32,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum ClipboardEvent {
    /// text content from clipboard
    Text(String),
    /// image content from clipboard, PNG-encoded
    Image(Vec<u8>),
}

/// the kind of payload a [`ClipboardEvent`] carries
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClipboardContentKind {
    Text,
    Image,
}

impl ClipboardEvent {
    pub fn kind(&self) -> ClipboardContentKind {
        match self {
            ClipboardEvent::Text(_) => ClipboardContentKind::Text,
            ClipboardEvent::Image(_) => ClipboardContentKind::Image,
        }
    }

    /// size of the content in bytes (text characters / PNG bytes)
    pub fn content_len(&self) -> usize {
        match self {
            ClipboardEvent::Text(t) => t.len(),
            ClipboardEvent::Image(png) => png.len(),
        }
    }
}

/// encode raw RGBA pixels into a PNG byte stream for [`ClipboardEvent::Image`]
pub fn encode_image_rgba(width: u32, height: u32, rgba: &[u8]) -> Option<Vec<u8>> {
    let mut png = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut png, width, height);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().ok()?;
        writer.write_image_data(rgba).ok()?;
    }
    Some(png)
}

/// decode a [`ClipboardEvent::Image`] PNG payload into (width, height, RGBA pixels)
pub fn decode_image_rgba(png: &[u8]) -> Option<(u32, u32, Vec<u8>)> {
    let decoder = png::Decoder::new(png);
    let mut reader = decoder.read_info().ok()?;
    let mut buf = vec![0u8; reader.output_buffer_size()];
    let info = reader.next_frame(&mut buf).ok()?;
    buf.truncate(info.buffer_size());
    Some((info.width, info.height, buf))
}

#[derive(PartialEq, Debug, Clone)]
pub enum Event {
    /// pointer event (motion / button / axis)
    Pointer(PointerEvent),
    /// keyboard events (key / modifiers)
    Keyboard(KeyboardEvent),
    /// clipboard events
    Clipboard(ClipboardEvent),
}

impl Display for PointerEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PointerEvent::Motion { time: _, dx, dy } => write!(f, "motion({dx},{dy})"),
            PointerEvent::Button {
                time: _,
                button,
                state,
            } => {
                let str = match *button {
                    BTN_LEFT => Some("left"),
                    BTN_RIGHT => Some("right"),
                    BTN_MIDDLE => Some("middle"),
                    BTN_FORWARD => Some("forward"),
                    BTN_BACK => Some("back"),
                    _ => None,
                };
                if let Some(button) = str {
                    write!(f, "button({button}, {state})")
                } else {
                    write!(f, "button({button}, {state}")
                }
            }
            PointerEvent::Axis {
                time: _,
                axis,
                value,
            } => write!(f, "scroll({axis}, {value})"),
            PointerEvent::AxisDiscrete120 { axis, value } => {
                write!(f, "scroll-120 ({axis}, {value})")
            }
        }
    }
}

impl Display for KeyboardEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyboardEvent::Key {
                time: _,
                key,
                state,
            } => {
                let scan = scancode::Linux::try_from(*key);
                if let Ok(scan) = scan {
                    write!(f, "key({scan:?}, {state})")
                } else {
                    write!(f, "key({key}, {state})")
                }
            }
            KeyboardEvent::Modifiers {
                depressed: mods_depressed,
                latched: mods_latched,
                locked: mods_locked,
                group,
            } => write!(
                f,
                "modifiers({mods_depressed},{mods_latched},{mods_locked},{group})"
            ),
        }
    }
}

impl Display for ClipboardEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClipboardEvent::Text(text) => {
                let preview = if text.len() > 50 {
                    format!("{}...", &text[..50])
                } else {
                    text.clone()
                };
                write!(f, "clipboard(text: {})", preview)
            }
            ClipboardEvent::Image(png) => {
                write!(f, "clipboard(image: {} bytes)", png.len())
            }
        }
    }
}

impl Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Pointer(p) => write!(f, "{p}"),
            Event::Keyboard(k) => write!(f, "{k}"),
            Event::Clipboard(c) => write!(f, "{c}"),
        }
    }
}
