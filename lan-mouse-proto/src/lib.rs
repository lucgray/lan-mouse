use input_event::{
    ClipboardEvent, ClipboardFile, Event as InputEvent, KeyboardEvent, PointerEvent,
};
use num_enum::{IntoPrimitive, TryFromPrimitive, TryFromPrimitiveError};
use paste::paste;
use std::{
    fmt::{Debug, Display, Formatter},
    mem::size_of,
};
use thiserror::Error;

/// defines the maximum size an encoded event can take up
/// For most events this is the pointer motion event: type: u8, time: u32, dx: f64, dy: f64
/// For clipboard events, we have a separate MAX_CLIPBOARD_SIZE limit
pub const MAX_EVENT_SIZE: usize = size_of::<u8>() + size_of::<u32>() + 2 * size_of::<f64>();

/// maximum clipboard data size for the single-datagram format (64KB)
/// Clipboard events up to this size are sent as one message: the legacy
/// wire format every peer understands. Larger payloads are sent as
/// [`EventType::ClipboardFragment`] datagrams instead.
pub const MAX_CLIPBOARD_SIZE: usize = 64 * 1024;

/// maximum clipboard payload size including fragmented transfers (256MB)
pub const MAX_CLIPBOARD_TRANSFER_SIZE: usize = 256 * 1024 * 1024;

/// payload bytes carried by one clipboard fragment datagram.
/// Kept below the typical LAN MTU so no IP fragmentation is needed.
pub const CLIPBOARD_FRAGMENT_PAYLOAD: usize = 1200;

/// [u8 type][u32 total_len][u32 seq][u32 num][u32 transfer_id] before
/// the payload bytes
pub const CLIPBOARD_FRAGMENT_HEADER: usize = 17;

/// FNV-1a over the encoded event — identifies a transfer so a new
/// clipboard payload with the same total/fragment count cannot be
/// merged into an in-flight reassembly
fn transfer_id(encoded: &[u8]) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for b in encoded {
        h = (h ^ *b as u32).wrapping_mul(0x01000193);
    }
    h
}

/// error type for protocol violations
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// event type does not exist
    #[error("invalid event id: `{0}`")]
    InvalidEventId(#[from] TryFromPrimitiveError<EventType>),
    /// position type does not exist
    #[error("invalid event id: `{0}`")]
    InvalidPosition(#[from] TryFromPrimitiveError<Position>),
    /// clipboard data too large
    #[error("clipboard data exceeds maximum size: {0} bytes")]
    ClipboardTooLarge(usize),
    /// invalid UTF-8 in clipboard text
    #[error("invalid UTF-8 in clipboard text")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    /// buffer too small for clipboard data
    #[error("buffer too small for clipboard data")]
    BufferTooSmall,
}

/// Position of a client
#[derive(Clone, Copy, Debug, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum Position {
    Left,
    Right,
    Top,
    Bottom,
}

impl Display for Position {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let pos = match self {
            Position::Left => "left",
            Position::Right => "right",
            Position::Top => "top",
            Position::Bottom => "bottom",
        };
        write!(f, "{pos}")
    }
}

/// main lan-mouse protocol event type
#[derive(Clone, Debug)]
pub enum ProtoEvent {
    /// notify a client that the cursor entered its region at the given position.
    /// The `f64` is the normalized (`0.0..=1.0`) position along the crossed
    /// edge, e.g. how far down a `Left`/`Right` edge or how far across a
    /// `Top`/`Bottom` edge, so the receiving side can warp the cursor to
    /// the matching spot instead of a fixed point on the edge.
    /// [`ProtoEvent::Ack`] with the same serial is used for synchronization between devices
    Enter(Position, f64),
    /// notify a client that the cursor left its region. The `f64` is the
    /// normalized (`0.0..=1.0`) position along the edge this side's
    /// cursor should reappear at — set when the *other* side detected
    /// its own local crossing back (see the `EnterOnly` capture in
    /// `src/service.rs::add_incoming`), `0.5` for a plain release with
    /// no such crossing (release-bind, explicit release request, etc).
    /// [`ProtoEvent::Ack`] with the same serial is used for synchronization between devices
    Leave(u32, f64),
    /// acknowledge of an [`ProtoEvent::Enter`] or [`ProtoEvent::Leave`] event
    Ack(u32),
    /// Input event
    Input(InputEvent),
    /// Ping event for tracking unresponsive clients.
    /// A client has to respond with [`ProtoEvent::Pong`].
    Ping,
    /// Response to [`ProtoEvent::Ping`], true if emulation is enabled / available
    Pong(bool),
    /// Build identification for the sending peer. Sent by the
    /// connect side once after the connection authenticates, and
    /// echoed back by the listen side in reply, so each end can
    /// display the peer's build hash and warn (soft) on mismatch.
    /// `commit` is the 8-byte ASCII short commit hash from
    /// `shadow_rs`'s `SHORT_COMMIT`. Old peers that don't
    /// recognize the event type silently skip it per the
    /// forward-compat handling in the receive loop.
    Hello { commit: [u8; 8] },
}

impl Display for ProtoEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtoEvent::Enter(s, t) => write!(f, "Enter({s}, {t:.2})"),
            ProtoEvent::Leave(s, t) => write!(f, "Leave({s}, {t:.2})"),
            ProtoEvent::Ack(s) => write!(f, "Ack({s})"),
            ProtoEvent::Input(e) => write!(f, "{e}"),
            ProtoEvent::Ping => write!(f, "ping"),
            ProtoEvent::Pong(alive) => {
                write!(
                    f,
                    "pong: {}",
                    if *alive { "alive" } else { "not available" }
                )
            }
            ProtoEvent::Hello { commit } => {
                let s = std::str::from_utf8(commit).unwrap_or("????????");
                write!(f, "Hello({s})")
            }
        }
    }
}

#[derive(TryFromPrimitive, IntoPrimitive, Debug)]
#[repr(u8)]
pub enum EventType {
    PointerMotion,
    PointerButton,
    PointerAxis,
    PointerAxisValue120,
    KeyboardKey,
    KeyboardModifiers,
    Ping,
    Pong,
    Enter,
    Leave,
    Ack,
    Hello,
    ClipboardText,
    ClipboardImage,
    /// a fragment of an encoded clipboard event (text/image/file),
    /// used for payloads larger than [`MAX_CLIPBOARD_SIZE`]. Not a
    /// [`ProtoEvent`] variant — receivers collect the payload bytes and
    /// decode the reassembled event with [`decode_clipboard_event`].
    ClipboardFragment,
    /// one or more files copied in a file manager. Payload:
    /// `[u32 count]{[u32 name_len][name][u64 data_len][data]}`
    ClipboardFile,
}

impl ProtoEvent {
    fn event_type(&self) -> EventType {
        match self {
            ProtoEvent::Input(e) => match e {
                InputEvent::Pointer(p) => match p {
                    PointerEvent::Motion { .. } => EventType::PointerMotion,
                    PointerEvent::Button { .. } => EventType::PointerButton,
                    PointerEvent::Axis { .. } => EventType::PointerAxis,
                    PointerEvent::AxisDiscrete120 { .. } => EventType::PointerAxisValue120,
                },
                InputEvent::Keyboard(k) => match k {
                    KeyboardEvent::Key { .. } => EventType::KeyboardKey,
                    KeyboardEvent::Modifiers { .. } => EventType::KeyboardModifiers,
                },
                InputEvent::Clipboard(c) => match c {
                    ClipboardEvent::Text(_) => EventType::ClipboardText,
                    ClipboardEvent::Image(_) => EventType::ClipboardImage,
                    ClipboardEvent::Files(_) => EventType::ClipboardFile,
                },
            },
            ProtoEvent::Ping => EventType::Ping,
            ProtoEvent::Pong(_) => EventType::Pong,
            ProtoEvent::Enter(..) => EventType::Enter,
            ProtoEvent::Leave(..) => EventType::Leave,
            ProtoEvent::Ack(_) => EventType::Ack,
            ProtoEvent::Hello { .. } => EventType::Hello,
        }
    }
}

impl TryFrom<[u8; MAX_EVENT_SIZE]> for ProtoEvent {
    type Error = ProtocolError;

    fn try_from(buf: [u8; MAX_EVENT_SIZE]) -> Result<Self, Self::Error> {
        let mut buf = &buf[..];
        let event_type = decode_u8(&mut buf)?;
        match EventType::try_from(event_type)? {
            EventType::PointerMotion => {
                Ok(Self::Input(InputEvent::Pointer(PointerEvent::Motion {
                    time: decode_u32(&mut buf)?,
                    dx: decode_f64(&mut buf)?,
                    dy: decode_f64(&mut buf)?,
                })))
            }
            EventType::PointerButton => {
                Ok(Self::Input(InputEvent::Pointer(PointerEvent::Button {
                    time: decode_u32(&mut buf)?,
                    button: decode_u32(&mut buf)?,
                    state: decode_u32(&mut buf)?,
                })))
            }
            EventType::PointerAxis => Ok(Self::Input(InputEvent::Pointer(PointerEvent::Axis {
                time: decode_u32(&mut buf)?,
                axis: decode_u8(&mut buf)?,
                value: decode_f64(&mut buf)?,
            }))),
            EventType::PointerAxisValue120 => Ok(Self::Input(InputEvent::Pointer(
                PointerEvent::AxisDiscrete120 {
                    axis: decode_u8(&mut buf)?,
                    value: decode_i32(&mut buf)?,
                },
            ))),
            EventType::KeyboardKey => Ok(Self::Input(InputEvent::Keyboard(KeyboardEvent::Key {
                time: decode_u32(&mut buf)?,
                key: decode_u32(&mut buf)?,
                state: decode_u8(&mut buf)?,
            }))),
            EventType::KeyboardModifiers => Ok(Self::Input(InputEvent::Keyboard(
                KeyboardEvent::Modifiers {
                    depressed: decode_u32(&mut buf)?,
                    latched: decode_u32(&mut buf)?,
                    locked: decode_u32(&mut buf)?,
                    group: decode_u32(&mut buf)?,
                },
            ))),
            EventType::Ping => Ok(Self::Ping),
            EventType::Pong => Ok(Self::Pong(decode_u8(&mut buf)? != 0)),
            EventType::Enter => Ok(Self::Enter(
                decode_u8(&mut buf)?.try_into()?,
                decode_f64(&mut buf)?,
            )),
            EventType::Leave => Ok(Self::Leave(decode_u32(&mut buf)?, decode_f64(&mut buf)?)),
            EventType::Ack => Ok(Self::Ack(decode_u32(&mut buf)?)),
            EventType::Hello => {
                let mut commit = [0u8; 8];
                for b in commit.iter_mut() {
                    *b = decode_u8(&mut buf)?;
                }
                Ok(Self::Hello { commit })
            }
            EventType::ClipboardText
            | EventType::ClipboardImage
            | EventType::ClipboardFile
            | EventType::ClipboardFragment => {
                // Clipboard events use variable-length encoding
                // This path should not be reached for fixed-size buffer decoding
                Err(ProtocolError::BufferTooSmall)
            }
        }
    }
}

impl From<ProtoEvent> for ([u8; MAX_EVENT_SIZE], usize) {
    fn from(event: ProtoEvent) -> Self {
        let mut buf = [0u8; MAX_EVENT_SIZE];
        let mut len = 0usize;
        {
            let mut buf = &mut buf[..];
            let buf = &mut buf;
            let len = &mut len;
            encode_u8(buf, len, event.event_type() as u8);
            match event {
                ProtoEvent::Input(event) => match event {
                    InputEvent::Pointer(p) => match p {
                        PointerEvent::Motion { time, dx, dy } => {
                            encode_u32(buf, len, time);
                            encode_f64(buf, len, dx);
                            encode_f64(buf, len, dy);
                        }
                        PointerEvent::Button {
                            time,
                            button,
                            state,
                        } => {
                            encode_u32(buf, len, time);
                            encode_u32(buf, len, button);
                            encode_u32(buf, len, state);
                        }
                        PointerEvent::Axis { time, axis, value } => {
                            encode_u32(buf, len, time);
                            encode_u8(buf, len, axis);
                            encode_f64(buf, len, value);
                        }
                        PointerEvent::AxisDiscrete120 { axis, value } => {
                            encode_u8(buf, len, axis);
                            encode_i32(buf, len, value);
                        }
                    },
                    InputEvent::Keyboard(k) => match k {
                        KeyboardEvent::Key { time, key, state } => {
                            encode_u32(buf, len, time);
                            encode_u32(buf, len, key);
                            encode_u8(buf, len, state);
                        }
                        KeyboardEvent::Modifiers {
                            depressed,
                            latched,
                            locked,
                            group,
                        } => {
                            encode_u32(buf, len, depressed);
                            encode_u32(buf, len, latched);
                            encode_u32(buf, len, locked);
                            encode_u32(buf, len, group);
                        }
                    },
                    InputEvent::Clipboard(_) => {
                        panic!("Clipboard events must use encode_clipboard_event");
                    }
                },
                ProtoEvent::Ping => {}
                ProtoEvent::Pong(alive) => encode_u8(buf, len, alive as u8),
                ProtoEvent::Enter(pos, t) => {
                    encode_u8(buf, len, pos as u8);
                    encode_f64(buf, len, t);
                }
                ProtoEvent::Leave(serial, t) => {
                    encode_u32(buf, len, serial);
                    encode_f64(buf, len, t);
                }
                ProtoEvent::Ack(serial) => encode_u32(buf, len, serial),
                ProtoEvent::Hello { commit } => {
                    for b in commit.iter() {
                        encode_u8(buf, len, *b);
                    }
                }
            }
        }
        (buf, len)
    }
}

macro_rules! decode_impl {
    ($t:ty) => {
        paste! {
            fn [<decode_ $t>](data: &mut &[u8]) -> Result<$t, ProtocolError> {
                let (int_bytes, rest) = data.split_at(size_of::<$t>());
                *data = rest;
                Ok($t::from_be_bytes(int_bytes.try_into().unwrap()))
            }
        }
    };
}

decode_impl!(u8);
decode_impl!(u32);
decode_impl!(i32);
decode_impl!(f64);

macro_rules! encode_impl {
    ($t:ty) => {
        paste! {
            fn [<encode_ $t>](buf: &mut &mut [u8], amt: &mut usize, n: $t) {
                let src = n.to_be_bytes();
                let data = std::mem::take(buf);
                let (int_bytes, rest) = data.split_at_mut(size_of::<$t>());
                int_bytes.copy_from_slice(&src);
                *amt += size_of::<$t>();
                *buf = rest
            }
        }
    };
}

encode_impl!(u8);
encode_impl!(u32);
encode_impl!(i32);
encode_impl!(f64);

/// Encode a clipboard event into a Vec<u8>
/// Format: [event_type: u8][length: u32][data bytes]
pub fn encode_clipboard_event(event: &ProtoEvent) -> Result<Vec<u8>, ProtocolError> {
    let (event_type, data): (EventType, &[u8]) = match event {
        ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Text(text))) => {
            (EventType::ClipboardText, text.as_bytes())
        }
        ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Image(png))) => {
            (EventType::ClipboardImage, png.as_slice())
        }
        ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Files(files))) => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&(files.len() as u32).to_be_bytes());
            for file in files {
                let name = file.name.as_bytes();
                payload.extend_from_slice(&(name.len() as u32).to_be_bytes());
                payload.extend_from_slice(name);
                payload.extend_from_slice(&(file.data.len() as u64).to_be_bytes());
                payload.extend_from_slice(&file.data);
            }
            let mut buf = Vec::with_capacity(1 + 4 + payload.len());
            buf.push(EventType::ClipboardFile as u8);
            buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&payload);
            return Ok(buf);
        }
        _ => panic!("encode_clipboard_event called on non-clipboard event"),
    };
    if data.len() > MAX_CLIPBOARD_TRANSFER_SIZE {
        return Err(ProtocolError::ClipboardTooLarge(data.len()));
    }
    let mut buf = Vec::with_capacity(1 + 4 + data.len());
    buf.push(event_type as u8);
    buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
    buf.extend_from_slice(data);
    Ok(buf)
}

/// whether an event type byte is a clipboard fragment datagram
pub fn is_clipboard_fragment_type(event_type: u8) -> bool {
    event_type == EventType::ClipboardFragment as u8
}

/// Split an encoded clipboard event into self-describing fragment
/// datagrams: `[type][u32 total_len][u32 seq][u32 num][payload]`.
/// Each datagram carries its position, so input events may interleave
/// and datagrams may arrive out of order or be dropped (the transfer
/// simply never completes; the next clipboard change resets state).
pub struct ClipboardFragmenter<'a> {
    encoded: &'a [u8],
    num: u32,
    next: u32,
    id: u32,
}

impl<'a> ClipboardFragmenter<'a> {
    /// `encoded` must come from [`encode_clipboard_event`]; callers
    /// decide the single-datagram vs fragmented path on its length.
    pub fn new(encoded: &'a [u8]) -> Self {
        let num = encoded.len().div_ceil(CLIPBOARD_FRAGMENT_PAYLOAD) as u32;
        Self {
            encoded,
            num,
            next: 0,
            id: transfer_id(encoded),
        }
    }

    pub fn len(&self) -> u32 {
        self.num
    }

    pub fn is_empty(&self) -> bool {
        self.num == 0
    }
}

impl Iterator for ClipboardFragmenter<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.num {
            return None;
        }
        let start = self.next as usize * CLIPBOARD_FRAGMENT_PAYLOAD;
        let end = (start + CLIPBOARD_FRAGMENT_PAYLOAD).min(self.encoded.len());
        let mut dgram = Vec::with_capacity(CLIPBOARD_FRAGMENT_HEADER + end - start);
        dgram.push(EventType::ClipboardFragment as u8);
        dgram.extend_from_slice(&(self.encoded.len() as u32).to_be_bytes());
        dgram.extend_from_slice(&self.next.to_be_bytes());
        dgram.extend_from_slice(&self.num.to_be_bytes());
        dgram.extend_from_slice(&self.id.to_be_bytes());
        dgram.extend_from_slice(&self.encoded[start..end]);
        self.next += 1;
        Some(dgram)
    }
}

/// Reassembles [`ClipboardFragment`] datagrams from one peer into the
/// original encoded clipboard event.
///
/// A fragment whose declared `total_len`/`num` differs from the
/// in-flight transfer resets the state — the clipboard changed mid
/// flight. Stale state is also dropped after [`REASSEMBLY_TIMEOUT`]
/// without a new fragment.
#[derive(Default)]
pub struct ClipboardReassembler {
    id: u32,
    total: u32,
    num: u32,
    got: u32,
    /// payload bytes actually written so far
    got_bytes: u64,
    /// seq bitmap; num <= 445k for the size cap so this stays small
    seen: Vec<u64>,
    data: Vec<u8>,
    last_fragment: Option<std::time::Instant>,
}

/// drop an incomplete reassembly after this much silence
const REASSEMBLY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

impl ClipboardReassembler {
    pub fn new() -> Self {
        Self::default()
    }

    fn reset(&mut self) {
        self.id = 0;
        self.num = 0;
        self.got = 0;
        self.got_bytes = 0;
        self.seen.clear();
        self.data.clear();
        self.last_fragment = None;
    }

    /// (transferred, total) payload bytes while a transfer is in
    /// flight; None when idle
    pub fn progress(&self) -> Option<(u64, u64)> {
        if self.num == 0 {
            None
        } else {
            Some((self.got_bytes, self.total as u64))
        }
    }

    /// feed one received datagram; Ok(Some(encoded)) once the full
    /// payload is assembled — pass it to [`decode_clipboard_event`]
    pub fn push(&mut self, dgram: &[u8]) -> Result<Option<Vec<u8>>, ProtocolError> {
        if !is_clipboard_fragment_type(dgram.first().copied().unwrap_or(0)) {
            return Err(ProtocolError::BufferTooSmall);
        }
        if dgram.len() < CLIPBOARD_FRAGMENT_HEADER {
            return Err(ProtocolError::BufferTooSmall);
        }
        let total = u32::from_be_bytes(dgram[1..5].try_into().unwrap());
        let seq = u32::from_be_bytes(dgram[5..9].try_into().unwrap());
        let num = u32::from_be_bytes(dgram[9..13].try_into().unwrap());
        let id = u32::from_be_bytes(dgram[13..17].try_into().unwrap());
        if total == 0
            || num == 0
            || seq >= num
            || total as usize > MAX_CLIPBOARD_TRANSFER_SIZE
            || (num as u64) < (total as u64).div_ceil(CLIPBOARD_FRAGMENT_PAYLOAD as u64)
            || dgram.len() - CLIPBOARD_FRAGMENT_HEADER > CLIPBOARD_FRAGMENT_PAYLOAD
        {
            self.reset();
            return Err(ProtocolError::BufferTooSmall);
        }
        let stale = self
            .last_fragment
            .is_some_and(|t| t.elapsed() > REASSEMBLY_TIMEOUT);
        if stale || self.id != id || self.num != num || self.total != total {
            self.reset();
            self.id = id;
            self.total = total;
            self.num = num;
            self.seen.resize(num.div_ceil(64) as usize, 0);
            self.data.resize(total as usize, 0);
        }
        let word = (seq / 64) as usize;
        let bit = 1u64 << (seq % 64);
        if self.seen[word] & bit != 0 {
            // duplicate fragment: harmless, ignore
            self.last_fragment = Some(std::time::Instant::now());
            return Ok(None);
        }
        self.seen[word] |= bit;
        self.got += 1;
        let start = seq as usize * CLIPBOARD_FRAGMENT_PAYLOAD;
        let payload = &dgram[CLIPBOARD_FRAGMENT_HEADER..];
        let end = (start + payload.len()).min(self.data.len());
        self.data[start..end].copy_from_slice(&payload[..end - start]);
        self.got_bytes += (end - start) as u64;
        self.last_fragment = Some(std::time::Instant::now());
        if self.got == self.num {
            self.num = 0;
            self.got = 0;
            self.got_bytes = 0;
            self.seen.clear();
            self.last_fragment = None;
            return Ok(Some(std::mem::take(&mut self.data)));
        }
        Ok(None)
    }
}

/// Decode a clipboard event from a byte slice
/// Format: [event_type: u8][length: u32][data bytes]
pub fn decode_clipboard_event(buf: &[u8]) -> Result<ProtoEvent, ProtocolError> {
    if buf.is_empty() {
        return Err(ProtocolError::BufferTooSmall);
    }
    let event_type = buf[0];
    let is_file = event_type == EventType::ClipboardFile as u8;
    let is_image = match event_type {
        t if t == EventType::ClipboardText as u8 => false,
        t if t == EventType::ClipboardImage as u8 || is_file => true,
        _ => {
            return Err(ProtocolError::InvalidEventId(
                EventType::try_from(event_type).unwrap_err(),
            ));
        }
    };
    if buf.len() < 5 {
        return Err(ProtocolError::BufferTooSmall);
    }
    let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if length > MAX_CLIPBOARD_TRANSFER_SIZE {
        return Err(ProtocolError::ClipboardTooLarge(length));
    }
    if buf.len() < 5 + length {
        return Err(ProtocolError::BufferTooSmall);
    }
    let data = &buf[5..5 + length];
    let clipboard_event = if is_file {
        ClipboardEvent::Files(decode_clipboard_files(data)?)
    } else if is_image {
        ClipboardEvent::Image(data.to_vec())
    } else {
        ClipboardEvent::Text(String::from_utf8(data.to_vec())?)
    };
    Ok(ProtoEvent::Input(InputEvent::Clipboard(clipboard_event)))
}

/// decode the `[u32 count]{[u32 name_len][name][u64 data_len][data]}`
/// payload of a [`EventType::ClipboardFile`] event
fn decode_clipboard_files(mut data: &[u8]) -> Result<Vec<ClipboardFile>, ProtocolError> {
    if data.len() < 4 {
        return Err(ProtocolError::BufferTooSmall);
    }
    let count = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
    data = &data[4..];
    let mut files = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if data.len() < 4 {
            return Err(ProtocolError::BufferTooSmall);
        }
        let name_len = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        data = &data[4..];
        if data.len() < name_len + 8 {
            return Err(ProtocolError::BufferTooSmall);
        }
        let name = String::from_utf8(data[..name_len].to_vec())?;
        data = &data[name_len..];
        let data_len = u64::from_be_bytes(data[..8].try_into().unwrap()) as usize;
        data = &data[8..];
        if data.len() < data_len {
            return Err(ProtocolError::BufferTooSmall);
        }
        let file_data = data[..data_len].to_vec();
        data = &data[data_len..];
        files.push(ClipboardFile {
            name,
            data: file_data,
        });
    }
    Ok(files)
}

/// whether an event type byte is one of the variable-length clipboard types
pub fn is_clipboard_event_type(event_type: u8) -> bool {
    event_type == EventType::ClipboardText as u8
        || event_type == EventType::ClipboardImage as u8
        || event_type == EventType::ClipboardFile as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(event: ProtoEvent) -> ProtoEvent {
        let (buf, len) = event.into();
        let _ = len;
        buf.try_into().expect("decode")
    }

    fn as_input(event: ProtoEvent) -> InputEvent {
        match event {
            ProtoEvent::Input(e) => e,
            other => panic!("expected Input, got {other:?}"),
        }
    }

    #[test]
    fn roundtrips_all_fixed_size_variants() {
        assert!(matches!(roundtrip(ProtoEvent::Ping), ProtoEvent::Ping));
        assert!(matches!(
            roundtrip(ProtoEvent::Pong(true)),
            ProtoEvent::Pong(true)
        ));
        assert!(matches!(
            roundtrip(ProtoEvent::Pong(false)),
            ProtoEvent::Pong(false)
        ));
        assert!(matches!(
            roundtrip(ProtoEvent::Ack(0xdeadbeef)),
            ProtoEvent::Ack(0xdeadbeef)
        ));

        // the f64 cross-axis t added in #483 must survive the wire
        match roundtrip(ProtoEvent::Enter(Position::Left, 0.25)) {
            ProtoEvent::Enter(p, t) => {
                assert!(matches!(p, Position::Left));
                assert!((t - 0.25).abs() < f64::EPSILON);
            }
            other => panic!("expected Enter, got {other:?}"),
        }
        match roundtrip(ProtoEvent::Leave(42, 0.75)) {
            ProtoEvent::Leave(s, t) => {
                assert_eq!(s, 42);
                assert!((t - 0.75).abs() < f64::EPSILON);
            }
            other => panic!("expected Leave, got {other:?}"),
        }

        let commit = *b"abc12345";
        match roundtrip(ProtoEvent::Hello { commit }) {
            ProtoEvent::Hello { commit: c } => assert_eq!(c, commit),
            other => panic!("expected Hello, got {other:?}"),
        }
    }

    #[test]
    fn roundtrips_pointer_events() {
        match as_input(roundtrip(ProtoEvent::Input(InputEvent::Pointer(
            PointerEvent::Motion {
                time: 7,
                dx: -1.5,
                dy: 2.5,
            },
        )))) {
            InputEvent::Pointer(PointerEvent::Motion { time, dx, dy }) => {
                assert_eq!(time, 7);
                assert!((dx - -1.5).abs() < f64::EPSILON);
                assert!((dy - 2.5).abs() < f64::EPSILON);
            }
            other => panic!("expected Motion, got {other:?}"),
        }
        match as_input(roundtrip(ProtoEvent::Input(InputEvent::Pointer(
            PointerEvent::Button {
                time: 1,
                button: 0x110,
                state: 1,
            },
        )))) {
            InputEvent::Pointer(PointerEvent::Button { button, state, .. }) => {
                assert_eq!(button, 0x110);
                assert_eq!(state, 1);
            }
            other => panic!("expected Button, got {other:?}"),
        }
        match as_input(roundtrip(ProtoEvent::Input(InputEvent::Pointer(
            PointerEvent::Axis {
                time: 3,
                axis: 0,
                value: -120.0,
            },
        )))) {
            InputEvent::Pointer(PointerEvent::Axis { axis, value, .. }) => {
                assert_eq!(axis, 0);
                assert!((value - -120.0).abs() < f64::EPSILON);
            }
            other => panic!("expected Axis, got {other:?}"),
        }
        match as_input(roundtrip(ProtoEvent::Input(InputEvent::Pointer(
            PointerEvent::AxisDiscrete120 { axis: 1, value: -2 },
        )))) {
            InputEvent::Pointer(PointerEvent::AxisDiscrete120 { axis, value }) => {
                assert_eq!(axis, 1);
                assert_eq!(value, -2);
            }
            other => panic!("expected AxisDiscrete120, got {other:?}"),
        }
    }

    #[test]
    fn roundtrips_keyboard_events() {
        match as_input(roundtrip(ProtoEvent::Input(InputEvent::Keyboard(
            KeyboardEvent::Key {
                time: 9,
                key: 30,
                state: 1,
            },
        )))) {
            InputEvent::Keyboard(KeyboardEvent::Key { time, key, state }) => {
                assert_eq!(time, 9);
                assert_eq!(key, 30);
                assert_eq!(state, 1);
            }
            other => panic!("expected Key, got {other:?}"),
        }
        match as_input(roundtrip(ProtoEvent::Input(InputEvent::Keyboard(
            KeyboardEvent::Modifiers {
                depressed: 4,
                latched: 0,
                locked: 2,
                group: 1,
            },
        )))) {
            InputEvent::Keyboard(KeyboardEvent::Modifiers {
                depressed,
                latched,
                locked,
                group,
            }) => {
                assert_eq!(depressed, 4);
                assert_eq!(latched, 0);
                assert_eq!(locked, 2);
                assert_eq!(group, 1);
            }
            other => panic!("expected Modifiers, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_event_type() {
        // forward-compat: a newer peer's unknown event type must fail
        // decode (the receive loop skips it) rather than mis-parse
        let mut buf = [0u8; MAX_EVENT_SIZE];
        buf[0] = u8::MAX;
        assert!(ProtoEvent::try_from(buf).is_err());
    }

    #[test]
    fn clipboard_text_is_not_fixed_size_decodable() {
        // the variable-length clipboard event must not decode via the
        // fixed-size path, which dispatches on buf[0] first
        let mut buf = [0u8; MAX_EVENT_SIZE];
        buf[0] = EventType::ClipboardText as u8;
        assert!(matches!(
            ProtoEvent::try_from(buf),
            Err(ProtocolError::BufferTooSmall)
        ));
    }

    #[test]
    fn clipboard_roundtrip() {
        let event = ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Text(
            "hello clipboard".to_owned(),
        )));
        let buf = encode_clipboard_event(&event).expect("encode");
        match decode_clipboard_event(&buf).expect("decode") {
            ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Text(t))) => {
                assert_eq!(t, "hello clipboard");
            }
            other => panic!("expected clipboard, got {other:?}"),
        }
    }

    #[test]
    fn clipboard_rejects_oversize_and_truncated() {
        let big = ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Text(
            "x".repeat(MAX_CLIPBOARD_TRANSFER_SIZE + 1),
        )));
        assert!(matches!(
            encode_clipboard_event(&big),
            Err(ProtocolError::ClipboardTooLarge(_))
        ));

        // declared length larger than the payload actually carries
        let mut buf = vec![EventType::ClipboardText as u8];
        buf.extend_from_slice(&10u32.to_be_bytes());
        buf.extend_from_slice(b"hi");
        assert!(matches!(
            decode_clipboard_event(&buf),
            Err(ProtocolError::BufferTooSmall)
        ));

        assert!(matches!(
            decode_clipboard_event(&[]),
            Err(ProtocolError::BufferTooSmall)
        ));
    }

    #[test]
    fn clipboard_image_roundtrip() {
        let png = input_event::encode_image_rgba(2, 2, &[255u8; 16]).expect("png encode");
        let event = ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Image(png.clone())));
        let buf = encode_clipboard_event(&event).expect("encode");
        assert_eq!(buf[0], EventType::ClipboardImage as u8);
        assert!(is_clipboard_event_type(buf[0]));
        match decode_clipboard_event(&buf).expect("decode") {
            ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Image(data))) => {
                assert_eq!(data, png);
                let (w, h, rgba) = input_event::decode_image_rgba(&data).expect("png decode");
                assert_eq!((w, h), (2, 2));
                assert_eq!(rgba, vec![255u8; 16]);
            }
            other => panic!("expected clipboard image, got {other:?}"),
        }
    }

    #[test]
    fn clipboard_image_rejects_oversize() {
        let big = ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Image(vec![
            0u8;
            MAX_CLIPBOARD_TRANSFER_SIZE
                + 1
        ])));
        assert!(matches!(
            encode_clipboard_event(&big),
            Err(ProtocolError::ClipboardTooLarge(_))
        ));
    }

    #[test]
    fn clipboard_files_roundtrip() {
        let event = ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Files(vec![
            ClipboardFile {
                name: "报告.txt".to_string(),
                data: b"hello".to_vec(),
            },
            ClipboardFile {
                name: "a.bin".to_string(),
                data: vec![0u8; 3000],
            },
        ])));
        let buf = encode_clipboard_event(&event).expect("encode");
        assert_eq!(buf[0], EventType::ClipboardFile as u8);
        assert!(is_clipboard_event_type(buf[0]));
        match decode_clipboard_event(&buf).expect("decode") {
            ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Files(files))) => {
                assert_eq!(files.len(), 2);
                assert_eq!(files[0].name, "报告.txt");
                assert_eq!(files[0].data, b"hello");
                assert_eq!(files[1].name, "a.bin");
                assert_eq!(files[1].data.len(), 3000);
            }
            other => panic!("expected clipboard files, got {other:?}"),
        }
    }

    #[test]
    fn clipboard_files_reject_truncated() {
        // count=1 then a name_len that overruns the payload
        let mut data = vec![];
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&100u32.to_be_bytes());
        data.extend_from_slice(b"x");
        let mut buf = vec![EventType::ClipboardFile as u8];
        buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
        buf.extend_from_slice(&data);
        assert!(matches!(
            decode_clipboard_event(&buf),
            Err(ProtocolError::BufferTooSmall)
        ));
    }

    fn fragment_event(data: Vec<u8>) -> (Vec<u8>, Vec<Vec<u8>>) {
        let event = ProtoEvent::Input(InputEvent::Clipboard(ClipboardEvent::Image(data)));
        let encoded = encode_clipboard_event(&event).expect("encode");
        let fragments = ClipboardFragmenter::new(&encoded).collect();
        (encoded, fragments)
    }

    #[test]
    fn fragmenter_splits_to_payload_size() {
        let (encoded, fragments) = fragment_event(vec![0u8; 10_000]);
        assert_eq!(
            fragments.len(),
            encoded.len().div_ceil(CLIPBOARD_FRAGMENT_PAYLOAD)
        );
        for f in &fragments {
            assert_eq!(f[0], EventType::ClipboardFragment as u8);
            assert!(is_clipboard_fragment_type(f[0]));
            assert!(f.len() <= CLIPBOARD_FRAGMENT_HEADER + CLIPBOARD_FRAGMENT_PAYLOAD);
            let total = u32::from_be_bytes(f[1..5].try_into().unwrap());
            let num = u32::from_be_bytes(f[9..13].try_into().unwrap());
            assert_eq!(total as usize, encoded.len());
            assert_eq!(num as usize, fragments.len());
        }
    }

    #[test]
    fn reassembler_reassembles_out_of_order_and_dups() {
        let (encoded, fragments) = fragment_event(vec![7u8; 5000]);
        let mut reasm = ClipboardReassembler::new();
        // feed all but the first fragment in reverse order, each twice
        // (duplicates must be ignored)
        for f in fragments.iter().skip(1).rev() {
            assert!(reasm.push(f).unwrap().is_none());
            assert!(reasm.push(f).unwrap().is_none());
        }
        let done = reasm.push(&fragments[0]).unwrap().expect("complete");
        assert_eq!(done, encoded);
    }

    #[test]
    fn reassembler_reassembles_in_order() {
        let (encoded, fragments) = fragment_event(vec![1u8; 3000]);
        let mut reasm = ClipboardReassembler::new();
        let mut out = None;
        for f in &fragments {
            out = reasm.push(f).unwrap();
        }
        assert_eq!(out.as_deref(), Some(encoded.as_slice()));
    }

    #[test]
    fn reassembler_new_transfer_resets_partial() {
        let (_, frags_a) = fragment_event(vec![1u8; 3000]);
        let (enc_b, frags_b) = fragment_event(vec![2u8; 3000]);
        let mut reasm = ClipboardReassembler::new();
        // half of transfer A, then a different transfer's header
        assert!(reasm.push(&frags_a[0]).unwrap().is_none());
        assert!(reasm.progress().is_some());
        for f in &frags_b {
            if let Some(done) = reasm.push(f).unwrap() {
                assert_eq!(done, enc_b);
                return;
            }
        }
        panic!("transfer B never completed");
    }

    #[test]
    fn reassembler_rejects_malformed() {
        let mut reasm = ClipboardReassembler::new();
        // wrong event type
        assert!(reasm.push(&[0u8; 20]).is_err());
        // too short
        assert!(
            reasm
                .push(&[EventType::ClipboardFragment as u8, 1, 2])
                .is_err()
        );
        // seq >= num
        let mut bad = vec![EventType::ClipboardFragment as u8];
        bad.extend_from_slice(&100u32.to_be_bytes());
        bad.extend_from_slice(&5u32.to_be_bytes());
        bad.extend_from_slice(&2u32.to_be_bytes());
        bad.extend_from_slice(&[0u8; 10]);
        assert!(reasm.push(&bad).is_err());
        // oversized total
        let mut bad = vec![EventType::ClipboardFragment as u8];
        bad.extend_from_slice(&(u32::MAX).to_be_bytes());
        bad.extend_from_slice(&0u32.to_be_bytes());
        bad.extend_from_slice(&1u32.to_be_bytes());
        bad.extend_from_slice(&[0u8; 10]);
        assert!(reasm.push(&bad).is_err());
    }
}
