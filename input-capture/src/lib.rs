use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
    sync::{Arc, Mutex},
    task::{Poll, ready},
};

use async_trait::async_trait;
use futures::StreamExt;
use futures_core::Stream;

use input_event::{Event, KeyboardEvent, scancode};

pub use error::{CaptureCreationError, CaptureError, InputCaptureError};

pub mod clipboard;
pub mod error;

#[cfg(any(windows, target_os = "macos"))]
mod enter_bind;

#[cfg(libei)]
mod libei;

#[cfg(target_os = "macos")]
mod macos;

#[cfg(layer_shell)]
mod layer_shell;

#[cfg(windows)]
mod windows;

#[cfg(any(windows, test))]
mod hook_queue;

#[cfg(x11)]
mod x11;

/// fallback input capture (does not produce events)
mod dummy;

pub type CaptureHandle = u64;

#[derive(Clone, Debug, PartialEq)]
pub enum CaptureEvent {
    /// capture on this capture handle is now active. The `f64` is the
    /// normalized (`0.0..=1.0`) position along the crossed edge, so the
    /// receiving side can warp its cursor to the matching spot. Backends
    /// that can't determine it report `0.5` (the edge's midpoint).
    Begin(f64),
    /// input event coming from capture handle
    Input(Event),
}

impl Display for CaptureEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CaptureEvent::Begin(t) => write!(f, "begin capture ({t:.2})"),
            CaptureEvent::Input(e) => write!(f, "{e}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum Position {
    Left,
    Right,
    Top,
    Bottom,
}

impl Position {
    pub fn opposite(&self) -> Self {
        match self {
            Position::Left => Self::Right,
            Position::Right => Self::Left,
            Position::Top => Self::Bottom,
            Position::Bottom => Self::Top,
        }
    }
}

impl Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pos = match self {
            Position::Left => "left",
            Position::Right => "right",
            Position::Top => "top",
            Position::Bottom => "bottom",
        };
        write!(f, "{pos}")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Backend {
    #[cfg(libei)]
    InputCapturePortal,
    #[cfg(layer_shell)]
    LayerShell,
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
            #[cfg(libei)]
            Backend::InputCapturePortal => write!(f, "input-capture-portal"),
            #[cfg(layer_shell)]
            Backend::LayerShell => write!(f, "layer-shell"),
            #[cfg(x11)]
            Backend::X11 => write!(f, "X11"),
            #[cfg(windows)]
            Backend::Windows => write!(f, "windows"),
            #[cfg(target_os = "macos")]
            Backend::MacOs => write!(f, "MacOS"),
            Backend::Dummy => write!(f, "dummy"),
        }
    }
}

pub struct InputCapture {
    /// capture backend
    capture: Box<dyn Capture>,
    /// handles used only to return an incoming emulated pointer
    enter_only_handles: HashSet<CaptureHandle>,
    /// number of enter-only handles at each position
    enter_only_positions: HashMap<Position, usize>,
    /// keys pressed by active capture
    pressed_keys: HashSet<scancode::Linux>,
    /// map from position to ids
    position_map: HashMap<Position, Vec<CaptureHandle>>,
    /// map from id to position
    id_map: HashMap<CaptureHandle, Position>,
    /// pending events
    pending: VecDeque<(CaptureHandle, CaptureEvent)>,
}

#[derive(Clone, Debug)]
pub enum WindowIdentifier {
    Wayland(String),
    X11(u32),
}

#[cfg(libei)]
impl From<WindowIdentifier> for ashpd::WindowIdentifier {
    fn from(identifier: WindowIdentifier) -> Self {
        match identifier {
            WindowIdentifier::Wayland(handle) => {
                ashpd::WindowIdentifier::from_xdg_foreign_exported(handle)
            }
            WindowIdentifier::X11(_) => todo!(),
        }
    }
}

impl InputCapture {
    /// create a new client with the given id
    pub async fn create(&mut self, id: CaptureHandle, pos: Position) -> Result<(), CaptureError> {
        self.create_inner(id, pos, false).await
    }

    /// Create a capture used only to return an incoming emulated pointer.
    pub async fn create_enter_only(
        &mut self,
        id: CaptureHandle,
        pos: Position,
    ) -> Result<(), CaptureError> {
        self.create_inner(id, pos, true).await
    }

    async fn create_inner(
        &mut self,
        id: CaptureHandle,
        pos: Position,
        enter_only: bool,
    ) -> Result<(), CaptureError> {
        assert!(!self.id_map.contains_key(&id));

        self.id_map.insert(id, pos);

        let result = if let Some(v) = self.position_map.get_mut(&pos) {
            v.push(id);
            Ok(())
        } else {
            self.position_map.insert(pos, vec![id]);
            self.capture.create(pos).await
        };
        result?;

        if enter_only {
            self.enter_only_handles.insert(id);
            let count = self.enter_only_positions.entry(pos).or_default();
            *count += 1;
            if *count == 1 {
                self.capture.set_enter_only(pos, true).await?;
            }
        }
        Ok(())
    }

    /// destroy the client with the given id, if it exists
    pub async fn destroy(&mut self, id: CaptureHandle) -> Result<(), CaptureError> {
        let pos = self
            .id_map
            .remove(&id)
            .expect("no position for this handle");

        if self.enter_only_handles.remove(&id) {
            let count = self
                .enter_only_positions
                .get_mut(&pos)
                .expect("no enter-only count for this handle");
            *count -= 1;
            if *count == 0 {
                self.enter_only_positions.remove(&pos);
                self.capture.set_enter_only(pos, false).await?;
            }
        }

        log::debug!("destroying capture {id} @ {pos}");
        let remaining = self.position_map.get_mut(&pos).expect("id vector");
        remaining.retain(|&i| i != id);

        log::debug!("remaining ids @ {pos}: {remaining:?}");
        if remaining.is_empty() {
            log::debug!("destroying capture @ {pos} - no remaining ids");
            self.position_map.remove(&pos);
            self.capture.destroy(pos).await?;
        }
        Ok(())
    }

    /// release mouse
    pub async fn release(&mut self) -> Result<(), CaptureError> {
        self.pressed_keys.clear();
        self.capture.release().await
    }

    /// release mouse, first warping the cursor to the normalized
    /// (`0.0..=1.0`) cross-axis position `t` along the edge it's
    /// currently captured at — so a peer that detected its own local
    /// crossing back can hand the cursor back at the matching spot.
    pub async fn release_to(&mut self, t: f64) -> Result<(), CaptureError> {
        self.pressed_keys.clear();
        self.capture.release_to(t).await
    }

    /// Drain and return every key the capture has forwarded as
    /// down-but-not-up. The caller is expected to synthesize key-up
    /// events to the remote peer for each — otherwise the peer
    /// retains phantom-held keys after capture is released. The
    /// canonical case is the release-bind chord
    /// (Ctrl+Shift+Alt+Meta): the down events were sent while
    /// capture was active, but the matching up events arrive after
    /// the local tap has flipped to passthrough and never reach
    /// the peer.
    pub fn take_pressed_keys(&mut self) -> HashSet<scancode::Linux> {
        std::mem::take(&mut self.pressed_keys)
    }

    /// destroy the input capture
    pub async fn terminate(&mut self) -> Result<(), CaptureError> {
        self.capture.terminate().await
    }

    /// creates a new [`InputCapture`]
    pub async fn new(
        backend: Option<Backend>,
        window_identifier: Arc<Mutex<Option<WindowIdentifier>>>,
    ) -> Result<Self, CaptureCreationError> {
        let capture = create(backend, window_identifier).await?;
        Ok(Self {
            capture,
            enter_only_handles: Default::default(),
            enter_only_positions: Default::default(),
            id_map: Default::default(),
            pending: Default::default(),
            position_map: Default::default(),
            pressed_keys: HashSet::new(),
        })
    }

    /// check whether the given keys are pressed
    pub fn keys_pressed(&self, keys: &[scancode::Linux]) -> bool {
        keys.iter().all(|k| self.pressed_keys.contains(k))
    }

    /// Set the key binds that begin capture at the given position
    /// without the pointer having to cross the corresponding screen
    /// edge.
    ///
    /// A bind only takes effect while capture is inactive and while a
    /// client exists at that position — the resulting capture is
    /// indistinguishable from one started by an edge crossing.
    ///
    /// Backends that cannot observe key events while capture is
    /// inactive ignore this.
    pub fn set_enter_binds(&mut self, binds: HashMap<Position, Vec<scancode::Linux>>) {
        self.capture.set_enter_binds(binds);
    }

    fn update_pressed_keys(&mut self, key: u32, state: u8) {
        if let Ok(scancode) = scancode::Linux::try_from(key) {
            log::debug!("key: {key}, state: {state}, scancode: {scancode:?}");
            match state {
                1 => self.pressed_keys.insert(scancode),
                _ => self.pressed_keys.remove(&scancode),
            };
        }
    }
}

impl Stream for InputCapture {
    type Item = Result<(CaptureHandle, CaptureEvent), CaptureError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(e) = self.pending.pop_front() {
            return Poll::Ready(Some(Ok(e)));
        }

        loop {
            // ready
            let event = ready!(self.capture.poll_next_unpin(cx));

            // stream closed
            let event = match event {
                Some(e) => e,
                None => return Poll::Ready(None),
            };

            // error occurred
            let (pos, event) = match event {
                Ok(e) => e,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };
            let event_requires_enter_only = self.capture.last_event_requires_enter_only();

            // handle key presses
            if let CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key { key, state, .. })) =
                event
            {
                self.update_pressed_keys(key, state);
            }

            let handles = self
                .position_map
                .get(&pos)
                .map(|ids| {
                    route_handles(
                        ids,
                        &self.enter_only_handles,
                        &event,
                        event_requires_enter_only,
                    )
                })
                .unwrap_or_default();

            match handles.len() {
                0 => continue,
                1 => return Poll::Ready(Some(Ok((handles[0], event)))),
                _ => {
                    for id in handles {
                        self.pending.push_back((id, event.clone()));
                    }

                    return Poll::Ready(Some(Ok(self.pending.pop_front().expect("event"))));
                }
            }
        }
    }
}

#[async_trait]
trait Capture: Stream<Item = Result<(Position, CaptureEvent), CaptureError>> + Unpin {
    /// create a new client with the given id
    async fn create(&mut self, pos: Position) -> Result<(), CaptureError>;

    /// destroy the client with the given id, if it exists
    async fn destroy(&mut self, pos: Position) -> Result<(), CaptureError>;

    /// Mark a position as an incoming-pointer return edge.
    async fn set_enter_only(&mut self, pos: Position, enabled: bool) -> Result<(), CaptureError>;

    /// Whether the most recently yielded event must go only to enter-only handles.
    fn last_event_requires_enter_only(&self) -> bool {
        false
    }

    /// release mouse
    async fn release(&mut self) -> Result<(), CaptureError>;

    /// release, first warping the cursor to normalized cross-axis
    /// position `t` along the edge currently captured at. Best-effort:
    /// backends that can't warp just release, same as before this
    /// existed — the cursor stays wherever it already was.
    async fn release_to(&mut self, t: f64) -> Result<(), CaptureError>;

    /// destroy the input capture
    async fn terminate(&mut self) -> Result<(), CaptureError>;

    /// see [`InputCapture::set_enter_binds`]
    ///
    /// Implementing this is optional: a backend that cannot observe
    /// key events while capture is inactive (e.g. the input capture
    /// portal) keeps the default no-op, which leaves entering a
    /// client to edge crossings only.
    fn set_enter_binds(&mut self, _binds: HashMap<Position, Vec<scancode::Linux>>) {}
}

fn route_handles(
    handles: &[CaptureHandle],
    enter_only_handles: &HashSet<CaptureHandle>,
    event: &CaptureEvent,
    event_requires_enter_only: bool,
) -> Vec<CaptureHandle> {
    if matches!(event, CaptureEvent::Begin(_)) && event_requires_enter_only {
        handles
            .iter()
            .copied()
            .filter(|handle| enter_only_handles.contains(handle))
            .collect()
    } else {
        handles.to_vec()
    }
}

async fn create_backend(
    backend: Backend,
    // only the libei backend consumes the identifier; other platforms
    // compile that branch out, leaving the parameter unused
    #[cfg_attr(not(libei), allow(unused_variables))] window_identifier: Arc<
        Mutex<Option<WindowIdentifier>>,
    >,
) -> Result<
    Box<dyn Capture<Item = Result<(Position, CaptureEvent), CaptureError>>>,
    CaptureCreationError,
> {
    match backend {
        #[cfg(libei)]
        Backend::InputCapturePortal => Ok(Box::new(
            libei::LibeiInputCapture::new(window_identifier).await?,
        )),
        #[cfg(layer_shell)]
        Backend::LayerShell => Ok(Box::new(layer_shell::LayerShellInputCapture::new()?)),
        #[cfg(x11)]
        Backend::X11 => Ok(Box::new(x11::X11InputCapture::new()?)),
        #[cfg(windows)]
        Backend::Windows => Ok(Box::new(windows::WindowsInputCapture::new())),
        #[cfg(target_os = "macos")]
        Backend::MacOs => Ok(Box::new(macos::MacOSInputCapture::new().await?)),
        Backend::Dummy => Ok(Box::new(dummy::DummyInputCapture::new())),
    }
}

async fn create(
    backend: Option<Backend>,
    window_identifier: Arc<Mutex<Option<WindowIdentifier>>>,
) -> Result<
    Box<dyn Capture<Item = Result<(Position, CaptureEvent), CaptureError>>>,
    CaptureCreationError,
> {
    if let Some(backend) = backend {
        let b = create_backend(backend, window_identifier).await;
        if b.is_ok() {
            log::info!("using capture backend: {backend}");
        }
        return b;
    }

    for backend in [
        #[cfg(libei)]
        Backend::InputCapturePortal,
        #[cfg(layer_shell)]
        Backend::LayerShell,
        #[cfg(x11)]
        Backend::X11,
        #[cfg(windows)]
        Backend::Windows,
        #[cfg(target_os = "macos")]
        Backend::MacOs,
    ] {
        match create_backend(backend, window_identifier.clone()).await {
            Ok(b) => {
                log::info!("using capture backend: {backend}");
                return Ok(b);
            }
            Err(e) if e.cancelled_by_user() => return Err(e),
            Err(e) => log::warn!("{backend} input capture backend unavailable: {e}"),
        }
    }
    Err(CaptureCreationError::NoAvailableBackend)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet, VecDeque},
        pin::Pin,
        task::{Context, Poll},
    };

    use async_trait::async_trait;
    use futures::task::noop_waker_ref;
    use futures_core::Stream;

    use super::{Capture, CaptureError, CaptureEvent, InputCapture, Position, route_handles};

    struct QueuedCapture {
        events: VecDeque<(Position, CaptureEvent, bool)>,
        last_event_requires_enter_only: bool,
    }

    impl Stream for QueuedCapture {
        type Item = Result<(Position, CaptureEvent), CaptureError>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.events.pop_front() {
                Some((pos, event, requires_enter_only)) => {
                    self.last_event_requires_enter_only = requires_enter_only;
                    Poll::Ready(Some(Ok((pos, event))))
                }
                None => Poll::Pending,
            }
        }
    }

    #[async_trait]
    impl Capture for QueuedCapture {
        async fn create(&mut self, _pos: Position) -> Result<(), CaptureError> {
            Ok(())
        }

        async fn destroy(&mut self, _pos: Position) -> Result<(), CaptureError> {
            Ok(())
        }

        async fn set_enter_only(
            &mut self,
            _pos: Position,
            _enabled: bool,
        ) -> Result<(), CaptureError> {
            Ok(())
        }

        fn last_event_requires_enter_only(&self) -> bool {
            self.last_event_requires_enter_only
        }

        async fn release(&mut self) -> Result<(), CaptureError> {
            Ok(())
        }

        async fn release_to(&mut self, _t: f64) -> Result<(), CaptureError> {
            Ok(())
        }

        async fn terminate(&mut self) -> Result<(), CaptureError> {
            Ok(())
        }
    }

    #[test]
    fn emulated_begin_is_routed_only_to_enter_only_handles() {
        let enter_only = HashSet::from([2]);
        assert_eq!(
            route_handles(&[1, 2], &enter_only, &CaptureEvent::Begin(0.5), true),
            vec![2]
        );
    }

    #[test]
    fn physical_begin_is_routed_to_all_handles() {
        let enter_only = HashSet::from([2]);
        assert_eq!(
            route_handles(&[1, 2], &enter_only, &CaptureEvent::Begin(0.5), false),
            vec![1, 2]
        );
    }

    #[test]
    fn unroutable_event_does_not_stall_the_next_ready_event() {
        let backend = QueuedCapture {
            events: VecDeque::from([
                (Position::Left, CaptureEvent::Begin(0.5), true),
                (Position::Left, CaptureEvent::Begin(0.5), false),
            ]),
            last_event_requires_enter_only: false,
        };
        let mut capture = InputCapture {
            capture: Box::new(backend),
            enter_only_handles: HashSet::new(),
            enter_only_positions: HashMap::new(),
            pressed_keys: HashSet::new(),
            id_map: HashMap::from([(7, Position::Left)]),
            pending: VecDeque::new(),
            position_map: HashMap::from([(Position::Left, vec![7])]),
        };
        let mut context = Context::from_waker(noop_waker_ref());

        match Pin::new(&mut capture).poll_next(&mut context) {
            Poll::Ready(Some(Ok((7, CaptureEvent::Begin(0.5))))) => {}
            other => panic!("unexpected poll result: {other:?}"),
        }
    }
}
