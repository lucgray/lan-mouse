use async_trait::async_trait;
use futures_core::Stream;
use std::{
    collections::{HashSet, VecDeque},
    env,
    fmt::{self, Display},
    io::{self, ErrorKind},
    os::fd::{AsFd, RawFd},
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::io::unix::AsyncFd;

use std::{
    fs::File,
    io::{BufWriter, Write},
    os::unix::prelude::AsRawFd,
    sync::Arc,
};

use wayland_protocols::{
    wp::{
        keyboard_shortcuts_inhibit::zv1::client::{
            zwp_keyboard_shortcuts_inhibit_manager_v1::ZwpKeyboardShortcutsInhibitManagerV1,
            zwp_keyboard_shortcuts_inhibitor_v1::ZwpKeyboardShortcutsInhibitorV1,
        },
        pointer_constraints::zv1::client::{
            zwp_locked_pointer_v1::ZwpLockedPointerV1,
            zwp_pointer_constraints_v1::{Lifetime, ZwpPointerConstraintsV1},
        },
        relative_pointer::zv1::client::{
            zwp_relative_pointer_manager_v1::ZwpRelativePointerManagerV1,
            zwp_relative_pointer_v1::{self, ZwpRelativePointerV1},
        },
    },
    xdg::xdg_output::zv1::client::{
        zxdg_output_manager_v1::ZxdgOutputManagerV1,
        zxdg_output_v1::{self, ZxdgOutputV1},
    },
};

use wayland_protocols_wlr::layer_shell::v1::client::{
    zwlr_layer_shell_v1::{Layer, ZwlrLayerShellV1},
    zwlr_layer_surface_v1::{self, Anchor, KeyboardInteractivity, ZwlrLayerSurfaceV1},
};

use wayland_client::{
    Connection, Dispatch, DispatchError, EventQueue, QueueHandle, WEnum,
    backend::{ReadEventsGuard, WaylandError},
    delegate_noop,
    globals::{Global, GlobalList, GlobalListContents, registry_queue_init},
    protocol::{
        wl_buffer, wl_compositor,
        wl_keyboard::{self, WlKeyboard},
        wl_output::{self, WlOutput},
        wl_pointer::{self, WlPointer},
        wl_region,
        wl_registry::{self, WlRegistry},
        wl_seat, wl_shm, wl_shm_pool,
        wl_surface::WlSurface,
    },
};

use input_event::{Event, KeyboardEvent, PointerEvent};

use crate::{CaptureError, CaptureEvent};

use super::{
    Capture, Position,
    error::{LayerShellCaptureCreationError, WaylandBindError},
};

struct Globals {
    compositor: wl_compositor::WlCompositor,
    pointer_constraints: ZwpPointerConstraintsV1,
    relative_pointer_manager: ZwpRelativePointerManagerV1,
    shortcut_inhibit_manager: Option<ZwpKeyboardShortcutsInhibitManagerV1>,
    seat: wl_seat::WlSeat,
    shm: wl_shm::WlShm,
    layer_shell: ZwlrLayerShellV1,
    xdg_output_manager: ZxdgOutputManagerV1,
}

#[derive(Clone, Debug)]
struct Output {
    wl_output: WlOutput,
    global: Global,
    info: Option<OutputInfo>,
    pending_info: OutputInfo,
    has_xdg_info: bool,
}

impl Display for Output {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(info) = &self.info {
            write!(
                f,
                "{} {}x{} @pos {:?} ({})",
                info.name, info.size.0, info.size.1, info.position, info.description
            )
        } else {
            write!(f, "unknown output")
        }
    }
}

#[derive(Clone, Debug, Default)]
struct OutputInfo {
    description: String,
    name: String,
    position: (i32, i32),
    size: (i32, i32),
}

const MAX_LAYER_SHELL_EVENTS: usize = 256;

#[derive(Default)]
struct PendingCaptureEvents {
    events: VecDeque<(Position, CaptureEvent)>,
    overloaded: bool,
    report_overload: bool,
}

impl PendingCaptureEvents {
    fn push_back(&mut self, event: (Position, CaptureEvent)) -> bool {
        if self.overloaded {
            return false;
        }
        if self.events.len() == MAX_LAYER_SHELL_EVENTS {
            self.overloaded = true;
            self.report_overload = true;
            self.events.clear();
            return true;
        }
        self.events.push_back(event);
        false
    }

    fn pop_front(&mut self) -> Option<Result<(Position, CaptureEvent), CaptureError>> {
        if self.report_overload {
            self.report_overload = false;
            return Some(Err(CaptureError::LayerShellQueueOverloaded));
        }
        self.events.pop_front().map(Ok)
    }
}

struct State {
    active_positions: HashSet<Position>,
    pointer: Option<WlPointer>,
    keyboard: Option<WlKeyboard>,
    pointer_lock: Option<ZwpLockedPointerV1>,
    rel_pointer: Option<ZwpRelativePointerV1>,
    shortcut_inhibitor: Option<ZwpKeyboardShortcutsInhibitorV1>,
    active_windows: Vec<Arc<Window>>,
    focused: Option<Arc<Window>>,
    global_list: GlobalList,
    globals: Globals,
    read_guard: Option<ReadEventsGuard>,
    qh: QueueHandle<Self>,
    pending_events: PendingCaptureEvents,
    outputs: Vec<Output>,
    scroll_discrete_pending: bool,
}

struct Inner {
    state: State,
    queue: EventQueue<State>,
}

impl AsRawFd for Inner {
    fn as_raw_fd(&self) -> RawFd {
        self.queue.as_fd().as_raw_fd()
    }
}

pub struct LayerShellInputCapture {
    inner: AsyncFd<Inner>,
    terminated: bool,
}

struct Window {
    buffer: wl_buffer::WlBuffer,
    surface: WlSurface,
    layer_surface: ZwlrLayerSurfaceV1,
    pos: Position,
}

impl Window {
    fn new(
        state: &State,
        qh: &QueueHandle<State>,
        output: &WlOutput,
        pos: Position,
        size: (i32, i32),
    ) -> Window {
        log::debug!("creating window output: {output:?}, size: {size:?}");
        let g = &state.globals;

        let (width, height) = match pos {
            Position::Left | Position::Right => (1, size.1 as u32),
            Position::Top | Position::Bottom => (size.0 as u32, 1),
        };
        let mut file = tempfile::tempfile().unwrap();
        draw(&mut file, (width, height));
        let pool = g
            .shm
            .create_pool(file.as_fd(), (width * height * 4) as i32, qh, ());
        let buffer = pool.create_buffer(
            0,
            width as i32,
            height as i32,
            (width * 4) as i32,
            wl_shm::Format::Argb8888,
            qh,
            (),
        );
        let surface = g.compositor.create_surface(qh, ());

        let layer_surface = g.layer_shell.get_layer_surface(
            &surface,
            Some(output),
            Layer::Overlay,
            "LAN Mouse Sharing".into(),
            qh,
            (),
        );
        let anchor = match pos {
            Position::Left => Anchor::Left,
            Position::Right => Anchor::Right,
            Position::Top => Anchor::Top,
            Position::Bottom => Anchor::Bottom,
        };

        layer_surface.set_anchor(anchor);
        layer_surface.set_size(width, height);
        layer_surface.set_exclusive_zone(-1);
        layer_surface.set_margin(0, 0, 0, 0);
        surface.set_input_region(None);
        surface.commit();
        Window {
            pos,
            buffer,
            surface,
            layer_surface,
        }
    }
}

impl Drop for Window {
    fn drop(&mut self) {
        log::debug!("destroying window!");
        self.layer_surface.destroy();
        self.surface.destroy();
        self.buffer.destroy();
    }
}

fn get_edges(outputs: &[Output], pos: Position) -> Vec<(Output, i32)> {
    outputs
        .iter()
        .filter_map(|output| {
            output.info.as_ref().map(|info| {
                (
                    output.clone(),
                    match pos {
                        Position::Left => info.position.0,
                        Position::Right => info.position.0 + info.size.0,
                        Position::Top => info.position.1,
                        Position::Bottom => info.position.1 + info.size.1,
                    },
                )
            })
        })
        .collect()
}

fn get_output_configuration(state: &State, pos: Position) -> Vec<Output> {
    // get all output edges corresponding to the position
    let edges = get_edges(&state.outputs, pos);
    let opposite_edges = get_edges(&state.outputs, pos.opposite());

    // remove those edges that are at the same position
    // as an opposite edge of a different output
    edges
        .iter()
        .filter(|(_, edge)| !opposite_edges.iter().map(|(_, e)| *e).any(|e| &e == edge))
        .map(|(o, _)| o.clone())
        .collect()
}

fn draw(f: &mut File, (width, height): (u32, u32)) {
    let mut buf = BufWriter::new(f);
    for _ in 0..height {
        for _ in 0..width {
            if env::var("LM_DEBUG_LAYER_SHELL").ok().is_some() {
                // AARRGGBB
                buf.write_all(&0xff11d116u32.to_ne_bytes()).unwrap();
            } else {
                // AARRGGBB
                buf.write_all(&0x00000000u32.to_ne_bytes()).unwrap();
            }
        }
    }
}

impl LayerShellInputCapture {
    pub fn new() -> std::result::Result<Self, LayerShellCaptureCreationError> {
        let conn = Connection::connect_to_env()?;
        let (global_list, mut queue) = registry_queue_init::<State>(&conn)?;

        let qh = queue.handle();

        let compositor: wl_compositor::WlCompositor = global_list
            .bind(&qh, 4..=5, ())
            .map_err(|e| WaylandBindError::new(e, "wl_compositor 4..=5"))?;
        let xdg_output_manager: ZxdgOutputManagerV1 = global_list
            .bind(&qh, 1..=3, ())
            .map_err(|e| WaylandBindError::new(e, "xdg_output_manager 1..=3"))?;
        let shm: wl_shm::WlShm = global_list
            .bind(&qh, 1..=1, ())
            .map_err(|e| WaylandBindError::new(e, "wl_shm"))?;
        let layer_shell: ZwlrLayerShellV1 = global_list
            .bind(&qh, 3..=4, ())
            .map_err(|e| WaylandBindError::new(e, "wlr_layer_shell 3..=4"))?;
        let seat: wl_seat::WlSeat = global_list
            .bind(&qh, 7..=8, ())
            .map_err(|e| WaylandBindError::new(e, "wl_seat 7..=8"))?;

        let pointer_constraints: ZwpPointerConstraintsV1 = global_list
            .bind(&qh, 1..=1, ())
            .map_err(|e| WaylandBindError::new(e, "zwp_pointer_constraints_v1"))?;
        let relative_pointer_manager: ZwpRelativePointerManagerV1 = global_list
            .bind(&qh, 1..=1, ())
            .map_err(|e| WaylandBindError::new(e, "zwp_relative_pointer_manager_v1"))?;
        let shortcut_inhibit_manager: Result<
            ZwpKeyboardShortcutsInhibitManagerV1,
            WaylandBindError,
        > = global_list
            .bind(&qh, 1..=1, ())
            .map_err(|e| WaylandBindError::new(e, "zwp_keyboard_shortcuts_inhibit_manager_v1"));
        // layer-shell backend still works without this protocol so we make it an optional dependency
        if let Err(e) = &shortcut_inhibit_manager {
            log::warn!("shortcut_inhibit_manager not supported: {e}\nkeybinds handled by the compositor will not be passed
                to the client");
        }
        let shortcut_inhibit_manager = shortcut_inhibit_manager.ok();

        let mut state = State {
            active_positions: Default::default(),
            pointer: None,
            keyboard: None,
            global_list,
            globals: Globals {
                compositor,
                shm,
                layer_shell,
                seat,
                pointer_constraints,
                relative_pointer_manager,
                shortcut_inhibit_manager,
                xdg_output_manager,
            },
            pointer_lock: None,
            rel_pointer: None,
            shortcut_inhibitor: None,
            active_windows: Vec::new(),
            focused: None,
            qh,
            read_guard: None,
            pending_events: PendingCaptureEvents::default(),
            outputs: vec![],
            scroll_discrete_pending: false,
        };

        for global in state.global_list.contents().clone_list() {
            state.register_global(global);
        }

        // flush outgoing events
        queue.flush()?;

        let read_guard = loop {
            match queue.prepare_read() {
                Some(r) => break r,
                None => {
                    queue.dispatch_pending(&mut state)?;
                    continue;
                }
            }
        };
        state.read_guard = Some(read_guard);

        let inner = AsyncFd::new(Inner { queue, state })?;

        Ok(LayerShellInputCapture {
            inner,
            terminated: false,
        })
    }

    fn add_client(&mut self, pos: Position) {
        self.inner.get_mut().state.add_client(pos);
    }

    fn delete_client(&mut self, pos: Position) {
        let inner = self.inner.get_mut();
        inner.state.active_positions.remove(&pos);
        inner.state.retire_windows(Some(pos));
    }
}

impl State {
    fn queue_capture_event(&mut self, event: (Position, CaptureEvent)) {
        if self.pending_events.push_back(event) {
            self.ungrab();
        }
    }

    fn update_output_info(&mut self, name: u32) {
        let output = self
            .outputs
            .iter_mut()
            .find(|o| o.global.name == name)
            .expect("output not found");
        if output.has_xdg_info {
            output.info.replace(output.pending_info.clone());
            self.update_windows();
        }
    }

    fn register_global(&mut self, global: Global) {
        if global.interface.as_str() == "wl_output" {
            log::debug!("new output global: wl_output {}", global.name);
            let wl_output = self.global_list.registry().bind::<WlOutput, _, _>(
                global.name,
                4,
                &self.qh,
                global.name,
            );
            self.globals
                .xdg_output_manager
                .get_xdg_output(&wl_output, &self.qh, global.name);
            self.outputs.push(Output {
                wl_output,
                global,
                info: None,
                has_xdg_info: false,
                pending_info: Default::default(),
            })
        }
    }

    fn deregister_global(&mut self, name: u32) {
        self.outputs.retain(|o| {
            if o.global.name == name {
                log::debug!("{o} (global {:?}) removed", o.global);
                o.wl_output.release();
                false
            } else {
                true
            }
        });
    }

    fn grab(
        &mut self,
        surface: &WlSurface,
        pointer: &WlPointer,
        serial: u32,
        qh: &QueueHandle<State>,
    ) {
        if self.pending_events.overloaded {
            return;
        }
        let window = self.focused.as_ref().unwrap();

        // hide the cursor
        pointer.set_cursor(serial, None, 0, 0);

        // capture input
        window
            .layer_surface
            .set_keyboard_interactivity(KeyboardInteractivity::Exclusive);
        window.surface.commit();

        // lock pointer
        if self.pointer_lock.is_none() {
            self.pointer_lock = Some(self.globals.pointer_constraints.lock_pointer(
                surface,
                pointer,
                None,
                Lifetime::Persistent,
                qh,
                (),
            ));
        }

        // request relative input
        if self.rel_pointer.is_none() {
            self.rel_pointer = Some(self.globals.relative_pointer_manager.get_relative_pointer(
                pointer,
                qh,
                (),
            ));
        }

        // capture modifier keys
        if let Some(shortcut_inhibit_manager) = &self.globals.shortcut_inhibit_manager {
            if self.shortcut_inhibitor.is_none() {
                self.shortcut_inhibitor = Some(shortcut_inhibit_manager.inhibit_shortcuts(
                    surface,
                    &self.globals.seat,
                    qh,
                    (),
                ));
            }
        }
    }

    fn ungrab(&mut self) {
        release_layer_capture(
            self.focused.take(),
            &mut self.pointer_lock,
            &mut self.rel_pointer,
            &mut self.shortcut_inhibitor,
        );
    }

    fn retire_windows(&mut self, position: Option<Position>) {
        retire_capture_windows(
            &mut self.active_windows,
            &mut self.focused,
            &mut self.pending_events,
            position,
            |window| window.pos,
            |focus| {
                release_layer_capture(
                    focus,
                    &mut self.pointer_lock,
                    &mut self.rel_pointer,
                    &mut self.shortcut_inhibitor,
                );
            },
        );
    }

    fn add_client(&mut self, pos: Position) {
        self.active_positions.insert(pos);
        let outputs = get_output_configuration(self, pos);

        log::info!(
            "adding capture for position {pos} - using outputs: {:?}",
            outputs
                .iter()
                .map(|o| o
                    .info
                    .as_ref()
                    .map(|i| i.name.to_owned())
                    .unwrap_or("unknown output".to_owned()))
                .collect::<Vec<_>>()
        );
        outputs.iter().for_each(|o| {
            if let Some(info) = o.info.as_ref() {
                let window = Window::new(self, &self.qh, &o.wl_output, pos, info.size);
                let window = Arc::new(window);
                self.active_windows.push(window);
            }
        });
    }

    fn update_windows(&mut self) {
        log::info!("active outputs: ");
        for output in self.outputs.iter().filter(|o| o.info.is_some()) {
            log::info!(" * {output}");
        }

        self.retire_windows(None);

        let active_positions = self.active_positions.iter().cloned().collect::<Vec<_>>();
        for pos in active_positions {
            self.add_client(pos);
        }
    }
}

fn release_layer_capture(
    focused: Option<Arc<Window>>,
    pointer_lock: &mut Option<ZwpLockedPointerV1>,
    rel_pointer: &mut Option<ZwpRelativePointerV1>,
    shortcut_inhibitor: &mut Option<ZwpKeyboardShortcutsInhibitorV1>,
) {
    ungrab_resources(
        focused,
        |window| {
            window
                .layer_surface
                .set_keyboard_interactivity(KeyboardInteractivity::None);
            window.surface.commit();
        },
        || {
            if let Some(pointer_lock) = pointer_lock.take() {
                pointer_lock.destroy();
            }
            if let Some(rel_pointer) = rel_pointer.take() {
                rel_pointer.destroy();
            }
            if let Some(shortcut_inhibitor) = shortcut_inhibitor.take() {
                shortcut_inhibitor.destroy();
            }
        },
    );
}

fn retire_capture_windows<W>(
    windows: &mut Vec<W>,
    focused: &mut Option<W>,
    pending: &mut PendingCaptureEvents,
    position: Option<Position>,
    window_position: impl Fn(&W) -> Position,
    release: impl FnOnce(Option<W>),
) {
    let removes = |window: &W| position.is_none_or(|pos| window_position(window) == pos);
    // Release while the surface still lives. Missing focus may leave orphaned
    // native capture resources; an unrelated live focus must remain untouched.
    if focused.as_ref().is_none_or(&removes) {
        release(focused.take());
    }
    windows.retain(|window| !removes(window));
    pending
        .events
        .retain(|(pos, _)| position.is_some_and(|removed| *pos != removed));
    // Removing windows does not recover a previously overloaded backend.
}

fn ungrab_resources<F>(
    focused: Option<F>,
    release_focus: impl FnOnce(&F),
    release_resources: impl FnOnce(),
) {
    // A retired focus can be the last surface owner. Keep it alive until all
    // capture objects referencing the surface have been destroyed.
    if let Some(focused) = focused.as_ref() {
        release_focus(focused);
    }
    release_resources();
}

fn terminate_capture(
    terminated: &mut bool,
    cleanup: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    // A previously failed stream still needs resource cleanup; do not skip it.
    *terminated = true;
    cleanup()
}

fn wayland_io_error(error: WaylandError) -> io::Error {
    match error {
        WaylandError::Io(error) => error,
        WaylandError::Protocol(error) => {
            io::Error::other(format!("Wayland protocol violation: {error}"))
        }
    }
}

fn read_wayland_result(result: Result<usize, WaylandError>) -> io::Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(WaylandError::Io(error)) if error.kind() == ErrorKind::WouldBlock => Ok(()),
        Err(error) => Err(wayland_io_error(error)),
    }
}

fn dispatch_wayland_result(result: Result<usize, DispatchError>) -> io::Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(DispatchError::Backend(error)) => Err(wayland_io_error(error)),
        Err(error) => Err(io::Error::other(format!("Wayland dispatch error: {error}"))),
    }
}

fn flush_wayland_result(result: Result<(), WaylandError>) -> io::Result<()> {
    result.map_err(wayland_io_error)
}

fn poll_capture_stream<T>(
    terminated: &mut bool,
    poll: impl FnOnce() -> Poll<Option<Result<T, CaptureError>>>,
) -> Poll<Option<Result<T, CaptureError>>> {
    if *terminated {
        return Poll::Ready(None);
    }
    let result = poll();
    if matches!(result, Poll::Ready(Some(Err(_))) | Poll::Ready(None)) {
        *terminated = true;
    }
    result
}

impl Inner {
    fn read(&mut self) -> io::Result<()> {
        let guard = self
            .state
            .read_guard
            .take()
            .ok_or_else(|| io::Error::other("Wayland read guard is missing"))?;
        read_wayland_result(guard.read())
    }

    fn prepare_read(&mut self) -> io::Result<()> {
        loop {
            if let Some(guard) = self.queue.prepare_read() {
                self.state.read_guard = Some(guard);
                break Ok(());
            } else {
                self.dispatch_events()?;
            }
        }
    }

    fn dispatch_events(&mut self) -> io::Result<()> {
        dispatch_wayland_result(self.queue.dispatch_pending(&mut self.state))
    }

    fn flush_events(&mut self) -> io::Result<()> {
        flush_wayland_result(self.queue.flush())
    }
}

#[async_trait]
impl Capture for LayerShellInputCapture {
    async fn create(&mut self, pos: Position) -> Result<(), CaptureError> {
        self.add_client(pos);
        let inner = self.inner.get_mut();
        Ok(inner.flush_events()?)
    }

    async fn destroy(&mut self, pos: Position) -> Result<(), CaptureError> {
        self.delete_client(pos);
        let inner = self.inner.get_mut();
        Ok(inner.flush_events()?)
    }

    async fn set_enter_only(&mut self, _pos: Position, _enabled: bool) -> Result<(), CaptureError> {
        Ok(())
    }

    async fn release(&mut self) -> Result<(), CaptureError> {
        log::debug!("releasing pointer");
        let inner = self.inner.get_mut();
        inner.state.ungrab();
        Ok(inner.flush_events()?)
    }

    async fn release_to(&mut self, _t: f64) -> Result<(), CaptureError> {
        self.release().await
    }

    async fn terminate(&mut self) -> Result<(), CaptureError> {
        let inner = self.inner.get_mut();
        Ok(terminate_capture(&mut self.terminated, || {
            inner.state.ungrab();
            inner.state.active_windows.clear();
            inner.state.active_positions.clear();
            inner.state.pending_events.events.clear();
            inner.state.pending_events.report_overload = false;
            inner.state.read_guard.take();
            inner.flush_events()
        })?)
    }
}

impl Stream for LayerShellInputCapture {
    type Item = Result<(Position, CaptureEvent), CaptureError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        poll_capture_stream(&mut this.terminated, || {
            poll_wayland_capture(&mut this.inner, cx)
        })
    }
}

fn poll_wayland_capture(
    backend: &mut AsyncFd<Inner>,
    cx: &mut Context<'_>,
) -> Poll<Option<Result<(Position, CaptureEvent), CaptureError>>> {
    if let Some(event) = backend.get_mut().state.pending_events.pop_front() {
        return Poll::Ready(Some(event));
    }

    loop {
        let mut guard = ready!(backend.poll_read_ready_mut(cx))?;

        {
            let inner = guard.get_inner_mut();

            // read events
            inner.read()?;

            // dispatch the events
            inner.dispatch_events()?;

            // flush outgoing events
            if let Err(e) = inner.flush_events() {
                if e.kind() != ErrorKind::WouldBlock {
                    return Poll::Ready(Some(Err(e.into())));
                }
            }

            // prepare for the next read
            match inner.prepare_read() {
                Ok(_) => {}
                Err(e) => return Poll::Ready(Some(Err(e.into()))),
            }
        }

        // clear read readiness for tokio read guard
        // guard.clear_ready_matching(Ready::READABLE);
        guard.clear_ready();

        // if an event has been queued during dispatch_events() we return it
        match guard.get_inner_mut().state.pending_events.pop_front() {
            Some(event) => return Poll::Ready(Some(event)),
            None => continue,
        }
    }
}

impl Dispatch<wl_seat::WlSeat, ()> for State {
    fn event(
        state: &mut Self,
        seat: &wl_seat::WlSeat,
        event: <wl_seat::WlSeat as wayland_client::Proxy>::Event,
        _: &(),
        _: &Connection,
        qh: &QueueHandle<Self>,
    ) {
        if let wl_seat::Event::Capabilities {
            capabilities: WEnum::Value(capabilities),
        } = event
        {
            if capabilities.contains(wl_seat::Capability::Pointer) {
                if let Some(p) = state.pointer.take() {
                    p.release();
                }
                state.pointer.replace(seat.get_pointer(qh, ()));
            }
            if capabilities.contains(wl_seat::Capability::Keyboard) {
                if let Some(k) = state.keyboard.take() {
                    k.release();
                }
                seat.get_keyboard(qh, ());
            }
        }
    }
}

impl Dispatch<WlPointer, ()> for State {
    fn event(
        app: &mut Self,
        pointer: &WlPointer,
        event: <WlPointer as wayland_client::Proxy>::Event,
        _: &(),
        _: &Connection,
        qh: &QueueHandle<Self>,
    ) {
        match event {
            wl_pointer::Event::Enter {
                serial,
                surface,
                surface_x: _,
                surface_y: _,
            } => {
                // get client corresponding to the focused surface
                {
                    if let Some(window) = app.active_windows.iter().find(|w| w.surface == surface) {
                        app.focused = Some(window.clone());
                        app.grab(&surface, pointer, serial, qh);
                    } else {
                        return;
                    }
                }
                let pos = app
                    .active_windows
                    .iter()
                    .find(|w| w.surface == surface)
                    .map(|w| w.pos)
                    .unwrap();
                app.queue_capture_event((pos, CaptureEvent::Begin(0.5)));
            }
            wl_pointer::Event::Leave { .. } => {
                /* There are rare cases, where when a window is opened in
                 * just the wrong moment, the pointer is released, while
                 * still grabbed.
                 * In that case, the pointer must be ungrabbed, otherwise
                 * it is impossible to grab it again (since the pointer
                 * lock, relative pointer,... objects are still in place)
                 */
                if app.pointer_lock.is_some() {
                    log::warn!("compositor released mouse");
                }
                app.ungrab();
            }
            wl_pointer::Event::Button {
                serial: _,
                time,
                button,
                state,
            } => {
                let Some(pos) = app.focused.as_ref().map(|window| window.pos) else {
                    return;
                };
                app.queue_capture_event((
                    pos,
                    CaptureEvent::Input(Event::Pointer(PointerEvent::Button {
                        time,
                        button,
                        state: u32::from(state),
                    })),
                ));
            }
            wl_pointer::Event::Axis { time, axis, value } => {
                let Some(pos) = app.focused.as_ref().map(|window| window.pos) else {
                    return;
                };
                if app.scroll_discrete_pending {
                    // each axisvalue120 event is coupled with
                    // a corresponding axis event, which needs to
                    // be ignored to not duplicate the scrolling
                    app.scroll_discrete_pending = false;
                } else {
                    app.queue_capture_event((
                        pos,
                        CaptureEvent::Input(Event::Pointer(PointerEvent::Axis {
                            time,
                            axis: u32::from(axis) as u8,
                            value,
                        })),
                    ));
                }
            }
            wl_pointer::Event::AxisValue120 { axis, value120 } => {
                let Some(pos) = app.focused.as_ref().map(|window| window.pos) else {
                    return;
                };
                app.scroll_discrete_pending = true;
                app.queue_capture_event((
                    pos,
                    CaptureEvent::Input(Event::Pointer(PointerEvent::AxisDiscrete120 {
                        axis: u32::from(axis) as u8,
                        value: value120,
                    })),
                ));
            }
            wl_pointer::Event::Frame => {
                // TODO properly handle frame events
                // we simply insert a frame event on the client side
                // after each event for now
            }
            _ => {}
        }
    }
}

impl Dispatch<WlKeyboard, ()> for State {
    fn event(
        app: &mut Self,
        _: &WlKeyboard,
        event: wl_keyboard::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
        let focused_position = app.focused.as_ref().map(|window| window.pos);
        match event {
            wl_keyboard::Event::Key {
                serial: _,
                time,
                key,
                state,
            } => {
                if let Some(pos) = focused_position {
                    app.queue_capture_event((
                        pos,
                        CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key {
                            time,
                            key,
                            state: u32::from(state) as u8,
                        })),
                    ));
                }
            }
            wl_keyboard::Event::Modifiers {
                serial: _,
                mods_depressed,
                mods_latched,
                mods_locked,
                group,
            } => {
                if let Some(pos) = focused_position {
                    app.queue_capture_event((
                        pos,
                        CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Modifiers {
                            depressed: mods_depressed,
                            latched: mods_latched,
                            locked: mods_locked,
                            group,
                        })),
                    ));
                }
            }
            _ => (),
        }
    }
}

impl Dispatch<ZwpRelativePointerV1, ()> for State {
    fn event(
        app: &mut Self,
        _: &ZwpRelativePointerV1,
        event: <ZwpRelativePointerV1 as wayland_client::Proxy>::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
        if let zwp_relative_pointer_v1::Event::RelativeMotion {
            utime_hi,
            utime_lo,
            dx_unaccel: dx,
            dy_unaccel: dy,
            ..
        } = event
        {
            if let Some(pos) = app.focused.as_ref().map(|window| window.pos) {
                let time = ((((utime_hi as u64) << 32) | utime_lo as u64) / 1000) as u32;
                app.queue_capture_event((
                    pos,
                    CaptureEvent::Input(Event::Pointer(PointerEvent::Motion { time, dx, dy })),
                ));
            }
        }
    }
}

impl Dispatch<ZwlrLayerSurfaceV1, ()> for State {
    fn event(
        app: &mut Self,
        layer_surface: &ZwlrLayerSurfaceV1,
        event: <ZwlrLayerSurfaceV1 as wayland_client::Proxy>::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
        if let zwlr_layer_surface_v1::Event::Configure { serial, .. } = event {
            if let Some(window) = app
                .active_windows
                .iter()
                .find(|w| &w.layer_surface == layer_surface)
            {
                // client corresponding to the layer_surface
                let surface = &window.surface;
                let buffer = &window.buffer;
                surface.attach(Some(buffer), 0, 0);
                layer_surface.ack_configure(serial);
                surface.commit();
            }
        }
    }
}

// delegate wl_registry events to App itself
impl Dispatch<WlRegistry, GlobalListContents> for State {
    fn event(
        state: &mut Self,
        _registry: &WlRegistry,
        event: <WlRegistry as wayland_client::Proxy>::Event,
        _data: &GlobalListContents,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        match event {
            wl_registry::Event::Global {
                name,
                interface,
                version,
            } => {
                state.register_global(Global {
                    name,
                    interface,
                    version,
                });
            }
            wl_registry::Event::GlobalRemove { name } => {
                state.deregister_global(name);
            }
            _ => {}
        }
    }
}

impl Dispatch<ZxdgOutputV1, u32> for State {
    fn event(
        state: &mut Self,
        _: &ZxdgOutputV1,
        event: <ZxdgOutputV1 as wayland_client::Proxy>::Event,
        name: &u32,
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
        let output = state
            .outputs
            .iter_mut()
            .find(|o| o.global.name == *name)
            .expect("output");

        log::debug!("xdg_output {name} - {event:?}");
        match event {
            zxdg_output_v1::Event::LogicalPosition { x, y } => {
                output.pending_info.position = (x, y);
                output.has_xdg_info = true;
            }
            zxdg_output_v1::Event::LogicalSize { width, height } => {
                output.pending_info.size = (width, height);
                output.has_xdg_info = true;
            }
            zxdg_output_v1::Event::Done => {
                log::warn!("Use of deprecated xdg-output event \"done\"");
                state.update_output_info(*name);
            }
            zxdg_output_v1::Event::Name { name } => {
                output.pending_info.name = name;
                output.has_xdg_info = true;
            }
            zxdg_output_v1::Event::Description { description } => {
                output.pending_info.description = description;
                output.has_xdg_info = true;
            }
            _ => todo!(),
        }
    }
}

impl Dispatch<WlOutput, u32> for State {
    fn event(
        state: &mut Self,
        _wl_output: &WlOutput,
        event: <WlOutput as wayland_client::Proxy>::Event,
        name: &u32,
        _conn: &Connection,
        _qhandle: &QueueHandle<Self>,
    ) {
        log::debug!("wl_output {name} - {event:?}");
        if let wl_output::Event::Done = event {
            state.update_output_info(*name);
        }
    }
}

// don't emit any events
delegate_noop!(State: wl_region::WlRegion);
delegate_noop!(State: wl_shm_pool::WlShmPool);
delegate_noop!(State: wl_compositor::WlCompositor);
delegate_noop!(State: ZwlrLayerShellV1);
delegate_noop!(State: ZwpRelativePointerManagerV1);
delegate_noop!(State: ZwpKeyboardShortcutsInhibitManagerV1);
delegate_noop!(State: ZwpPointerConstraintsV1);

// ignore events
delegate_noop!(State: ignore ZxdgOutputManagerV1);
delegate_noop!(State: ignore wl_shm::WlShm);
delegate_noop!(State: ignore wl_buffer::WlBuffer);
delegate_noop!(State: ignore WlSurface);
delegate_noop!(State: ignore ZwpKeyboardShortcutsInhibitorV1);
delegate_noop!(State: ignore ZwpLockedPointerV1);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wayland_closed_socket_read_is_reported() {
        let (client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        let connection = Connection::from_socket(client).unwrap();
        let guard = connection.prepare_read().unwrap();
        drop(server);
        let result = guard.read();
        assert!(
            result.is_err(),
            "closed socket should produce native read failure"
        );
        assert!(read_wayland_result(result).is_err());
    }

    #[test]
    fn wayland_dispatch_io_error_is_reported() {
        let result = dispatch_wayland_result(Err(DispatchError::Backend(WaylandError::Io(
            io::Error::new(ErrorKind::BrokenPipe, "controlled disconnection"),
        ))));
        assert_eq!(result.unwrap_err().kind(), ErrorKind::BrokenPipe);
    }

    #[test]
    fn wayland_flush_protocol_error_is_reported_without_panic() {
        let protocol = wayland_client::backend::protocol::ProtocolError {
            code: 1,
            object_id: 1,
            object_interface: "wl_display".into(),
            message: "controlled protocol failure".into(),
        };
        let result = flush_wayland_result(Err(WaylandError::Protocol(protocol)));
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("controlled protocol failure")
        );
    }
    #[test]
    fn layer_queue_burst_never_retains_more_than_capacity() {
        let mut queue = PendingCaptureEvents::default();
        for _ in 0..8000 {
            queue.push_back((Position::Left, CaptureEvent::Begin(0.5)));
            assert!(
                queue.events.len() <= MAX_LAYER_SHELL_EVENTS,
                "native event queue grew without a limit"
            );
        }
        assert!(queue.overloaded);
    }

    #[test]
    fn layer_queue_overflow_reports_failure_before_old_input() {
        let mut queue = PendingCaptureEvents::default();
        for _ in 0..MAX_LAYER_SHELL_EVENTS {
            queue.push_back((Position::Left, CaptureEvent::Begin(0.5)));
        }
        let release = CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key {
            time: 1,
            key: 30,
            state: 0,
        }));
        assert!(queue.push_back((Position::Left, release)));
        assert!(matches!(
            queue.pop_front(),
            Some(Err(CaptureError::LayerShellQueueOverloaded))
        ));
        assert!(queue.pop_front().is_none());
    }

    #[test]
    fn layer_queue_healthy_fifo_reuses_capacity_without_failure() {
        let mut queue = PendingCaptureEvents::default();
        for time in 0..MAX_LAYER_SHELL_EVENTS as u32 {
            assert!(!queue.push_back((
                Position::Left,
                CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key {
                    time,
                    key: 30,
                    state: (time % 2) as u8
                }))
            )));
        }
        assert!(!queue.overloaded);
        for time in 0..MAX_LAYER_SHELL_EVENTS as u32 {
            assert_eq!(
                queue.pop_front().unwrap().unwrap(),
                (
                    Position::Left,
                    CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key {
                        time,
                        key: 30,
                        state: (time % 2) as u8
                    }))
                )
            );
        }
        assert!(queue.pop_front().is_none());
        assert!(!queue.push_back((Position::Right, CaptureEvent::Begin(0.75))));
        assert_eq!(
            queue.pop_front().unwrap().unwrap(),
            (Position::Right, CaptureEvent::Begin(0.75))
        );
    }

    #[test]
    fn layer_queue_overload_trips_once_and_requires_new_queue() {
        let mut queue = PendingCaptureEvents::default();
        let mut releases = 0;
        for _ in 0..8000 {
            if queue.push_back((Position::Left, CaptureEvent::Begin(0.5))) {
                releases += 1;
            }
        }
        assert_eq!(releases, 1);
        assert!(queue.events.is_empty());
        assert!(matches!(
            queue.pop_front(),
            Some(Err(CaptureError::LayerShellQueueOverloaded))
        ));
        assert!(queue.pop_front().is_none());
        assert!(!queue.push_back((Position::Left, CaptureEvent::Begin(0.5))));
        assert!(queue.events.is_empty());
        let mut replacement = PendingCaptureEvents::default();
        assert!(!replacement.push_back((Position::Right, CaptureEvent::Begin(0.5))));
        assert!(!replacement.overloaded);
        assert!(matches!(replacement.pop_front(), Some(Ok(_))));
    }

    #[test]
    fn layer_queue_overload_terminates_stream_before_old_events() {
        let mut queue = PendingCaptureEvents::default();
        for _ in 0..=MAX_LAYER_SHELL_EVENTS {
            queue.push_back((Position::Left, CaptureEvent::Begin(0.5)));
        }
        let mut terminated = false;
        assert!(matches!(
            poll_capture_stream(&mut terminated, || Poll::Ready(queue.pop_front())),
            Poll::Ready(Some(Err(CaptureError::LayerShellQueueOverloaded)))
        ));
        assert!(terminated);
        assert!(matches!(
            poll_capture_stream::<()>(&mut terminated, || panic!(
                "overloaded native backend must not be polled"
            )),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn layer_retire_unrelated_client_preserves_focus_and_capture() {
        let mut windows = vec![Position::Left, Position::Right, Position::Left];
        let mut focus = Some(Position::Right);
        let mut pending = PendingCaptureEvents::default();
        retire_capture_windows(
            &mut windows,
            &mut focus,
            &mut pending,
            Some(Position::Left),
            |position| *position,
            |_| panic!("unrelated live capture must remain active"),
        );
        assert_eq!(focus, Some(Position::Right));
        assert_eq!(windows, vec![Position::Right]);
    }

    #[test]
    fn layer_retire_focused_client_releases_before_window_drop() {
        use std::{cell::RefCell, rc::Rc};
        struct TestWindow(Rc<RefCell<Vec<&'static str>>>);
        impl Drop for TestWindow {
            fn drop(&mut self) {
                self.0.borrow_mut().push("window destroyed");
            }
        }
        let history = Rc::new(RefCell::new(Vec::new()));
        let window = Rc::new(TestWindow(history.clone()));
        let weak = Rc::downgrade(&window);
        let mut windows = vec![window.clone()];
        let mut focus = Some(window);
        let mut pending = PendingCaptureEvents::default();
        retire_capture_windows(
            &mut windows,
            &mut focus,
            &mut pending,
            Some(Position::Left),
            |_| Position::Left,
            |focus| {
                assert!(focus.is_some());
                assert!(weak.upgrade().is_some(), "surface destroyed before release");
                history.borrow_mut().push("capture released");
            },
        );
        assert!(focus.is_none() && windows.is_empty());
        assert!(weak.upgrade().is_none());
        assert_eq!(
            *history.borrow(),
            vec!["capture released", "window destroyed"]
        );
    }

    #[test]
    fn layer_retire_output_rebuild_clears_focus_and_old_events() {
        let mut windows = vec![Position::Left, Position::Right];
        let mut focus = Some(Position::Left);
        let mut pending = PendingCaptureEvents::default();
        pending.push_back((Position::Left, CaptureEvent::Begin(0.25)));
        pending.push_back((Position::Right, CaptureEvent::Begin(0.75)));
        let mut released = false;
        retire_capture_windows(
            &mut windows,
            &mut focus,
            &mut pending,
            None,
            |position| *position,
            |focus| {
                assert_eq!(focus, Some(Position::Left));
                released = true;
            },
        );
        assert!(released && focus.is_none() && windows.is_empty());
        assert!(pending.pop_front().is_none());
    }

    #[test]
    fn layer_retire_deleted_route_discards_only_its_queued_events() {
        let mut windows = vec![Position::Left, Position::Right];
        let mut focus = Some(Position::Right);
        let mut pending = PendingCaptureEvents::default();
        for (position, t) in [
            (Position::Right, 0.1),
            (Position::Left, 0.2),
            (Position::Right, 0.3),
            (Position::Left, 0.4),
        ] {
            pending.push_back((position, CaptureEvent::Begin(t)));
        }
        retire_capture_windows(
            &mut windows,
            &mut focus,
            &mut pending,
            Some(Position::Left),
            |position| *position,
            |_| panic!("unrelated focus"),
        );
        for t in [0.1, 0.3] {
            let (pos, event) = pending.pop_front().unwrap().unwrap();
            assert_eq!(pos, Position::Right);
            assert!(matches!(event, CaptureEvent::Begin(actual) if actual == t));
        }
        assert!(pending.pop_front().is_none());
        assert_eq!(focus, Some(Position::Right));
    }

    #[test]
    fn layer_retire_stale_focus_not_in_window_list_still_releases() {
        let mut windows = vec![Position::Right];
        let mut focus = Some(Position::Left);
        let mut pending = PendingCaptureEvents::default();
        let mut released = None;
        retire_capture_windows(
            &mut windows,
            &mut focus,
            &mut pending,
            Some(Position::Left),
            |position| *position,
            |focus| released = focus,
        );
        assert_eq!(released, Some(Position::Left));
        assert!(focus.is_none());
        assert_eq!(windows, vec![Position::Right]);
    }

    #[test]
    fn layer_retire_missing_focus_cleans_orphans_without_double_release() {
        let mut windows = vec![Position::Left];
        let mut focus = None;
        let mut pending = PendingCaptureEvents::default();
        let mut resource = Some(7);
        let mut released = Vec::new();
        for _ in 0..2 {
            retire_capture_windows(
                &mut windows,
                &mut focus,
                &mut pending,
                Some(Position::Left),
                |position| *position,
                |focus| {
                    assert!(focus.is_none());
                    if let Some(resource) = resource.take() {
                        released.push(resource);
                    }
                },
            );
        }
        assert_eq!(released, vec![7]);
        assert!(windows.is_empty());
    }

    #[test]
    fn layer_retire_rebuild_does_not_hide_queue_overload() {
        let mut windows = vec![Position::Left];
        let mut focus = None;
        let mut pending = PendingCaptureEvents::default();
        for _ in 0..=MAX_LAYER_SHELL_EVENTS {
            pending.push_back((Position::Left, CaptureEvent::Begin(0.5)));
        }
        retire_capture_windows(
            &mut windows,
            &mut focus,
            &mut pending,
            None,
            |position| *position,
            |_| {},
        );
        assert!(matches!(
            pending.pop_front(),
            Some(Err(CaptureError::LayerShellQueueOverloaded))
        ));
        assert!(pending.overloaded);
        assert!(!pending.push_back((Position::Right, CaptureEvent::Begin(0.5))));
        assert!(pending.pop_front().is_none());
    }

    #[test]
    fn layer_ungrab_sole_focus_lives_through_capture_resource_release() {
        use std::rc::Rc;
        let focus = Rc::new(7);
        let weak = Rc::downgrade(&focus);
        ungrab_resources(
            Some(focus),
            |focus| assert_eq!(**focus, 7),
            || {
                assert!(
                    weak.upgrade().is_some(),
                    "surface dropped before native resource release"
                )
            },
        );
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn layer_ungrab_without_focus_still_releases_all_resources() {
        let mut resources = [Some(1), Some(2), Some(3)];
        ungrab_resources(
            None::<()>,
            |_| panic!("no focused surface exists"),
            || {
                for resource in &mut resources {
                    resource.take();
                }
            },
        );
        assert!(
            resources.iter().all(Option::is_none),
            "missing focus skipped capture resource cleanup"
        );
    }

    #[test]
    fn layer_terminate_marks_stream_and_runs_cleanup() {
        let mut terminated = false;
        let mut queued = vec![1, 2, 3];
        terminate_capture(&mut terminated, || {
            queued.clear();
            Ok(())
        })
        .unwrap();
        assert!(terminated, "terminate left the stream pollable");
        assert!(queued.is_empty());
    }

    #[test]
    fn layer_ungrab_focus_release_precedes_other_resources() {
        let order = std::cell::RefCell::new(Vec::new());
        ungrab_resources(
            Some(7),
            |focus| {
                assert_eq!(*focus, 7);
                order.borrow_mut().push("focus");
            },
            || order.borrow_mut().push("resources"),
        );
        assert_eq!(*order.borrow(), vec!["focus", "resources"]);
    }

    #[test]
    fn layer_ungrab_repeated_cleanup_takes_each_resource_once() {
        let mut resources = [Some(1), Some(2), Some(3)];
        let mut destroyed = Vec::new();
        for _ in 0..2 {
            ungrab_resources(
                None::<()>,
                |_| unreachable!(),
                || {
                    for resource in &mut resources {
                        if let Some(id) = resource.take() {
                            destroyed.push(id);
                        }
                    }
                },
            );
        }
        assert_eq!(destroyed, vec![1, 2, 3]);
        assert!(resources.iter().all(Option::is_none));
    }

    #[test]
    fn layer_terminate_flush_failure_preserves_error_and_terminal_state() {
        let mut terminated = false;
        let mut queued = vec![1, 2, 3];
        let error = terminate_capture(&mut terminated, || {
            queued.clear();
            Err(io::Error::new(
                ErrorKind::BrokenPipe,
                "controlled flush failure",
            ))
        })
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::BrokenPipe);
        assert!(terminated && queued.is_empty());
        assert!(matches!(
            poll_capture_stream::<()>(&mut terminated, || panic!(
                "cleanup failure must not resume polling"
            )),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn layer_terminate_already_failed_stream_still_cleans_resources() {
        let mut terminated = true;
        let mut cleaned = false;
        terminate_capture(&mut terminated, || {
            cleaned = true;
            Ok(())
        })
        .unwrap();
        assert!(cleaned && terminated);
    }

    fn protocol_failure() -> WaylandError {
        WaylandError::Protocol(wayland_client::backend::protocol::ProtocolError {
            code: 1,
            object_id: 1,
            object_interface: "wl_display".into(),
            message: "controlled protocol failure".into(),
        })
    }

    #[test]
    fn wayland_read_and_dispatch_protocol_errors_are_reported() {
        assert!(
            read_wayland_result(Err(protocol_failure()))
                .unwrap_err()
                .to_string()
                .contains("controlled protocol failure")
        );
        assert!(dispatch_wayland_result(Err(DispatchError::Backend(protocol_failure()))).is_err());
    }

    #[test]
    fn wayland_success_and_read_would_block_keep_existing_behavior() {
        assert!(read_wayland_result(Ok(3)).is_ok());
        assert!(dispatch_wayland_result(Ok(3)).is_ok());
        assert!(flush_wayland_result(Ok(())).is_ok());
        assert!(
            read_wayland_result(Err(WaylandError::Io(io::Error::from(
                ErrorKind::WouldBlock
            ))))
            .is_ok()
        );
        assert_eq!(
            flush_wayland_result(Err(WaylandError::Io(io::Error::from(
                ErrorKind::WouldBlock
            ))))
            .unwrap_err()
            .kind(),
            ErrorKind::WouldBlock
        );
    }

    #[test]
    fn wayland_stream_reports_fatal_error_once_and_stops_polling() {
        let mut terminated = false;
        let error = read_wayland_result(Err(WaylandError::Io(io::Error::from(
            ErrorKind::BrokenPipe,
        ))))
        .unwrap_err();
        let result =
            poll_capture_stream::<()>(&mut terminated, || Poll::Ready(Some(Err(error.into()))));
        assert!(
            matches!(result, Poll::Ready(Some(Err(CaptureError::Io(ref error)))) if error.kind() == ErrorKind::BrokenPipe)
        );
        assert!(terminated);
        assert!(matches!(
            poll_capture_stream::<()>(&mut terminated, || panic!(
                "terminal backend must not read again"
            )),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn wayland_stream_pending_and_healthy_events_are_not_terminal() {
        let mut terminated = false;
        assert!(matches!(
            poll_capture_stream::<()>(&mut terminated, || Poll::Pending),
            Poll::Pending
        ));
        assert!(!terminated);
        assert!(matches!(
            poll_capture_stream(&mut terminated, || Poll::Ready(Some(Ok(7)))),
            Poll::Ready(Some(Ok(7)))
        ));
        assert!(!terminated);
    }
}
