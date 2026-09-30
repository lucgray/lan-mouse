use crate::error::EmulationError;

use super::{Emulation, error::WlrootsEmulationCreationError};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use wayland_client::WEnum;
use wayland_client::backend::WaylandError;

use wayland_client::protocol::wl_keyboard::{self, KeymapFormat, WlKeyboard};
use wayland_client::protocol::wl_pointer::{Axis, AxisSource, ButtonState};
use wayland_client::protocol::wl_seat::WlSeat;
use wayland_protocols_wlr::virtual_pointer::v1::client::{
    zwlr_virtual_pointer_manager_v1::ZwlrVirtualPointerManagerV1 as VpManager,
    zwlr_virtual_pointer_v1::ZwlrVirtualPointerV1 as Vp,
};
use xkbcommon::xkb;

use wayland_protocols_misc::zwp_virtual_keyboard_v1::client::{
    zwp_virtual_keyboard_manager_v1::ZwpVirtualKeyboardManagerV1 as VkManager,
    zwp_virtual_keyboard_v1::ZwpVirtualKeyboardV1 as Vk,
};

use wayland_client::{
    Connection, Dispatch, EventQueue, QueueHandle, delegate_noop,
    globals::{GlobalListContents, registry_queue_init},
    protocol::{wl_registry, wl_seat},
};

use input_event::{Event, KeyboardEvent, PointerEvent, scancode};

use super::EmulationHandle;
use super::error::WaylandBindError;
use super::scroll_accumulator::Scroll120Accumulator;

struct State {
    keymap: Option<(u32, OwnedFd, u32)>,
    /// shared by all clients, so the keymap fd is sent once instead of once per client
    keyboard: Option<Vk>,
    /// modifier bits each modifier/lock key drives on the compositor's
    /// keymap, probed once when it arrives
    modmap: Option<Arc<ModMap>>,
    input_for_client: HashMap<EmulationHandle, VirtualInput>,
    seat: wl_seat::WlSeat,
    qh: QueueHandle<Self>,
    vpm: VpManager,
    vkm: VkManager,
}

// App State, implements Dispatch event handlers
pub(crate) struct WlrootsEmulation {
    last_flush_failed: bool,
    state: State,
    queue: EventQueue<State>,
}

impl WlrootsEmulation {
    pub(crate) fn new() -> Result<Self, WlrootsEmulationCreationError> {
        let conn = Connection::connect_to_env()?;
        let (globals, queue) = registry_queue_init::<State>(&conn)?;
        let qh = queue.handle();

        let seat: wl_seat::WlSeat = globals
            .bind(&qh, 7..=8, ())
            .map_err(|e| WaylandBindError::new(e, "wl_seat 7..=8"))?;

        let vpm: VpManager = globals
            .bind(&qh, 1..=1, ())
            .map_err(|e| WaylandBindError::new(e, "wlr-virtual-pointer-unstable-v1"))?;
        let vkm: VkManager = globals
            .bind(&qh, 1..=1, ())
            .map_err(|e| WaylandBindError::new(e, "virtual-keyboard-unstable-v1"))?;

        let input_for_client: HashMap<EmulationHandle, VirtualInput> = HashMap::new();

        let mut emulate = WlrootsEmulation {
            last_flush_failed: false,
            state: State {
                keymap: None,
                keyboard: None,
                modmap: None,
                input_for_client,
                seat,
                vpm,
                vkm,
                qh,
            },
            queue,
        };
        while emulate.state.keymap.is_none() {
            emulate.queue.blocking_dispatch(&mut emulate.state)?;
        }
        emulate.state.modmap = Some(Arc::new(ModMap::new(&emulate.state.keymap)));
        // let fd = unsafe { &File::from_raw_fd(emulate.state.keymap.unwrap().1.as_raw_fd()) };
        // let mmap = unsafe { MmapOptions::new().map_copy(fd).unwrap() };
        // log::debug!("{:?}", &mmap[..100]);
        Ok(emulate)
    }
}

impl State {
    fn add_client(&mut self, client: EmulationHandle) {
        let pointer: Vp = self.vpm.create_virtual_pointer(None, &self.qh, ());
        let modmap = self
            .modmap
            .clone()
            .unwrap_or_else(|| Arc::new(ModMap::x11()));

        let keyboard = match self.keyboard.as_ref() {
            Some(keyboard) => keyboard.clone(),
            None => {
                let keyboard: Vk = self.vkm.create_virtual_keyboard(&self.seat, &self.qh, ());
                // TODO: use server side keymap
                let Some((format, fd, size)) = self.keymap.as_ref() else {
                    panic!("no keymap");
                };
                keyboard.keymap(*format, fd.as_fd(), *size);
                // The virtual keyboard starts with no locks held, so a
                // receiver would report NumLock off regardless of the
                // sender's state and the numpad would produce
                // navigation keys instead of digits. Lock NumLock by
                // default; senders that track lock state correct this
                // through a Modifiers event when the pointer enters.
                keyboard.modifiers(0, 0, modmap.default_locked, 0);
                self.keyboard = Some(keyboard.clone());
                keyboard
            }
        };

        let vinput = VirtualInput {
            pointer,
            keyboard,
            modifiers: Mutex::new(ModState::new(&modmap)),
            modmap,
            scroll_accumulator: Default::default(),
        };

        self.input_for_client.insert(client, vinput);
    }

    fn destroy_client(&mut self, handle: EmulationHandle) {
        // the shared keyboard outlives every client; keys are released by InputEmulation::destroy
        if let Some(input) = self.input_for_client.remove(&handle) {
            input.pointer.destroy();
        }
    }
}

#[async_trait]
impl Emulation for WlrootsEmulation {
    async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), EmulationError> {
        if let Some(virtual_input) = self.state.input_for_client.get(&handle) {
            if self.last_flush_failed {
                match self.queue.flush() {
                    Err(WaylandError::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {
                        /*
                         * outgoing buffer is full - sending more events
                         * will overwhelm the output buffer and leave the
                         * wayland connection in a broken state
                         */
                        log::warn!("can't keep up, discarding event: ({handle}) - {event:?}");
                        return Ok(());
                    }
                    _ => {}
                }
            }
            let event_debug = format!("{event:?}");
            virtual_input
                .consume_event(event)
                .unwrap_or_else(|_| panic!("failed to convert event: {event_debug}"));
            match self.queue.flush() {
                Err(WaylandError::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.last_flush_failed = true;
                    log::warn!("can't keep up, discarding event: ({handle}) - {event_debug}");
                }
                Err(WaylandError::Protocol(e)) => panic!("wayland protocol violation: {e}"),
                Ok(()) => self.last_flush_failed = false,
                Err(e) => Err(e)?,
            }
        }
        Ok(())
    }

    async fn create(&mut self, handle: EmulationHandle) {
        self.state.add_client(handle);
        if let Err(e) = self.queue.flush() {
            log::error!("{e}");
        }
    }
    async fn destroy(&mut self, handle: EmulationHandle) {
        self.state.destroy_client(handle);
        if let Err(e) = self.queue.flush() {
            log::error!("{e}");
        }
    }
    async fn terminate(&mut self) {
        /* nothing to do */
    }
}

struct VirtualInput {
    pointer: Vp,
    keyboard: Vk,
    modifiers: Mutex<ModState>,
    modmap: Arc<ModMap>,
    scroll_accumulator: Mutex<Scroll120Accumulator>,
}

impl VirtualInput {
    fn consume_event(&self, event: Event) -> Result<(), ()> {
        let now: u32 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u32;

        match event {
            Event::Pointer(e) => {
                match e {
                    PointerEvent::Motion { time, dx, dy } => self.pointer.motion(time, dx, dy),
                    PointerEvent::Button {
                        time,
                        button,
                        state,
                    } => {
                        let state: ButtonState = state.try_into()?;
                        self.pointer.button(time, button, state);
                    }
                    PointerEvent::Axis { time, axis, value } => {
                        let axis: Axis = (axis as u32).try_into()?;
                        self.pointer.axis(time, axis, value);
                        self.pointer.axis_source(AxisSource::Continuous);
                        self.pointer.frame();
                    }
                    PointerEvent::AxisDiscrete120 { axis, value } => {
                        // High-resolution wheels send fractions of a detent
                        // (16, 24, ...) and `value / 120` truncates every one
                        // of them to zero, so scrolling does nothing at all.
                        // Accumulate them into whole steps instead.
                        let steps = self
                            .scroll_accumulator
                            .lock()
                            .unwrap()
                            .accumulate(axis, value);
                        if steps != 0 {
                            let axis: Axis = (axis as u32).try_into()?;
                            self.pointer
                                .axis_discrete(now, axis, steps as f64 * 15., steps);
                            self.pointer.axis_source(AxisSource::Wheel);
                            self.pointer.frame();
                        }
                    }
                }
                self.pointer.frame();
            }
            Event::Keyboard(e) => match e {
                KeyboardEvent::Key {
                    time: _,
                    key,
                    state,
                } => {
                    // The event's timestamp comes from the sender's
                    // clock, which is meaningless (and possibly far in
                    // the past/future) on this machine, so stamp keys
                    // with local time like axis_discrete does.
                    self.keyboard.key(now, key, state as u32);
                    if let Ok(mut mods) = self.modifiers.lock() {
                        if mods.update_by_key_event(&self.modmap, key, state) {
                            log::trace!("Key triggers modifier change: {mods:?}");
                            self.keyboard
                                .modifiers(mods.mask_pressed(), 0, mods.mask_locks(), 0);
                        }
                    }
                }
                KeyboardEvent::Modifiers {
                    depressed: mods_depressed,
                    latched: mods_latched,
                    locked: mods_locked,
                    group,
                } => {
                    // Synchronize internal modifier state, assuming server is authoritative
                    if let Ok(mut mods) = self.modifiers.lock() {
                        mods.update_by_mods_event(e);
                    }
                    self.keyboard
                        .modifiers(mods_depressed, mods_latched, mods_locked, group);
                }
            },
            Event::Clipboard(_) => {
                // Clipboard events are not supported by wlroots emulation
                log::debug!("ignoring clipboard event in wlroots emulation");
            }
        }
        Ok(())
    }
}

delegate_noop!(State: Vp);
delegate_noop!(State: Vk);
delegate_noop!(State: VpManager);
delegate_noop!(State: VkManager);

impl Dispatch<wl_registry::WlRegistry, GlobalListContents> for State {
    fn event(
        _: &mut State,
        _: &wl_registry::WlRegistry,
        _: wl_registry::Event,
        _: &GlobalListContents,
        _: &Connection,
        _: &QueueHandle<State>,
    ) {
    }
}

impl Dispatch<WlKeyboard, ()> for State {
    fn event(
        state: &mut Self,
        _: &WlKeyboard,
        event: <WlKeyboard as wayland_client::Proxy>::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
        if let wl_keyboard::Event::Keymap { format, fd, size } = event {
            state.keymap = Some((u32::from(format), fd, size));
        }
    }
}

impl Dispatch<WlSeat, ()> for State {
    fn event(
        _: &mut Self,
        seat: &WlSeat,
        event: <WlSeat as wayland_client::Proxy>::Event,
        _: &(),
        _: &Connection,
        qhandle: &QueueHandle<Self>,
    ) {
        if let wl_seat::Event::Capabilities {
            capabilities: WEnum::Value(capabilities),
        } = event
        {
            if capabilities.contains(wl_seat::Capability::Keyboard) {
                seat.get_keyboard(qhandle, ());
            }
        }
    }
}

/// Keys that can drive a modifier bit; the mask each of them sets is
/// layout-dependent, so it is probed from the compositor's keymap.
const MODIFIER_KEYS: &[scancode::Linux] = &[
    scancode::Linux::KeyLeftShift,
    scancode::Linux::KeyRightShift,
    scancode::Linux::KeyLeftCtrl,
    scancode::Linux::KeyRightCtrl,
    scancode::Linux::KeyLeftAlt,
    scancode::Linux::KeyRightalt,
    scancode::Linux::KeyLeftMeta,
    scancode::Linux::KeyRightmeta,
    scancode::Linux::KeyCapsLock,
    scancode::Linux::KeyNumlock,
    scancode::Linux::KeyScrollLock,
];

/// Which modifier bits each modifier/lock key drives on the keymap the
/// compositor gave us. This is probed from the keymap rather than
/// hardcoded because, for example, RightAlt is Mod1 (`Alt_R`) on US
/// layouts but Mod5 (`ISO_Level3_Shift`) on layouts with
/// `level3(ralt_switch)` — only the keymap knows which. The mask bits
/// are raw xkb modifier indices, exactly what `wl_keyboard.modifiers`
/// expects.
struct ModMap {
    /// linux keycode -> modifier bits depressed while the key is held
    pressed: HashMap<u32, u32>,
    /// linux keycode -> modifier bits toggled in the locked mask on press
    locked: HashMap<u32, u32>,
    /// locked bits a fresh virtual keyboard starts with: receivers
    /// generally want their numpad to produce digits; senders that
    /// track lock state correct this with a Modifiers event on entry
    default_locked: u32,
}

impl ModMap {
    fn new(keymap: &Option<(u32, OwnedFd, u32)>) -> Self {
        if let Some((format, fd, size)) = keymap.as_ref() {
            if let Some(map) = Self::probe(*format, fd, *size) {
                return map;
            }
        }
        Self::x11()
    }

    /// The conventional X11 modifier mask, used when the compositor's
    /// keymap can't be probed.
    fn x11() -> Self {
        use scancode::Linux::*;
        let pressed = HashMap::from([
            (KeyLeftShift as u32, 1 << 0),
            (KeyRightShift as u32, 1 << 0),
            (KeyLeftCtrl as u32, 1 << 2),
            (KeyRightCtrl as u32, 1 << 2),
            (KeyLeftAlt as u32, 1 << 3),
            (KeyRightalt as u32, 1 << 3),
            (KeyLeftMeta as u32, 1 << 6),
            (KeyRightmeta as u32, 1 << 6),
        ]);
        let locked = HashMap::from([
            (KeyCapsLock as u32, 1 << 1),
            (KeyNumlock as u32, 1 << 4),
            (KeyScrollLock as u32, 1 << 5),
        ]);
        Self::assemble(pressed, locked)
    }

    /// Press and release each modifier key in a fresh `xkb::State` and
    /// record which modifier bits that depresses / locks on this
    /// keymap.
    fn probe(format: u32, fd: &OwnedFd, size: u32) -> Option<Self> {
        if format != KeymapFormat::XkbV1 as u32 {
            return None;
        }
        let context = xkb::Context::new(xkb::CONTEXT_NO_FLAGS);
        // SAFETY: the fd is a fresh dup of the keymap fd handed to us by
        // the compositor; `new_from_fd` takes ownership of the dup.
        let keymap = unsafe {
            xkb::Keymap::new_from_fd(
                &context,
                fd.try_clone().ok()?,
                size as usize,
                xkb::KEYMAP_FORMAT_TEXT_V1,
                xkb::KEYMAP_COMPILE_NO_FLAGS,
            )
        }
        .ok()??;
        let mut pressed = HashMap::new();
        let mut locked = HashMap::new();
        for &key in MODIFIER_KEYS {
            // xkb keycodes are evdev keycodes offset by 8
            let keycode = xkb::Keycode::new(key as u32 + 8);
            let mut state = xkb::State::new(&keymap);
            state.update_key(keycode, xkb::KeyDirection::Down);
            let pressed_bits = state.serialize_mods(xkb::STATE_MODS_DEPRESSED);
            state.update_key(keycode, xkb::KeyDirection::Up);
            let locked_bits = state.serialize_mods(xkb::STATE_MODS_LOCKED);
            if pressed_bits != 0 {
                pressed.insert(key as u32, pressed_bits);
            }
            if locked_bits != 0 {
                locked.insert(key as u32, locked_bits);
            }
        }
        Some(Self::assemble(pressed, locked))
    }

    fn assemble(pressed: HashMap<u32, u32>, locked: HashMap<u32, u32>) -> Self {
        let default_locked = locked
            .get(&(scancode::Linux::KeyNumlock as u32))
            .copied()
            .unwrap_or(0);
        Self {
            pressed,
            locked,
            default_locked,
        }
    }
}

/// Tracked modifier state for one virtual keyboard.
#[derive(Debug, Default)]
struct ModState {
    /// depressed modifier bits, packed as xkb mod indices
    pressed: u32,
    /// locked modifier bits, packed as xkb mod indices
    ///
    /// Kept separate from `pressed`: on layouts where a lock key's mod
    /// bit also shows up in the depressed set, folding the two into one
    /// mask would leak locked bits into the depressed mask sent to the
    /// compositor (e.g. NumLock appearing as a held modifier and
    /// breaking exact-match keybinds).
    locked: u32,
    /// lock keys currently held down; auto-repeated presses must not
    /// toggle a lock twice
    held_locks: HashSet<u32>,
}

impl ModState {
    fn new(map: &ModMap) -> Self {
        Self {
            pressed: 0,
            locked: map.default_locked,
            held_locks: HashSet::new(),
        }
    }

    fn update_by_mods_event(&mut self, evt: KeyboardEvent) {
        if let KeyboardEvent::Modifiers {
            depressed, locked, ..
        } = evt
        {
            self.pressed = depressed;
            self.locked = locked;
        }
    }

    /// Apply a key event, returning whether the modifier mask changed.
    fn update_by_key_event(&mut self, map: &ModMap, key: u32, state: u8) -> bool {
        log::trace!("Attempting to process modifier from keycode: {key:#?}");
        let pressed = map.pressed.get(&key).copied().unwrap_or(0);
        let locked = map.locked.get(&key).copied().unwrap_or(0);
        if pressed == 0 && locked == 0 {
            return false;
        }
        let before = (self.pressed, self.locked);
        match state {
            1 => {
                self.pressed |= pressed;
                // lock keys toggle on press, once per physical press;
                // senders can repeat the press event while the key is held
                if locked != 0 && self.held_locks.insert(key) {
                    self.locked ^= locked;
                }
            }
            _ => {
                self.pressed &= !pressed;
                self.held_locks.remove(&key);
            }
        }
        (self.pressed, self.locked) != before
    }

    fn mask_locks(&self) -> u32 {
        self.locked
    }

    fn mask_pressed(&self) -> u32 {
        self.pressed
    }
}
