use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::ptr::addr_of_mut;

use crate::hook_queue::HookSender;
use std::default::Default;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use windows::Win32::Foundation::{FALSE, HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::Graphics::Gdi::{
    DEVMODEW, DISPLAY_DEVICE_ATTACHED_TO_DESKTOP, DISPLAY_DEVICEW, ENUM_CURRENT_SETTINGS,
    EnumDisplayDevicesW, EnumDisplaySettingsW,
};
use windows::Win32::System::LibraryLoader::GetModuleHandleW;
use windows::Win32::System::Threading::GetCurrentThreadId;
use windows::core::{PCWSTR, w};

use windows::Win32::UI::Input::KeyboardAndMouse::{
    GetKeyState, VIRTUAL_KEY, VK_CAPITAL, VK_CONTROL, VK_LCONTROL, VK_NUMLOCK, VK_RMENU, VK_SCROLL,
};
use windows::Win32::UI::WindowsAndMessaging::{
    CallNextHookEx, CreateWindowExW, DispatchMessageW, EDD_GET_DEVICE_INTERFACE_NAME, GetCursorPos,
    GetMessageW, HOOKPROC, KBDLLHOOKSTRUCT, LLKHF_EXTENDED, MSG, MSLLHOOKSTRUCT, PM_NOREMOVE,
    PeekMessageW, PostThreadMessageW, RegisterClassW, SetCursorPos, SetWindowsHookExW,
    TranslateMessage, WH_KEYBOARD_LL, WH_MOUSE_LL, WINDOW_STYLE, WM_DISPLAYCHANGE, WM_KEYDOWN,
    WM_KEYUP, WM_LBUTTONDOWN, WM_LBUTTONUP, WM_MBUTTONDOWN, WM_MBUTTONUP, WM_MOUSEHWHEEL,
    WM_MOUSEMOVE, WM_MOUSEWHEEL, WM_RBUTTONDOWN, WM_RBUTTONUP, WM_SYSKEYDOWN, WM_SYSKEYUP, WM_USER,
    WM_XBUTTONDOWN, WM_XBUTTONUP, WNDCLASSW, WNDPROC,
};

use input_event::{
    BTN_BACK, BTN_FORWARD, BTN_LEFT, BTN_MIDDLE, BTN_RIGHT, Event, KeyboardEvent, PointerEvent,
    scancode::{self, Linux},
};

use super::{CaptureEvent, Position, display_util};
use crate::enter_bind::EnterBindTracker;

pub(crate) struct EventThread {
    request_buffer: Arc<Mutex<Vec<ThreadRequest>>>,
    thread: Option<thread::JoinHandle<()>>,
    thread_id: u32,
}

impl EventThread {
    pub(crate) fn new(event_tx: HookSender) -> Self {
        let request_buffer = Default::default();
        let (thread, thread_id) = start(event_tx, Arc::clone(&request_buffer));
        Self {
            request_buffer,
            thread: Some(thread),
            thread_id,
        }
    }

    pub(crate) fn release_capture(&self) {
        self.queue_request(ThreadRequest::Release(None));
    }

    pub(crate) fn release_capture_to(&self, t: f64) {
        self.queue_request(ThreadRequest::Release(Some(t)));
    }

    pub(crate) fn create(&self, pos: Position) {
        self.queue_request(ThreadRequest::Create(pos));
    }

    pub(crate) fn destroy(&self, pos: Position) {
        self.queue_request(ThreadRequest::Destroy(pos));
    }

    pub(crate) fn set_enter_binds(&self, binds: HashMap<Position, Vec<scancode::Linux>>) {
        self.queue_request(ThreadRequest::SetEnterBinds(binds));
    }

    fn exit(&self) {
        self.signal(RequestType::Exit);
    }

    fn queue_request(&self, request: ThreadRequest) {
        {
            let mut requests = self.request_buffer.lock().unwrap();
            requests.push(request);
        }
        self.signal(RequestType::ProcessRequests);
    }

    fn signal(&self, event_type: RequestType) {
        let id = self.thread_id;
        unsafe { PostThreadMessageW(id, WM_USER, WPARAM(event_type as usize), LPARAM(0)).unwrap() };
    }
}

impl Drop for EventThread {
    fn drop(&mut self) {
        self.exit();
        let _ = self.thread.take().expect("thread").join();
    }
}

enum RequestType {
    ProcessRequests = 0,
    Exit = 1,
}

enum ThreadRequest {
    Release(Option<f64>),
    Create(Position),
    Destroy(Position),
    SetEnterBinds(HashMap<Position, Vec<scancode::Linux>>),
}

fn send_event(pos: Position, event: CaptureEvent) -> bool {
    let sent = EVENT_TX.with_borrow(|tx| tx.as_ref().unwrap().send(pos, event).is_ok());
    if !sent {
        // The queue remains failed until the backend is recreated. Hooks
        // become passthrough immediately, even if the daemon is stalled.
        ACTIVE_CLIENT.take();
        reset_key_tracking();
        ENTER_BINDS.with_borrow_mut(|binds| binds.clear());
    }
    sent
}

fn queue_available() -> bool {
    EVENT_TX.with_borrow(|tx| tx.as_ref().is_some_and(HookSender::available))
}

thread_local! {
    /// all configured clients
    static CLIENTS: RefCell<HashSet<Position>> = RefCell::new(HashSet::new());
    /// currently active client
    static ACTIVE_CLIENT: Cell<Option<Position>> = const { Cell::new(None) };
    /// input event channel
    static EVENT_TX: RefCell<Option<HookSender>> = const { RefCell::new(None) };
    /// position of barrier entry
    static ENTRY_POINT: Cell<(i32, i32)> = const { Cell::new((0, 0)) };
    /// previous mouse position
    static PREV_POS: Cell<Option<(i32, i32)>> = const { Cell::new(None) };
    /// displays and generation counter
    static DISPLAYS: RefCell<(Vec<RECT>, i32)> = const { RefCell::new((Vec::new(), 0)) };
    /// binds that enter a client without crossing a screen edge
    static ENTER_BINDS: RefCell<EnterBindTracker> = RefCell::new(EnterBindTracker::default());
    /// hook timestamp of a left-control press that was forwarded to the
    /// client and has not been released there yet
    static LCTRL_FORWARDED: Cell<Option<u32>> = const { Cell::new(None) };
    /// the forwarded left-control press turned out to be the synthetic
    /// keypress Windows generates together with AltGr, so a compensating
    /// release was already sent and the matching physical key-up must be
    /// swallowed rather than forwarded
    static FAKE_LCTRL: Cell<bool> = const { Cell::new(false) };
}

fn reset_key_tracking() {
    LCTRL_FORWARDED.take();
    FAKE_LCTRL.take();
}

fn get_msg() -> Option<MSG> {
    unsafe {
        let mut msg = std::mem::zeroed();
        let ret = GetMessageW(addr_of_mut!(msg), None, 0, 0);
        match ret.0 {
            0 => None,
            x if x > 0 => Some(msg),
            _ => panic!("error in GetMessageW"),
        }
    }
}

fn start(
    event_tx: HookSender,
    request_buffer: Arc<Mutex<Vec<ThreadRequest>>>,
) -> (thread::JoinHandle<()>, u32) {
    /* condition variable to wait for thead id */
    let thread_id = Arc::new((Condvar::new(), Mutex::new(None)));
    let thread_id_ = Arc::clone(&thread_id);

    let msg_thread = thread::spawn(|| start_routine(thread_id_, event_tx, request_buffer));

    /* wait for thread to set its id */
    let (cond, thread_id) = &*thread_id;
    let mut thread_id = thread_id.lock().unwrap();
    while (*thread_id).is_none() {
        thread_id = cond.wait(thread_id).expect("channel closed");
    }
    (msg_thread, thread_id.expect("thread id"))
}

/// Windows does not guarantee a message queue merely because a thread exists.
/// Create it before publishing the id used by PostThreadMessageW.
fn publish_thread_ready(ready: &Arc<(Condvar, Mutex<Option<u32>>)>) {
    let mut msg = MSG::default();
    unsafe {
        let _ = PeekMessageW(&mut msg, None, WM_USER, WM_USER, PM_NOREMOVE);
    }
    let (cnd, mtx) = &**ready;
    *mtx.lock().unwrap() = Some(unsafe { GetCurrentThreadId() });
    cnd.notify_one();
}

fn start_routine(
    ready: Arc<(Condvar, Mutex<Option<u32>>)>,
    event_tx: HookSender,
    request_buffer: Arc<Mutex<Vec<ThreadRequest>>>,
) {
    EVENT_TX.replace(Some(event_tx));
    publish_thread_ready(&ready);

    let mouse_proc: HOOKPROC = Some(mouse_proc);
    let kybrd_proc: HOOKPROC = Some(kybrd_proc);
    let window_proc: WNDPROC = Some(window_proc);

    /* register hooks */
    unsafe {
        let _ = SetWindowsHookExW(WH_MOUSE_LL, mouse_proc, None, 0).unwrap();
        let _ = SetWindowsHookExW(WH_KEYBOARD_LL, kybrd_proc, None, 0).unwrap();
    }

    let instance = unsafe { GetModuleHandleW(None).unwrap() };
    let instance = instance.into();
    let window_class: WNDCLASSW = WNDCLASSW {
        lpfnWndProc: window_proc,
        hInstance: instance,
        lpszClassName: w!("lan-mouse-message-window-class"),
        ..Default::default()
    };

    static WINDOW_CLASS_REGISTERED: AtomicBool = AtomicBool::new(false);
    if WINDOW_CLASS_REGISTERED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        /* register window class if not yet done so */
        unsafe {
            let ret = RegisterClassW(&window_class);
            if ret == 0 {
                panic!("RegisterClassW");
            }
        }
    }

    /* window is used ro receive WM_DISPLAYCHANGE messages */
    unsafe {
        CreateWindowExW(
            Default::default(),
            w!("lan-mouse-message-window-class"),
            w!("lan-mouse-msg-window"),
            WINDOW_STYLE::default(),
            0,
            0,
            0,
            0,
            None,
            None,
            Some(instance),
            None,
        )
        .expect("CreateWindowExW");
    }

    /* run message loop */
    while let Some(msg) = get_msg() {
        // mouse / keybrd proc do not actually return a message
        if msg.hwnd.0.is_null() {
            /* messages sent via PostThreadMessage */
            match msg.wParam.0 {
                x if x == RequestType::Exit as usize => break,
                x if x == RequestType::ProcessRequests as usize => {
                    let requests = {
                        let mut res = vec![];
                        let mut requests = request_buffer.lock().unwrap();
                        for request in requests.drain(..) {
                            res.push(request);
                        }
                        res
                    };

                    for request in requests {
                        process_request(request)
                    }
                }
                _ => {}
            }
        } else {
            /* other messages for window_procs */
            unsafe {
                let _ = TranslateMessage(&msg);
                DispatchMessageW(&msg);
            }
        }
    }
}

fn check_client_activation(wparam: WPARAM, lparam: LPARAM) -> bool {
    if !queue_available() {
        return false;
    }
    if wparam.0 != WM_MOUSEMOVE as usize {
        return ACTIVE_CLIENT.get().is_some();
    }
    let mouse_low_level: MSLLHOOKSTRUCT = unsafe { *(lparam.0 as *const MSLLHOOKSTRUCT) };
    let curr_pos = (mouse_low_level.pt.x, mouse_low_level.pt.y);
    let prev_pos = PREV_POS.get().unwrap_or(curr_pos);
    PREV_POS.replace(Some(curr_pos));

    /* next event is the first actual event */
    let ret = ACTIVE_CLIENT.get().is_some();

    /* client already active, no need to check */
    if ACTIVE_CLIENT.get().is_some() {
        return ret;
    }

    /* check if a client was activated */
    let entered = DISPLAYS.with_borrow_mut(|(displays, generation)| {
        update_display_regions(displays, generation);
        display_util::entered_barrier(prev_pos, curr_pos, displays)
    });

    let Some(pos) = entered else {
        return ret;
    };

    /* check if a client is registered for the barrier */
    if !CLIENTS.with_borrow(|clients| clients.contains(&pos)) {
        return ret;
    }

    /* update active client and entry point */
    reset_key_tracking();
    ACTIVE_CLIENT.replace(Some(pos));
    let (entry_point, t) = DISPLAYS.with_borrow(|(displays, _)| {
        (
            display_util::clamp_to_display_bounds(displays, prev_pos, curr_pos),
            display_util::cross_axis_position(displays, prev_pos, curr_pos, pos),
        )
    });
    ENTRY_POINT.replace(entry_point);
    /* keys held now stop being observed for the duration of the
     * capture, so they must not still count as held afterwards */
    ENTER_BINDS.with_borrow_mut(|binds| binds.clear());

    /* notify main thread */
    log::debug!("ENTERED @ {prev_pos:?} -> {curr_pos:?}");
    let active = ACTIVE_CLIENT.get().expect("active client");
    if send_event(active, CaptureEvent::Begin(t)) {
        send_lock_state(active);
    }

    ret
}

/// Push this machine's lock-key state to the client so the receiver's
/// NumLock / CapsLock / ScrollLock match the keyboard the user is
/// actually typing on.
fn send_lock_state(pos: Position) {
    // SAFETY: GetKeyState is always safe to call for these constants.
    let (num, caps, scroll) = unsafe {
        (
            GetKeyState(i32::from(VK_NUMLOCK.0)) & 1,
            GetKeyState(i32::from(VK_CAPITAL.0)) & 1,
            GetKeyState(i32::from(VK_SCROLL.0)) & 1,
        )
    };
    // same X-style modifier mask the other capture backends use:
    // LockMask, Mod2 (NumLock) and Mod3 (ScrollLock)
    let mut locked = 0;
    if caps != 0 {
        locked |= 1 << 1;
    }
    if num != 0 {
        locked |= 1 << 4;
    }
    if scroll != 0 {
        locked |= 1 << 5;
    }
    send_event(
        pos,
        CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Modifiers {
            depressed: 0,
            latched: 0,
            locked,
            group: 0,
        })),
    );
}

unsafe extern "system" fn mouse_proc(ncode: i32, wparam: WPARAM, lparam: LPARAM) -> LRESULT {
    if ncode < 0 {
        return CallNextHookEx(None, ncode, wparam, lparam);
    }
    let active = check_client_activation(wparam, lparam);

    /* no client was active */
    if !active {
        return CallNextHookEx(None, ncode, wparam, lparam);
    }

    /* get active client if any */
    let Some(pos) = ACTIVE_CLIENT.get() else {
        return LRESULT(1);
    };

    /* convert to lan-mouse event */
    let Some(pointer_event) = to_mouse_event(wparam, lparam) else {
        return LRESULT(1);
    };

    if !send_event(pos, CaptureEvent::Input(Event::Pointer(pointer_event))) {
        return CallNextHookEx(None, ncode, wparam, lparam);
    }

    /* don't pass event to applications */
    LRESULT(1)
}

/// Enter a client because its enter-bind was pressed rather than
/// because the pointer crossed a screen edge.
///
/// The pointer is left where it is and simply becomes the reference
/// for relative motion, the same role [`ENTRY_POINT`] plays after a
/// barrier crossing.
fn enter_via_bind(pos: Position) {
    let mut point = Default::default();
    let entry_point = match unsafe { GetCursorPos(&mut point) } {
        Ok(()) => (point.x, point.y),
        Err(e) => {
            log::warn!("failed to query cursor position: {e}");
            PREV_POS.get().unwrap_or((0, 0))
        }
    };
    reset_key_tracking();
    ACTIVE_CLIENT.replace(Some(pos));
    ENTRY_POINT.replace(entry_point);
    PREV_POS.replace(Some(entry_point));
    log::info!("entering client @ {pos}: enter-bind pressed");
    let t = DISPLAYS.with_borrow(|(displays, _)| {
        display_util::cross_axis_position(displays, entry_point, entry_point, pos)
    });
    if send_event(pos, CaptureEvent::Begin(t)) {
        send_lock_state(pos);
    }
}

/// Feed a key event seen while no client is active into the
/// enter-bind tracker, entering a client if one of the binds just
/// completed. Returns whether the key was consumed.
fn check_enter_bind(wparam: WPARAM, lparam: LPARAM) -> bool {
    if !queue_available() || ENTER_BINDS.with_borrow(|binds| binds.is_empty()) {
        return false;
    }
    let Some(KeyboardEvent::Key { key, state, .. }) = to_key_event(wparam, lparam) else {
        return false;
    };
    let Ok(key) = Linux::try_from(key) else {
        return false;
    };
    let entered = CLIENTS.with_borrow(|clients| {
        ENTER_BINDS.with_borrow_mut(|binds| binds.key_event(key, state == 1, clients))
    });
    let Some(pos) = entered else {
        return false;
    };
    enter_via_bind(pos);
    // The bind keys stay physically held — see EnterBindTracker::clear
    ENTER_BINDS.with_borrow_mut(|binds| binds.clear());
    ACTIVE_CLIENT.get().is_some()
}

unsafe extern "system" fn kybrd_proc(ncode: i32, wparam: WPARAM, lparam: LPARAM) -> LRESULT {
    if ncode < 0 {
        return CallNextHookEx(None, ncode, wparam, lparam);
    }
    /* get active client if any */
    let Some(client) = ACTIVE_CLIENT.get() else {
        /* no client active: the keys belong to this machine, unless
         * they complete an enter-bind */
        if check_enter_bind(wparam, lparam) {
            /* swallow the key that triggered the switch */
            return LRESULT(1);
        }
        return CallNextHookEx(None, ncode, wparam, lparam);
    };

    let hook: KBDLLHOOKSTRUCT = *(lparam.0 as *const KBDLLHOOKSTRUCT);
    let vk = VIRTUAL_KEY(hook.vkCode as u16);
    let is_down = matches!(
        wparam.0,
        w if w == WM_KEYDOWN as usize || w == WM_SYSKEYDOWN as usize
    );
    let is_up = matches!(
        wparam.0,
        w if w == WM_KEYUP as usize || w == WM_SYSKEYUP as usize
    );

    /* Pressing AltGr makes Windows report a synthetic left-control
     * press right before the right-alt press; both events carry the
     * same hook timestamp. Forwarding it would pin Control down on the
     * client for every AltGr combination, so release it before the
     * right-alt press goes out. Its matching release is swallowed
     * below via FAKE_LCTRL. A real left-control press carries a
     * different timestamp, so pressing Ctrl+AltGr on purpose still
     * works. */
    if is_down && (vk == VK_LCONTROL || vk == VK_CONTROL) {
        LCTRL_FORWARDED.replace(Some(hook.time));
    } else if is_down && vk == VK_RMENU && LCTRL_FORWARDED.get() == Some(hook.time) {
        LCTRL_FORWARDED.take();
        FAKE_LCTRL.replace(true);
        let lctrl_up = CaptureEvent::Input(Event::Keyboard(KeyboardEvent::Key {
            time: 0,
            key: Linux::KeyLeftCtrl as u32,
            state: 0,
        }));
        if !send_event(client, lctrl_up) {
            return CallNextHookEx(None, ncode, wparam, lparam);
        }
    } else if is_up && (vk == VK_LCONTROL || vk == VK_CONTROL) {
        LCTRL_FORWARDED.take();
        if FAKE_LCTRL.take() {
            /* the compensating release for AltGr's synthetic control
             * press was already sent */
            return LRESULT(1);
        }
    }

    /* convert to key event */
    let Some(key_event) = to_key_event(wparam, lparam) else {
        return LRESULT(1);
    };

    if !send_event(client, CaptureEvent::Input(Event::Keyboard(key_event))) {
        return CallNextHookEx(None, ncode, wparam, lparam);
    }

    /* don't pass event to applications */
    LRESULT(1)
}

unsafe extern "system" fn window_proc(
    _hwnd: HWND,
    uint: u32,
    _wparam: WPARAM,
    _lparam: LPARAM,
) -> LRESULT {
    if uint == WM_DISPLAYCHANGE {
        log::debug!("display resolution changed");
        DISPLAY_RESOLUTION_GENERATION.fetch_add(1, Ordering::Release);
    }
    LRESULT(1)
}

static DISPLAY_RESOLUTION_GENERATION: AtomicI32 = AtomicI32::new(1);

fn update_display_regions(displays: &mut Vec<RECT>, generation: &mut i32) {
    let global_generation = DISPLAY_RESOLUTION_GENERATION.load(Ordering::Acquire);
    if *generation != global_generation {
        enumerate_displays(displays);
        log::debug!("displays: {displays:?}");
        *generation = global_generation;
    }
}

fn enumerate_displays(display_rects: &mut Vec<RECT>) {
    display_rects.clear();
    unsafe {
        let mut devices = vec![];
        for i in 0.. {
            let mut device: DISPLAY_DEVICEW = std::mem::zeroed();
            device.cb = std::mem::size_of::<DISPLAY_DEVICEW>() as u32;
            let ret = EnumDisplayDevicesW(None, i, &mut device, EDD_GET_DEVICE_INTERFACE_NAME);
            if ret == FALSE {
                break;
            }
            if device
                .StateFlags
                .contains(DISPLAY_DEVICE_ATTACHED_TO_DESKTOP)
            {
                devices.push(device.DeviceName);
            }
        }
        for device in devices {
            let mut dev_mode: DEVMODEW = std::mem::zeroed();
            dev_mode.dmSize = std::mem::size_of::<DEVMODEW>() as u16;
            let ret = EnumDisplaySettingsW(
                PCWSTR::from_raw(&device as *const _),
                ENUM_CURRENT_SETTINGS,
                &mut dev_mode,
            );
            if ret == FALSE {
                log::warn!("no display mode");
            }

            let pos = dev_mode.Anonymous1.Anonymous2.dmPosition;
            let (x, y) = (pos.x, pos.y);
            let (width, height) = (dev_mode.dmPelsWidth, dev_mode.dmPelsHeight);

            display_rects.push(RECT {
                left: x,
                right: x + width as i32,
                top: y,
                bottom: y + height as i32,
            });
        }
    }
}

fn release_capture(t: Option<f64>) {
    let active = ACTIVE_CLIENT.take();
    reset_key_tracking();
    let (Some(pos), Some(t)) = (active, t) else {
        return;
    };

    let target = DISPLAYS.with_borrow_mut(|(displays, generation)| {
        update_display_regions(displays, generation);
        display_util::return_point(displays, ENTRY_POINT.get(), pos, t)
    });
    let Some((x, y)) = target else {
        log::warn!("could not find the entry display to return the cursor");
        return;
    };
    // The hook may observe the synthetic move after capture was released.
    // Start its next crossing check from the returned position.
    let prev_pos = PREV_POS.replace(Some((x, y)));
    if let Err(e) = unsafe { SetCursorPos(x, y) } {
        PREV_POS.replace(prev_pos);
        log::warn!("failed to return cursor to ({x}, {y}): {e:?}");
    } else {
        log::debug!("returned cursor to ({x}, {y})");
    }
}

fn process_request(request: ThreadRequest) {
    match request {
        ThreadRequest::Release(t) => release_capture(t),
        ThreadRequest::Create(pos) => {
            CLIENTS.with_borrow_mut(|clients| clients.insert(pos));
        }
        ThreadRequest::Destroy(pos) => {
            if let Some(active_pos) = ACTIVE_CLIENT.get() {
                if pos == active_pos {
                    let _ = ACTIVE_CLIENT.take();
                }
            }
            CLIENTS.with_borrow_mut(|clients| clients.remove(&pos));
        }
        ThreadRequest::SetEnterBinds(binds) => {
            ENTER_BINDS.with_borrow_mut(|tracker| tracker.set_binds(binds));
        }
    }
}

fn to_key_event(wparam: WPARAM, lparam: LPARAM) -> Option<KeyboardEvent> {
    let kybrdllhookstruct: KBDLLHOOKSTRUCT = unsafe { *(lparam.0 as *const KBDLLHOOKSTRUCT) };
    let mut scan_code = kybrdllhookstruct.scanCode;
    log::trace!("scan_code: {scan_code}");
    if kybrdllhookstruct.flags.contains(LLKHF_EXTENDED) {
        scan_code |= 0xE000;
    }
    /* NumLock is reported either as scan code 0x45 or as the extended
     * 0xE045, which the scancode table cannot express, so it used to
     * be dropped here (the hook still swallowed it). Identifying the
     * lock keys by virtual-key code sidesteps that ambiguity. */
    let vk = VIRTUAL_KEY(kybrdllhookstruct.vkCode as u16);
    let win_scan_code = if vk == VK_NUMLOCK {
        scancode::Windows::KeypadNumLock
    } else if vk == VK_CAPITAL {
        scancode::Windows::KeyCapsLock
    } else if vk == VK_SCROLL {
        scancode::Windows::KeyScrollLock
    } else {
        match scancode::Windows::try_from(scan_code) {
            Ok(code) => code,
            Err(_) => {
                log::warn!("failed to translate to windows scancode: {scan_code}");
                return None;
            }
        }
    };
    log::trace!("windows_scan: {win_scan_code:?}");
    let Ok(linux_scan_code): Result<Linux, ()> = win_scan_code.try_into() else {
        log::warn!("failed to translate into linux scancode: {win_scan_code:?}");
        return None;
    };
    log::trace!("windows_scan: {linux_scan_code:?}");
    let scan_code = linux_scan_code as u32;
    match wparam {
        WPARAM(p) if p == WM_KEYDOWN as usize => Some(KeyboardEvent::Key {
            time: 0,
            key: scan_code,
            state: 1,
        }),
        WPARAM(p) if p == WM_KEYUP as usize => Some(KeyboardEvent::Key {
            time: 0,
            key: scan_code,
            state: 0,
        }),
        WPARAM(p) if p == WM_SYSKEYDOWN as usize => Some(KeyboardEvent::Key {
            time: 0,
            key: scan_code,
            state: 1,
        }),
        WPARAM(p) if p == WM_SYSKEYUP as usize => Some(KeyboardEvent::Key {
            time: 0,
            key: scan_code,
            state: 0,
        }),
        _ => None,
    }
}

fn to_mouse_event(wparam: WPARAM, lparam: LPARAM) -> Option<PointerEvent> {
    let mouse_low_level: MSLLHOOKSTRUCT = unsafe { *(lparam.0 as *const MSLLHOOKSTRUCT) };
    match wparam {
        WPARAM(p) if p == WM_LBUTTONDOWN as usize => Some(PointerEvent::Button {
            time: 0,
            button: BTN_LEFT,
            state: 1,
        }),
        WPARAM(p) if p == WM_MBUTTONDOWN as usize => Some(PointerEvent::Button {
            time: 0,
            button: BTN_MIDDLE,
            state: 1,
        }),
        WPARAM(p) if p == WM_RBUTTONDOWN as usize => Some(PointerEvent::Button {
            time: 0,
            button: BTN_RIGHT,
            state: 1,
        }),
        WPARAM(p) if p == WM_LBUTTONUP as usize => Some(PointerEvent::Button {
            time: 0,
            button: BTN_LEFT,
            state: 0,
        }),
        WPARAM(p) if p == WM_MBUTTONUP as usize => Some(PointerEvent::Button {
            time: 0,
            button: BTN_MIDDLE,
            state: 0,
        }),
        WPARAM(p) if p == WM_RBUTTONUP as usize => Some(PointerEvent::Button {
            time: 0,
            button: BTN_RIGHT,
            state: 0,
        }),
        WPARAM(p) if p == WM_MOUSEMOVE as usize => {
            let (x, y) = (mouse_low_level.pt.x, mouse_low_level.pt.y);
            let (ex, ey) = ENTRY_POINT.get();
            let (dx, dy) = (x - ex, y - ey);
            let (dx, dy) = (dx as f64, dy as f64);
            Some(PointerEvent::Motion { time: 0, dx, dy })
        }
        WPARAM(p) if p == WM_MOUSEWHEEL as usize => Some(PointerEvent::AxisDiscrete120 {
            axis: 0,
            value: -(mouse_low_level.mouseData as i32 >> 16),
        }),
        WPARAM(p) if p == WM_XBUTTONDOWN as usize || p == WM_XBUTTONUP as usize => {
            let hb = mouse_low_level.mouseData >> 16;
            let button = match hb {
                1 => BTN_BACK,
                2 => BTN_FORWARD,
                _ => {
                    log::warn!("unknown mouse button");
                    return None;
                }
            };
            Some(PointerEvent::Button {
                time: 0,
                button,
                state: if p == WM_XBUTTONDOWN as usize { 1 } else { 0 },
            })
        }
        WPARAM(p) if p == WM_MOUSEHWHEEL as usize => Some(PointerEvent::AxisDiscrete120 {
            axis: 1, // Horizontal
            value: mouse_low_level.mouseData as i32 >> 16,
        }),
        w => {
            log::warn!("unknown mouse event: {w:?}");
            None
        }
    }
}

#[cfg(test)]
mod startup_tests {
    use super::*;
    use std::time::Duration;
    use windows::Win32::UI::WindowsAndMessaging::PM_REMOVE;

    #[test]
    fn ready_thread_accepts_first_message_before_other_windows_calls() {
        let ready = Arc::new((Condvar::new(), Mutex::new(None)));
        let worker_ready = ready.clone();
        let (posted_tx, posted_rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            publish_thread_ready(&worker_ready);
            // Do not let PeekMessage in the test itself create the queue
            // before the caller attempts the first post.
            posted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            let mut msg = MSG::default();
            let received = unsafe { PeekMessageW(&mut msg, None, WM_USER, WM_USER, PM_REMOVE) };
            received
                .as_bool()
                .then_some((msg.message, msg.wParam.0, msg.lParam.0))
        });
        let (condvar, id) = &*ready;
        let (id, timeout) = condvar
            .wait_timeout_while(id.lock().unwrap(), Duration::from_secs(2), |id| {
                id.is_none()
            })
            .unwrap();
        assert!(!timeout.timed_out());
        let id = id.unwrap();
        let posted = unsafe { PostThreadMessageW(id, WM_USER, WPARAM(1234), LPARAM(5678)) };
        posted_tx.send(()).unwrap();
        let received = worker.join().unwrap();
        posted.expect("ready thread must already have a message queue");
        assert_eq!(received, Some((WM_USER, 1234, 5678)));
    }
}
