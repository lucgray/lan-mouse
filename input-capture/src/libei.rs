use ashpd::{
    desktop::{
        PersistMode, Session,
        input_capture::{
            Activated, ActivatedBarrier, Barrier, BarrierID, Capabilities, CreateSessionOptions,
            InputCapture, Region, ReleaseOptions, StartOptions, Zones,
        },
    },
    enumflags2::BitFlags,
};
use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use reis::{
    ei::{self, handshake::ContextType},
    event::{Connection, DeviceCapability, EiEvent},
    tokio::EiConvertEventStream,
};
use std::{
    cell::Cell,
    collections::HashMap,
    env, fs,
    io::{self, Write},
    num::NonZeroU32,
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        net::UnixStream,
    },
    path::PathBuf,
    pin::Pin,
    rc::Rc,
    sync::{Arc, LazyLock, Mutex, Once},
    task::{Context, Poll},
};
use tokio::{
    sync::{
        Notify,
        mpsc::{self, Receiver, Sender},
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use futures_core::Stream;

use input_event::Event;

use crate::{CaptureEvent, WindowIdentifier};

use super::{
    Capture as LanMouseInputCapture, Position,
    error::{CaptureError, LibeiCaptureCreationError},
};

/* there is a bug in xdg-remote-desktop-portal-gnome / mutter that
 * prevents receiving further events after a session has been disabled once.
 * Therefore the session needs to be recreated when the barriers are updated */

/* mutter also kills the session whenever ei devices come and go, so there the
 * whole session has to be torn down and recreated on every device change.
 * Elsewhere that is pure overhead: each restart costs a CreateSession +
 * ConnectToEIS round trip, and compositors that keep per-session state around
 * (hyprland leaks a keymap fd per eis session, see hyprwm/Hyprland) can be
 * driven out of file descriptors by the churn.
 * Set LM_RESTART_SESSION_ON_DEVICE_CHANGE=1/0 to override the default. */
static RESTART_SESSION_ON_DEVICE_CHANGE: LazyLock<bool> =
    LazyLock::new(restart_session_on_device_change);

fn restart_session_on_device_change() -> bool {
    match env::var("LM_RESTART_SESSION_ON_DEVICE_CHANGE").as_deref() {
        Ok("1") => true,
        Ok("0") => false,
        _ => env::var("XDG_CURRENT_DESKTOP")
            .is_ok_and(|desktops| desktops.to_uppercase().split(':').any(|d| d == "GNOME")),
    }
}

/// events that necessitate restarting the capture session
#[derive(Clone, Copy, Debug)]
enum LibeiNotifyEvent {
    Create(Position),
    Destroy(Position),
}

// Keep only the next desired state while the current session still uses its snapshot.
struct CaptureClientUpdates {
    clients: Vec<Position>,
}

impl CaptureClientUpdates {
    fn new(clients: &[Position]) -> Self {
        Self {
            clients: clients.to_vec(),
        }
    }

    fn record(&mut self, event: LibeiNotifyEvent) {
        match event {
            LibeiNotifyEvent::Create(pos) => {
                if !self.clients.contains(&pos) {
                    self.clients.push(pos);
                }
            }
            LibeiNotifyEvent::Destroy(pos) => self.clients.retain(|p| *p != pos),
        }
    }

    fn finish(self) -> Vec<Position> {
        self.clients
    }

    #[cfg(test)]
    fn retained_positions(&self) -> usize {
        self.clients.len()
    }
}

#[allow(dead_code)]
pub struct LibeiInputCapture {
    input_capture: Pin<Box<InputCapture>>,
    capture_task: CaptureTaskCompletion,
    event_rx: Receiver<(Position, CaptureEvent)>,
    notify_capture: Sender<LibeiNotifyEvent>,
    notify_release: Arc<Notify>,
    cancellation_token: CancellationToken,
    terminated: bool,
}

struct CaptureTaskCompletion {
    handle: JoinHandle<Result<(), CaptureError>>,
    joined: bool,
}

impl CaptureTaskCompletion {
    fn result(
        result: Result<Result<(), CaptureError>, tokio::task::JoinError>,
    ) -> Result<(), CaptureError> {
        result.unwrap_or_else(|error| {
            Err(io::Error::other(format!("libei capture task failed: {error}")).into())
        })
    }

    fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<(), CaptureError>>> {
        if self.joined {
            return Poll::Ready(None);
        }
        match self.handle.poll_unpin(cx) {
            Poll::Ready(result) => {
                self.joined = true;
                Poll::Ready(Some(Self::result(result)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    async fn join(&mut self) -> Result<(), CaptureError> {
        if self.joined {
            return Ok(());
        }
        // is_finished does not mean the result was consumed. Await even an
        // already-finished handle, and mark it joined only after completion.
        let result = (&mut self.handle).await;
        self.joined = true;
        Self::result(result)
    }
}

/// returns (start pos, end pos), inclusive
fn pos_to_barrier(r: &Region, pos: Position) -> (i32, i32, i32, i32) {
    let (x, y) = (r.x_offset(), r.y_offset());
    let (w, h) = (r.width() as i32, r.height() as i32);
    match pos {
        Position::Left => (x, y, x, y + h - 1),
        Position::Right => (x + w, y, x + w, y + h - 1),
        Position::Top => (x, y, x + w - 1, y),
        Position::Bottom => (x, y + h, x + w - 1, y + h),
    }
}

/// Ashpd does not expose fields
#[derive(Clone, Copy, Debug)]
struct ICBarrier {
    barrier_id: BarrierID,
    position: (i32, i32, i32, i32),
}

impl ICBarrier {
    fn new(barrier_id: BarrierID, position: (i32, i32, i32, i32)) -> Self {
        Self {
            barrier_id,
            position,
        }
    }
}

impl From<ICBarrier> for Barrier {
    fn from(barrier: ICBarrier) -> Self {
        Barrier::new(barrier.barrier_id, barrier.position)
    }
}

fn select_barriers(
    zones: &Zones,
    clients: &[Position],
    next_barrier_id: &mut NonZeroU32,
) -> (Vec<ICBarrier>, HashMap<BarrierID, Position>) {
    let mut pos_for_barrier = HashMap::new();
    let mut barriers: Vec<ICBarrier> = vec![];

    for pos in clients {
        let mut client_barriers = zones
            .regions()
            .iter()
            .map(|r| {
                let id = *next_barrier_id;
                *next_barrier_id = next_barrier_id
                    .checked_add(1)
                    .expect("barrier id out of range");
                let position = pos_to_barrier(r, *pos);
                pos_for_barrier.insert(id, *pos);
                ICBarrier::new(id, position)
            })
            .collect();
        barriers.append(&mut client_barriers);
    }
    (barriers, pos_for_barrier)
}

async fn update_barriers(
    input_capture: &InputCapture,
    session: &Session<InputCapture>,
    active_clients: &[Position],
    next_barrier_id: &mut NonZeroU32,
) -> Result<(Vec<ICBarrier>, HashMap<BarrierID, Position>), ashpd::Error> {
    let zones = input_capture
        .zones(session, Default::default())
        .await?
        .response()?;
    log::debug!("zones: {zones:?}");

    let (barriers, id_map) = select_barriers(&zones, active_clients, next_barrier_id);
    log::debug!("barriers: {barriers:?}");
    log::debug!("client for barrier id: {id_map:?}");

    let ashpd_barriers: Vec<Barrier> = barriers.iter().copied().map(|b| b.into()).collect();
    let response = input_capture
        .set_pointer_barriers(
            session,
            &ashpd_barriers,
            zones.zone_set(),
            Default::default(),
        )
        .await?;
    let response = response.response()?;
    log::debug!("{response:?}");
    Ok((barriers, id_map))
}

fn capabilities() -> BitFlags<Capabilities> {
    Capabilities::Keyboard | Capabilities::Pointer | Capabilities::Touchscreen
}

/// Get the path to the InputCapture token file
fn get_token_file_path() -> PathBuf {
    let cache_dir = env::var("XDG_CACHE_HOME")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = env::var("HOME").expect("HOME not set");
            PathBuf::from(home).join(".cache")
        });

    cache_dir.join("lan-mouse").join("input-capture.token")
}

/// Read the InputCapture token from file
fn read_token() -> Option<String> {
    let token_path = get_token_file_path();
    match fs::read_to_string(&token_path) {
        // an interrupted write leaves the file empty, which is no token at all
        Ok(token) => Some(token.trim().to_string()).filter(|t| !t.is_empty()),
        Err(_) => None,
    }
}

/// Write the InputCapture token to file
fn write_token(token: &str) -> io::Result<()> {
    let token_path = get_token_file_path();
    if let Some(parent) = token_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // the token lets its holder skip the consent dialog, so keep it private;
    // mode() only applies on create, hence set_permissions for older files,
    // and only best-effort: some filesystems refuse chmod, and the file is
    // already truncated by then
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&token_path)?;
    if let Err(e) = file.set_permissions(fs::Permissions::from_mode(0o600)) {
        log::warn!("could not restrict {}: {e}", token_path.display());
    }
    file.write_all(token.as_bytes())?;
    Ok(())
}

async fn create_session(
    input_capture: &InputCapture,
    window_identifier: Arc<Mutex<Option<WindowIdentifier>>>,
) -> std::result::Result<(Session<InputCapture>, BitFlags<Capabilities>), ashpd::Error> {
    log::debug!("creating input capture session: {window_identifier:?}");
    let ashpd_window_identifier: Option<ashpd::WindowIdentifier> =
        window_identifier.lock().unwrap().clone().map(|i| i.into());
    match input_capture.create_session2(Default::default()).await {
        Ok(session) => {
            log::debug!("starting input capture session ...");
            let options = StartOptions::default()
                .set_capabilities(capabilities())
                .set_persist_mode(PersistMode::ExplicitlyRevoked)
                .set_restore_token(read_token());
            let response = match input_capture
                .start(&session, ashpd_window_identifier.as_ref(), options)
                .await
                .and_then(|request| request.response())
            {
                Ok(response) => response,
                Err(e) => {
                    // ashpd's Session has no Drop, so an unclosed one stays on the bus
                    if let Err(close_err) = session.close().await {
                        log::warn!("session.close(): {close_err}");
                    }
                    return Err(e);
                }
            };

            // The restore token is only valid once, we need to re-save it each time
            if let Some(token_str) = response.restore_token() {
                if let Err(e) = write_token(token_str) {
                    log::warn!("failed to save InputCapture token: {e}");
                }
            }
            Ok((session, response.capabilities()))
        }
        Err(ashpd::Error::RequiresVersion(required, current)) => {
            // create_session runs again on every barrier or device change
            static LOGGED: Once = Once::new();
            LOGGED.call_once(|| {
                log::info!(
                    "InputCapture portal is v{current}, persistence needs v{required}: permission cannot be remembered"
                )
            });
            let options = CreateSessionOptions::default().set_capabilities(capabilities());
            input_capture
                .create_session(ashpd_window_identifier.as_ref(), options)
                .await
        }
        Err(e) => Err(e),
    }
}

async fn connect_to_eis(
    input_capture: &InputCapture,
    session: &Session<InputCapture>,
) -> Result<(ei::Context, Connection, EiConvertEventStream), CaptureError> {
    log::debug!("connect_to_eis");
    let fd = input_capture
        .connect_to_eis(session, Default::default())
        .await?;

    // create unix stream from fd
    let stream = UnixStream::from(fd);
    stream.set_nonblocking(true)?;

    // create ei context
    let context = ei::Context::new(stream)?;
    let (conn, event_stream) = context
        .handshake_tokio("de.feschber.LanMouse", ContextType::Receiver)
        .await?;

    Ok((context, conn, event_stream))
}

async fn libei_event_handler(
    mut ei_event_stream: EiConvertEventStream,
    context: ei::Context,
    event_tx: Sender<(Position, CaptureEvent)>,
    release_session: Arc<Notify>,
    current_pos: Rc<Cell<Option<Position>>>,
) -> Result<(), CaptureError> {
    loop {
        let ei_event = ei_event_stream
            .next()
            .await
            .ok_or(CaptureError::EndOfStream)??;
        log::trace!("from ei: {ei_event:?}");
        let client = current_pos.get();
        handle_ei_event(ei_event, client, &context, &event_tx, &release_session).await?;
    }
}

impl LibeiInputCapture {
    /// creates a new libei input capture
    /// `window_id` is a window identifier for user prompts
    pub async fn new(
        window_identifier: Arc<Mutex<Option<WindowIdentifier>>>,
    ) -> std::result::Result<Self, LibeiCaptureCreationError> {
        let input_capture = Box::pin(InputCapture::new().await?);
        let input_capture_ptr = input_capture.as_ref().get_ref() as *const InputCapture;
        let first_session =
            Some(create_session(unsafe { &*input_capture_ptr }, window_identifier.clone()).await?);

        let (event_tx, event_rx) = mpsc::channel(1);
        let (notify_capture, notify_rx) = mpsc::channel(1);
        let notify_release = Arc::new(Notify::new());

        let cancellation_token = CancellationToken::new();

        let capture = do_capture(
            input_capture_ptr,
            notify_rx,
            notify_release.clone(),
            first_session,
            event_tx,
            cancellation_token.clone(),
            window_identifier,
        );
        let capture_task = tokio::task::spawn_local(capture);

        let producer = Self {
            input_capture,
            event_rx,
            capture_task: CaptureTaskCompletion {
                handle: capture_task,
                joined: false,
            },
            notify_capture,
            notify_release,
            cancellation_token,
            terminated: false,
        };

        Ok(producer)
    }
}

async fn do_capture(
    input_capture: *const InputCapture,
    mut capture_event: Receiver<LibeiNotifyEvent>,
    notify_release: Arc<Notify>,
    session: Option<(Session<InputCapture>, BitFlags<Capabilities>)>,
    event_tx: Sender<(Position, CaptureEvent)>,
    cancellation_token: CancellationToken,
    window_identifier: Arc<Mutex<Option<WindowIdentifier>>>,
) -> Result<(), CaptureError> {
    let mut session = session.map(|s| s.0);

    /* safety: libei_task does not outlive Self */
    let input_capture = unsafe { &*input_capture };
    let mut active_clients: Vec<Position> = vec![];
    let mut next_barrier_id = NonZeroU32::new(1).expect("id must be non-zero");

    let mut zones_changed = input_capture.receive_zones_changed().await?;

    loop {
        // do capture session
        let cancel_session = CancellationToken::new();
        let cancel_update = CancellationToken::new();

        let mut client_updates = CaptureClientUpdates::new(&active_clients);
        let handle_session_update_request = cancel_sibling_on_completion(
            wait_session_updates(
                &mut zones_changed,
                &mut capture_event,
                &mut client_updates,
                &cancellation_token,
                &cancel_update,
            ),
            cancel_session.clone(),
        );

        if !active_clients.is_empty() {
            // create session
            let mut session = match session.take() {
                Some(s) => s,
                None => {
                    create_session(input_capture, window_identifier.clone())
                        .await?
                        .0
                }
            };

            let capture_session = do_capture_session(
                input_capture,
                &mut session,
                &event_tx,
                &active_clients,
                &mut next_barrier_id,
                &notify_release,
                cancel_session.clone(),
            );
            let capture_session =
                cancel_sibling_on_completion(capture_session, cancel_update.clone());

            let (capture_result, update_result) =
                tokio::join!(capture_session, handle_session_update_request);
            log::debug!("capture session + session_update task done!");

            // disable capture
            log::debug!("disabling input capture");
            if let Err(e) = input_capture.disable(&session, Default::default()).await {
                log::warn!("input_capture.disable(&session) {e}");
            }
            if let Err(e) = session.close().await {
                log::warn!("session.close(): {e}");
            }

            // propagate error from capture session
            capture_result?;
            update_result?;
        } else {
            handle_session_update_request.await?;
        }

        // update clients if requested
        active_clients = client_updates.finish();

        // break
        if cancellation_token.is_cancelled() {
            break Ok(());
        }
    }
}

async fn next_session_update(
    zones_changed: &mut (impl Stream + Unpin),
    capture_event: &mut Receiver<LibeiNotifyEvent>,
) -> Result<Option<LibeiNotifyEvent>, CaptureError> {
    // Keep the two data sources fair when either produces a burst.
    tokio::select! {
        change = zones_changed.next() => change
            .map(|_| None)
            .ok_or_else(|| io::Error::other("libei zones change stream closed").into()),
        event = capture_event.recv() => event
            .map(Some)
            .ok_or_else(|| io::Error::other("libei client notification channel closed").into()),
    }
}

const MAX_SESSION_UPDATES_PER_YIELD: usize = 32;

async fn wait_session_updates(
    zones_changed: &mut (impl Stream + Unpin),
    capture_event: &mut Receiver<LibeiNotifyEvent>,
    client_updates: &mut CaptureClientUpdates,
    cancellation_token: &CancellationToken,
    cancel_update: &CancellationToken,
) -> Result<(), CaptureError> {
    let update = tokio::select! {
        biased;
        _ = cancellation_token.cancelled() => return Ok(()),
        _ = cancel_update.cancelled() => return Ok(()),
        update = next_session_update(zones_changed, capture_event) => update?,
    };
    if let Some(event) = update {
        client_updates.record(event);
    }

    let sleep = tokio::time::sleep(std::time::Duration::from_millis(50));
    tokio::pin!(sleep);
    let mut processed = 1;
    loop {
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => return Ok(()),
            _ = cancel_update.cancelled() => return Ok(()),
            _ = &mut sleep => return Ok(()),
            update = next_session_update(zones_changed, capture_event) => {
                if let Some(event) = update? {
                    client_updates.record(event);
                }
            },
        }
        processed += 1;
        if processed == MAX_SESSION_UPDATES_PER_YIELD {
            tokio::task::yield_now().await;
            processed = 0;
        }
    }
}

async fn run_ei_handler(
    handler: impl std::future::Future<Output = Result<(), CaptureError>>,
    cancel_session: CancellationToken,
    cancel_ei_handler: CancellationToken,
) -> Result<(), CaptureError> {
    tokio::select! {
        biased;
        // A requested session teardown is not an unexpected EIS failure.
        _ = cancel_ei_handler.cancelled() => Ok(()),
        result = handler => {
            log::debug!("libei exited: {result:?} cancelling session task");
            cancel_session.cancel();
            result
        }
    }
}

async fn cancel_sibling_on_completion(
    branch: impl std::future::Future<Output = Result<(), CaptureError>>,
    cancel_sibling: CancellationToken,
) -> Result<(), CaptureError> {
    let result = branch.await;
    // Errors must also wake the sibling waiting in the join.
    cancel_sibling.cancel();
    result
}

async fn do_capture_session(
    input_capture: &InputCapture,
    session: &mut Session<InputCapture>,
    event_tx: &Sender<(Position, CaptureEvent)>,
    active_clients: &[Position],
    next_barrier_id: &mut NonZeroU32,
    notify_release: &Notify,
    cancel_session: CancellationToken,
) -> Result<(), CaptureError> {
    // current client
    let current_pos = Rc::new(Cell::new(None));

    // connect to eis server
    let (context, _conn, ei_event_stream) = connect_to_eis(input_capture, session).await?;

    // set barriers
    let (barriers, pos_for_barrier_id) =
        update_barriers(input_capture, session, active_clients, next_barrier_id).await?;

    log::debug!("enabling session");
    input_capture.enable(session, Default::default()).await?;

    // cancellation token to release session
    let release_session = Arc::new(Notify::new());

    // async event task
    let cancel_ei_handler = CancellationToken::new();
    let event_chan = event_tx.clone();
    let pos = current_pos.clone();
    let release_session_clone = release_session.clone();
    let ei_task = run_ei_handler(
        libei_event_handler(
            ei_event_stream,
            context,
            event_chan,
            release_session_clone,
            pos,
        ),
        cancel_session.clone(),
        cancel_ei_handler.clone(),
    );

    let capture_session_task = async {
        // receiver for activation tokens
        let mut activated = input_capture.receive_activated().await?;
        let mut ei_devices_changed = false;
        loop {
            tokio::select! {
                activated = activated.next() => {
                    let activated = activated.ok_or(CaptureError::ActivationClosed)?;
                    log::debug!("activated: {activated:?}");

                    // get barrier id from activation
                    let barrier_id = match activated.barrier_id() {
                        Some(ActivatedBarrier::Barrier(id)) => id,
                        // workaround for KDE plasma not reporting barrier ids
                        Some(ActivatedBarrier::UnknownBarrier) | None => find_corresponding_client(&barriers, activated.cursor_position().expect("no cursor position reported by compositor")),
                    };

                    // find client corresponding to barrier
                    let pos = match pos_for_barrier_id.get(&barrier_id) {
                        Some(id) => *id,
                        None => {
                            log::warn!("INVALID BARRIER ID: Id {barrier_id} does not exist!");
                            let id = find_corresponding_client(&barriers, activated.cursor_position().expect("no cursor position reported by compositor"));
                            let pos = *pos_for_barrier_id.get(&id).expect("invalid barrier id");
                            pos
                        },
                    };
                    current_pos.replace(Some(pos));

                    // client entered => send event
                    event_tx
                        .send((pos, CaptureEvent::Begin(0.5)))
                        .await
                        .expect("no channel");

                    tokio::select! {
                        _ = notify_release.notified() => { /* capture release */
                            log::debug!("release session requested");
                        },
                        _ = release_session.notified() => { /* release session */
                            log::debug!("ei devices changed");
                            ei_devices_changed = true;
                        },
                        _ = cancel_session.cancelled() => { /* kill session notify */
                            log::debug!("session cancel requested");
                            break
                        },
                    }

                    release_capture(input_capture, session, activated, pos).await?;

                }
                _ = notify_release.notified() => { /* capture release -> we are not capturing anyway, so ignore */
                    log::debug!("release session requested");
                },
                _ = release_session.notified() => { /* release session */
                    log::debug!("ei devices changed");
                    ei_devices_changed = true;
                },
                _ = cancel_session.cancelled() => { /* kill session notify */
                    log::debug!("session cancel requested");
                    break
                },
            }
            if ei_devices_changed {
                /* for whatever reason, GNOME seems to kill the session
                 * as soon as devices are added or removed, so we need
                 * to cancel */
                break;
            }
        }
        Ok::<(), CaptureError>(())
    };

    let capture_session_task =
        cancel_sibling_on_completion(capture_session_task, cancel_ei_handler);
    let (a, b) = tokio::join!(ei_task, capture_session_task);

    log::debug!("both session and ei task finished!");
    a?;
    b?;

    Ok(())
}

async fn release_capture(
    input_capture: &InputCapture,
    session: &Session<InputCapture>,
    activated: Activated,
    current_pos: Position,
) -> Result<(), CaptureError> {
    if let Some(activation_id) = activated.activation_id() {
        log::debug!("releasing input capture {activation_id}");
    }
    let (x, y) = activated
        .cursor_position()
        .expect("compositor did not report cursor position!");
    log::debug!("client entered @ ({x}, {y})");
    let (dx, dy) = match current_pos {
        // offset cursor position to not enter again immediately
        Position::Left => (1., 0.),
        Position::Right => (-1., 0.),
        Position::Top => (0., 1.),
        Position::Bottom => (0., -1.),
    };
    // release 1px to the right of the entered zone
    let cursor_position = (x as f64 + dx, y as f64 + dy);
    let release_options = ReleaseOptions::default()
        .set_activation_id(activated.activation_id())
        .set_cursor_position(Some(cursor_position));
    input_capture.release(session, release_options).await?;
    Ok(())
}

fn find_corresponding_client(barriers: &[ICBarrier], pos: (f32, f32)) -> BarrierID {
    barriers
        .iter()
        .copied()
        .min_by_key(|b| {
            let (x1, y1, x2, y2) = b.position;
            let (x1, y1, x2, y2) = (x1 as f32, y1 as f32, x2 as f32, y2 as f32);
            distance_to_line(((x1, y1), (x2, y2)), pos) as i32
        })
        .expect("could not find barrier corresponding to client")
        .barrier_id
}

fn distance_to_line(line: ((f32, f32), (f32, f32)), p: (f32, f32)) -> f32 {
    let ((x1, y1), (x2, y2)) = line;
    let (x0, y0) = p;
    /*
     * we use the fact that for the triangle spanned by the line and p,
     * the height of the triangle is the desired distance and can be calculated by
     * h = 2A / b with b being the line_length and
     */
    let double_triangle_area = ((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1).abs();
    let line_length = ((y2 - y1).powf(2.0) + (x2 - x1).powf(2.0)).sqrt();
    let distance = double_triangle_area / line_length;
    log::debug!("distance to line({line:?}, {p:?}) = {distance}");
    distance
}

async fn handle_ei_event(
    ei_event: EiEvent,
    current_client: Option<Position>,
    context: &ei::Context,
    event_tx: &Sender<(Position, CaptureEvent)>,
    release_session: &Notify,
) -> Result<(), CaptureError> {
    let all_capabilities = DeviceCapability::Pointer
        | DeviceCapability::PointerAbsolute
        | DeviceCapability::Keyboard
        | DeviceCapability::Touch
        | DeviceCapability::Scroll
        | DeviceCapability::Button;
    match ei_event {
        EiEvent::SeatAdded(s) => {
            s.seat.bind_capabilities(all_capabilities);
            context.flush().map_err(|e| io::Error::new(e.kind(), e))?;
        }
        EiEvent::SeatRemoved(_) => {
            log::debug!("releasing session: {ei_event:?}");
            release_session.notify_waiters();
        }
        /* EiEvent::DeviceAdded(_) | */
        EiEvent::DeviceRemoved(_) => {
            if *RESTART_SESSION_ON_DEVICE_CHANGE {
                log::debug!("releasing session: {ei_event:?}");
                release_session.notify_waiters();
            } else {
                log::debug!("ignoring device change: {ei_event:?}");
            }
        }
        EiEvent::DevicePaused(_) | EiEvent::DeviceResumed(_) => {}
        EiEvent::DeviceStartEmulating(_) => log::debug!("START EMULATING"),
        EiEvent::DeviceStopEmulating(_) => log::debug!("STOP EMULATING"),
        EiEvent::Disconnected(d) => {
            return Err(CaptureError::Disconnected(format!("{:?}", d.reason)));
        }
        _ => {
            if let Some(pos) = current_client {
                for event in Event::from_ei_event(ei_event) {
                    event_tx
                        .send((pos, CaptureEvent::Input(event)))
                        .await
                        .expect("no channel");
                }
            }
        }
    }
    Ok(())
}

#[async_trait]
impl LanMouseInputCapture for LibeiInputCapture {
    async fn create(&mut self, pos: Position) -> Result<(), CaptureError> {
        let _ = self
            .notify_capture
            .send(LibeiNotifyEvent::Create(pos))
            .await;
        Ok(())
    }

    async fn destroy(&mut self, pos: Position) -> Result<(), CaptureError> {
        let _ = self
            .notify_capture
            .send(LibeiNotifyEvent::Destroy(pos))
            .await;
        Ok(())
    }

    async fn set_enter_only(&mut self, _pos: Position, _enabled: bool) -> Result<(), CaptureError> {
        Ok(())
    }

    async fn release(&mut self) -> Result<(), CaptureError> {
        self.notify_release.notify_waiters();
        Ok(())
    }

    async fn release_to(&mut self, _t: f64) -> Result<(), CaptureError> {
        self.release().await
    }

    async fn terminate(&mut self) -> Result<(), CaptureError> {
        self.cancellation_token.cancel();
        log::debug!("waiting for capture to terminate...");
        let res = self.capture_task.join().await;
        self.terminated = true;
        log::debug!("done!");
        res
    }
}

impl Drop for LibeiInputCapture {
    fn drop(&mut self) {
        if !self.terminated {
            // async drop not stabilized; a panic here takes down the daemon on suspend/resume
            // / compositor EIS restarts, upstream issue #386
            log::error!(
                "LibeiInputCapture dropped without being terminated! Cancelling capture task."
            );
            self.cancellation_token.cancel();
        }
    }
}

impl Stream for LibeiInputCapture {
    type Item = Result<(Position, CaptureEvent), CaptureError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.capture_task.poll_result(cx) {
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(Some(Ok(())) | None) => Poll::Ready(None),
            Poll::Pending => self.event_rx.poll_recv(cx).map(|e| e.map(Result::Ok)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::poll_fn;

    struct ReadyZoneBurst {
        remaining: usize,
        changes: Rc<Cell<usize>>,
    }

    impl Stream for ReadyZoneBurst {
        type Item = ();
        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<()>> {
            if self.remaining == 0 {
                return Poll::Pending;
            }
            self.remaining -= 1;
            self.changes.set(self.changes.get() + 1);
            Poll::Ready(Some(()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn update_burst_yields_before_draining_ready_signals() {
        let changes = Rc::new(Cell::new(0));
        let mut zones = ReadyZoneBurst {
            remaining: 256,
            changes: changes.clone(),
        };
        let (_send, mut events) = mpsc::channel(1);
        let mut updates = CaptureClientUpdates::new(&[Position::Left]);
        let stop = CancellationToken::new();
        let session_finished = CancellationToken::new();
        {
            let wait = wait_session_updates(
                &mut zones,
                &mut events,
                &mut updates,
                &stop,
                &session_finished,
            );
            tokio::pin!(wait);
            assert!(wait.as_mut().now_or_never().is_none());
            assert!(
                changes.get() <= 32,
                "drained {} ready changes",
                changes.get()
            );
            stop.cancel();
            assert!(wait.await.is_ok());
        }
        assert_eq!(changes.get(), 32);
        assert_eq!(updates.finish(), vec![Position::Left]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ready_control_runs_before_update_burst_drains() {
        let changes = Rc::new(Cell::new(0));
        let mut zones = ReadyZoneBurst {
            remaining: 256,
            changes: changes.clone(),
        };
        let (_send, mut events) = mpsc::channel(1);
        let mut updates = CaptureClientUpdates::new(&[]);
        let stop = CancellationToken::new();
        let session_finished = CancellationToken::new();
        let (send_control, receive_control) = tokio::sync::oneshot::channel();
        send_control.send(()).unwrap();
        tokio::select! {
            biased;
            result = wait_session_updates(&mut zones, &mut events, &mut updates,
                &stop, &session_finished) => panic!("update watcher exited early: {result:?}"),
            result = receive_control => result.unwrap(),
        }
        assert_eq!(changes.get(), MAX_SESSION_UPDATES_PER_YIELD);
        assert_eq!(zones.remaining, 256 - MAX_SESSION_UPDATES_PER_YIELD);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_shutdown_interrupts_ready_update_burst() {
        let changes = Rc::new(Cell::new(0));
        let mut zones = ReadyZoneBurst {
            remaining: 256,
            changes: changes.clone(),
        };
        let (_send, mut events) = mpsc::channel(1);
        let mut updates = CaptureClientUpdates::new(&[]);
        let stop = CancellationToken::new();
        let stop_task = stop.clone();
        let cancel_task = tokio::spawn(async move {
            stop_task.cancel();
        });
        assert!(
            wait_session_updates(
                &mut zones,
                &mut events,
                &mut updates,
                &stop,
                &CancellationToken::new()
            )
            .await
            .is_ok()
        );
        cancel_task.await.unwrap();
        assert!(changes.get() < 256, "shutdown waited for the entire burst");
        assert_eq!(updates.retained_positions(), 0);
    }

    // Prevent a regressed EOF busy loop from trapping the test runtime.
    struct EofProbe {
        initial_change: bool,
        polls: Rc<Cell<usize>>,
    }

    impl Stream for EofProbe {
        type Item = ();
        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<()>> {
            self.polls.set(self.polls.get() + 1);
            if self.initial_change {
                self.initial_change = false;
                Poll::Ready(Some(()))
            } else if self.polls.get() <= 16 {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        }
    }

    async fn assert_closed_zone_stream(initial_change: bool) {
        let polls = Rc::new(Cell::new(0));
        let mut zones = EofProbe {
            initial_change,
            polls: polls.clone(),
        };
        let (_send, mut events) = mpsc::channel(1);
        let mut updates = CaptureClientUpdates::new(&[]);
        let result = wait_session_updates(
            &mut zones,
            &mut events,
            &mut updates,
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .now_or_never();
        let error = result
            .expect("EOF caused repeated polling instead of failure")
            .unwrap_err();
        assert!(error.to_string().contains("zones"));
        assert_eq!(polls.get(), if initial_change { 2 } else { 1 });
    }

    async fn assert_closed_client_channel(initial_change: bool) {
        let mut zones = futures::stream::pending::<()>();
        let (send, mut events) = mpsc::channel(1);
        if initial_change {
            send.send(LibeiNotifyEvent::Create(Position::Right))
                .await
                .unwrap();
        }
        drop(send);
        let mut updates = CaptureClientUpdates::new(&[]);
        let result = wait_session_updates(
            &mut zones,
            &mut events,
            &mut updates,
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .now_or_never();
        let error = result.expect("closed channel did not finish").unwrap_err();
        assert!(error.to_string().contains("client notification"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_zone_stream_before_debounce_reports_failure() {
        assert_closed_zone_stream(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_zone_stream_during_debounce_reports_failure() {
        assert_closed_zone_stream(true).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_client_channel_before_debounce_reports_failure() {
        assert_closed_client_channel(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_client_channel_during_debounce_reports_failure() {
        assert_closed_client_channel(true).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn requested_update_shutdown_has_priority_over_closed_sources() {
        for stop_backend in [false, true] {
            let polls = Rc::new(Cell::new(0));
            let mut zones = EofProbe {
                initial_change: false,
                polls: polls.clone(),
            };
            let (send, mut events) = mpsc::channel(1);
            drop(send);
            let stop = CancellationToken::new();
            let session_finished = CancellationToken::new();
            if stop_backend {
                stop.cancel();
            } else {
                session_finished.cancel();
            }
            let mut updates = CaptureClientUpdates::new(&[Position::Left]);
            let result = wait_session_updates(
                &mut zones,
                &mut events,
                &mut updates,
                &stop,
                &session_finished,
            )
            .await;
            assert!(result.is_ok());
            assert_eq!(polls.get(), 0);
            assert_eq!(updates.finish(), vec![Position::Left]);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn healthy_updates_merge_until_original_debounce_finishes() {
        let mut zones = futures::stream::pending::<()>();
        let (send, mut events) = mpsc::channel(3);
        for event in [
            LibeiNotifyEvent::Create(Position::Left),
            LibeiNotifyEvent::Destroy(Position::Left),
            LibeiNotifyEvent::Create(Position::Right),
        ] {
            send.send(event).await.unwrap();
        }
        let mut updates = CaptureClientUpdates::new(&[Position::Top]);
        let stop = CancellationToken::new();
        let session_finished = CancellationToken::new();
        {
            let wait = wait_session_updates(
                &mut zones,
                &mut events,
                &mut updates,
                &stop,
                &session_finished,
            );
            tokio::pin!(wait);
            assert!(wait.as_mut().now_or_never().is_none());
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(200), wait)
                    .await
                    .unwrap()
                    .is_ok()
            );
        }
        // The sender stays open throughout the debounce.
        drop(send);
        assert_eq!(updates.finish(), vec![Position::Top, Position::Right]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn session_setup_failure_cancels_waiting_update_branch() {
        let cancel_update = CancellationToken::new();
        let joined = async {
            tokio::join!(
                cancel_sibling_on_completion(
                    async { Err(CaptureError::Io(io::Error::other("setup failed"))) },
                    cancel_update.clone(),
                ),
                cancel_update.cancelled(),
            )
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_millis(100), joined)
            .await
            .expect("failed setup left update sibling pending");
        assert!(
            matches!(result, Err(CaptureError::Io(error)) if error.to_string() == "setup failed")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ei_handler_failure_reaches_session_owner() {
        let cancel_session = CancellationToken::new();
        let result = run_ei_handler(
            async { Err(CaptureError::EndOfStream) },
            cancel_session.clone(),
            CancellationToken::new(),
        )
        .await;
        assert!(cancel_session.is_cancelled());
        assert!(matches!(result, Err(CaptureError::EndOfStream)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capture_session_failure_cancels_waiting_ei_sibling() {
        let cancel_session = CancellationToken::new();
        let cancel_ei = CancellationToken::new();
        let joined = async {
            tokio::join!(
                run_ei_handler(std::future::pending(), cancel_session, cancel_ei.clone()),
                cancel_sibling_on_completion(
                    async { Err(CaptureError::ActivationClosed) },
                    cancel_ei
                ),
            )
        };
        let (ei_result, session_result) =
            tokio::time::timeout(std::time::Duration::from_millis(100), joined)
                .await
                .expect("failed session left EI sibling pending");
        assert!(ei_result.is_ok());
        assert!(matches!(
            session_result,
            Err(CaptureError::ActivationClosed)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ei_failure_waits_for_session_cleanup_before_returning() {
        let cancel_session = CancellationToken::new();
        let cancel_ei = CancellationToken::new();
        let (cleanup_finished, cleanup_wait) = tokio::sync::oneshot::channel();
        let (cleanup_started, mut started_wait) = tokio::sync::oneshot::channel();
        let session = async {
            cancel_session.cancelled().await;
            cleanup_started.send(()).unwrap();
            cleanup_wait.await.unwrap();
            Ok(())
        };
        let joined = async {
            tokio::join!(
                run_ei_handler(
                    async { Err(CaptureError::Disconnected("test".into())) },
                    cancel_session.clone(),
                    cancel_ei.clone()
                ),
                cancel_sibling_on_completion(session, cancel_ei.clone()),
            )
        };
        tokio::pin!(joined);
        assert!(joined.as_mut().now_or_never().is_none());
        assert!(started_wait.try_recv().is_ok());
        assert!(!cancel_ei.is_cancelled());
        cleanup_finished.send(()).unwrap();
        let (ei_result, session_result) = joined.await;
        assert!(matches!(ei_result, Err(CaptureError::Disconnected(reason)) if reason == "test"));
        assert!(session_result.is_ok());
        assert!(cancel_ei.is_cancelled());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn requested_ei_teardown_has_priority_over_ready_stream_error() {
        let cancel_session = CancellationToken::new();
        let cancel_ei = CancellationToken::new();
        cancel_ei.cancel();
        let handler_polled = Cell::new(false);
        let result = run_ei_handler(
            async {
                handler_polled.set(true);
                Err(CaptureError::EndOfStream)
            },
            cancel_session.clone(),
            cancel_ei,
        )
        .await;
        assert!(result.is_ok());
        assert!(!handler_polled.get());
        assert!(!cancel_session.is_cancelled());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn successful_session_exit_cancels_ei_without_failure() {
        let cancel_session = CancellationToken::new();
        let cancel_ei = CancellationToken::new();
        let (ei_result, session_result) = tokio::join!(
            run_ei_handler(
                std::future::pending(),
                cancel_session.clone(),
                cancel_ei.clone()
            ),
            cancel_sibling_on_completion(async { Ok(()) }, cancel_ei),
        );
        assert!(ei_result.is_ok());
        assert!(session_result.is_ok());
        assert!(!cancel_session.is_cancelled());
    }

    #[test]
    fn client_update_burst_retains_only_four_positions() {
        let mut updates = CaptureClientUpdates::new(&[Position::Left]);
        for _ in 0..10_000 {
            for pos in [
                Position::Left,
                Position::Right,
                Position::Top,
                Position::Bottom,
            ] {
                updates.record(LibeiNotifyEvent::Destroy(pos));
                updates.record(LibeiNotifyEvent::Create(pos));
            }
        }
        assert!(
            updates.retained_positions() <= 4,
            "retained {} positions",
            updates.retained_positions()
        );
        assert_eq!(
            updates.finish(),
            vec![
                Position::Left,
                Position::Right,
                Position::Top,
                Position::Bottom
            ]
        );
    }

    #[test]
    fn client_updates_preserve_serial_membership_and_barrier_order() {
        let positions = [
            Position::Left,
            Position::Right,
            Position::Top,
            Position::Bottom,
        ];
        let initial_states = [
            vec![],
            vec![Position::Top],
            vec![Position::Right, Position::Left],
            positions.to_vec(),
        ];
        // All five-operation sequences, including duplicates and destroy/recreate.
        for initial in initial_states {
            for mut sequence in 0..8usize.pow(5) {
                let mut expected = initial.clone();
                let mut updates = CaptureClientUpdates::new(&initial);
                for _ in 0..5 {
                    let operation = sequence % 8;
                    sequence /= 8;
                    let pos = positions[operation / 2];
                    if operation % 2 == 0 {
                        updates.record(LibeiNotifyEvent::Create(pos));
                        if !expected.contains(&pos) {
                            expected.push(pos);
                        }
                    } else {
                        updates.record(LibeiNotifyEvent::Destroy(pos));
                        expected.retain(|p| *p != pos);
                    }
                    assert!(updates.retained_positions() <= 4);
                }
                assert_eq!(updates.finish(), expected);
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finished_capture_task_failure_is_not_silently_successful() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let handle =
                    tokio::task::spawn_local(async { Err(CaptureError::ActivationClosed) });
                tokio::task::yield_now().await;
                assert!(handle.is_finished());
                let mut completion = CaptureTaskCompletion {
                    handle,
                    joined: false,
                };
                assert!(matches!(
                    completion.join().await,
                    Err(CaptureError::ActivationClosed)
                ));
                assert!(completion.join().await.is_ok());
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_capture_task_reports_error_without_panicking() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let handle =
                    tokio::task::spawn_local(std::future::pending::<Result<(), CaptureError>>());
                handle.abort();
                let mut completion = CaptureTaskCompletion {
                    handle,
                    joined: false,
                };
                let result = poll_fn(|cx| completion.poll_result(cx)).await.unwrap();
                assert!(result.unwrap_err().to_string().contains("cancel"));
                assert!(poll_fn(|cx| completion.poll_result(cx)).await.is_none());
                assert!(completion.join().await.is_ok());
            })
            .await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn capture_task_panic_is_returned_once_as_cleanup_error() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let handle: JoinHandle<Result<(), CaptureError>> =
                    tokio::task::spawn_local(async { panic!("simulated EIS panic") });
                let mut completion = CaptureTaskCompletion {
                    handle,
                    joined: false,
                };
                let error = completion.join().await.unwrap_err().to_string();
                assert!(
                    error.contains("libei capture task failed")
                        && error.contains("simulated EIS panic")
                );
                assert!(completion.join().await.is_ok());
                assert!(poll_fn(|cx| completion.poll_result(cx)).await.is_none());
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_join_wait_preserves_task_and_later_result() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let (send, wait) = tokio::sync::oneshot::channel();
                let handle = tokio::task::spawn_local(async {
                    wait.await.unwrap();
                    Err(CaptureError::ActivationClosed)
                });
                let mut completion = CaptureTaskCompletion {
                    handle,
                    joined: false,
                };
                assert!(completion.join().now_or_never().is_none());
                assert!(!completion.joined);
                send.send(()).unwrap();
                assert!(matches!(
                    completion.join().await,
                    Err(CaptureError::ActivationClosed)
                ));
                assert!(poll_fn(|cx| completion.poll_result(cx)).await.is_none());
            })
            .await;
    }
}
