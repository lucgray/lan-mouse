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
    collections::{HashMap, HashSet},
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
    input_capture: Arc<InputCapture>,
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
    fn spawn_owned<T: 'static, F, Fut>(owner: Arc<T>, run: F) -> Self
    where
        F: FnOnce(Arc<T>) -> Fut + 'static,
        Fut: std::future::Future<Output = Result<(), CaptureError>> + 'static,
    {
        let handle = tokio::task::spawn_local(async move {
            // Retain the resource even if the frontend/JoinHandle is dropped.
            let result = run(owner.clone()).await;
            drop(owner);
            result
        });
        Self {
            handle,
            joined: false,
        }
    }

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
fn pos_to_barrier(r: &Region, pos: Position) -> Result<(i32, i32, i32, i32), CaptureError> {
    if r.width() == 0 || r.height() == 0 {
        return Err(io::Error::other("libei region has empty dimensions").into());
    }
    let (x, y) = (i64::from(r.x_offset()), i64::from(r.y_offset()));
    let (w, h) = (i64::from(r.width()), i64::from(r.height()));
    let coordinate = |value| {
        i32::try_from(value).map_err(|_| {
            CaptureError::from(io::Error::other(
                "libei region endpoint exceeds i32 coordinates",
            ))
        })
    };
    // Validate the owning region as well as the selected boundary. A valid left
    // barrier must not carry out-of-range bounds for later release/entry math.
    let (last_x, last_y) = (coordinate(x + w - 1)?, coordinate(y + h - 1)?);
    let (origin_x, origin_y) = (r.x_offset(), r.y_offset());
    Ok(match pos {
        Position::Left => (origin_x, origin_y, origin_x, last_y),
        Position::Right => {
            let right = coordinate(x + w)?;
            (right, origin_y, right, last_y)
        }
        Position::Top => (origin_x, origin_y, last_x, origin_y),
        Position::Bottom => {
            let bottom = coordinate(y + h)?;
            (origin_x, bottom, last_x, bottom)
        }
    })
}

/// Ashpd does not expose fields
#[derive(Clone, Copy, Debug)]
struct ICBarrier {
    barrier_id: BarrierID,
    position: (i32, i32, i32, i32),
    zone_bounds: Option<(f64, f64, f64, f64)>,
}

impl ICBarrier {
    fn new(barrier_id: BarrierID, position: (i32, i32, i32, i32)) -> Self {
        Self {
            barrier_id,
            position,
            zone_bounds: None,
        }
    }

    fn for_region(
        barrier_id: BarrierID,
        region: &Region,
        pos: Position,
    ) -> Result<Self, CaptureError> {
        let mut barrier = Self::new(barrier_id, pos_to_barrier(region, pos)?);
        let (x, y) = (f64::from(region.x_offset()), f64::from(region.y_offset()));
        barrier.zone_bounds = Some((
            x,
            y,
            x + f64::from(region.width()) - 1.,
            y + f64::from(region.height()) - 1.,
        ));
        Ok(barrier)
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
) -> Result<(Vec<ICBarrier>, HashMap<BarrierID, Position>), CaptureError> {
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
                pos_for_barrier.insert(id, *pos);
                ICBarrier::for_region(id, r, *pos)
            })
            .collect::<Result<Vec<_>, _>>()?;
        barriers.append(&mut client_barriers);
    }
    Ok((barriers, pos_for_barrier))
}

fn accepted_barriers(
    mut barriers: Vec<ICBarrier>,
    mut id_map: HashMap<BarrierID, Position>,
    failed: &[BarrierID],
) -> Result<(Vec<ICBarrier>, HashMap<BarrierID, Position>), CaptureError> {
    let requested = barriers.len();
    if !failed.is_empty() {
        let failed: HashSet<_> = failed.iter().copied().collect();
        barriers.retain(|barrier| !failed.contains(&barrier.barrier_id));
        id_map.retain(|id, _| !failed.contains(id));
        log::warn!(
            "portal rejected pointer barriers {failed:?}; {} of {requested} remain",
            barriers.len()
        );
    }
    if barriers.is_empty() {
        return Err(io::Error::other(format!(
            "portal accepted no pointer barriers ({requested} requested)"
        ))
        .into());
    }
    Ok((barriers, id_map))
}

async fn update_barriers(
    input_capture: &InputCapture,
    session: &Session<InputCapture>,
    active_clients: &[Position],
    next_barrier_id: &mut NonZeroU32,
) -> Result<(Vec<ICBarrier>, HashMap<BarrierID, Position>), CaptureError> {
    let zones = input_capture
        .zones(session, Default::default())
        .await?
        .response()?;
    log::debug!("zones: {zones:?}");

    let (barriers, id_map) = select_barriers(&zones, active_clients, next_barrier_id)?;
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
    accepted_barriers(barriers, id_map, response.failed_barriers())
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
        let input_capture = Arc::new(InputCapture::new().await?);
        let first_session = Some(create_session(&input_capture, window_identifier.clone()).await?);

        let (event_tx, event_rx) = mpsc::channel(1);
        let (notify_capture, notify_rx) = mpsc::channel(1);
        let notify_release = Arc::new(Notify::new());

        let cancellation_token = CancellationToken::new();

        let task_cancel = cancellation_token.clone();
        let task_release = notify_release.clone();
        let capture_task =
            CaptureTaskCompletion::spawn_owned(input_capture.clone(), move |input_capture| {
                do_capture(
                    input_capture,
                    notify_rx,
                    task_release,
                    first_session,
                    event_tx,
                    task_cancel,
                    window_identifier,
                )
            });

        let producer = Self {
            input_capture,
            event_rx,
            capture_task,
            notify_capture,
            notify_release,
            cancellation_token,
            terminated: false,
        };

        Ok(producer)
    }
}

async fn do_capture(
    input_capture: Arc<InputCapture>,
    mut capture_event: Receiver<LibeiNotifyEvent>,
    notify_release: Arc<Notify>,
    session: Option<(Session<InputCapture>, BitFlags<Capabilities>)>,
    event_tx: Sender<(Position, CaptureEvent)>,
    cancellation_token: CancellationToken,
    window_identifier: Arc<Mutex<Option<WindowIdentifier>>>,
) -> Result<(), CaptureError> {
    let mut session = session.map(|s| s.0);

    let input_capture = input_capture.as_ref();
    let mut active_clients: Vec<Position> = vec![];
    let mut next_barrier_id = NonZeroU32::new(1).expect("id must be non-zero");

    let result = async {
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
    .await;
    finish_pending_session(result, session, |session| async move {
        session.close().await.map_err(CaptureError::from)
    })
    .await
}

async fn finish_pending_session<S, F>(
    result: Result<(), CaptureError>,
    pending_session: Option<S>,
    close: impl FnOnce(S) -> F,
) -> Result<(), CaptureError>
where
    F: std::future::Future<Output = Result<(), CaptureError>>,
{
    if let Some(session) = pending_session {
        // The first session can remain unused on idle shutdown or setup failure.
        // Finish native cleanup before publishing the task's original result.
        if let Err(error) = close(session).await {
            log::warn!("unused capture session.close(): {error}");
        }
    }
    result
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

fn capture_session_handle(
    session: &Session<InputCapture>,
) -> Result<ashpd::zvariant::OwnedObjectPath, CaptureError> {
    // Session exposes its object path through Serialize, but its path() is private.
    let context = ashpd::zvariant::serialized::Context::new_dbus(ashpd::zvariant::LE, 0);
    let data = ashpd::zvariant::to_bytes(context, session).map_err(|error| {
        io::Error::other(format!("could not serialize capture session: {error}"))
    })?;
    let (handle, _) = data
        .deserialize::<ashpd::zvariant::OwnedObjectPath>()
        .map_err(|error| {
            io::Error::other(format!("could not read capture session handle: {error}"))
        })?;
    Ok(handle)
}

fn activation_position(
    activated: &Activated,
    expected_session: &str,
    barriers: &[ICBarrier],
    routes: &HashMap<BarrierID, Position>,
) -> Result<Option<Position>, CaptureError> {
    if activated.session_handle().as_str() != expected_session {
        log::debug!(
            "ignoring activation for another capture session: {}",
            activated.session_handle()
        );
        return Ok(None);
    }
    let cursor = activated.cursor_position();
    if cursor.is_some_and(|(x, y)| !x.is_finite() || !y.is_finite()) {
        return Err(io::Error::other("libei activation has nonfinite cursor coordinates").into());
    }
    if let Some(ActivatedBarrier::Barrier(id)) = activated.barrier_id() {
        if let Some(pos) = routes.get(&id) {
            return Ok(Some(*pos));
        }
        log::warn!("INVALID BARRIER ID: Id {id} does not exist!");
    }
    let cursor = cursor.ok_or_else(|| {
        io::Error::other("libei activation cannot locate a barrier without cursor coordinates")
    })?;
    let id = find_corresponding_client(barriers, cursor)?;
    let pos = routes.get(&id).copied().ok_or_else(|| {
        io::Error::other("libei activation barrier geometry has no position route")
    })?;
    Ok(Some(pos))
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
    let session_handle = capture_session_handle(session)?;
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

                    let Some(pos) = activation_position(&activated, session_handle.as_str(),
                        &barriers, &pos_for_barrier_id)? else { continue; };
                    current_pos.replace(Some(pos));

                    // client entered => send event
                    let t = activation_edge_position(&activated, pos, &barriers, &pos_for_barrier_id);
                    if !send_activation_event(event_tx, pos, t, &cancel_session).await? {
                        break;
                    }

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

                    release_capture(input_capture, session, activated, pos, &barriers, &pos_for_barrier_id).await?;

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

fn activation_edge_position(
    activated: &Activated,
    pos: Position,
    barriers: &[ICBarrier],
    routes: &HashMap<BarrierID, Position>,
) -> f64 {
    let Some((x, y)) = activated
        .cursor_position()
        .filter(|(x, y)| x.is_finite() && y.is_finite())
    else {
        return 0.5;
    };
    let Some((min_x, min_y, max_x, max_y)) =
        release_barrier(activated, pos, barriers, routes).and_then(|barrier| barrier.zone_bounds)
    else {
        return 0.5;
    };
    // Bounds store the last pixel; normalize against the full logical extent,
    // matching the exclusive screen bounds used by X11 and Windows capture.
    let (coordinate, min, max) = match pos {
        Position::Left | Position::Right => (f64::from(y), min_y, max_y),
        Position::Top | Position::Bottom => (f64::from(x), min_x, max_x),
    };
    ((coordinate - min) / (max - min + 1.)).clamp(0., 1.)
}

fn release_cursor_position(
    cursor: Option<(f32, f32)>,
    current_pos: Position,
    barrier: Option<ICBarrier>,
) -> Option<(f64, f64)> {
    let (x, y) = cursor.filter(|(x, y)| x.is_finite() && y.is_finite())?;
    let barrier = barrier?;
    let (min_x, min_y, max_x, max_y) = barrier.zone_bounds?;
    let (x, y) = closest_point_on_segment(barrier.position, (x, y));
    let (dx, dy) = match current_pos {
        // offset cursor position to not enter again immediately
        Position::Left => (1., 0.),
        Position::Right => (-1., 0.),
        Position::Top => (0., 1.),
        Position::Bottom => (0., -1.),
    };
    // Keep corner overshoot and one-pixel zones inside the owning region.
    Some(((x + dx).clamp(min_x, max_x), (y + dy).clamp(min_y, max_y)))
}

fn release_barrier(
    activated: &Activated,
    pos: Position,
    barriers: &[ICBarrier],
    routes: &HashMap<BarrierID, Position>,
) -> Option<ICBarrier> {
    if let Some(ActivatedBarrier::Barrier(id)) = activated.barrier_id() {
        if routes.get(&id) == Some(&pos) {
            return barriers
                .iter()
                .find(|barrier| barrier.barrier_id == id)
                .copied();
        }
    }
    let cursor = activated
        .cursor_position()
        .filter(|(x, y)| x.is_finite() && y.is_finite())?;
    barriers
        .iter()
        .filter(|b| routes.get(&b.barrier_id) == Some(&pos))
        .map(|b| (b, distance_to_segment_squared(b.position, cursor)))
        .min_by(|(_, a), (_, b)| a.total_cmp(b))
        .map(|(b, _)| *b)
}

fn activation_release_options(
    activated: &Activated,
    pos: Position,
    barriers: &[ICBarrier],
    routes: &HashMap<BarrierID, Position>,
) -> ReleaseOptions {
    ReleaseOptions::default()
        .set_activation_id(activated.activation_id())
        .set_cursor_position(release_cursor_position(
            activated.cursor_position(),
            pos,
            release_barrier(activated, pos, barriers, routes),
        ))
}

async fn release_capture(
    input_capture: &InputCapture,
    session: &Session<InputCapture>,
    activated: Activated,
    current_pos: Position,
    barriers: &[ICBarrier],
    routes: &HashMap<BarrierID, Position>,
) -> Result<(), CaptureError> {
    if let Some(activation_id) = activated.activation_id() {
        log::debug!("releasing input capture {activation_id}");
    }
    let release_options = activation_release_options(&activated, current_pos, barriers, routes);
    input_capture.release(session, release_options).await?;
    Ok(())
}

fn find_corresponding_client(
    barriers: &[ICBarrier],
    pos: (f32, f32),
) -> Result<BarrierID, CaptureError> {
    if !pos.0.is_finite() || !pos.1.is_finite() {
        return Err(io::Error::other("libei activation has nonfinite cursor coordinates").into());
    }
    barriers
        .iter()
        .map(|barrier| {
            (
                barrier.barrier_id,
                distance_to_segment_squared(barrier.position, pos),
            )
        })
        .min_by(|(_, a), (_, b)| a.total_cmp(b))
        .map(|(id, _)| id)
        .ok_or_else(|| io::Error::other("libei activation has no matching barrier geometry").into())
}

fn distance_to_segment_squared(segment: (i32, i32, i32, i32), pos: (f32, f32)) -> f64 {
    let (nearest_x, nearest_y) = closest_point_on_segment(segment, pos);
    let (offset_x, offset_y) = (f64::from(pos.0) - nearest_x, f64::from(pos.1) - nearest_y);
    offset_x * offset_x + offset_y * offset_y
}

fn closest_point_on_segment(segment: (i32, i32, i32, i32), pos: (f32, f32)) -> (f64, f64) {
    // Preserve integer endpoint precision and avoid f32 overflow for finite cursors.
    let (x1, y1, x2, y2) = segment;
    let (x1, y1, x2, y2) = (f64::from(x1), f64::from(y1), f64::from(x2), f64::from(y2));
    let (x, y) = (f64::from(pos.0), f64::from(pos.1));
    // Portal barriers are axis aligned; preserve an in-range cursor exactly.
    if x1 == x2 {
        return (x1, y.clamp(y1.min(y2), y1.max(y2)));
    }
    if y1 == y2 {
        return (x.clamp(x1.min(x2), x1.max(x2)), y1);
    }
    let (dx, dy) = (x2 - x1, y2 - y1);
    let length_squared = dx * dx + dy * dy;
    let projection = if length_squared == 0. {
        0.
    } else {
        (((x - x1) * dx + (y - y1) * dy) / length_squared).clamp(0., 1.)
    };
    (x1 + projection * dx, y1 + projection * dy)
}

async fn send_capture_event(
    sender: &Sender<(Position, CaptureEvent)>,
    pos: Position,
    event: CaptureEvent,
) -> Result<(), CaptureError> {
    sender.send((pos, event)).await.map_err(|_| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "libei capture event receiver closed",
        )
        .into()
    })
}

async fn send_activation_event(
    sender: &Sender<(Position, CaptureEvent)>,
    pos: Position,
    t: f64,
    cancel_session: &CancellationToken,
) -> Result<bool, CaptureError> {
    tokio::select! {
        biased;
        _ = cancel_session.cancelled() => Ok(false),
        result = send_capture_event(sender, pos, CaptureEvent::Begin(t)) => {
            result?;
            Ok(true)
        }
    }
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
                    send_capture_event(event_tx, pos, CaptureEvent::Input(event)).await?;
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

    struct TaskResourceProbe(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for TaskResourceProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn detached_capture_task_keeps_resource_until_cleanup_finishes() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let resource = Arc::new(TaskResourceProbe(drops.clone()));
                let weak = Arc::downgrade(&resource);
                let task_weak = weak.clone();
                let cancel = CancellationToken::new();
                let task_cancel = cancel.clone();
                let (started, started_wait) = tokio::sync::oneshot::channel();
                let (release_cleanup, cleanup_wait) = tokio::sync::oneshot::channel();
                let (observed, observed_wait) = tokio::sync::oneshot::channel();
                let task = CaptureTaskCompletion::spawn_owned(
                    resource.clone(),
                    move |_resource| async move {
                        task_cancel.cancelled().await;
                        started.send(()).unwrap();
                        cleanup_wait.await.unwrap();
                        // Observe ownership without ever dereferencing a potentially freed pointer.
                        observed.send(task_weak.upgrade().is_some()).unwrap();
                        Ok(())
                    },
                );
                drop(resource);
                cancel.cancel();
                drop(task); // JoinHandle drop detaches the task, as frontend Drop does.
                started_wait.await.unwrap();
                let alive_during_cleanup = weak.upgrade().is_some();
                let drops_during_cleanup = drops.load(std::sync::atomic::Ordering::SeqCst);
                release_cleanup.send(()).unwrap();
                let alive_at_cleanup_end = observed_wait.await.unwrap();
                tokio::task::yield_now().await;
                assert!(
                    alive_during_cleanup,
                    "frontend freed resource while cleanup was waiting"
                );
                assert_eq!(drops_during_cleanup, 0);
                assert!(alive_at_cleanup_end);
                assert!(weak.upgrade().is_none());
                assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_owned_join_wait_keeps_resource_and_task_result() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let resource = Arc::new(TaskResourceProbe(drops.clone()));
                let weak = Arc::downgrade(&resource);
                let (release_cleanup, cleanup_wait) = tokio::sync::oneshot::channel();
                let mut task = CaptureTaskCompletion::spawn_owned(
                    resource.clone(),
                    move |_resource| async move {
                        cleanup_wait.await.unwrap();
                        Err(CaptureError::EndOfStream)
                    },
                );
                drop(resource);
                assert!(task.join().now_or_never().is_none());
                assert!(weak.upgrade().is_some());
                assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
                release_cleanup.send(()).unwrap();
                assert!(matches!(task.join().await, Err(CaptureError::EndOfStream)));
                assert!(weak.upgrade().is_none());
                assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
                assert!(task.join().await.is_ok());
            })
            .await;
    }

    fn barrier(id: u32, position: (i32, i32, i32, i32)) -> ICBarrier {
        ICBarrier::new(NonZeroU32::new(id).unwrap(), position)
    }

    #[test]
    fn nearest_barrier_respects_segment_endpoints() {
        let barriers = [
            barrier(1, (0, 0, 0, 100)),
            barrier(2, (-100, 500, 100, 500)),
        ];
        assert_eq!(
            find_corresponding_client(&barriers, (0., 500.))
                .unwrap()
                .get(),
            2
        );
    }

    #[test]
    fn nearest_barrier_preserves_fractional_distance() {
        let barriers = [barrier(1, (0, 0, 0, 100)), barrier(2, (1, 0, 1, 100))];
        assert_eq!(
            find_corresponding_client(&barriers, (0.75, 50.))
                .unwrap()
                .get(),
            2
        );
    }

    #[test]
    fn nearest_barrier_handles_point_segments() {
        let barriers = [barrier(1, (0, 0, 0, 0)), barrier(2, (100, 0, 100, 100))];
        assert_eq!(
            find_corresponding_client(&barriers, (100., 50.))
                .unwrap()
                .get(),
            2
        );
    }

    #[test]
    fn nearest_barrier_preserves_integer_endpoint_precision() {
        let barriers = [
            barrier(1, (16_777_217, 0, 16_777_217, 100)),
            barrier(2, (16_777_216, 0, 16_777_216, 100)),
        ];
        assert_eq!(
            find_corresponding_client(&barriers, (16_777_216., 50.))
                .unwrap()
                .get(),
            2
        );
    }

    #[test]
    fn nearest_barrier_reports_empty_geometry() {
        assert!(find_corresponding_client(&[], (0., 0.)).is_err());
    }

    #[test]
    fn nearest_barrier_rejects_nonfinite_cursor_coordinates() {
        let barriers = [barrier(1, (0, 0, 0, 100))];
        for pos in [(f32::NAN, 0.), (0., f32::INFINITY), (f32::NEG_INFINITY, 0.)] {
            assert!(find_corresponding_client(&barriers, pos).is_err());
        }
    }

    #[test]
    fn nearest_barrier_keeps_first_exact_tie() {
        let barriers = [barrier(5, (0, 0, 0, 100)), barrier(6, (0, 0, 100, 0))];
        assert_eq!(
            find_corresponding_client(&barriers, (0., 0.))
                .unwrap()
                .get(),
            5
        );
    }

    #[test]
    fn barrier_distance_handles_reversed_endpoints_and_large_finite_coordinates() {
        assert_eq!(distance_to_segment_squared((0, 100, 0, 0), (1., 50.)), 1.);
        assert_eq!(distance_to_segment_squared((0, 0, 0, 100), (1., 101.)), 2.);
        assert_eq!(distance_to_segment_squared((2, 3, 2, 3), (5., 7.)), 25.);
        let distance = distance_to_segment_squared(
            (i32::MIN, i32::MAX, i32::MAX, i32::MIN),
            (f32::MAX, f32::MIN),
        );
        assert!(distance.is_finite() && distance > 0.);
    }

    #[test]
    fn rejected_barriers_do_not_participate_in_fallback_or_routes() {
        let rejected = barrier(1, (0, 0, 0, 100));
        let accepted = barrier(2, (1, 0, 1, 100));
        let routes = HashMap::from([
            (rejected.barrier_id, Position::Left),
            (accepted.barrier_id, Position::Right),
        ]);
        let (barriers, routes) =
            accepted_barriers(vec![rejected, accepted], routes, &[rejected.barrier_id]).unwrap();
        assert_eq!(
            find_corresponding_client(&barriers, (0., 50.)).unwrap(),
            accepted.barrier_id
        );
        assert!(!routes.contains_key(&rejected.barrier_id));
        assert_eq!(routes.get(&accepted.barrier_id), Some(&Position::Right));
    }

    #[test]
    fn all_rejected_barriers_report_setup_failure() {
        let rejected = barrier(1, (0, 0, 0, 100));
        let routes = HashMap::from([(rejected.barrier_id, Position::Left)]);
        assert!(accepted_barriers(vec![rejected], routes, &[rejected.barrier_id]).is_err());
        assert!(accepted_barriers(vec![], HashMap::new(), &[]).is_err());
    }

    #[test]
    fn partial_barrier_rejection_preserves_order_and_ignores_unknown_duplicates() {
        let first = barrier(1, (0, 0, 0, 100));
        let rejected = barrier(2, (0, 0, 100, 0));
        let last = barrier(3, (100, 0, 100, 100));
        let routes = HashMap::from([
            (first.barrier_id, Position::Left),
            (rejected.barrier_id, Position::Top),
            (last.barrier_id, Position::Right),
        ]);
        let unknown = NonZeroU32::new(99).unwrap();
        let (barriers, routes) = accepted_barriers(
            vec![first, rejected, last],
            routes,
            &[unknown, rejected.barrier_id, rejected.barrier_id],
        )
        .unwrap();
        assert_eq!(
            barriers.iter().map(|b| b.barrier_id).collect::<Vec<_>>(),
            vec![first.barrier_id, last.barrier_id]
        );
        assert_eq!(routes.len(), 2);
        assert_eq!(routes.get(&first.barrier_id), Some(&Position::Left));
        assert_eq!(routes.get(&last.barrier_id), Some(&Position::Right));
    }

    #[test]
    fn accepted_barrier_response_retains_all_requested_routes() {
        let first = barrier(1, (0, 0, 0, 100));
        let last = barrier(2, (100, 0, 100, 100));
        let original = HashMap::from([
            (first.barrier_id, Position::Left),
            (last.barrier_id, Position::Right),
        ]);
        let (barriers, routes) =
            accepted_barriers(vec![first, last], original.clone(), &[]).unwrap();
        assert_eq!(routes, original);
        assert_eq!(
            barriers.iter().map(|b| b.barrier_id).collect::<Vec<_>>(),
            vec![first.barrier_id, last.barrier_id]
        );
    }

    fn activation_fixture(path: &str, id: Option<u32>, cursor: Option<(f32, f32)>) -> Activated {
        use ashpd::zvariant::{LE, ObjectPath, Value, serialized::Context};
        let path = ObjectPath::try_from(path).unwrap();
        let mut options = HashMap::<&str, Value<'_>>::new();
        options.insert("activation_id", Value::from(7u32));
        if let Some(id) = id {
            options.insert("barrier_id", Value::from(id));
        }
        if let Some(cursor) = cursor {
            options.insert("cursor_position", Value::from(cursor));
        }
        let data = ashpd::zvariant::to_bytes(Context::new_dbus(LE, 0), &(path, options)).unwrap();
        data.deserialize::<Activated>().unwrap().0
    }

    #[test]
    fn foreign_activation_with_reused_barrier_id_is_ignored() {
        let barrier = barrier(1, (0, 0, 0, 100));
        let routes = HashMap::from([(barrier.barrier_id, Position::Left)]);
        let activation = activation_fixture("/session/old", Some(1), Some((0., 50.)));
        assert_eq!(
            activation_position(&activation, "/session/current", &[barrier], &routes).unwrap(),
            None
        );
    }

    #[test]
    fn foreign_activation_does_not_enter_current_geometry_fallback() {
        let barrier = barrier(1, (0, 0, 0, 100));
        let routes = HashMap::from([(barrier.barrier_id, Position::Left)]);
        let activation = activation_fixture("/session/other", None, None);
        assert_eq!(
            activation_position(&activation, "/session/current", &[barrier], &routes).unwrap(),
            None
        );
    }

    #[test]
    fn current_activation_retains_explicit_and_geometry_routing() {
        let barrier = barrier(1, (0, 0, 0, 100));
        let routes = HashMap::from([(barrier.barrier_id, Position::Left)]);
        for id in [Some(1), Some(99), None] {
            let activation = activation_fixture("/session/current", id, Some((0., 50.)));
            assert_eq!(
                activation_position(&activation, "/session/current", &[barrier], &routes).unwrap(),
                Some(Position::Left)
            );
        }
    }

    #[test]
    fn current_fallback_without_cursor_reports_error() {
        let barrier = barrier(1, (0, 0, 0, 100));
        let routes = HashMap::from([(barrier.barrier_id, Position::Left)]);
        for id in [None, Some(99)] {
            let activation = activation_fixture("/session/current", id, None);
            assert!(
                activation_position(&activation, "/session/current", &[barrier], &routes).is_err()
            );
        }
    }

    #[test]
    fn current_explicit_activation_rejects_nonfinite_coordinates() {
        let barrier = barrier(1, (0, 0, 0, 100));
        let routes = HashMap::from([(barrier.barrier_id, Position::Left)]);
        let activation = activation_fixture("/session/current", Some(1), Some((f32::NAN, 50.)));
        assert!(activation_position(&activation, "/session/current", &[barrier], &routes).is_err());
    }

    #[test]
    fn release_without_usable_cursor_omits_suggestion() {
        for cursor in [None, Some((f32::NAN, 0.)), Some((0., f32::INFINITY))] {
            assert_eq!(release_cursor_position(cursor, Position::Left, None), None);
        }
    }

    #[test]
    fn explicit_activation_without_cursor_keeps_known_route() {
        let barrier = barrier(1, (0, 0, 0, 100));
        let routes = HashMap::from([(barrier.barrier_id, Position::Left)]);
        let activation = activation_fixture("/session/current", Some(1), None);
        assert_eq!(
            activation_position(&activation, "/session/current", &[barrier], &routes).unwrap(),
            Some(Position::Left)
        );
    }

    fn decoded_release_options(
        activated: &Activated,
        pos: Position,
    ) -> HashMap<String, ashpd::zvariant::OwnedValue> {
        use ashpd::zvariant::{LE, serialized::Context};
        let (x, y) = match pos {
            Position::Left => (10, 0),
            Position::Right => (-90, 0),
            Position::Top => (0, 20),
            Position::Bottom => (0, -80),
        };
        let region = region_fixture(100, 100, x, y);
        let id = NonZeroU32::new(1).unwrap();
        let barrier = ICBarrier::for_region(id, &region, pos).unwrap();
        let options =
            activation_release_options(activated, pos, &[barrier], &HashMap::from([(id, pos)]));
        let data = ashpd::zvariant::to_bytes(Context::new_dbus(LE, 0), &options).unwrap();
        data.deserialize().unwrap().0
    }

    #[test]
    fn release_options_without_cursor_keep_activation_id() {
        let activation = activation_fixture("/session/current", Some(1), None);
        let options = decoded_release_options(&activation, Position::Left);
        assert_eq!(options["activation_id"].downcast_ref::<u32>().unwrap(), 7);
        assert!(!options.contains_key("cursor_position"));
    }

    #[test]
    fn release_options_preserve_finite_inward_offsets() {
        let activation = activation_fixture("/session/current", Some(1), Some((10., 20.)));
        for (edge, expected) in [
            (Position::Left, (11., 20.)),
            (Position::Right, (9., 20.)),
            (Position::Top, (10., 21.)),
            (Position::Bottom, (10., 19.)),
        ] {
            let options = decoded_release_options(&activation, edge);
            assert_eq!(
                options["cursor_position"]
                    .downcast_ref::<(f64, f64)>()
                    .unwrap(),
                expected
            );
            assert_eq!(options["activation_id"].downcast_ref::<u32>().unwrap(), 7);
        }
    }

    #[test]
    fn fallback_without_position_route_reports_error() {
        let barrier = barrier(1, (0, 0, 0, 100));
        let activation = activation_fixture("/session/current", None, Some((0., 50.)));
        assert!(
            activation_position(&activation, "/session/current", &[barrier], &HashMap::new())
                .is_err()
        );
    }

    fn region_fixture(width: u32, height: u32, x: i32, y: i32) -> Region {
        use ashpd::zvariant::{LE, serialized::Context};
        let data =
            ashpd::zvariant::to_bytes(Context::new_dbus(LE, 0), &(width, height, x, y)).unwrap();
        data.deserialize().unwrap().0
    }

    fn assert_overshoot_release(edge: Position, cursor: (f32, f32), expected: (f64, f64)) {
        let region = region_fixture(100, 80, 10, -20);
        let barrier = ICBarrier::for_region(NonZeroU32::new(1).unwrap(), &region, edge).unwrap();
        assert_eq!(
            release_cursor_position(Some(cursor), edge, Some(barrier)),
            Some(expected)
        );
    }

    #[test]
    fn release_overshoot_left_projects_into_zone() {
        assert_overshoot_release(Position::Left, (-50., 10.), (11., 10.));
    }
    #[test]
    fn release_overshoot_right_projects_into_zone() {
        assert_overshoot_release(Position::Right, (200., 10.), (109., 10.));
    }
    #[test]
    fn release_overshoot_top_projects_into_zone() {
        assert_overshoot_release(Position::Top, (50., -80.), (50., -19.));
    }
    #[test]
    fn release_overshoot_bottom_projects_into_zone() {
        assert_overshoot_release(Position::Bottom, (50., 200.), (50., 59.));
    }

    #[test]
    fn release_corner_overshoot_stays_inside_region() {
        for (edge, cursor, expected) in [
            (Position::Left, (-50., 500.), (11., 59.)),
            (Position::Right, (200., -100.), (109., -20.)),
            (Position::Top, (-50., -80.), (10., -19.)),
            (Position::Bottom, (200., 200.), (109., 59.)),
        ] {
            assert_overshoot_release(edge, cursor, expected);
        }
    }

    #[test]
    fn release_one_pixel_region_stays_inside_region() {
        let region = region_fixture(1, 1, 10, -20);
        for edge in [
            Position::Left,
            Position::Right,
            Position::Top,
            Position::Bottom,
        ] {
            let barrier =
                ICBarrier::for_region(NonZeroU32::new(1).unwrap(), &region, edge).unwrap();
            assert_eq!(
                release_cursor_position(Some((-100., 200.)), edge, Some(barrier)),
                Some((10., -20.))
            );
        }
    }

    #[test]
    fn release_uses_reported_region_and_falls_back_within_route() {
        use ashpd::zvariant::{LE, serialized::Context};
        let first = ICBarrier::for_region(
            NonZeroU32::new(1).unwrap(),
            &region_fixture(100, 80, 10, -20),
            Position::Left,
        )
        .unwrap();
        let second = ICBarrier::for_region(
            NonZeroU32::new(2).unwrap(),
            &region_fixture(100, 80, 400, -20),
            Position::Left,
        )
        .unwrap();
        let routes = HashMap::from([
            (first.barrier_id, Position::Left),
            (second.barrier_id, Position::Left),
        ]);
        for (id, expected) in [
            (Some(2), (401., 10.)),
            (Some(99), (11., 10.)),
            (None, (11., 10.)),
        ] {
            let activation = activation_fixture("/session/current", id, Some((-50., 10.)));
            let options =
                activation_release_options(&activation, Position::Left, &[first, second], &routes);
            let data = ashpd::zvariant::to_bytes(Context::new_dbus(LE, 0), &options).unwrap();
            let (options, _) = data
                .deserialize::<HashMap<String, ashpd::zvariant::OwnedValue>>()
                .unwrap();
            assert_eq!(
                options["cursor_position"]
                    .downcast_ref::<(f64, f64)>()
                    .unwrap(),
                expected
            );
            assert_eq!(options["activation_id"].downcast_ref::<u32>().unwrap(), 7);
        }
    }

    #[test]
    fn release_without_region_geometry_omits_suggestion() {
        let cursor = Some((10., 20.));
        assert_eq!(release_cursor_position(cursor, Position::Left, None), None);
        assert_eq!(
            release_cursor_position(cursor, Position::Left, Some(barrier(1, (0, 0, 0, 100)))),
            None
        );
        assert!(
            ICBarrier::for_region(
                NonZeroU32::new(1).unwrap(),
                &region_fixture(0, 0, 0, 0),
                Position::Left
            )
            .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_session_closes_on_idle_shutdown_and_setup_error() {
        for result in [Ok(()), Err(CaptureError::EndOfStream)] {
            let expected_error = result.is_err();
            let closed = Rc::new(Cell::new(0));
            let observed = closed.clone();
            let result = finish_pending_session(result, Some(7), move |session| async move {
                assert_eq!(session, 7);
                observed.set(observed.get() + 1);
                Ok(())
            })
            .await;
            assert_eq!(
                closed.get(),
                1,
                "unused first session must be explicitly closed"
            );
            assert_eq!(result.is_err(), expected_error);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn consumed_session_is_not_closed_again_at_capture_exit() {
        let result = finish_pending_session(Ok(()), None::<()>, |_| async {
            panic!("active branch already owns and closes the session");
        })
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_session_close_failure_preserves_original_result() {
        for original in [Ok(()), Err(CaptureError::EndOfStream)] {
            let expected_error = original.is_err();
            let result = finish_pending_session(original, Some(()), |_| async {
                Err(io::Error::other("controlled close failure").into())
            })
            .await;
            if expected_error {
                assert!(matches!(result, Err(CaptureError::EndOfStream)));
            } else {
                assert!(result.is_ok());
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_session_waits_for_close_and_retains_resource() {
        let session = Arc::new(());
        let weak = Arc::downgrade(&session);
        let (release, wait) = tokio::sync::oneshot::channel();
        let mut finish = Box::pin(finish_pending_session(
            Err(CaptureError::EndOfStream),
            Some(session),
            |session| async move {
                wait.await.unwrap();
                assert_eq!(Arc::strong_count(&session), 1);
                Ok(())
            },
        ));
        assert!((&mut finish).now_or_never().is_none());
        assert!(weak.upgrade().is_some());
        release.send(()).unwrap();
        assert!(matches!(finish.await, Err(CaptureError::EndOfStream)));
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capture_send_closed_begin_reports_error() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        assert!(
            send_activation_event(&sender, Position::Left, 0.5, &CancellationToken::new())
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capture_send_closed_input_reports_error() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let input = CaptureEvent::Input(Event::Pointer(input_event::PointerEvent::Motion {
            time: 0,
            dx: 1.,
            dy: 2.,
        }));
        assert!(
            send_capture_event(&sender, Position::Left, input)
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capture_send_full_begin_is_interruptible() {
        let (sender, mut receiver) = mpsc::channel(1);
        send_capture_event(&sender, Position::Right, CaptureEvent::Begin(0.25))
            .await
            .unwrap();
        let cancel = CancellationToken::new();
        let mut send = Box::pin(send_activation_event(&sender, Position::Left, 0.5, &cancel));
        assert!((&mut send).now_or_never().is_none());
        cancel.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), send).await;
        assert!(
            matches!(result, Ok(Ok(false))),
            "shutdown must interrupt full Begin channel: {result:?}"
        );
        assert_eq!(
            receiver.recv().await.unwrap(),
            (Position::Right, CaptureEvent::Begin(0.25))
        );
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capture_send_requested_shutdown_precedes_ready_begin() {
        let (sender, mut receiver) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(
            !send_activation_event(&sender, Position::Left, 0.5, &cancel)
                .await
                .unwrap()
        );
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capture_send_healthy_begin_and_input_keep_order() {
        let (sender, mut receiver) = mpsc::channel(2);
        assert!(
            send_activation_event(&sender, Position::Left, 0.5, &CancellationToken::new())
                .await
                .unwrap()
        );
        let input = CaptureEvent::Input(Event::Pointer(input_event::PointerEvent::Motion {
            time: 1,
            dx: 2.,
            dy: 3.,
        }));
        send_capture_event(&sender, Position::Left, input.clone())
            .await
            .unwrap();
        assert_eq!(
            receiver.recv().await.unwrap(),
            (Position::Left, CaptureEvent::Begin(0.5))
        );
        assert_eq!(receiver.recv().await.unwrap(), (Position::Left, input));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capture_send_error_wakes_session_cleanup_without_panic() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let cancel_session = CancellationToken::new();
        let cancel_ei = CancellationToken::new();
        let cleaned = Cell::new(false);
        let handler = run_ei_handler(
            send_capture_event(&sender, Position::Left, CaptureEvent::Begin(0.5)),
            cancel_session.clone(),
            cancel_ei.clone(),
        );
        let session = cancel_sibling_on_completion(
            async {
                cancel_session.cancelled().await;
                cleaned.set(true);
                Ok(())
            },
            cancel_ei,
        );
        let (handler, session) =
            tokio::time::timeout(std::time::Duration::from_millis(100), async {
                tokio::join!(handler, session)
            })
            .await
            .expect("send failure must wake joined session cleanup");
        assert!(
            matches!(handler, Err(CaptureError::Io(ref error)) if error.kind() == io::ErrorKind::BrokenPipe)
        );
        assert!(session.is_ok());
        assert!(cleaned.get());
    }

    #[test]
    fn activation_edge_position_preserves_crossed_fraction() {
        let region = region_fixture(100, 80, -200, -20);
        for (edge, cursor) in [
            (Position::Left, (-250., 0.)),
            (Position::Right, (-50., 0.)),
            (Position::Top, (-175., -80.)),
            (Position::Bottom, (-175., 200.)),
        ] {
            let barrier =
                ICBarrier::for_region(NonZeroU32::new(1).unwrap(), &region, edge).unwrap();
            let activation = activation_fixture("/session/current", Some(1), Some(cursor));
            assert_eq!(
                activation_edge_position(
                    &activation,
                    edge,
                    &[barrier],
                    &HashMap::from([(barrier.barrier_id, edge)])
                ),
                0.25
            );
        }
    }

    #[test]
    fn activation_edge_position_clamps_cross_axis_overshoot() {
        let region = region_fixture(100, 80, 10, -20);
        for (edge, low, high) in [
            (Position::Left, (-50., -100.), (-50., 200.)),
            (Position::Right, (200., -100.), (200., 200.)),
            (Position::Top, (-50., -80.), (200., -80.)),
            (Position::Bottom, (-50., 200.), (200., 200.)),
        ] {
            let barrier =
                ICBarrier::for_region(NonZeroU32::new(1).unwrap(), &region, edge).unwrap();
            let routes = HashMap::from([(barrier.barrier_id, edge)]);
            for (cursor, expected) in [(low, 0.), (high, 1.)] {
                let activation = activation_fixture("/session/current", Some(1), Some(cursor));
                assert_eq!(
                    activation_edge_position(&activation, edge, &[barrier], &routes),
                    expected
                );
            }
        }
    }

    #[test]
    fn activation_edge_position_uses_owning_region_and_geometry_fallback() {
        let first = ICBarrier::for_region(
            NonZeroU32::new(1).unwrap(),
            &region_fixture(100, 100, 0, 0),
            Position::Left,
        )
        .unwrap();
        let second = ICBarrier::for_region(
            NonZeroU32::new(2).unwrap(),
            &region_fixture(100, 200, 0, 100),
            Position::Left,
        )
        .unwrap();
        let routes = HashMap::from([
            (first.barrier_id, Position::Left),
            (second.barrier_id, Position::Left),
        ]);
        for (id, cursor) in [
            (Some(2), (-30., 150.)),
            (Some(99), (-30., 25.)),
            (None, (-30., 25.)),
        ] {
            let activation = activation_fixture("/session/current", id, Some(cursor));
            assert_eq!(
                activation_edge_position(&activation, Position::Left, &[first, second], &routes),
                0.25
            );
        }
    }

    #[test]
    fn activation_edge_position_missing_metadata_keeps_midpoint() {
        let edge = Position::Left;
        let barrier = ICBarrier::for_region(
            NonZeroU32::new(1).unwrap(),
            &region_fixture(100, 100, 0, 0),
            edge,
        )
        .unwrap();
        let routes = HashMap::from([(barrier.barrier_id, edge)]);
        for cursor in [None, Some((f32::NAN, 25.)), Some((0., f32::INFINITY))] {
            let activation = activation_fixture("/session/current", Some(1), cursor);
            assert_eq!(
                activation_edge_position(&activation, edge, &[barrier], &routes),
                0.5
            );
        }
        let activation = activation_fixture("/session/current", Some(1), Some((0., 25.)));
        assert_eq!(
            activation_edge_position(&activation, edge, &[], &routes),
            0.5
        );
        let no_bounds = super::ICBarrier::new(barrier.barrier_id, barrier.position);
        assert_eq!(
            activation_edge_position(&activation, edge, &[no_bounds], &routes),
            0.5
        );
    }

    #[test]
    fn activation_edge_position_preserves_fractional_coordinate() {
        let edge = Position::Top;
        let barrier = ICBarrier::for_region(
            NonZeroU32::new(1).unwrap(),
            &region_fixture(100, 80, -20, 10),
            edge,
        )
        .unwrap();
        let activation = activation_fixture("/session/current", Some(1), Some((5.5, -50.)));
        assert_eq!(
            activation_edge_position(
                &activation,
                edge,
                &[barrier],
                &HashMap::from([(barrier.barrier_id, edge)])
            ),
            0.255
        );
    }

    #[test]
    fn activation_edge_position_one_pixel_extent_is_finite() {
        let edge = Position::Left;
        let barrier = ICBarrier::for_region(
            NonZeroU32::new(1).unwrap(),
            &region_fixture(1, 1, 10, -20),
            edge,
        )
        .unwrap();
        let routes = HashMap::from([(barrier.barrier_id, edge)]);
        for (cursor, expected) in [((10., -20.), 0.), ((10., -19.), 1.)] {
            let activation = activation_fixture("/session/current", Some(1), Some(cursor));
            assert_eq!(
                activation_edge_position(&activation, edge, &[barrier], &routes),
                expected
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn activation_edge_position_reaches_begin_channel() {
        let edge = Position::Left;
        let barrier = ICBarrier::for_region(
            NonZeroU32::new(1).unwrap(),
            &region_fixture(100, 80, 10, -20),
            edge,
        )
        .unwrap();
        let activation = activation_fixture("/session/current", Some(1), Some((-50., 0.)));
        let t = activation_edge_position(
            &activation,
            edge,
            &[barrier],
            &HashMap::from([(barrier.barrier_id, edge)]),
        );
        let (sender, mut receiver) = mpsc::channel(1);
        assert!(
            send_activation_event(&sender, edge, t, &CancellationToken::new())
                .await
                .unwrap()
        );
        assert_eq!(
            receiver.recv().await.unwrap(),
            (edge, CaptureEvent::Begin(0.25))
        );
    }

    #[test]
    fn barrier_region_rejects_empty_dimensions() {
        for (width, height) in [(0, 80), (100, 0), (0, 0)] {
            for edge in [
                Position::Left,
                Position::Right,
                Position::Top,
                Position::Bottom,
            ] {
                assert!(pos_to_barrier(&region_fixture(width, height, 10, -20), edge).is_err());
            }
        }
    }

    #[test]
    fn barrier_region_rejects_unrepresentable_endpoints() {
        let region = region_fixture(2, 2, i32::MAX, i32::MAX);
        for edge in [
            Position::Left,
            Position::Right,
            Position::Top,
            Position::Bottom,
        ] {
            assert!(pos_to_barrier(&region, edge).is_err());
        }
    }

    #[test]
    fn barrier_region_preserves_large_unsigned_extent() {
        let region = region_fixture(u32::MAX, 2, i32::MIN, 0);
        assert_eq!(
            pos_to_barrier(&region, Position::Top).unwrap(),
            (i32::MIN, 0, i32::MAX - 1, 0)
        );
        assert_eq!(
            pos_to_barrier(&region, Position::Right).unwrap(),
            (i32::MAX, 0, i32::MAX, 1)
        );
    }

    #[test]
    fn barrier_region_preserves_normal_edges_and_limit_coordinates() {
        let region = region_fixture(100, 80, 10, -20);
        for (edge, expected) in [
            (Position::Left, (10, -20, 10, 59)),
            (Position::Right, (110, -20, 110, 59)),
            (Position::Top, (10, -20, 109, -20)),
            (Position::Bottom, (10, 60, 109, 60)),
        ] {
            assert_eq!(pos_to_barrier(&region, edge).unwrap(), expected);
        }
        let corner = region_fixture(1, 1, i32::MAX, i32::MAX);
        assert_eq!(
            pos_to_barrier(&corner, Position::Left).unwrap(),
            (i32::MAX, i32::MAX, i32::MAX, i32::MAX)
        );
        assert!(pos_to_barrier(&corner, Position::Right).is_err());
        assert!(pos_to_barrier(&corner, Position::Bottom).is_err());
    }

    #[test]
    fn barrier_region_validation_reaches_factory_and_selection() {
        use ashpd::zvariant::{LE, Value, serialized::Context};
        let invalid = region_fixture(0, 80, 10, -20);
        assert!(
            ICBarrier::for_region(NonZeroU32::new(1).unwrap(), &invalid, Position::Left).is_err()
        );
        let options = HashMap::from([
            ("zones", Value::from(vec![(0u32, 80u32, 10i32, -20i32)])),
            ("zone_set", Value::from(1u32)),
        ]);
        let data = ashpd::zvariant::to_bytes(Context::new_dbus(LE, 0), &options).unwrap();
        let (zones, _) = data.deserialize::<Zones>().unwrap();
        assert!(
            select_barriers(&zones, &[Position::Left], &mut NonZeroU32::new(1).unwrap()).is_err()
        );
    }

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
