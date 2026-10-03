use lan_mouse_ipc::ClientHandle;
use local_channel::mpsc::{Receiver, Sender, channel};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    future::Future,
    io,
    net::{IpAddr, ToSocketAddrs},
    rc::Rc,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{
    sync::{Notify, Semaphore},
    task::{AbortHandle, JoinHandle, JoinSet, spawn_local},
};
use tokio_util::sync::CancellationToken;

const DNS_TIMEOUT: Duration = Duration::from_secs(5);
static LOOKUP_SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
type Revisions = Rc<RefCell<HashMap<ClientHandle, u64>>>;
type Pending = Rc<RefCell<HashMap<ClientHandle, Option<(String, u64)>>>>;

pub(crate) struct DnsResolver {
    cancellation_token: CancellationToken,
    task: Option<JoinHandle<()>>,
    pending: Pending,
    ready: Rc<Notify>,
    current: Revisions,
    next_revision: Cell<u64>,
    event_rx: Receiver<DnsEvent>,
}

pub(crate) enum DnsEvent {
    Resolving(ClientHandle, u64),
    Resolved(ClientHandle, u64, String, io::Result<Vec<IpAddr>>),
}

type LookupResult = (ClientHandle, u64, String, io::Result<Vec<IpAddr>>);
struct DnsTask {
    pending: Pending,
    ready: Rc<Notify>,
    current: Revisions,
    event_tx: Sender<DnsEvent>,
    cancellation_token: CancellationToken,
    active: HashMap<ClientHandle, (u64, AbortHandle)>,
    tasks: JoinSet<LookupResult>,
}

impl DnsResolver {
    pub(crate) fn new() -> io::Result<Self> {
        Ok(Self::with_lookup(resolve_hostname, DNS_TIMEOUT))
    }

    fn with_lookup<F, Fut>(lookup: F, timeout: Duration) -> Self
    where
        F: Fn(String) -> Fut + Clone + 'static,
        Fut: Future<Output = io::Result<Vec<IpAddr>>> + 'static,
    {
        let (event_tx, event_rx) = channel();
        let cancellation_token = CancellationToken::new();
        let pending = Pending::default();
        let current = Revisions::default();
        let ready = Rc::new(Notify::new());
        let task = DnsTask {
            pending: pending.clone(),
            ready: ready.clone(),
            current: current.clone(),
            active: Default::default(),
            tasks: JoinSet::new(),
            event_tx,
            cancellation_token: cancellation_token.clone(),
        };
        Self {
            cancellation_token,
            pending,
            ready,
            current,
            next_revision: Cell::new(0),
            event_rx,
            task: Some(spawn_local(task.run(lookup, timeout))),
        }
    }

    pub(crate) fn resolve(&self, handle: ClientHandle, hostname: String) {
        let revision = self
            .next_revision
            .get()
            .checked_add(1)
            .expect("DNS revision space exhausted");
        self.next_revision.set(revision);
        self.current.borrow_mut().insert(handle, revision);
        // Coalesce requests before the actor runs instead of allocating an
        // unbounded hostname queue for repeated edits of the same device.
        self.pending
            .borrow_mut()
            .insert(handle, Some((hostname, revision)));
        self.ready.notify_one();
    }

    pub(crate) fn cancel(&self, handle: ClientHandle) {
        self.current.borrow_mut().remove(&handle);
        self.pending.borrow_mut().insert(handle, None);
        self.ready.notify_one();
    }

    pub(crate) fn is_current(&self, handle: ClientHandle, revision: u64) -> bool {
        self.current.borrow().get(&handle) == Some(&revision)
    }

    pub(crate) async fn event(&mut self) -> DnsEvent {
        self.event_rx.recv().await.expect("channel closed")
    }

    pub(crate) async fn terminate(&mut self) {
        self.cancellation_token.cancel();
        self.pending.borrow_mut().clear();
        self.current.borrow_mut().clear();
        if let Some(task) = self.task.take() {
            task.await.expect("DNS task join error");
        }
    }
}

impl Drop for DnsResolver {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl DnsTask {
    async fn run<F, Fut>(mut self, lookup: F, timeout: Duration)
    where
        F: Fn(String) -> Fut + Clone + 'static,
        Fut: Future<Output = io::Result<Vec<IpAddr>>> + 'static,
    {
        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => break,
                _ = self.ready.notified() => {
                    let requests: Vec<_> = self.pending.borrow_mut().drain().collect();
                    for (handle, request) in requests {
                        if let Some((_, task)) = self.active.remove(&handle) { task.abort(); }
                        let Some((hostname, revision)) = request else { continue; };
                        if self.current.borrow().get(&handle) != Some(&revision) { continue; }
                        self.event_tx.send(DnsEvent::Resolving(handle, revision)).expect("channel closed");
                        let lookup = lookup.clone();
                        let task = self.tasks.spawn_local(async move {
                            let result = tokio::time::timeout(timeout, lookup(hostname.clone())).await
                                .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "hostname lookup timed out")));
                            (handle, revision, hostname, result)
                        });
                        self.active.insert(handle, (revision, task));
                    }
                },
                result = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    match result.expect("lookup task") {
                        Ok((handle, revision, hostname, result)) => {
                            if self.active.get(&handle).is_some_and(|(r, _)| *r == revision) { self.active.remove(&handle); }
                            if self.current.borrow().get(&handle) == Some(&revision) {
                                self.event_tx.send(DnsEvent::Resolved(handle, revision, hostname, result)).expect("channel closed");
                            }
                        },
                        Err(error) => {
                            self.active.retain(|_, (_, task)| task.id() != error.id());
                            if !error.is_cancelled() { log::warn!("DNS task failed: {error}"); }
                        }
                    }
                }
            }
        }
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
}

/// Use the native system name resolver (NSS, hosts files, mDNS/Bonjour and DNS).
/// The slot is owned by the blocking call, so cancelling its async wrapper does
/// not release capacity while getaddrinfo is still running. Across resolver
/// instances at most four such calls can occupy the blocking pool at once.
async fn resolve_hostname(hostname: String) -> io::Result<Vec<IpAddr>> {
    let slots = LOOKUP_SLOTS
        .get_or_init(|| Arc::new(Semaphore::new(4)))
        .clone();
    with_lookup_slot(slots, move || {
        (hostname.as_str(), 0)
            .to_socket_addrs()
            .map(|addrs| addrs.map(|addr| addr.ip()).collect())
    })
    .await
}

async fn with_lookup_slot<F, T>(slots: Arc<Semaphore>, lookup: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let permit = slots.acquire_owned().await.map_err(io::Error::other)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        lookup()
    })
    .await
    .map_err(io::Error::other)?
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Dropped(Rc<Cell<usize>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[tokio::test]
    async fn cancelled_wrapper_does_not_release_running_system_call_slot() {
        let slots = Arc::new(Semaphore::new(1));
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let (started, start) = tokio::sync::oneshot::channel();
        let release = gate.clone();
        let first_slots = slots.clone();
        let first = tokio::spawn(with_lookup_slot(first_slots, move || {
            started.send(()).unwrap();
            let (lock, condition) = &*release;
            let mut ready = lock.lock().unwrap();
            while !*ready {
                let (next, timeout) = condition
                    .wait_timeout(ready, Duration::from_secs(2))
                    .unwrap();
                ready = next;
                if timeout.timed_out() {
                    return Err(io::ErrorKind::TimedOut.into());
                }
            }
            Ok(())
        }));
        start.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(slots.available_permits(), 0);
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let second_entered = entered.clone();
        let second = tokio::spawn(with_lookup_slot(slots.clone(), move || {
            second_entered.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }));
        tokio::task::yield_now().await;
        assert!(!entered.load(std::sync::atomic::Ordering::SeqCst));
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn pending_edits_coalesce_and_deleted_lookup_is_cancelled() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let dropped = Rc::new(Cell::new(0));
                let count = dropped.clone();
                let mut resolver = DnsResolver::with_lookup(
                    move |_| {
                        let guard = Dropped(count.clone());
                        async move {
                            let _guard = guard;
                            std::future::pending().await
                        }
                    },
                    Duration::from_secs(30),
                );
                for index in 0..100 {
                    resolver.resolve(1, format!("host-{index}"));
                }
                assert_eq!(resolver.pending.borrow().len(), 1);
                let DnsEvent::Resolving(handle, revision) = resolver.event().await else {
                    panic!("start event");
                };
                assert_eq!(handle, 1);
                assert!(resolver.is_current(1, revision));
                tokio::task::yield_now().await;
                resolver.cancel(1);
                assert!(!resolver.is_current(1, revision));
                tokio::time::timeout(Duration::from_secs(1), async {
                    while dropped.get() == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                resolver.terminate().await;
                assert!(resolver.current.borrow().is_empty());
            })
            .await;
    }

    #[tokio::test]
    async fn new_request_supersedes_old_result_and_timeout_is_reported() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let mut resolver = DnsResolver::with_lookup(
                    |hostname| async move {
                        if hostname == "slow" {
                            std::future::pending().await
                        } else {
                            Ok(vec!["127.0.0.1".parse().unwrap()])
                        }
                    },
                    Duration::from_millis(20),
                );
                resolver.resolve(1, "slow".into());
                let DnsEvent::Resolving(_, old) = resolver.event().await else {
                    panic!("start");
                };
                resolver.resolve(1, "fast".into());
                let DnsEvent::Resolving(_, new) = resolver.event().await else {
                    panic!("new start");
                };
                assert!(!resolver.is_current(1, old));
                assert!(resolver.is_current(1, new));
                let DnsEvent::Resolved(_, revision, hostname, result) = resolver.event().await
                else {
                    panic!("result");
                };
                assert_eq!(revision, new);
                assert_eq!(hostname, "fast");
                assert_eq!(
                    result.unwrap(),
                    vec!["127.0.0.1".parse::<IpAddr>().unwrap()]
                );
                resolver.resolve(2, "slow".into());
                let _ = resolver.event().await;
                let DnsEvent::Resolved(_, _, _, result) = resolver.event().await else {
                    panic!("timeout result");
                };
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
                resolver.terminate().await;
            })
            .await;
    }

    #[tokio::test]
    async fn dns_revision_is_independent_of_transport_target_changes() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let clients = crate::client::ClientManager::default();
                let handle = clients.add_client();
                clients.set_hostname(handle, Some("localhost".into()));
                let mut resolver = DnsResolver::new().unwrap();
                resolver.resolve(handle, "localhost".into());
                let DnsEvent::Resolving(_, revision) = resolver.event().await else {
                    panic!("start");
                };
                clients.set_resolving(handle, true);
                clients.set_port(handle, 1234);
                assert!(clients.get_state(handle).unwrap().1.resolving);
                assert!(resolver.is_current(handle, revision));
                let DnsEvent::Resolved(_, _, _, result) = resolver.event().await else {
                    panic!("result");
                };
                assert!(!result.unwrap().is_empty());
                resolver.cancel(handle);
                assert!(!resolver.is_current(handle, revision));
                resolver.terminate().await;
            })
            .await;
    }
}
