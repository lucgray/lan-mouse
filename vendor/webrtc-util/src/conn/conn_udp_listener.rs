use core::sync::atomic::Ordering;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Weak;

use portable_atomic::{AtomicBool, AtomicU8};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, mpsc, watch};

use super::*;
use crate::Buffer;
use crate::error::Error;

const RECEIVE_MTU: usize = 8192;
const DEFAULT_LISTEN_BACKLOG: usize = 128; // same as Linux default
// Bound raw datagrams before DTLS/application admission. Overflow is dropped
// by the existing read loop, preserving UDP semantics and other peer dispatch.
const SESSION_BUFFER_PACKETS: usize = 256;
const SESSION_BUFFER_BYTES: usize = 256 * 1024;
const MAX_UDP_SESSIONS: usize = 128;
const PENDING_SESSION_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(2);
const SESSION_QUEUED: u8 = 0;
const SESSION_ACCEPTED: u8 = 1;
const SESSION_CLOSED: u8 = 2;

pub type AcceptFilterFn =
    Box<dyn (Fn(&[u8]) -> Pin<Box<dyn Future<Output = bool> + Send + 'static>>) + Send + Sync>;

type AcceptDoneCh = (mpsc::Receiver<Arc<UdpConn>>, watch::Receiver<()>);

/// listener is used in the [DTLS](https://github.com/webrtc-rs/dtls) and
/// [SCTP](https://github.com/webrtc-rs/sctp) transport to provide a connection-oriented
/// listener over a UDP.
struct ListenerImpl {
    pconn: Arc<dyn Conn + Send + Sync>,
    accepting: Arc<AtomicBool>,
    accept_ch_tx: Arc<Mutex<Option<mpsc::Sender<Arc<UdpConn>>>>>,
    done_ch_tx: Arc<Mutex<Option<watch::Sender<()>>>>,
    ch_rx: Arc<Mutex<AcceptDoneCh>>,
    conns: Arc<Mutex<HashMap<String, Arc<UdpConn>>>>,
}

#[async_trait]
impl Listener for ListenerImpl {
    /// accept waits for and returns the next connection to the listener.
    async fn accept(&self) -> Result<(Arc<dyn Conn + Send + Sync>, SocketAddr)> {
        let (accept_ch_rx, done_ch_rx) = &mut *self.ch_rx.lock().await;

        loop {
            if !self.accepting.load(Ordering::SeqCst) {
                return Err(Error::ErrClosedListener);
            }
            tokio::select! {
                c = accept_ch_rx.recv() => {
                    if let Some(c) = c {
                        // Claim and expiry compete atomically. Never start a
                        // handshake for an entry already retired by housekeeping.
                        if c.mark_expired() {
                            let _ = c.close().await;
                            continue;
                        }
                        if c.state.compare_exchange(SESSION_QUEUED, SESSION_ACCEPTED,
                            Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                            let raddr = c.raddr;
                            return Ok((c, raddr));
                        }
                    } else { return Err(Error::ErrClosedListenerAcceptCh); }
                }
                _ = done_ch_rx.changed() => return Err(Error::ErrClosedListener),
            }
        }
    }

    /// close closes the listener.
    /// Any blocked Accept operations will be unblocked and return errors.
    async fn close(&self) -> Result<()> {
        if self.accepting.load(Ordering::SeqCst) {
            self.accepting.store(false, Ordering::SeqCst);
            {
                let mut done_ch = self.done_ch_tx.lock().await;
                done_ch.take();
            }
            {
                let mut accept_ch = self.accept_ch_tx.lock().await;
                accept_ch.take();
            }
        }

        Ok(())
    }

    /// Addr returns the listener's network address.
    async fn addr(&self) -> Result<SocketAddr> {
        self.pconn.local_addr()
    }
}

/// ListenConfig stores options for listening to an address.
#[derive(Default)]
pub struct ListenConfig {
    /// Backlog defines the maximum length of the queue of pending
    /// connections. It is equivalent of the backlog argument of
    /// POSIX listen function.
    /// If a connection request arrives when the queue is full,
    /// the request will be silently discarded, unlike TCP.
    /// Set zero to use default value 128 which is same as Linux default.
    pub backlog: usize,

    /// AcceptFilter determines whether the new conn should be made for
    /// the incoming packet. If not set, any packet creates new conn.
    pub accept_filter: Option<AcceptFilterFn>,
}

pub async fn listen<A: ToSocketAddrs>(laddr: A) -> Result<impl Listener> {
    ListenConfig::default().listen(laddr).await
}

impl ListenConfig {
    /// Listen creates a new listener based on the ListenConfig.
    pub async fn listen<A: ToSocketAddrs>(&mut self, laddr: A) -> Result<impl Listener> {
        if self.backlog == 0 {
            self.backlog = DEFAULT_LISTEN_BACKLOG;
        }

        let pconn = Arc::new(UdpSocket::bind(laddr).await?);
        let (accept_ch_tx, accept_ch_rx) = mpsc::channel(self.backlog);
        let (done_ch_tx, done_ch_rx) = watch::channel(());

        let l = ListenerImpl {
            pconn,
            accepting: Arc::new(AtomicBool::new(true)),
            accept_ch_tx: Arc::new(Mutex::new(Some(accept_ch_tx))),
            done_ch_tx: Arc::new(Mutex::new(Some(done_ch_tx))),
            ch_rx: Arc::new(Mutex::new((accept_ch_rx, done_ch_rx.clone()))),
            conns: Arc::new(Mutex::new(HashMap::new())),
        };

        let pconn = Arc::clone(&l.pconn);
        let accepting = Arc::clone(&l.accepting);
        let accept_filter = self.accept_filter.take();
        let accept_ch_tx = Arc::clone(&l.accept_ch_tx);
        let conns = Arc::clone(&l.conns);
        tokio::spawn(async move {
            ListenConfig::read_loop(
                done_ch_rx,
                pconn,
                accepting,
                accept_filter,
                accept_ch_tx,
                conns,
            )
            .await;
        });

        Ok(l)
    }

    /// read_loop has to tasks:
    /// 1. Dispatching incoming packets to the correct Conn.
    ///    It can therefore not be ended until all Conns are closed.
    /// 2. Creating a new Conn when receiving from a new remote.
    async fn read_loop(
        mut done_ch_rx: watch::Receiver<()>,
        pconn: Arc<dyn Conn + Send + Sync>,
        accepting: Arc<AtomicBool>,
        accept_filter: Option<AcceptFilterFn>,
        accept_ch_tx: Arc<Mutex<Option<mpsc::Sender<Arc<UdpConn>>>>>,
        conns: Arc<Mutex<HashMap<String, Arc<UdpConn>>>>,
    ) {
        let mut buf = vec![0u8; RECEIVE_MTU];
        let mut housekeeping = tokio::time::interval(tokio::time::Duration::from_secs(1));
        housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut listening = true;
        loop {
            if !listening && conns.lock().await.is_empty() {
                break;
            }
            tokio::select! {
                _ = done_ch_rx.changed(), if listening => {
                    listening = false;
                    // Listener close stops admission, not accepted transports.
                    // Retire queued entries now; continue dispatch until the
                    // final accepted connection closes its table entry.
                    let retired: Vec<_> = {
                        let sessions = conns.lock().await;
                        sessions.values().filter(|conn| conn.state.compare_exchange(
                            SESSION_QUEUED, SESSION_CLOSED,
                            Ordering::SeqCst, Ordering::SeqCst).is_ok()).cloned().collect()
                    };
                    for conn in retired { let _ = conn.close().await; }
                }
                _ = housekeeping.tick() => {
                    ListenConfig::expire_pending_sessions(&conns).await;
                }
                result = pconn.recv_from(&mut buf) => {
                    match result {
                        Ok((n, raddr)) => {
                            let udp_conn = match ListenConfig::get_udp_conn(
                                &pconn,
                                &accepting,
                                &accept_filter,
                                &accept_ch_tx,
                                &conns,
                                raddr,
                                &buf[..n],
                            )
                            .await
                            {
                                Ok(conn) => conn,
                                Err(_) => continue,
                            };

                            if let Some(conn) = udp_conn {
                                let _ = conn.buffer.write(&buf[..n]).await;
                            }
                        }
                        Err(err) => {
                            log::warn!("ListenConfig pconn.recv_from error: {}", err);
                            break;
                        }
                    };
                }
            }
        }
    }

    async fn expire_pending_sessions(conns: &Arc<Mutex<HashMap<String, Arc<UdpConn>>>>) {
        let retired: Vec<_> = {
            let sessions = conns.lock().await;
            sessions
                .values()
                .filter(|conn| conn.mark_expired())
                .cloned()
                .collect()
        };
        // Do not hold the map lock while close removes entries/frees buffers.
        for conn in retired {
            let _ = conn.close().await;
        }
    }

    async fn get_udp_conn(
        pconn: &Arc<dyn Conn + Send + Sync>,
        accepting: &Arc<AtomicBool>,
        accept_filter: &Option<AcceptFilterFn>,
        accept_ch_tx: &Arc<Mutex<Option<mpsc::Sender<Arc<UdpConn>>>>>,
        conns: &Arc<Mutex<HashMap<String, Arc<UdpConn>>>>,
        raddr: SocketAddr,
        buf: &[u8],
    ) -> Result<Option<Arc<UdpConn>>> {
        {
            let m = conns.lock().await;
            if let Some(conn) = m.get(raddr.to_string().as_str()) {
                return Ok(Some(conn.clone()));
            }
        }

        if !accepting.load(Ordering::SeqCst) {
            return Err(Error::ErrClosedListener);
        }

        if let Some(f) = accept_filter {
            if !(f(buf).await) {
                return Ok(None);
            }
        }

        let udp_conn = Arc::new(UdpConn::new(
            Arc::clone(pconn),
            raddr,
            Arc::downgrade(conns),
        ));
        {
            let accept_ch = accept_ch_tx.lock().await;
            if let Some(tx) = &*accept_ch {
                // Install before publishing: the receiver can close immediately
                // after try_send, and close must find its table entry.
                let mut sessions = conns.lock().await;
                if let Some(existing) = sessions.get(&raddr.to_string()) {
                    return Ok(Some(existing.clone()));
                }
                // Recheck after the async filter/locks: close may have stopped
                // admission since the first check, before queue retirement.
                if !accepting.load(Ordering::SeqCst) {
                    return Err(Error::ErrClosedListener);
                }
                if sessions.len() >= MAX_UDP_SESSIONS {
                    return Err(Error::ErrListenQueueExceeded);
                }
                sessions.insert(raddr.to_string(), Arc::clone(&udp_conn));
                if tx.try_send(Arc::clone(&udp_conn)).is_err() {
                    sessions.remove(&raddr.to_string());
                    return Err(Error::ErrListenQueueExceeded);
                }
            } else {
                return Err(Error::ErrClosedListenerAcceptCh);
            }
        }

        Ok(Some(udp_conn))
    }
}

/// UdpConn augments a connection-oriented connection over a UdpSocket
pub struct UdpConn {
    pconn: Arc<dyn Conn + Send + Sync>,
    raddr: SocketAddr,
    buffer: Buffer,
    state: AtomicU8,
    queued_at: tokio::time::Instant,
    sessions: Weak<Mutex<HashMap<String, Arc<UdpConn>>>>,
}

impl UdpConn {
    fn mark_expired(&self) -> bool {
        self.queued_at.elapsed() >= PENDING_SESSION_TIMEOUT
            && self
                .state
                .compare_exchange(
                    SESSION_QUEUED,
                    SESSION_CLOSED,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
    }

    fn new(
        pconn: Arc<dyn Conn + Send + Sync>,
        raddr: SocketAddr,
        sessions: Weak<Mutex<HashMap<String, Arc<UdpConn>>>>,
    ) -> Self {
        UdpConn {
            pconn,
            raddr,
            buffer: Buffer::new(SESSION_BUFFER_PACKETS, SESSION_BUFFER_BYTES),
            state: AtomicU8::new(SESSION_QUEUED),
            queued_at: tokio::time::Instant::now(),
            sessions,
        }
    }
}

#[async_trait]
impl Conn for UdpConn {
    async fn connect(&self, addr: SocketAddr) -> Result<()> {
        self.pconn.connect(addr).await
    }

    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        Ok(self.buffer.read(buf, None).await?)
    }

    async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let n = self.buffer.read(buf, None).await?;
        Ok((n, self.raddr))
    }

    async fn send(&self, buf: &[u8]) -> Result<usize> {
        if self.state.load(Ordering::SeqCst) == SESSION_CLOSED || self.buffer.is_closed().await {
            return Err(Error::ErrUseClosedNetworkConn);
        }
        self.pconn.send_to(buf, self.raddr).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> Result<usize> {
        if self.state.load(Ordering::SeqCst) == SESSION_CLOSED || self.buffer.is_closed().await {
            return Err(Error::ErrUseClosedNetworkConn);
        }
        self.pconn.send_to(buf, target).await
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        self.pconn.local_addr()
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        Some(self.raddr)
    }

    async fn close(&self) -> Result<()> {
        self.state.store(SESSION_CLOSED, Ordering::SeqCst);
        // Release queued allocation before freeing the table slot. Closing
        // sessions remain in the budget until their raw buffer is discarded.
        self.buffer.close_and_discard().await;
        // Remove only this generation. A repeated close must not remove a
        // newly accepted connection that reused the same source address.
        if let Some(sessions) = self.sessions.upgrade() {
            let mut sessions = sessions.lock().await;
            let key = self.raddr.to_string();
            if sessions
                .get(&key)
                .is_some_and(|current| std::ptr::eq(current.as_ref(), self))
            {
                sessions.remove(&key);
            }
        }
        Ok(())
    }

    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
}

#[cfg(test)]
mod bounded_session_tests {
    use super::*;

    #[tokio::test]
    async fn session_packet_budget_preserves_fifo_and_recovers_after_drain() {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let conn = UdpConn::new(socket, "127.0.0.1:1".parse().unwrap(), Weak::new());
        for index in 0..256 {
            conn.buffer.write(&[index as u8]).await.unwrap();
        }
        assert_eq!(conn.buffer.write(&[99]).await, Err(Error::ErrBufferFull));
        let mut packet = [0u8; 1];
        assert_eq!(conn.recv(&mut packet).await.unwrap(), 1);
        assert_eq!(packet[0], 0);
        conn.buffer.write(&[99]).await.unwrap();
        for index in 1..256 {
            assert_eq!(conn.recv(&mut packet).await.unwrap(), 1);
            assert_eq!(packet[0], index as u8);
        }
        assert_eq!(conn.recv(&mut packet).await.unwrap(), 1);
        assert_eq!(packet[0], 99);
        conn.close().await.unwrap();
        assert!(conn.recv(&mut packet).await.is_err());
    }

    #[tokio::test]
    async fn session_byte_budget_counts_framing_and_keeps_other_session_available() {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let conn = UdpConn::new(socket.clone(), "127.0.0.1:1".parse().unwrap(), Weak::new());
        let other = UdpConn::new(socket, "127.0.0.1:2".parse().unwrap(), Weak::new());
        let packet = vec![7u8; 8190];
        for _ in 0..32 {
            conn.buffer.write(&packet).await.unwrap();
        }
        assert_eq!(conn.buffer.write(&[1]).await, Err(Error::ErrBufferFull));
        other.buffer.write(&[42]).await.unwrap();
        let mut received = [0u8; 1];
        assert_eq!(other.recv(&mut received).await.unwrap(), 1);
        assert_eq!(received[0], 42);
        let mut received = vec![0u8; 8190];
        assert_eq!(conn.recv(&mut received).await.unwrap(), packet.len());
        assert_eq!(received, packet);
        conn.buffer.write(&packet).await.unwrap();
        conn.close().await.unwrap();
        other.close().await.unwrap();
    }
    #[tokio::test]
    async fn listener_close_keeps_accepted_peer_receiving_and_retires_queue() {
        let listener = listen("127.0.0.1:0").await.unwrap();
        let addr = listener.addr().await.unwrap();
        let active = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        active.send_to(b"first", addr).await.unwrap();
        let (conn, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 32];
        conn.recv(&mut buf).await.unwrap();
        let queued = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        queued.send_to(b"queued", addr).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        listener.close().await.unwrap();
        active.send_to(b"after-close", addr).await.unwrap();
        let n = tokio::time::timeout(tokio::time::Duration::from_millis(250), conn.recv(&mut buf))
            .await
            .expect("accepted peer must keep receiving after listener close")
            .unwrap();
        assert_eq!(&buf[..n], b"after-close");
        assert!(listener.accept().await.is_err());
        conn.close().await.unwrap();
        listener.close().await.unwrap();
    }

    #[tokio::test]
    async fn closed_listener_retires_queue_and_dispatch_task_exits_after_last_peer() {
        let socket: Arc<dyn Conn + Send + Sync> =
            Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let accepted = Arc::new(UdpConn::new(
            socket.clone(),
            "127.0.0.1:1".parse().unwrap(),
            Arc::downgrade(&sessions),
        ));
        accepted.state.store(SESSION_ACCEPTED, Ordering::SeqCst);
        let queued = Arc::new(UdpConn::new(
            socket.clone(),
            "127.0.0.1:2".parse().unwrap(),
            Arc::downgrade(&sessions),
        ));
        queued.buffer.write(b"queued").await.unwrap();
        for conn in [&accepted, &queued] {
            sessions
                .lock()
                .await
                .insert(conn.raddr.to_string(), conn.clone());
        }
        let (done_tx, done_rx) = watch::channel(());
        let (accept_tx, _accept_rx) = mpsc::channel(128);
        let task = tokio::spawn(ListenConfig::read_loop(
            done_rx,
            socket,
            Arc::new(AtomicBool::new(false)),
            None,
            Arc::new(Mutex::new(Some(accept_tx))),
            sessions.clone(),
        ));
        drop(done_tx);
        tokio::time::timeout(tokio::time::Duration::from_millis(250), async {
            while sessions.lock().await.len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(queued.buffer.is_closed().await);
        assert!(queued.recv(&mut [0u8; 32]).await.is_err());
        assert!(!task.is_finished(), "accepted session still owns dispatch");
        accepted.close().await.unwrap();
        tokio::time::timeout(tokio::time::Duration::from_millis(1250), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn close_during_filter_prevents_late_session_publication() {
        let socket: Arc<dyn Conn + Send + Sync> =
            Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let accepting = Arc::new(AtomicBool::new(true));
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let (tx, mut rx) = mpsc::channel(128);
        let accepts = Arc::new(Mutex::new(Some(tx)));
        let filter_accepting = accepting.clone();
        let filter: Option<AcceptFilterFn> = Some(Box::new(move |_| {
            let accepting = filter_accepting.clone();
            Box::pin(async move {
                accepting.store(false, Ordering::SeqCst);
                true
            })
        }));
        assert!(matches!(
            ListenConfig::get_udp_conn(
                &socket,
                &accepting,
                &filter,
                &accepts,
                &sessions,
                "127.0.0.1:1".parse().unwrap(),
                b"hello"
            )
            .await,
            Err(Error::ErrClosedListener)
        ));
        assert!(sessions.lock().await.is_empty());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn session_table_budget_preserves_existing_peer_and_recovers_after_close() {
        let socket: Arc<dyn Conn + Send + Sync> =
            Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let accepting = Arc::new(AtomicBool::new(true));
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _rx) = mpsc::channel(256);
        let accepts = Arc::new(Mutex::new(Some(tx)));
        for port in 10000..10128 {
            ListenConfig::get_udp_conn(
                &socket,
                &accepting,
                &None,
                &accepts,
                &sessions,
                format!("127.0.0.1:{port}").parse().unwrap(),
                b"hello",
            )
            .await
            .unwrap()
            .unwrap();
        }
        let extra: SocketAddr = "127.0.0.1:10128".parse().unwrap();
        assert!(matches!(
            ListenConfig::get_udp_conn(
                &socket, &accepting, &None, &accepts, &sessions, extra, b"hello"
            )
            .await,
            Err(Error::ErrListenQueueExceeded)
        ));
        assert_eq!(sessions.lock().await.len(), 128);
        let first_addr: SocketAddr = "127.0.0.1:10000".parse().unwrap();
        let first = sessions
            .lock()
            .await
            .get(&first_addr.to_string())
            .unwrap()
            .clone();
        let existing = ListenConfig::get_udp_conn(
            &socket, &accepting, &None, &accepts, &sessions, first_addr, b"hello",
        )
        .await
        .unwrap()
        .unwrap();
        assert!(Arc::ptr_eq(&first, &existing));
        first.close().await.unwrap();
        ListenConfig::get_udp_conn(
            &socket, &accepting, &None, &accepts, &sessions, extra, b"hello",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(sessions.lock().await.len(), 128);
    }

    #[tokio::test]
    async fn session_close_discards_queued_raw_data() {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let conn = UdpConn::new(socket, "127.0.0.1:1".parse().unwrap(), Weak::new());
        conn.buffer.write(&[9]).await.unwrap();
        conn.close().await.unwrap();
        assert!(conn.recv(&mut [0u8; 1]).await.is_err());
    }
    #[tokio::test]
    async fn pending_expiry_keeps_accepted_and_fresh_sessions_and_prevents_late_accept() {
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let mut queued = UdpConn::new(
            socket.clone(),
            "127.0.0.1:1".parse().unwrap(),
            Arc::downgrade(&sessions),
        );
        queued.queued_at -= tokio::time::Duration::from_secs(3);
        let queued = Arc::new(queued);
        let mut accepted = UdpConn::new(
            socket.clone(),
            "127.0.0.1:2".parse().unwrap(),
            Arc::downgrade(&sessions),
        );
        accepted.queued_at -= tokio::time::Duration::from_secs(3);
        accepted.state.store(SESSION_ACCEPTED, Ordering::SeqCst);
        let accepted = Arc::new(accepted);
        let fresh = Arc::new(UdpConn::new(
            socket,
            "127.0.0.1:3".parse().unwrap(),
            Arc::downgrade(&sessions),
        ));
        for conn in [&queued, &accepted, &fresh] {
            conn.buffer.write(&[7]).await.unwrap();
            sessions
                .lock()
                .await
                .insert(conn.raddr.to_string(), conn.clone());
        }
        ListenConfig::expire_pending_sessions(&sessions).await;
        assert_eq!(sessions.lock().await.len(), 2);
        assert!(
            !sessions
                .lock()
                .await
                .contains_key(&queued.raddr.to_string())
        );
        assert!(
            queued
                .state
                .compare_exchange(
                    SESSION_QUEUED,
                    SESSION_ACCEPTED,
                    Ordering::SeqCst,
                    Ordering::SeqCst
                )
                .is_err()
        );
        assert!(queued.recv(&mut [0u8; 1]).await.is_err());
        assert_eq!(accepted.recv(&mut [0u8; 1]).await.unwrap(), 1);
        assert_eq!(fresh.recv(&mut [0u8; 1]).await.unwrap(), 1);
        accepted.close().await.unwrap();
        fresh.close().await.unwrap();
    }
}
