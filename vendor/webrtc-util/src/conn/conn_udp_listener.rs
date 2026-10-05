use core::sync::atomic::Ordering;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Weak;

use portable_atomic::AtomicBool;
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

        tokio::select! {
            c = accept_ch_rx.recv() =>{
                if let Some(c) = c{
                    let raddr = c.raddr;
                    Ok((c, raddr))
                }else{
                    Err(Error::ErrClosedListenerAcceptCh)
                }
            }
            _ = done_ch_rx.changed() =>  Err(Error::ErrClosedListener),
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

        loop {
            tokio::select! {
                _ = done_ch_rx.changed() => {
                    break;
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
    sessions: Weak<Mutex<HashMap<String, Arc<UdpConn>>>>,
}

impl UdpConn {
    fn new(
        pconn: Arc<dyn Conn + Send + Sync>,
        raddr: SocketAddr,
        sessions: Weak<Mutex<HashMap<String, Arc<UdpConn>>>>,
    ) -> Self {
        UdpConn {
            pconn,
            raddr,
            buffer: Buffer::new(SESSION_BUFFER_PACKETS, SESSION_BUFFER_BYTES),
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
        if self.buffer.is_closed().await {
            return Err(Error::ErrUseClosedNetworkConn);
        }
        self.pconn.send_to(buf, self.raddr).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> Result<usize> {
        if self.buffer.is_closed().await {
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
        // Wake pending reads and reject future writes without closing the
        // shared UDP socket used by other peers.
        self.buffer.close().await;
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
}
