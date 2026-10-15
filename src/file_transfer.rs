//! Files use a separate TLS stream. The authenticated DTLS peer supplies its
//! one-use certificate and capability; paths and whole file buffers stay local.
use input_event::ClipboardFile;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, Read, Write},
    net::{IpAddr, Shutdown, SocketAddr, TcpStream},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use webrtc_util::Conn;

pub const VERSION: u8 = 1;
pub const OFFER_TYPE: u8 = lan_mouse_proto::EventType::FileStreamOffer as u8;
const SERVER_NAME: &str = "lan-mouse-files";

#[derive(Clone)]
pub struct Offer {
    port: u16,
    token: [u8; 32],
    certificate: Vec<u8>,
    pub total: u64,
    pub count: u32,
}

impl Offer {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![OFFER_TYPE, VERSION];
        out.extend_from_slice(&self.port.to_be_bytes());
        out.extend_from_slice(&self.total.to_be_bytes());
        out.extend_from_slice(&self.count.to_be_bytes());
        out.extend_from_slice(&self.token);
        out.extend_from_slice(&self.certificate);
        out
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() <= 48
            || bytes.len() > lan_mouse_proto::MAX_CLIPBOARD_SIZE
            || bytes[0] != OFFER_TYPE
            || bytes[1] != VERSION
        {
            return Err(invalid("invalid file stream offer"));
        }
        let port = u16::from_be_bytes(bytes[2..4].try_into().unwrap());
        let count = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        if port == 0 || count == 0 {
            return Err(invalid("empty file stream offer"));
        }
        Ok(Self {
            port,
            total: u64::from_be_bytes(bytes[4..12].try_into().unwrap()),
            count,
            token: bytes[16..48].try_into().unwrap(),
            certificate: bytes[48..].to_vec(),
        })
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
fn plain_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
        _ => ip,
    }
}

struct CloseOnDrop(TcpStream);
impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if let Err(error) = self.0.shutdown(Shutdown::Both) {
            if error.kind() != io::ErrorKind::NotConnected {
                log::debug!("file stream shutdown: {error}");
            }
        }
    }
}

fn configure_socket(socket: &TcpStream) -> io::Result<()> {
    socket.set_nonblocking(false)?;
    socket.set_read_timeout(Some(lan_mouse_proto::REASSEMBLY_TIMEOUT))?;
    socket.set_write_timeout(Some(lan_mouse_proto::REASSEMBLY_TIMEOUT))?;
    Ok(())
}

async fn relay<T>(
    mut worker: JoinHandle<io::Result<T>>,
    mut updates: watch::Receiver<(u64, u64)>,
    progress: Option<&local_channel::mpsc::Sender<(u64, u64)>>,
) -> io::Result<T> {
    loop {
        tokio::select! {
            result = &mut worker => return result.map_err(other)?,
            result = updates.changed() => {
                if result.is_err() { return worker.await.map_err(other)?; }
                if let Some(progress) = progress {
                    if progress.send(*updates.borrow_and_update()).is_err() { log::debug!("file progress receiver closed"); }
                }
            }
        }
    }
}

/// Sends only the offer on the existing authenticated input connection.
pub async fn send(
    conn: Arc<dyn Conn + Send + Sync>,
    peer: IpAddr,
    files: Vec<ClipboardFile>,
    port: u16,
    progress: Option<&local_channel::mpsc::Sender<(u64, u64)>>,
) -> io::Result<()> {
    let peer = plain_ip(peer);
    let total = files.iter().try_fold(0u64, |sum, file| {
        sum.checked_add(file.size)
            .ok_or_else(|| invalid("file sizes overflow"))
    })?;
    let count = u32::try_from(files.len()).map_err(other)?;
    if count == 0 {
        return Err(invalid("no files selected"));
    }
    let (certificate, key, token) = tokio::task::spawn_blocking(|| {
        let generated =
            rcgen::generate_simple_self_signed(vec![SERVER_NAME.into()]).map_err(other)?;
        let key = generated.key_pair.serialize_der();
        let token: [u8; 32] = Sha256::digest(&key).into();
        Ok::<_, io::Error>((generated.cert.der().clone(), key, token))
    })
    .await
    .map_err(other)??;
    let bind = SocketAddr::new(
        if peer.is_ipv4() {
            std::net::Ipv4Addr::UNSPECIFIED.into()
        } else {
            std::net::Ipv6Addr::UNSPECIFIED.into()
        },
        port,
    );
    let listener = TcpListener::bind(bind).await?;
    let offer = Offer {
        port: listener.local_addr()?.port(),
        token,
        certificate: certificate.as_ref().to_vec(),
        total,
        count,
    };
    conn.send(&offer.encode()).await.map_err(other)?;
    log::info!("file stream offered: files={count} bytes={total} version={VERSION}");
    let (socket, addr) = tokio::time::timeout(
        crate::connect::DEFAULT_CONNECTION_TIMEOUT,
        listener.accept(),
    )
    .await
    .map_err(other)??;
    if plain_ip(addr.ip()) != peer {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "file peer address mismatch",
        ));
    }
    let socket = socket.into_std()?;
    configure_socket(&socket)?;
    let _close = CloseOnDrop(socket.try_clone()?);
    let (updates, rx) = watch::channel((0, total));
    let worker = tokio::task::spawn_blocking(move || {
        let config =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(other)?
                .with_no_client_auth()
                .with_single_cert(vec![certificate], PrivatePkcs8KeyDer::from(key).into())
                .map_err(other)?;
        let mut stream = StreamOwned::new(
            ServerConnection::new(Arc::new(config)).map_err(other)?,
            socket,
        );
        let mut received = [0u8; 32];
        stream.read_exact(&mut received)?;
        if received != token {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid file capability",
            ));
        }
        send_body(&mut stream, &files, &updates)?;
        let mut ack = [0u8; 1];
        stream.read_exact(&mut ack)?;
        if ack != [VERSION] {
            return Err(invalid("file receiver did not commit transfer"));
        }
        log::info!("file stream confirmed: files={count} bytes={total}");
        Ok(())
    });
    relay(worker, rx, progress).await
}

pub async fn receive(
    offer: Offer,
    peer: IpAddr,
    directory: PathBuf,
    progress: Option<&local_channel::mpsc::Sender<(u64, u64)>>,
) -> io::Result<Vec<ClipboardFile>> {
    let addr = SocketAddr::new(plain_ip(peer), offer.port);
    let socket = tokio::time::timeout(
        crate::connect::DEFAULT_CONNECTION_TIMEOUT,
        tokio::net::TcpStream::connect(addr),
    )
    .await
    .map_err(other)??
    .into_std()?;
    configure_socket(&socket)?;
    let _close = CloseOnDrop(socket.try_clone()?);
    let (updates, rx) = watch::channel((0, offer.total));
    let worker = tokio::task::spawn_blocking(move || {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(offer.certificate))
            .map_err(other)?;
        let config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(other)?
                .with_root_certificates(roots)
                .with_no_client_auth();
        let mut stream = StreamOwned::new(
            ClientConnection::new(
                Arc::new(config),
                ServerName::try_from(SERVER_NAME).map_err(other)?,
            )
            .map_err(other)?,
            socket,
        );
        stream.write_all(&offer.token)?;
        stream.flush()?;
        let files = receive_body(&mut stream, &directory, offer.count, offer.total, &updates)?;
        stream.write_all(&[VERSION])?;
        stream.flush()?;
        log::info!(
            "file stream received: files={} bytes={}",
            files.len(),
            offer.total
        );
        Ok(files)
    });
    relay(worker, rx, progress).await
}

struct Progress {
    updates: watch::Sender<(u64, u64)>,
    done: u64,
    total: u64,
    reported: u64,
}
impl Progress {
    fn add(&mut self, bytes: usize) {
        self.done += bytes as u64;
        if self.done - self.reported >= (self.total / 100).max(1) || self.done == self.total {
            self.reported = self.done;
            self.updates.send_replace((self.done, self.total));
        }
    }
}

struct HashedReader<'a, R> {
    inner: R,
    hash: Sha256,
    progress: &'a mut Progress,
    paced: usize,
    since: Instant,
}
impl<R: Read> Read for HashedReader<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(out)?;
        self.hash.update(&out[..n]);
        self.progress.add(n);
        self.paced += n;
        if self.paced
            >= crate::connect::CLIPBOARD_SEND_BURST * lan_mouse_proto::CLIPBOARD_FRAGMENT_PAYLOAD
        {
            std::thread::sleep(
                crate::connect::CLIPBOARD_BURST_PAUSE.saturating_sub(self.since.elapsed()),
            );
            self.since = Instant::now();
            self.paced = 0;
        }
        Ok(n)
    }
}

fn send_body(
    stream: &mut (impl Read + Write),
    files: &[ClipboardFile],
    updates: &watch::Sender<(u64, u64)>,
) -> io::Result<()> {
    let total = files.iter().map(|file| file.size).sum();
    let mut progress = Progress {
        updates: updates.clone(),
        done: 0,
        total,
        reported: 0,
    };
    for file in files {
        validate_name(&file.name)?;
        let mut source = File::open(&file.path)?;
        let before = source.metadata()?;
        if before.len() != file.size {
            return Err(invalid("source size changed since copy"));
        }
        stream.write_all(&(file.name.len() as u32).to_be_bytes())?;
        stream.write_all(file.name.as_bytes())?;
        stream.write_all(&file.size.to_be_bytes())?;
        let mut reader = HashedReader {
            inner: (&mut source).take(file.size),
            hash: Sha256::new(),
            progress: &mut progress,
            paced: 0,
            since: Instant::now(),
        };
        let copied = io::copy(&mut reader, stream)?;
        let digest = reader.hash.finalize();
        if copied != file.size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source file shortened",
            ));
        }
        let after = source.metadata()?;
        if before.len() != after.len() || before.modified()? != after.modified()? {
            return Err(invalid("source file changed during transfer"));
        }
        stream.write_all(&digest)?;
    }
    stream.flush()
}

fn validate_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains(['/', '\\', ':', '\0'])
        || name.len() > lan_mouse_proto::MAX_CLIPBOARD_SIZE
    {
        return Err(invalid("invalid file name"));
    }
    Ok(())
}

fn receive_body(
    stream: &mut impl Read,
    directory: &Path,
    count: u32,
    total: u64,
    updates: &watch::Sender<(u64, u64)>,
) -> io::Result<Vec<ClipboardFile>> {
    std::fs::create_dir_all(directory)?;
    let mut staged = Vec::new();
    let mut remaining = total;
    let mut progress = Progress {
        updates: updates.clone(),
        done: 0,
        total,
        reported: 0,
    };
    for _ in 0..count {
        let mut length = [0u8; 4];
        stream.read_exact(&mut length)?;
        let length = u32::from_be_bytes(length) as usize;
        if length == 0 || length > lan_mouse_proto::MAX_CLIPBOARD_SIZE {
            return Err(invalid("invalid file name length"));
        }
        let mut name = vec![0; length];
        stream.read_exact(&mut name)?;
        let name = String::from_utf8(name).map_err(other)?;
        validate_name(&name)?;
        let mut size = [0u8; 8];
        stream.read_exact(&mut size)?;
        let size = u64::from_be_bytes(size);
        remaining = remaining
            .checked_sub(size)
            .ok_or_else(|| invalid("file sizes exceed offer"))?;
        let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
        let mut reader = HashedReader {
            inner: (&mut *stream).take(size),
            hash: Sha256::new(),
            progress: &mut progress,
            paced: 0,
            since: Instant::now(),
        };
        let written = io::copy(&mut reader, temporary.as_file_mut())?;
        let digest = reader.hash.finalize();
        if written != size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "file transfer interrupted",
            ));
        }
        let mut expected = [0u8; 32];
        stream.read_exact(&mut expected)?;
        if digest[..] != expected {
            return Err(invalid("file checksum mismatch"));
        }
        temporary.as_file_mut().sync_all()?;
        staged.push((name, size, temporary));
    }
    if remaining != 0 {
        return Err(invalid("file sizes differ from offer"));
    }
    let mut files: Vec<ClipboardFile> = Vec::new();
    for (name, size, temporary) in staged {
        let path = input_emulation::clipboard::unique_download_path(directory, &name);
        if let Err(error) = temporary.persist_noclobber(&path) {
            for file in files {
                if let Err(error) = std::fs::remove_file(file.path) {
                    log::warn!("failed to remove incomplete file batch: {error}");
                }
            }
            return Err(error.error);
        }
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| invalid("invalid destination file name"))?
            .to_owned();
        files.push(ClipboardFile { name, path, size });
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn body(name: &str, bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(name.len() as u32).to_be_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        out.extend_from_slice(bytes);
        out.extend_from_slice(&Sha256::digest(bytes));
        out
    }

    #[test]
    fn streaming_offer_retains_64_bit_lengths() {
        let offer = Offer {
            port: 4242,
            token: [7; 32],
            certificate: vec![1, 2, 3],
            count: 3,
            total: u32::MAX as u64 + 123,
        };
        let decoded = Offer::decode(&offer.encode()).unwrap();
        assert_eq!(decoded.total, offer.total);
        assert_eq!(decoded.count, 3);
        let mut bad = offer.encode();
        bad[1] = VERSION + 1;
        assert!(Offer::decode(&bad).is_err());
    }

    #[test]
    fn streaming_receiver_checks_hash_before_exposing_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut data = body("a.txt", b"hello");
        *data.last_mut().unwrap() ^= 1;
        let (updates, _) = watch::channel((0, 5));
        assert!(receive_body(&mut Cursor::new(data), dir.path(), 1, 5, &updates).is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn streaming_interrupted_batch_cleans_up_and_preserves_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"existing").unwrap();
        let data = body("a.txt", b"first");
        let (updates, _) = watch::channel((0, 10));
        assert!(receive_body(&mut Cursor::new(data), dir.path(), 2, 10, &updates).is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        assert_eq!(
            std::fs::read(dir.path().join("a.txt")).unwrap(),
            b"existing"
        );
    }

    #[test]
    fn streaming_receiver_renames_without_overwriting() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"existing").unwrap();
        let (updates, _) = watch::channel((0, 5));
        let files = receive_body(
            &mut Cursor::new(body("a.txt", b"hello")),
            dir.path(),
            1,
            5,
            &updates,
        )
        .unwrap();
        assert_eq!(files[0].name, "a (1).txt");
        assert_eq!(std::fs::read(&files[0].path).unwrap(), b"hello");
        assert_eq!(
            std::fs::read(dir.path().join("a.txt")).unwrap(),
            b"existing"
        );
    }

    #[test]
    fn streaming_receiver_rejects_paths_and_inconsistent_sizes() {
        let dir = tempfile::tempdir().unwrap();
        let (updates, _) = watch::channel((0, 4));
        for name in ["../a", "..\\a", "C:a", "", ".", ".."] {
            assert!(validate_name(name).is_err());
        }
        assert!(
            receive_body(
                &mut Cursor::new(body("a", b"hello")),
                dir.path(),
                1,
                4,
                &updates
            )
            .is_err()
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn streaming_source_detects_size_change_without_loading_contents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a");
        std::fs::write(&path, b"small").unwrap();
        let (updates, _) = watch::channel((0, u32::MAX as u64 + 1));
        let file = ClipboardFile {
            name: "a".into(),
            path,
            size: u32::MAX as u64 + 1,
        };
        assert!(send_body(&mut Cursor::new(Vec::new()), &[file], &updates).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn streaming_tls_transfer_keeps_input_channel_responsive() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let source = tempfile::tempdir().unwrap();
                let destination = tempfile::tempdir().unwrap();
                let path = source.path().join("data.bin");
                let data = vec![0x5a; 2 * 1024 * 1024];
                std::fs::write(&path, &data).unwrap();
                let left = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
                let right = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
                left.connect(right.local_addr().unwrap()).await.unwrap();
                right.connect(left.local_addr().unwrap()).await.unwrap();
                let peer = right.local_addr().unwrap().ip();
                let send_conn: Arc<dyn Conn + Send + Sync> = left.clone();
                // The production outgoing DTLS socket also exposes no remote_addr.
                assert!(send_conn.remote_addr().is_none());
                let task = tokio::task::spawn_local(async move {
                    send(
                        send_conn,
                        peer,
                        vec![ClipboardFile {
                            name: "data.bin".into(),
                            path,
                            size: 2 * 1024 * 1024,
                        }],
                        0,
                        None,
                    )
                    .await
                });
                let mut buf = vec![0; lan_mouse_proto::MAX_CLIPBOARD_SIZE];
                let n = right.recv(&mut buf).await.unwrap();
                let offer = Offer::decode(&buf[..n]).unwrap();
                let target = destination.path().to_path_buf();
                let receive_task = tokio::task::spawn_local(async move {
                    receive(offer, "127.0.0.1".parse().unwrap(), target, None).await
                });
                for _ in 0..10 {
                    left.send(b"input").await.unwrap();
                    let n = tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        right.recv(&mut buf),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                    assert_eq!(&buf[..n], b"input");
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                task.await.unwrap().unwrap();
                let files = receive_task.await.unwrap().unwrap();
                assert_eq!(std::fs::read(&files[0].path).unwrap(), data);
            })
            .await;
    }
}
