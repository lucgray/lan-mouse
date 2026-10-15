//! Isolated two-host probe. Never opens the native clipboard or injects OS input.
//! JSON config: mode(init/send/receive), certificate, peer_fingerprint,
//! control_addr, file_port, files, output, report, timeout_secs.
use input_event::{ClipboardFile, Event, KeyboardEvent, PointerEvent};
use lan_mouse::file_transfer::{self, Offer};
use lan_mouse_proto::{MAX_EVENT_SIZE, ProtoEvent};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Read,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use webrtc_dtls::{
    config::{ClientAuthType, Config, ExtendedMasterSecretType},
    conn::DTLSConn,
    crypto::Certificate,
};
use webrtc_util::{Conn, conn::Listener};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Deserialize)]
struct Probe {
    mode: String,
    certificate: PathBuf,
    report: PathBuf,
    #[serde(default)]
    peer_fingerprint: String,
    #[serde(default)]
    control_addr: Option<SocketAddr>,
    #[serde(default)]
    file_port: u16,
    #[serde(default)]
    files: Vec<PathBuf>,
    #[serde(default)]
    output: PathBuf,
    timeout_secs: u64,
}
fn fingerprint(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
async fn packet(conn: &Arc<dyn Conn + Send + Sync>, event: ProtoEvent) -> Result<()> {
    let (bytes, len): ([u8; MAX_EVENT_SIZE], usize) = event.into();
    conn.send(&bytes[..len]).await?;
    Ok(())
}
fn decode(bytes: &[u8]) -> Result<ProtoEvent> {
    let mut fixed = [0; MAX_EVENT_SIZE];
    if bytes.len() > fixed.len() {
        return Err("oversized input packet".into());
    }
    fixed[..bytes.len()].copy_from_slice(bytes);
    Ok(ProtoEvent::try_from(fixed)?)
}
fn hashes(files: Vec<ClipboardFile>) -> Result<Vec<serde_json::Value>> {
    files
        .into_iter()
        .map(|f| {
            let mut file = fs::File::open(&f.path)?;
            let mut hash = Sha256::new();
            let mut buf = [0; 65536];
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hash.update(&buf[..n]);
            }
            Ok(json!({"name":f.name,"size":f.size,"sha256":format!("{:x}",hash.finalize())}))
        })
        .collect()
}
async fn run(p: &Probe) -> Result<serde_json::Value> {
    if p.mode == "init" {
        let cert = Certificate::generate_self_signed(["ignored".into()])?;
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        use std::io::Write;
        options
            .open(&p.certificate)?
            .write_all(cert.serialize_pem().as_bytes())?;
        return Ok(json!({"fingerprint":fingerprint(cert.certificate[0].as_ref())}));
    }
    let cert = Certificate::from_pem(&fs::read_to_string(&p.certificate)?)?;
    let expected = p.peer_fingerprint.clone();
    let cfg = Config {
        certificates: vec![cert],
        insecure_skip_verify: true,
        server_name: "ignored".into(),
        client_auth: ClientAuthType::RequireAnyClientCert,
        extended_master_secret: ExtendedMasterSecretType::Require,
        verify_peer_certificate: Some(Arc::new(move |certs, _| {
            if certs.first().is_some_and(|c| fingerprint(c) == expected) {
                Ok(())
            } else {
                Err(webrtc_dtls::Error::ErrVerifyDataMismatch)
            }
        })),
        ..Default::default()
    };
    let addr = p.control_addr.ok_or("control_addr required")?;
    let mut listener = None;
    let conn: Arc<dyn Conn + Send + Sync> = if p.mode == "send" {
        let socket = Arc::new(tokio::net::UdpSocket::bind("0.0.0.0:0").await?);
        socket.connect(addr).await?;
        Arc::new(DTLSConn::new(socket, cfg, true, None).await?)
    } else if p.mode == "receive" {
        listener = Some(webrtc_dtls::listener::listen(addr, cfg).await?);
        listener.as_ref().unwrap().accept().await?.0
    } else {
        return Err("unknown mode".into());
    };
    let dtls = conn
        .as_any()
        .downcast_ref::<DTLSConn>()
        .ok_or("expected DTLS")?;
    let state = dtls.connection_state().await;
    if state
        .peer_certificates
        .first()
        .is_none_or(|c| fingerprint(c) != p.peer_fingerprint)
    {
        return Err("peer pin mismatch".into());
    }
    let start = Instant::now();
    let result = if p.mode == "send" {
        let files = p
            .files
            .iter()
            .map(|path| {
                Ok(ClipboardFile {
                    name: path
                        .file_name()
                        .ok_or("file name")?
                        .to_str()
                        .ok_or("UTF-8 file name")?
                        .into(),
                    path: path.clone(),
                    size: fs::metadata(path)?.len(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let total: u64 = files.iter().map(|f| f.size).sum();
        let sender = conn.clone();
        let port = p.file_port;
        let mut transfer = tokio::task::spawn_local(async move {
            file_transfer::send(sender, addr.ip(), files, port, None).await
        });
        let mut sequence = 0u32;
        let mut rtt = Vec::new();
        let mut buf = [0; MAX_EVENT_SIZE];
        loop {
            tokio::select! {
             result=&mut transfer=>{result??;break;},
             _=tokio::time::sleep(Duration::from_millis(20))=> {
              sequence+=1;
              let input=if sequence.is_multiple_of(2) {Event::Pointer(PointerEvent::Motion{time:sequence,dx:1.0,dy:1.0})}
                else {Event::Keyboard(KeyboardEvent::Key{time:sequence,key:44,state:((sequence/2)%2) as u8})};
              let sent=Instant::now();packet(&conn,ProtoEvent::Input(input)).await?;
              let n=tokio::time::timeout(Duration::from_secs(5),conn.recv(&mut buf)).await??;
              if !matches!(decode(&buf[..n])?,ProtoEvent::Ack(n) if n==sequence) {return Err("input ACK mismatch".into());}
              rtt.push(sent.elapsed().as_secs_f64()*1000.0);
             }
            }
        }
        packet(&conn, ProtoEvent::Leave(sequence, 0.5)).await?;
        let n = tokio::time::timeout(Duration::from_secs(5), conn.recv(&mut buf)).await??;
        if !matches!(decode(&buf[..n])?,ProtoEvent::Ack(n) if n==sequence) {
            return Err("final ACK mismatch".into());
        }
        rtt.sort_by(f64::total_cmp);
        json!({"mode":"send","bytes":total,"duration_secs":start.elapsed().as_secs_f64(),"input_acks":sequence,"rtt_ms":{
   "mean":rtt.iter().sum::<f64>()/rtt.len().max(1) as f64,"p95":rtt.get(rtt.len()*95/100),"max":rtt.last()}})
    } else {
        let mut buf = vec![0; lan_mouse_proto::MAX_CLIPBOARD_SIZE];
        let mut transfer = None;
        let mut mouse = 0;
        let mut keys = 0;
        loop {
            let n = conn.recv(&mut buf).await?;
            if buf[0] == file_transfer::OFFER_TYPE {
                if transfer.is_some() {
                    return Err("duplicate file offer".into());
                }
                let offer = Offer::decode(&buf[..n])?;
                let peer = conn.remote_addr().ok_or("peer address")?.ip();
                let dir = p.output.clone();
                transfer = Some(tokio::task::spawn_local(async move {
                    file_transfer::receive(offer, peer, dir, None).await
                }));
                continue;
            }
            match decode(&buf[..n])? {
                ProtoEvent::Input(Event::Pointer(PointerEvent::Motion { time, .. })) => {
                    mouse += 1;
                    packet(&conn, ProtoEvent::Ack(time)).await?;
                }
                ProtoEvent::Input(Event::Keyboard(KeyboardEvent::Key { time, .. })) => {
                    keys += 1;
                    packet(&conn, ProtoEvent::Ack(time)).await?;
                }
                ProtoEvent::Leave(sequence, _) => {
                    packet(&conn, ProtoEvent::Ack(sequence)).await?;
                    break;
                }
                _ => return Err("unexpected input message".into()),
            }
        }
        let files = transfer.ok_or("missing file transfer")?.await??;
        let elapsed = start.elapsed().as_secs_f64();
        json!({"mode":"receive","duration_secs":elapsed,"mouse_events":mouse,"key_events":keys,"files":hashes(files)?})
    };
    conn.close().await?;
    if let Some(listener) = listener {
        listener.close().await?;
    }
    Ok(result)
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let p: Probe = serde_json::from_slice(&fs::read(
        std::env::args_os().nth(1).ok_or("config path required")?,
    )?)?;
    let result = tokio::task::LocalSet::new()
        .run_until(tokio::time::timeout(
            Duration::from_secs(p.timeout_secs),
            run(&p),
        ))
        .await;
    let report = match result {
        Ok(Ok(value)) => json!({"ok":true,"result":value}),
        Ok(Err(error)) => json!({"ok":false,"error":error.to_string()}),
        Err(error) => json!({"ok":false,"error":error.to_string()}),
    };
    fs::write(&p.report, serde_json::to_vec_pretty(&report)?)?;
    if report["ok"] != true {
        return Err(report["error"].to_string().into());
    }
    Ok(())
}
