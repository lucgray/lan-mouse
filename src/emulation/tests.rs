use super::*;
use crate::listen::test_support::{PausedClipboardConn, listener_with_conn};
use std::sync::{Arc, atomic::Ordering};

/// Exercise the actual listen loop without moving the user's physical pointer.
#[tokio::test(flavor = "current_thread")]
async fn clipboard_wait_keeps_input_responsive_and_preserves_reply_order() {
    tokio::task::LocalSet::new().run_until(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            let addr = "127.0.0.1:4242".parse().unwrap();
            let conn = Arc::new(PausedClipboardConn::default());
            let (listener, incoming) = listener_with_conn(addr, conn.clone());
            let (request_tx, request_rx) = channel();
            let (event_tx, mut event_rx) = channel();
            let (proxy_tx, mut proxy_rx) = channel();
            let (observed_tx, mut observed_rx) = channel();
            let (_proxy_event_tx, proxy_event_rx) = channel();
            let proxy_task = spawn_local(async move {
                while let Some(request) = proxy_rx.recv().await {
                    if matches!(request, ProxyRequest::Terminate) { break; }
                    assert!(observed_tx.send(request).is_ok());
                }
            });
            let proxy = EmulationProxy {
                emulation_active: Rc::new(Cell::new(true)),
                exit_requested: Rc::new(Cell::new(false)),
                request_tx: proxy_tx,
                event_rx: proxy_event_rx,
                task: proxy_task,
                input_config: InputConfig { invert_scroll: false, mouse_sensitivity: 1.0 },
            };
            let task = spawn_local(ListenTask {
                listener,
                emulation_proxy: proxy,
                request_rx,
                event_tx,
                client_manager: ClientManager::default(),
            }.run());
            let send_clipboard = |batch| {
                assert!(request_tx.send(EmulationRequest::SendClipboard(
                    addr, input_event::ClipboardEvent::Text(format!("copy {batch}")), None, batch,
                )).is_ok());
            };
            send_clipboard(1);
            conn.started.notified().await;
            send_clipboard(2);
            let motion = Event::Pointer(input_event::PointerEvent::Motion {
                time: 1, dx: 12.0, dy: -4.0,
            });
            assert!(incoming.send(ListenEvent::Msg { event: ProtoEvent::Input(motion.clone()), addr }).is_ok());
            assert!(matches!(observed_rx.recv().await, Some(ProxyRequest::Input(event, peer)) if event == motion && peer == addr));
            // Returning to the sending computer must work while the first copy waits.
            assert!(request_tx.send(EmulationRequest::Release(addr, 0.25)).is_ok());
            let leave: ([u8; lan_mouse_proto::MAX_EVENT_SIZE], usize) = ProtoEvent::Leave(0, 0.25).into();
            while conn.sent.lock().unwrap().is_empty() { tokio::task::yield_now().await; }
            assert_eq!(conn.sent.lock().unwrap()[0], leave.0[..leave.1]);
            assert_eq!(conn.starts.load(Ordering::SeqCst), 1, "second copy must not interleave");

            conn.permits.add_permits(1);
            assert!(matches!(event_rx.recv().await, Some(EmulationEvent::ClipboardSendDone { batch: 1, ok: true })));
            conn.started.notified().await;
            conn.fail.store(true, Ordering::SeqCst);
            conn.permits.add_permits(1);
            assert!(matches!(event_rx.recv().await, Some(EmulationEvent::ClipboardSendDone { batch: 2, ok: false })));

            // Shutdown must cancel an in-flight send and discard queued copies.
            send_clipboard(3);
            conn.started.notified().await;
            send_clipboard(4);
            assert!(request_tx.send(EmulationRequest::Terminate).is_ok());
            task.await.unwrap();
            assert!(conn.closed.load(Ordering::SeqCst));
            assert_eq!(conn.starts.load(Ordering::SeqCst), 3);
        }).await.expect("input/control handling stalled behind clipboard transfer");
    }).await;
}
