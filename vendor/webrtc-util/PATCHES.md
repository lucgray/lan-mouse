# Local UDP session close patch

Base: crates.io `webrtc-util 0.11.0`, registry checksum
`64bfb10dbe6d762f80169ae07cf252bafa1f764b9594d140008a0231c0cdce58`.
The original MIT and Apache-2.0 licenses and source files are retained.

Only `src/conn/conn_udp_listener.rs` is changed:

- UDP session close removes its exact session from the listener table.
- The session holds a weak table reference, avoiding an ownership cycle.
- Close wakes reads/closes the Buffer; send/send_to reject a closed session.
- Old repeated close cannot remove a replacement using the same address.
- Register before accept publication; rollback on a full accept queue.

The shared listening socket is kept open. Public signatures and DTLS wire
encoding are unchanged. Buffer/table/handshake global bounds remain separate
work; this patch fixes successful per-session close, not canceled or never
completed handshakes. Root `src/listen.rs` contains real UDP regressions.
