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
- Bound each receive buffer to 256 packets / 256KiB including packet headers;
  existing UDP dispatch drops overflowing datagrams without awaiting space.

The shared listening socket is kept open. Public signatures and DTLS wire
encoding are unchanged. Table/handshake global bounds remain separate
work; this patch fixes successful per-session close, not canceled or never
completed handshakes. Root `src/listen.rs` contains real UDP regressions.

The original zero-limit Buffer still had a 4MiB allocation ceiling. The new
per-session ring ceiling is 256KiB+1 slack byte; total session count is not
bounded here. Two in-module regressions exercise production buffer admission,
FIFO, draining and separate-session isolation; they are not socket flood tests.
