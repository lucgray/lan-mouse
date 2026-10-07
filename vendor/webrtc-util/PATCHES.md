# Local UDP session lifecycle patch

Base: crates.io `webrtc-util 0.11.0`, registry checksum
`64bfb10dbe6d762f80169ae07cf252bafa1f764b9594d140008a0231c0cdce58`.
Original MIT/Apache-2.0 licenses and source files are retained.
Production changes: `src/conn/conn_udp_listener.rs`, `src/buffer/mod.rs`.

- Exact-generation removal and weak table reference allow same-address reconnect;
  repeating an old close cannot remove its replacement or close the shared socket.
- Register before accept publication; roll back on a full accept queue.
- Each receive buffer holds at most 256 packets / 256KiB including headers.
  Overflow drops datagrams without waiting for reader capacity.
- Each listener indexes at most 128 raw sessions, preserving existing-peer lookup
  at capacity. Unknown peers above capacity receive the existing queue-full error.
- Queued sessions expire after two seconds, checked by a one-second sweep and
  again at dequeue. Atomic claim/expiry excludes already accepted sessions.
- Listener close stops admission and retires queued sessions, preserving
  accepted-peer dispatch until the final session closes. Recheck admission
  after async filtering to prevent late publication after close.
- UDP close discards data and releases allocation before freeing its table slot;
  pending reads wake, future sends/writes fail. Generic Buffer::close still drains.

The indexed raw rings are bounded to 128 * (256KiB + 1 slack byte) per listener.
This is not a whole-process RSS bound: multiple listeners, DTLS/application
buffers, cleanup tasks and allocator retention remain separate concerns.
Public signatures and wire encoding are unchanged. Dependency tests cover
admission, capacity recovery, expiry, discard, FIFO and close isolation; root
src/listen.rs additionally exercises actual loopback UDP/DTLS lifecycle paths.
Network flood availability and physical high-rate input remain unverified.
