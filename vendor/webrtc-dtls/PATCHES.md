# Local DTLS close ownership patch

Base: crates.io webrtc-dtls 0.12.0, checksum
`5ccbe4d9049390ab52695c3646c1395c877e16c15fb05d3bda8eee0c7351711c`.
The original MIT/Apache-2.0 licenses and source files are retained.
Production changes are confined to src/conn/mod.rs:

- One owned close task survives cancellation/drop of its callers and object.
- Concurrent/repeated pending closes wait for that task. The first completed
  waiter receives its result; subsequent completed calls retain Ok behavior.
- Close-notify has a 250ms best-effort deadline. Stop/join the outgoing writer
  and internal reader before closing the raw transport, including on error.
- Single notify/transport errors retain type; double errors preserve both causes.
- Packet writer disappearing before its result now returns ErrConnClosed,
  instead of reporting a write as successful.
- Constructor cleanup guard retains raw transport/runtime from first poll,
  stops workers/closes raw transport on errors/cancellation and disarms on
  successful handshake. Synchronous handle registration avoids a cancel gap.
- Constructor records worker handles; the original test constructor fixture in
  conn_test.rs initializes the new private fields. One upstream trailing space
  was removed. No other upstream source file changes.

Root real UDP/DTLS tests inject send/close errors and pending futures, cancel
all close waiters and drop the DTLS object, and check eventual cleanup. The
original close task-leak regression is run separately. No wire/public signature
changes. Synchronous stalls, truly failed/pending raw close recovery, runtime shutdown/never-polled futures and global task/session bounds remain
separate work.
A pending raw close retains its owned task/transport; task counts are not globally
bounded by this patch and the application reader budget does not account for it.

Standalone Cargo.toml links ../webrtc-util through a local patch so original
DTLS library tests exercise the same UDP close behavior as the application.
