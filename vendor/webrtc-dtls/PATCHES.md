# Local DTLS close error patch

Base: crates.io webrtc-dtls 0.12.0, checksum
`5ccbe4d9049390ab52695c3646c1395c877e16c15fb05d3bda8eee0c7351711c`.
The original MIT/Apache-2.0 licenses and source files are retained.
Only `src/conn/mod.rs` production close handling changes: after close-notify
returns, stop the internal reader and close the underlying transport even if
notification failed. Preserve typed single errors; report both causes for a
double failure. Repeated close retains the original once-only behavior.
Root tests perform a real UDP/DTLS handshake with a faulting transport wrapper.

Pending/canceled close-notify futures, failed/canceled constructor handshake
cleanup, task/global session bounds remain separate work. No wire or public
signature changes. This does not make close asynchronous or impose a timeout.
