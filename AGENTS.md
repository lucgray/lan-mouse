# Lan Mouse Agent Instructions

## Overview

Lan Mouse is an open-source Software KVM sharing mouse/keyboard input across local networks. The Rust workspace combines a GTK frontend, CLI/daemon mode, and multi-OS capture/emulation backends for Linux, Windows, and macOS.

## Core principles

- **Scope discipline.** Only implement what was requested; describe follow-up work instead of absorbing it.
- **Clarify OS behavior.** Ask when requirements touch OS-specific capture/emulation (they differ significantly).
- **Docs stay current.** Update [README.md](README.md) or [DOC.md](DOC.md) when touching public APIs or platform support.
- **Rust idioms.** Use `Result`/`Option`, `thiserror` for errors, descriptive logs, and concise comments for non-obvious invariants.

## Terminology

- **Client:** A remote machine that can receive or send input events. Each client is either _active_ (receiving events) or _inactive_ (can send events back). This mutual exclusion prevents feedback loops.
- **Backend:** OS-specific implementation for capture or emulation (e.g., libei, layer-shell, wlroots, X11, Windows, macOS).
- **Handle:** A per-client identifier used to route events and track state (pressed keys, position).

## Architecture

**Pipeline:** `input-capture` → `lan-mouse-ipc` → `input-emulation`

- **input-capture:** Reads OS events into a `Stream<CaptureEvent>`. Backends tried in priority order (libei → layer-shell → X11 → fallback). Tracks `pressed_keys` to avoid stuck modifiers. `position_map` queues events when multiple clients share a screen edge.
- **input-emulation:** Replays events via the `Emulation` trait (`consume`, `create`, `destroy`, `terminate`). Maintains `pressed_keys` and releases them on disconnect.
- **lan-mouse-ipc / lan-mouse-proto:** Protocol glue and serialization. Events are UDP; connection requests are TCP on the same port. Version bumps required when serialization changes.
- **input-event:** Shared scancode enums and abstract event types—extend here, don't duplicate translations.

## Feature & cfg discipline

- Feature flags live in root `Cargo.toml`. Gate OS-specific modules with the configs exported in build.rs (e.g., `cfg(layer_shell)`).
- Prefer module-level gating over per-function cfgs to avoid empty stubs.
- New backends: add feature in `Cargo.toml`, create gated module, log backend selection.

## Async patterns

- Tokio runtime with `futures` streams and `async_trait`. Model new flows as streams or async methods.
- Avoid blocking; use `spawn_blocking` if needed. Prefer existing single-threaded stream handling.
- `InputCapture` implements `Stream` and manually pumps backends—don't short-circuit this logic.

## Commands

```sh
cargo build --workspace                                    # full build
cargo build -p <crate>                                     # single crate
cargo test --workspace                                     # all tests
cargo fmt && cargo clippy --workspace --all-targets --all-features  # lint
LAN_MOUSE_LOG_LEVEL=debug cargo run                        # debug logging
```

Run from repo root—no `cd` in scripts.

## Testing

- Unit tests for utilities; integration tests for protocol behavior.
- OS-specific backends: test via GTK/CLI on target OS or document manual verification.
- Dummy backend exercises pipeline without real dependencies.
- Verify `terminate()` releases keys on unexpected disconnect.

## Workflow

1. Clarify ambiguous requirements, especially OS-specific behavior.
2. Implement minimal change; flag follow-up work.
3. Add proportional tests; run `cargo test` on affected crates.
4. Run `cargo fmt` and `cargo clippy --workspace --all-targets --all-features`.

## Commit conventions

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<optional scope>): <imperative summary ≤ 72 chars>

<optional body: motivation and approach, wrapped at 72 chars>
```

- Types: `feat`, `fix`, `refactor`, `perf`, `docs`, `test`, `build`, `ci`, `chore`, `revert`.
- Scope (optional, lowercase): the touched component — e.g. `gtk`, `wlroots`, `evdev`, `proto`, `ipc`, `clipboard`, `windows`, `macos`, `ci`.
- Summary: imperative mood ("add", "fix", "handle"), no trailing period. `fix:` must describe the user-visible bug, not the code change ("fix scroll direction on evdev receivers", not "negate value").
- Body: explain *why* (root cause, link to issue/PR number) and anything non-obvious about *how*. Reference issues as `#123` or full URLs to upstream (`feschber/lan-mouse#123`).
- Breaking protocol changes: footer `BREAKING-CHANGE: <what breaks>` and bump `PROTOCOL_VERSION`.
- One logical change per commit — don't mix a fix with unrelated cleanup.

## Pull request conventions

- Title: same format as commits (`fix(wlroots): keep locked modifiers out of depressed mask`).
- Body: fill in `.github/PULL_REQUEST_TEMPLATE.md` — Summary (what and why, for a reader who hasn't seen the diff), Changes, Testing (commands actually run and their results), Follow-ups. Attach screenshots/screencasts for UI changes.
- Link issues (`Fixes #123`, `Refs feschber/lan-mouse#456`) so they auto-close.
- One PR = one logical change. Split refactorings from behavior changes.
- Keep PRs reviewable: explain non-obvious decisions in the body rather than inline comments.
- Never `@mention` users; describe community contributions neutrally (e.g. "ported from upstream PR").

## Issue conventions

Use the templates under `.github/ISSUE_TEMPLATE/`. A well-formed report includes:

- **Environment**: OS + version, display server/compositor (`echo $XDG_SESSION_TYPE`), Lan Mouse version/commit, role (sender/receiver), backend flags.
- **Reproduction**: minimal numbered steps; state whether it reproduces on the physical keyboard/mouse.
- **Expected vs actual behavior.**
- **Logs**: `LAN_MOUSE_LOG_LEVEL=debug` output, attached as file — never screenshot text.
- For upstream issues fixed by this fork, note the fixing commit/tag.

## Logging

The codebase uses the `log` facade; `env_logger` selects output via `LAN_MOUSE_LOG_LEVEL` (e.g. `info,input_capture::clipboard=debug`).

- Never `println!`/`eprintln!` in library or daemon code — always the `log` macros.
- Levels:
  - `error!` — operation failed and the user is affected (connect lost, backend died, config write failed). Every `error!` should also reach the frontend when a user action caused it — see Error handling.
  - `warn!` — recovered from something abnormal (retry succeeded, peer vanished, oversized clipboard dropped).
  - `info!` — lifecycle and user-visible state changes (client connected/activated, port changed, config written). Sparse; a healthy session should be quiet.
  - `debug!` — diagnostics for bug reports (event details, decisions, fallbacks). May include event payloads but never secrets.
  - `trace!` — hot-path spam (per-event/packet); kept off by default.
- Messages: lowercase, no trailing punctuation, carry context — include the peer handle/addr, path, fd, or backend name so a stranger can locate the source (`"emulation backend wlroots failed to release key {key}: {e}"`, not `"release failed"`).
- No sensitive data: never log certificates, private keys, or fingerprints of other machines beyond what the UI already shows.
- Logs are for operators; user-facing notices go through `FrontendEvent` toasts instead of or in addition to logs.

## Error handling

- **No silent failures.** Every `Result` must be propagated with `?`, or explicitly logged if deliberately ignored (`let _ =` requires a comment justifying why the error is impossible/irrelevant). This is what kept bugs like the clipboard UTF-8 panic from being noticed.
- Library crates (input-*) return typed `thiserror` errors — one enum per failure surface, `#[error]` strings lowercase with context. Don't wrap everything in `anyhow`; the root `LanMouseError` aggregates typed errors.
- `Option`: `unwrap`/`expect` only when the invariant is locally provable (e.g. value just inserted) — prefer `let Some(x) = ... else { ... }` with a log line for "impossible" states that would indicate a bug elsewhere.
- **Panic containment**: anything that formats or slices user/peer-supplied data (clipboard text, hostnames, protocol bytes) must be boundary-safe — string slices use `floor_char_boundary`/`.chars()`, byte buffers use length checks. A malformed packet must not panic the daemon.
- **Surfacing to the user**: failures a user should see (config not writable, port busy, backend unavailable, clipboard transfer failed) go to `FrontendEvent::Error`/`Status` so the GTK frontend shows a toast — log *and* notify, don't pick one.
- Fatal startup errors: log `error!` and exit non-zero; daemon runtime errors: log `error!`, notify frontend, keep serving other clients.
- `std::process::exit`/`abort` only in `main.rs` paths, never inside library code.
