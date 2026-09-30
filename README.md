# Lan Mouse (lucgray fork)

> [!WARNING]
> **This is an AI-maintained fork of [feschber/lan-mouse](https://github.com/feschber/lan-mouse).**
>
> It exists for personal use: upstream merges pull requests slowly, so this
> fork cherry-picks and merges reviewed-but-unmerged community fix PRs.
> Merge review and conflict resolution are done with AI assistance and are
> necessarily rougher than upstream's own review — expect rough edges.
>
> For anything serious, use [upstream](https://github.com/feschber/lan-mouse).
> Fixes merged here so far: unbounded `SendInput` retry, unreachable-peer
> `Leave` spam, libei panic on drop, high-resolution scroll accumulation,
> macOS display hotplug, RustSec advisories, bounded per-peer cleanup +
> `WouldBlock` retry, libei capture-update coalescing, unified cross-OS
> scroll direction, macOS bidirectional capture loops, inactive-window raise,
> app relaunch, Hangul key, modifier injection (#450), Caps Lock toggle —
plus feature merges: per-client hotkey switching, key/chord remapping and
scroll inversion (`[input_pre_processing]`), cross-axis warp position
preservation, scroll-direction unification, macOS receive loops +
hotplug/relaunch fixes, a Windows service mode with MSI packaging, and
text clipboard sharing (`enable_clipboard`).

[![CI](https://github.com/feschber/lan-mouse/actions/workflows/rust.yml/badge.svg)](https://github.com/feschber/lan-mouse/actions/workflows/rust.yml) [![Cachix](https://github.com/feschber/lan-mouse/actions/workflows/cachix.yml/badge.svg)](https://github.com/feschber/lan-mouse/actions/workflows/cachix.yml) [![Release](https://github.com/feschber/lan-mouse/actions/workflows/release.yml/badge.svg)](https://github.com/feschber/lan-mouse/actions/workflows/release.yml)

[![crates.io](https://img.shields.io/crates/v/lan-mouse.svg)](https://crates.io/crates/lan-mouse)  [![license](https://img.shields.io/crates/l/lan-mouse.svg)](https://github.com/feschber/lan-mouse/blob/main/Cargo.toml)

Lan Mouse is a *cross-platform* mouse and keyboard sharing software similar to universal-control on Apple devices.
It allows for using multiple PCs via a single set of mouse and keyboard.
This is also known as a Software KVM switch.

Goal of this project is to be an open-source alternative to proprietary tools like [Synergy 2/3](https://symless.com/synergy), [Share Mouse](https://www.sharemouse.com/de/)
and other open source tools like [Deskflow](https://github.com/deskflow/deskflow) or [Input Leap](https://github.com/input-leap) (Synergy fork).

Focus lies on performance, ease of use and a maintainable implementation that can be expanded to support additional backends for e.g. Android, iOS, ... in the future.

***blazingly fast™*** because it's written in rust.

- _Now with a gtk frontend_

<picture>
    <source media="(prefers-color-scheme: dark)" srcset="/screenshots/dark.png?raw=true">
    <source media="(prefers-color-scheme: light)" srcset="/screenshots/light.png?raw=true">
    <img alt="Screenshot of Lan-Mouse" srcset="/screenshots/dark.png">
</picture>


## Encryption

Lan Mouse encrypts all network traffic using the DTLS implementation provided by [WebRTC.rs](https://github.com/webrtc-rs/webrtc).
There are currently no mitigations in place for timing side-channel attacks.

## OS Support

Most current desktop environments and operating systems are fully supported, this includes
- GNOME >= 45
- KDE Plasma >= 6.1
- Most wlroots based compositors, including Sway (>= 1.8), Hyprland and Wayfire
- Windows
- MacOS


### Caveats / Known Issues

> [!Important]
> - **X11** currently only has support for input emulation, i.e. can only be used on the receiving end.
>
> - **Sway / wlroots**: Wlroots based compositors without libei support on the receiving end currently do not handle modifier events on the client side.
> This results in CTRL / SHIFT / ALT / SUPER keys not working with a sending device that is NOT using the `layer-shell` backend
>
> - **Wayfire**: If you are using [Wayfire](https://github.com/WayfireWM/wayfire), make sure to use a recent version (must be newer than October 23rd) and **add `shortcuts-inhibit` to the list of plugins in your wayfire config!**
> Otherwise input capture will not work.
>
> - **Windows**: The mouse cursor will be invisible when sending input to a Windows system if
> there is no real mouse connected to the machine. As a workaround, enable
> *Settings → Bluetooth & devices → Mouse → Mouse keys* ("Control your mouse with a keypad")
> on the Windows machine — Windows then shows and moves a cursor even without a
> physical mouse attached.

For more detailed information about os support see [Detailed OS Support](#detailed-os-support)

### evdev emulation backend (Linux)

On Linux, Lan Mouse can emulate input through a virtual `evdev`/`uinput` device.
This backend works independently of the compositor and is useful as a fallback on
systems without a working remote desktop portal (for example COSMIC).

It requires access to `/dev/uinput`. Rather than running as root, grant your user
access with a dedicated group and a udev rule:

```sh
sudo groupadd --system lan-mouse
sudo usermod -aG lan-mouse <username>
echo 'KERNEL=="uinput", GROUP="lan-mouse"' | sudo tee /lib/udev/rules.d/05-lan-mouse.rules
```

Log out and back in (or reboot) for the group change to take effect. When
`/dev/uinput` is accessible, the `evdev` backend is preferred automatically; if it
is not accessible, Lan Mouse falls back to the other emulation backends. It can
also be selected explicitly with `--emulation-backend evdev` or via
`emulation_backend = "evdev"` in the config.

### Android & IOS

A proof of concept for an Android / IOS Application by [rohitsangwan01](https://github.com/rohitsangwan01) can be found [here](https://github.com/rohitsangwan01/lan-mouse-mobile).
It can be used as a remote control for any device supported by Lan Mouse.

## Installation

<details>
    <summary>Arch Linux</summary>

Lan Mouse can be installed from the [official repositories](https://archlinux.org/packages/extra/x86_64/lan-mouse/):

```sh
pacman -S lan-mouse
```

The prerelease version (following `main`) is available on the AUR:

```sh
paru -S lan-mouse-git
```
</details>


<details>
    <summary>Nix (OS)</summary>

- nixpkgs: [search.nixos.org](https://search.nixos.org/packages?channel=unstable&show=lan-mouse&from=0&size=50&sort=relevance&type=packages&query=lan-mouse)
- flake: [README.md](./nix/README.md)
</details>

<details>
    <summary>Fedora</summary>
You can install Lan Mouse from the [Terra Repository](https://terra.fyralabs.com).


After enabling Terra:

```sh
dnf install lan-mouse
```
</details>

<details>
    <summary>MacOS</summary>

- Download the package for your Mac (Intel or ARM) from the releases page
- Unzip it
- Remove the quarantine with `xattr -rd com.apple.quarantine "Lan Mouse.app"`
- Launch the app
- Use the menu bar item to open the settings window or quit Lan Mouse. Bundled macOS builds run as a menu bar app and do not keep a Dock icon visible.
- Grant accessibility permissions in System Preferences

</details>

<details>
    <summary>Windows</summary>

Lan Mouse can be installed from the [winget community repositories](https://github.com/microsoft/winget-pkgs/tree/master/manifests/f/feschber/LanMouse):

```sh
winget install lan-mouse
```

</details>

<details>
    <summary>Manual Installation</summary>

First make sure to [install the necessary dependencies](#installing-dependencies-for-development--compiling-from-source).

Precompiled release binaries for Windows, MacOS and Linux are available in the [releases section](https://github.com/feschber/lan-mouse/releases).
For Windows, the depenedencies are included in the .zip file, for other operating systems see [Installing Dependencies](#installing-dependencies-for-development--compiling-from-source).

Alternatively, the `lan-mouse` binary can be compiled from source (see below).

### Installing desktop file, app icon and firewall rules (optional)
```sh
# install lan-mouse (replace path/to/ with the correct path)
sudo cp path/to/lan-mouse /usr/local/bin/

# install app icon
sudo mkdir -p /usr/local/share/icons/hicolor/scalable/apps
sudo cp lan-mouse-gtk/resources/de.feschber.LanMouse.svg /usr/local/share/icons/hicolor/scalable/apps

# update icon cache
gtk-update-icon-cache /usr/local/share/icons/hicolor/

# install desktop entry
sudo mkdir -p /usr/local/share/applications
sudo cp de.feschber.LanMouse.desktop /usr/local/share/applications

# when using firewalld: install firewall rule
sudo cp firewall/lan-mouse.xml /etc/firewalld/services
# -> enable the service in firewalld settings
```

Instead of downloading from the releases, the `lan-mouse` binary
can be easily compiled via cargo or nix:

### Compiling and installing manually:
```sh
# compile in release mode
cargo build --release

# install lan-mouse
sudo cp target/release/lan-mouse /usr/local/bin/
```

### Compiling and installing via cargo:
```sh
# will end up in ~/.cargo/bin
cargo install lan-mouse
```

### Compiling and installing via nix:
```sh
# you can find the executable in result/bin/lan-mouse
nix-build
```
### Conditional compilation
Support for other platforms is omitted automatically based on the active
rust toolchain.

Additionally, available backends and frontends can be configured manually via
[cargo features](https://doc.rust-lang.org/cargo/reference/features.html).

E.g. if only support for sway is needed, the following command produces
an executable with support for only the `layer-shell` capture backend
and `wlroots` emulation backend:
```sh
cargo build --no-default-features --features layer_shell_capture,wlroots_emulation
```
For a detailed list of available features, checkout the [Cargo.toml](./Cargo.toml)
</details>



## Development

### Git pre-commit hook

This repository includes a local git hooks directory `.githooks/` with a `pre-commit` script that enforces formatting, lints, and tests before allowing a commit.  It is optional to enable it, but it will prevent you from committing code with failing unit tests or that needs clippy/fmt fixes. To enable the hook locally:

1. Make the hook executable:

```sh
chmod +x .githooks/pre-commit
```

2. Point git to the hooks directory (one-time per clone):

```sh
git config core.hooksPath .githooks
```

The `pre-commit` script runs `cargo fmt --all` (and fails if files were modified), `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `cargo test --workspace --all-features`.

### Dependencies & Compiling from Source
<details>
    <summary>MacOS</summary>

```sh
# Install dependencies
brew install libadwaita pkg-config imagemagick
cargo install cargo-bundle
# Create the macOS icon file
scripts/makeicns.sh
# Create the .app bundle
cargo bundle
# Copy all dynamic libraries into the bundle, and update the bundle to find them there
scripts/copy-macos-dylib.sh
```
</details>

<details>
    <summary>Ubuntu and derivatives</summary>

```sh
sudo apt install libadwaita-1-dev libgtk-4-dev libx11-dev libxtst-dev
```
</details>

<details>
    <summary>Arch and derivatives</summary>

```sh
sudo pacman -S libadwaita gtk libx11 libxtst
```
</details>

<details>
    <summary>Fedora and derivatives</summary>

```sh
sudo dnf install libadwaita-devel libXtst-devel libX11-devel
```
</details>
<details>
    <summary>Nix</summary>

```sh
nix-shell .
```
</details>
<details>
    <summary>Nix (flake)</summary>

```sh
nix develop
```
</details>

<details>
    <summary>Windows</summary>

- First install [Rust](https://www.rust-lang.org/tools/install).

- Then follow the instructions at [gtk-rs.org](https://gtk-rs.org/gtk4-rs/stable/latest/book/installation_windows.html)

*TLDR:*

Build gtk from source

- The following commands should be run in an **admin power shell** instance:
```sh
# install chocolatey
Set-ExecutionPolicy Bypass -Scope Process -Force; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# install gvsbuild dependencies
choco install python git msys2 visualstudio2022-workload-vctools
```

- The following commands should be run in a **regular power shell** instance:

```sh
# install gvsbuild with python
python -m pip install --user pipx
python -m pipx ensurepath
```

- Relaunch your powershell instance so the changes in the environment are reflected.
```sh
pipx install gvsbuild

# build gtk + libadwaita
gvsbuild build gtk4 libadwaita librsvg adwaita-icon-theme
```

- **Make sure to add the directory** `C:\gtk-build\gtk\x64\release\bin`
[**to the `PATH` environment variable**]((https://learn.microsoft.com/en-us/previous-versions/office/developer/sharepoint-2010/ee537574(v=office.14))). Otherwise the project will fail to build.

To avoid building GTK from source, it is possible to disable
the gtk frontend (see conditional compilation).
</details>

## Usage
<details>
    <summary>Gtk Frontend</summary>

By default the gtk frontend will open when running `lan-mouse`.

To connect a device you want to control, simply click the `Add` button and enter the hostname
of the device.

On the *remote* device, authorize your *local* device for incoming traffic using the `Authorize` button
under the "Incoming Connections" section.
The fingerprint for authorization can be found under the general section of your *local* device.
It is of the form "aa:bb:cc:..."

Authorized devices can be persisted using the configuration file (see [Configuration](#configuration)).

If the device still can not be entered, make sure you have UDP port `4242` (or the one selected) opened up in your firewall.

#### Tray icon (Linux & macOS)

On macOS and on Linux desktops with a system tray, Lan Mouse runs as a tray icon application:
Closing the window only hides it and Lan Mouse keeps running in the tray,
where the icon offers a menu to re-open the window or quit the app entirely.
Launching `lan-mouse` while it is already running re-presents the window of the running instance.
Set `LAN_MOUSE_HIDDEN=1` in the environment to start quietly into the tray without opening the window.

On Linux the tray icon uses the StatusNotifierItem specification, which is supported
out of the box on most desktops (KDE Plasma, XFCE, waybar, ...).
GNOME requires the [AppIndicator extension](https://extensions.gnome.org/extension/615/appindicator-support/).
If no tray is available, Lan Mouse falls back to the previous behavior: closing the window quits the app.

When the GUI connects to an already running daemon (e.g. the [systemd service](#systemd-service)),
it acts as a plain client window instead: no tray icon is created and closing the window
only quits the GUI, leaving the daemon running.

#### Tray icon (Windows)

On Windows, Lan Mouse runs as a notification-area application:
Closing the window only hides it and Lan Mouse keeps running in the tray,
where the icon offers a menu to re-open the window or quit the app entirely.
Launching `lan-mouse` again opens a separate window.
Set `LAN_MOUSE_HIDDEN=1` in the environment to start quietly into the tray without opening the window.
</details>

<details>
    <summary>Command Line Interface</summary>

The cli interface can be accessed by passing `cli` as a commandline argument.
Use
```sh
lan-mouse cli help
```
 to list the available commands and
```sh
lan-mouse cli <cmd> help
```
for information on how to use a specific command.

</details>

<details>
    <summary>Daemon Mode</summary>

Lan Mouse can be launched in daemon mode to keep it running in the background (e.g. for use in a systemd-service).

To do so, use the `daemon` subcommand:

```sh
lan-mouse daemon
```
</details>

## Systemd Service

In order to start lan-mouse with a graphical session automatically,
the [systemd-service](service/lan-mouse.service) can be used:

Copy the file to `~/.config/systemd/user/` and enable the service:

```sh
cp service/lan-mouse.service ~/.config/systemd/user
systemctl --user daemon-reload
systemctl --user enable --now lan-mouse.service
```
> [!Important]
> Make sure to point `ExecStart=/usr/bin/lan-mouse daemon` to the actual `lan-mouse` binary (in case it is not under `/usr/bin`, e.g. when installed manually.

## launchd Agent (macOS)

On macOS, lan-mouse must run inside a graphical **user** session — the
CGEvent taps it uses for capture and emulation do not exist in the
system context. This means a Launch**Daemon** (`/Library/LaunchDaemons`)
cannot work; use a Launch**Agent** (`~/Library/LaunchAgents`) instead:

```sh
cp service/de.feschber.LanMouse.plist ~/Library/LaunchAgents
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/de.feschber.LanMouse.plist
```

> [!Important]
> Point `ProgramArguments` at the actual `lan-mouse` binary. The launchd
> job itself must also be granted Accessibility and Input Monitoring
> permissions (System Settings → Privacy & Security), since TCC tracks
> the responsible process.


## Configuration
To automatically load clients on startup, the file `$XDG_CONFIG_HOME/lan-mouse/config.toml` is parsed.
`$XDG_CONFIG_HOME` defaults to `~/.config/`.

To create this file you can copy the following example config:

### Example config
> [!TIP]
> key symbols in the release bind are named according
> to their names in [input-event/src/scancode.rs#L172](input-event/src/scancode.rs#L176).
> This is bound to change

> [!TIP]
> Press the jail bind (*ScrollLock* by default) to toggle the "mouse jail".
> While enabled, your mouse stays confined on your host and cannot cross a
> screen edge onto a connected peer — press the bind again to disable.

```toml
# example configuration

# configure release bind
release_bind = [ "KeyA", "KeyS", "KeyD", "KeyF" ]

# configure jail bind
jail_bind = [ "KeyScrollLock" ]

# optional port (defaults to 4242)
port = 4242

# optional key-repeat timing for the *receiving* side, in milliseconds. Only the
# macOS and Windows emulation backends use these (other platforms let the OS
# generate key repeat): `key_repeat_delay` is how long a key must be held before
# it starts repeating, `key_repeat_interval` is the time between repeats.
# Defaults: 500 and 32.
key_repeat_delay = 500
key_repeat_interval = 32

# optional key binds that enter the client(s) at a position without
# the pointer having to cross that screen edge (see "Entering a client
# with a key bind" below)
[enter_binds]
right = [ "KeyLeftCtrl", "KeyLeftAlt", "KeyRight" ]


# optional transformations applied to input received from other devices
[input_post_processing]
# scale relative pointer motion (defaults to 1.0)
mouse_sensitivity = 1.0
# invert continuous and discrete scrolling (defaults to false)
invert_scroll = false

# list of authorized tls certificate fingerprints that
# are accepted for incoming traffic
[authorized_fingerprints]
"bc:05:ab:7a:a4:de:88:8c:2f:92:ac:bc:b8:49:b8:24:0d:44:b3:e6:a4:ef:d7:0b:6c:69:6d:77:53:0b:14:80" = "iridium"

# define a client on the right side with host name "iridium"
[[clients]]
# position (left | right | top | bottom)
position = "right"
# hostname
hostname = "iridium"
# activate this client immediately when lan-mouse is started
activate_on_startup = true
# optional list of (known) ip addresses
ips = ["192.168.178.156"]

# define a client on the left side with IP address 192.168.178.189
[[clients]]
position = "left"
# The hostname is optional: When no hostname is specified,
# at least one ip address needs to be specified.
hostname = "thorium"
# ips for ethernet and wifi
ips = ["192.168.178.189", "192.168.178.172"]
# optional port
port = 4242
```

Where `left` can be either `left`, `right`, `top` or `bottom`.
Input post-processing is configured on the receiving device and applies only to
events emulated there. Both options can also be changed at runtime:

```sh
lan-mouse cli set-mouse-sensitivity 1.5
lan-mouse cli invert-scrolling true
```

### Entering a client with a key bind

An `[enter_binds]` entry makes a position reachable by holding a
combination of keys, so the pointer no longer has to be moved into the
corresponding screen edge:

```toml
[enter_binds]
right = [ "KeyLeftCtrl", "KeyLeftAlt", "KeyRight" ]
top = [ "KeyLeftCtrl", "KeyLeftAlt", "KeyUp" ]
```

Binds are keyed by position rather than by client because entering is
position-based: crossing an edge enters *every* client at that edge,
and a bind is deliberately no different.

The bind takes effect on the machine that *sends* input and behaves
exactly like the matching edge crossing: capture begins, the pointer is
warped to that edge and the usual `release_bind` returns control to the
local machine. Consequently a bind only fires while capture is
inactive, and only for a position that currently has an active client.

The key combination itself is consumed locally and is not forwarded, so
the remote machine starts with no keys held — mirroring `release_bind`,
which releases everything before handing control back. Keep holding the
bind's modifiers after switching and the remote will not see them as
held; release and press them again for that.

> [!NOTE]
> Supported on the macOS and Windows capture backends. The Linux
> backends observe no input while capture is inactive, so binds are
> ignored there (see [#260](https://github.com/feschber/lan-mouse/issues/260)).

Crossing an edge carries the cursor's position along that edge over to the
peer — leaving near the top of the right edge enters the peer near the top
of its left edge, scaled to the peer's own display size if the two differ.
This also applies when handing control back without a matching `[[clients]]`
entry on the other side (moving the cursor further past the edge it just
entered at): the side you're leaving reports the spot it saw you reach, so
the cursor reappears there instead of back at the original entry point.

Support so far:

| | reports its own crossings (sending) | applies an incoming position (receiving) |
|---|---|---|
| macOS | yes | yes |
| Windows | yes | not yet — falls back to the edge's midpoint |
| X11 / Wayland | not yet — falls back to the edge's midpoint | not yet — falls back to the edge's midpoint |

Falling back just means that side behaves as before this existed; nothing
breaks, the position simply isn't preserved on that leg of the crossing.

### Remapping keys for other operating systems

`[input_pre_processing]` rewrites events on their way to other devices.
`remap_keys` swaps individual keys, which is mostly useful for reconciling
modifier layouts — sending Command from a Mac as Control, and Control as
Super, so that Cmd+C keeps copying on a Windows or Linux peer:

```toml
[input_pre_processing.remap_keys]
KeyLeftMeta = "KeyLeftCtrl"
KeyLeftCtrl = "KeyLeftMeta"
```

Remapping happens on the *sending* side, so the local machine is unaffected:
`release_bind` and the host's own shortcuts keep seeing the physical keys.

`remap_keys` is a plain per-key substitution — it can't tell "Command alone"
from "Command as part of a chord", so it always sends a given key as the
same thing. `remap_chords` covers that case: a modifier is sent as
something *else* specifically when a given trigger key is pressed while
it's held, without changing what it sends as the rest of the time. The
motivating case is `Cmd+Tab` reaching a Windows peer as `Alt+Tab` (its app
switcher) instead of `Ctrl+Tab`, while `Cmd+C` still becomes `Ctrl+C`:

```toml
[[input_pre_processing.remap_chords]]
modifier = "KeyLeftMeta"
trigger = "KeyTab"
to = "KeyLeftAlt"
```

Holding the modifier and pressing anything *other* than `trigger` — another
regular key, a click, a scroll — falls back to `remap_keys` as usual.
Pressing another modifier first (e.g. Shift, for `Cmd+Shift+Tab` to cycle
backwards) doesn't cancel the chord; only plain cursor motion is ignored
entirely, so an incidental mouse twitch while holding the modifier can't
break it either.

### Inverting scroll direction for other operating systems

`invert_scroll_vertical` and `invert_scroll_horizontal` flip the direction of
scroll events on their way to other devices. This is mostly useful when
macOS' "natural scrolling" reaches a Windows or Linux peer that scrolls the
traditional way, making the wheel feel backwards on the remote machine:

```toml
[input_pre_processing]
invert_scroll_vertical = true
invert_scroll_horizontal = false
```

Like `remap_keys`, this happens on the *sending* side only, so scrolling on
the local machine itself is unaffected.

### Clipboard sharing

Text copied to the clipboard is shared with connected peers and set on the
receiving machine automatically. Enabled by default; disable it with:

```toml
enable_clipboard = false
```

Only plain text is shared — images and other formats are ignored.

## Roadmap
- [x] Graphical frontend (gtk + libadwaita)
- [x] respect xdg-config-home for config file location.
- [x] IP Address switching
- [x] Liveness tracking Automatically ungrab mouse when client unreachable
- [x] Liveness tracking: Automatically release keys, when server offline
- [x] MacOS KeyCode Translation
- [x] Libei Input Capture
- [x] MacOS Input Capture
- [x] Windows Input Capture
- [x] Encryption
- [ ] X11 Input Capture
- [ ] Latency measurement and visualization
- [ ] Bandwidth usage measurement and visualization
- [ ] Clipboard support


## Detailed OS Support

In order to use a device for sending events, an **input-capture** backend is required, while receiving events requires
a supported **input-emulation** *and* **input-capture** backend.

A suitable backend is chosen automatically based on the active desktop environment / compositor.

The following sections detail the emulation and capture backends provided by lan-mouse and their support in desktop environments / operating systems.

### Input Emulation Support

| Desktop / Backend         | wlroots                  | libei                    | remote-desktop portal    | windows                  |   macos                                | x11                |
|---------------------------|--------------------------|--------------------------|--------------------------|--------------------------|----------------------------------------|--------------------|
| Wayland (wlroots)         | :heavy_check_mark:       |                          |                          |                          |                                        |                    |
| Wayland (KDE)             |                          | :heavy_check_mark:       | :heavy_check_mark:       |                          |                                        |                    |
| Wayland (Gnome)           |                          | :heavy_check_mark:       | :heavy_check_mark:       |                          |                                        |                    |
| Windows                   |                          |                          |                          | :heavy_check_mark:       |                                        |                    |
| MacOS                     |                          |                          |                          |                          |   :heavy_check_mark:                   |                    |
| X11                       |                          |                          |                          |                          |                                        | :heavy_check_mark: |

- `wlroots`: This backend makes use of the [wlr-virtual-pointer-unstable-v1](https://wayland.app/protocols/wlr-virtual-pointer-unstable-v1) and [virtual-keyboard-unstable-v1](https://wayland.app/protocols/virtual-keyboard-unstable-v1) protocols and is supported by most wlroots based compositors.
- `libei`: This backend uses [libei](https://gitlab.freedesktop.org/libinput/libei) and is supported by GNOME >= 45 or KDE Plasma >= 6.1.
- `xdp`: This backend uses the [freedesktop remote-desktop-portal](https://flatpak.github.io/xdg-desktop-portal/#gdbus-org.freedesktop.portal.RemoteDesktop) and is supported on GNOME and Plasma.
- `x11`: Backend for X11 sessions.
- `windows`: Backend for Windows.
- `macos`: Backend for MacOS.



### Input Capture Support

| Desktop / Backend         | layer-shell              | libei                    | windows                  |   macos                                | x11 |
|---------------------------|--------------------------|--------------------------|--------------------------|----------------------------------------|-----|
| Wayland (wlroots)         | :heavy_check_mark:       |                          |                          |                                        |     |
| Wayland (KDE)             | :heavy_check_mark:       | :heavy_check_mark:       |                          |                                        |     |
| Wayland (Gnome)           |                          | :heavy_check_mark:       |                          |                                        |     |
| Windows                   |                          |                          | :heavy_check_mark:       |                                        |     |
| MacOS                     |                          |                          |                          |   :heavy_check_mark:                   |     |
| X11                       |                          |                          |                          |                                        | WIP |

- `layer-shell`: This backend creates a single pixel wide window on the edges of Displays to capture the cursor using the [layer-shell protocol](https://wayland.app/protocols/wlr-layer-shell-unstable-v1).
- `libei`: This backend uses [libei](https://gitlab.freedesktop.org/libinput/libei) and is supported by GNOME >= 45 or KDE Plasma >= 6.1.
- `windows`: Backend for input capture on Windows.
- `macos`: Backend for input capture on MacOS.
- `x11`: TODO (not yet supported)
