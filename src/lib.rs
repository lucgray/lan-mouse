mod authentication;
mod capture;
pub mod capture_test;
pub mod client;
mod clipboard_network;
mod clipboard_writer;
pub mod config;
mod connect;
mod control_network;
mod crypto;
mod dns;
mod emulation;
pub mod emulation_test;
mod hooks;
mod listen;
mod remap;
mod scroll;
pub mod service;
#[cfg(windows)]
pub mod windows;
#[cfg(windows)]
pub mod windows_service;

#[cfg(windows)]
static IS_WINDOWS_SERVICE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(windows)]
pub fn set_is_windows_service(is_service: bool) {
    IS_WINDOWS_SERVICE.store(is_service, std::sync::atomic::Ordering::SeqCst);
}

#[cfg(windows)]
pub fn is_windows_service() -> bool {
    IS_WINDOWS_SERVICE.load(std::sync::atomic::Ordering::SeqCst)
}

pub use remap::ChordRemap;
