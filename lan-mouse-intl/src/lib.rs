//! `lan-mouse-intl` — a minimal gettext implementation for the Windows
//! GTK bundle.
//!
//! Built as a cdylib and packaged as `intl-8.dll`, replacing the
//! proxy-libintl stub gvsbuild ships (whose `g_libintl_dgettext`
//! returns the msgid verbatim and can never load a catalog). The
//! exported `g_libintl_*` ABI is what glib-2.0-0.dll imports, so
//! `.ui` `translatable` strings, g_dgettext, and GtkBuilder lookups
//! all resolve through here.
//!
//! The `catalog` module is also the rlib interface the GTK frontend
//! uses for its in-crate message lookup.

pub mod catalog;
#[cfg(any(windows, test))]
mod intl;

pub use catalog::Catalog;
pub use catalog::parse_mo;
