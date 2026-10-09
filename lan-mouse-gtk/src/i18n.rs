//! gettext/i18n initialization for the GTK frontend.
//!
//! Translations live in `po/<lang>.po` and are compiled to `.mo` catalogs by
//! build.rs, embedded into the binary via `include_bytes!`, and extracted to a
//! cache directory at startup so libintl can serve them. This keeps the app a
//! self-contained binary — no installed locale files required.

use gettextrs::LocaleCategory;
use gettextrs::{bind_textdomain_codeset, bindtextdomain, gettext, setlocale, textdomain};
use std::{env, fs, path::PathBuf};

include!(concat!(env!("OUT_DIR"), "/languages.rs"));

pub const DOMAIN: &str = "lan-mouse";

/// Initialize gettext: apply the language configured in config.toml,
/// extract the embedded catalogs, and bind the text domain.
/// Call before any UI is built so `translatable` strings in `.ui` files
/// resolve through the bound domain.
pub fn init() {
    let configured = apply_configured_language();

    // SAFETY: called once at startup before any UI or worker threads use
    // gettext — the standard early-init pattern for setlocale.
    unsafe {
        if configured {
            // gettext only honors LANGUAGE outside the "C" locale, and
            // the configured language may not be installed as a system
            // locale. C.UTF-8 exists on virtually every system, so it
            // serves as a neutral non-"C" base locale.
            setlocale(LocaleCategory::LcAll, "C.UTF-8");
        } else {
            setlocale(LocaleCategory::LcAll, "");
        }
    }

    let locale_dir = extract_translations();
    if let Err(e) = bindtextdomain(DOMAIN, &locale_dir) {
        log::warn!("failed to bind gettext domain {DOMAIN} to {locale_dir:?}: {e}");
        return;
    }
    if let Err(e) = bind_textdomain_codeset(DOMAIN, "UTF-8") {
        log::warn!("failed to set gettext codeset: {e}");
    }
    if let Err(e) = textdomain(DOMAIN) {
        log::warn!("failed to set gettext domain: {e}");
    }
}

/// `gettext` with named `{arg}` substitution.
///
/// Example: `tr("device connected: {addr}", &[("addr", &addr.to_string())])`
pub fn tr(msgid: &str, args: &[(&str, &str)]) -> String {
    let mut s = gettext(msgid);
    for (key, value) in args {
        s = s.replace(&format!("{{{key}}}"), value);
    }
    s
}

/// If config.toml sets a top-level `language`, export it via `LANGUAGE`
/// (which gettext gives precedence over the system locale).
fn apply_configured_language() -> bool {
    let Some(lang) = configured_language() else {
        return false;
    };
    if lang == "system" {
        return false;
    }
    log::info!("ui language from config: {lang}");
    env::set_var("LANGUAGE", &lang);
    true
}

/// Read the top-level `language` key from the daemon's config.toml.
/// Deliberately a line scan rather than a TOML parse: only the key above the
/// first `[section]` matters, and this runs before the daemon connection
/// exists.
fn configured_language() -> Option<String> {
    let path = config_file_path()?;
    let content = fs::read_to_string(&path).ok()?;
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            break;
        }
        if let Some(rest) = line.strip_prefix("language") {
            let value = rest.trim_start().strip_prefix('=')?.trim();
            return Some(value.trim_matches('"').to_string());
        }
    }
    None
}

/// Mirror of `Config::default_path` in the daemon: the directory holding
/// config.toml for this machine.
fn config_file_path() -> Option<PathBuf> {
    #[cfg(unix)]
    let base = match env::var("XDG_CONFIG_HOME") {
        Ok(dir) => dir,
        Err(_) => format!("{}/.config", env::var("HOME").ok()?),
    };
    #[cfg(not(unix))]
    let base = match env::var("LOCALAPPDATA") {
        Ok(dir) => dir,
        Err(_) => format!("{}/.config", env::var("USERPROFILE").ok()?),
    };
    Some(PathBuf::from(base).join("lan-mouse").join("config.toml"))
}

/// Write the embedded `.mo` catalogs to `<temp>/lan-mouse/locale/<lang>/
/// LC_MESSAGES/lan-mouse.mo` and return the locale root for bindtextdomain.
fn extract_translations() -> PathBuf {
    let root = env::temp_dir().join("lan-mouse").join("locale");
    for (lang, bytes) in LANGUAGES {
        let dir = root.join(lang).join("LC_MESSAGES");
        if let Err(e) = fs::create_dir_all(&dir) {
            log::warn!("cannot create locale dir {dir:?}: {e}");
            continue;
        }
        let path = dir.join(format!("{DOMAIN}.mo"));
        if let Err(e) = fs::write(&path, bytes) {
            log::warn!("cannot write translation catalog {path:?}: {e}");
        }
    }
    root
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zh_cn_catalog_loads() {
        env::set_var("LANGUAGE", "zh_CN");
        unsafe { setlocale(LocaleCategory::LcAll, "C.UTF-8") };
        let dir = extract_translations();
        bindtextdomain(DOMAIN, &dir).unwrap();
        bind_textdomain_codeset(DOMAIN, "UTF-8").unwrap();
        textdomain(DOMAIN).unwrap();
        assert_eq!(gettext("Connections"), "连接");
        assert_eq!(
            tr("{addr} disconnected", &[("addr", "1.2.3.4")]),
            "1.2.3.4 已断开"
        );
    }

    #[test]
    fn tr_leaves_unknown_args() {
        assert_eq!(tr("no {match}", &[("other", "x")]), "no {match}");
    }
}
