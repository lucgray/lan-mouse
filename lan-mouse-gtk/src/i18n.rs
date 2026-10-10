//! gettext/i18n initialization for the GTK frontend.
//!
//! Translations live in `po/<lang>.po` and are compiled to `.mo` catalogs by
//! build.rs, embedded into the binary via `include_bytes!`, and extracted to a
//! cache directory at startup so libintl can serve them. This keeps the app a
//! self-contained binary — no installed locale files required.

#[cfg(unix)]
use gettextrs::LocaleCategory;
#[cfg(unix)]
use gettextrs::gettext as platform_gettext;
#[cfg(unix)]
use gettextrs::{bind_textdomain_codeset, bindtextdomain, setlocale, textdomain};
#[cfg(not(unix))]
use std::path::Path;
use std::{env, fs, path::PathBuf};

/// Message lookup: on unix this is the platform libintl (shared with
/// GtkBuilder's translation registry); elsewhere a minimal in-crate
/// .mo parser — gettext-sys cannot build under MSVC.
#[cfg(unix)]
pub(crate) fn gettext(msgid: &str) -> String {
    platform_gettext(msgid)
}

#[cfg(not(unix))]
pub(crate) fn gettext(msgid: &str) -> String {
    catalog_for(&ui_language())
        .and_then(|cat| cat.gettext(msgid).map(str::to_string))
        .unwrap_or_else(|| msgid.to_string())
}

/// LANGUAGE env (set by tests or `apply_configured_language`), then
/// the config.toml `language` key, else the empty "system" value.
#[cfg(not(unix))]
fn ui_language() -> String {
    env::var("LANGUAGE")
        .ok()
        .filter(|l| !l.is_empty())
        .or_else(configured_language)
        .unwrap_or_default()
}

/// Per-language parsed catalogs — resolved lazily so a runtime
/// language change (or a test setting LANGUAGE after first use) is
/// honored instead of pinning whatever ran first.
#[cfg(not(unix))]
fn catalog_for(lang: &str) -> Option<std::sync::Arc<lan_mouse_intl::Catalog>> {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    static CATALOGS: OnceLock<Mutex<HashMap<String, Option<Arc<lan_mouse_intl::Catalog>>>>> =
        OnceLock::new();
    let cache = CATALOGS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(guard) = cache.lock() {
        if let Some(cat) = guard.get(lang) {
            return cat.clone();
        }
    }
    let cat = LANGUAGES
        .iter()
        .find(|(l, _)| *l == lang)
        .and_then(|(_, bytes)| lan_mouse_intl::parse_mo(bytes))
        .map(Arc::new);
    if let Ok(mut guard) = cache.lock() {
        guard.insert(lang.to_string(), cat.clone());
    }
    cat
}

include!(concat!(env!("OUT_DIR"), "/languages.rs"));

/// textdomain name — only libintl consumes it (unix); the in-crate
/// catalog lookup keys off LANGUAGES directly
#[cfg(unix)]
pub const DOMAIN: &str = "lan-mouse";

/// Initialize gettext: apply the language configured in config.toml,
/// extract the embedded catalogs, and bind the text domain.
/// Call before any UI is built so `translatable` strings in `.ui` files
/// resolve through the bound domain.
#[cfg(unix)]
pub fn init() {
    let configured = apply_configured_language();

    // SAFETY: called once at startup before any UI or worker threads use
    // gettext — the standard early-init pattern for setlocale.
    unsafe {
        if configured {
            apply_language_base_locale();
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

/// Non-unix: runtime strings resolve through the in-crate catalog.
/// `.ui` translatable strings go through GTK's own intl
/// (proxy-libintl inside libglib, or a standalone libintl dll), which
/// only looks in a real locale directory — so extract the embedded
/// catalogs and bind the domain in that intl too.
#[cfg(not(unix))]
pub fn init() {
    apply_configured_language();
    let root = extract_translations();
    bind_platform_domain(&root);
}

/// Extract embedded `.mo` catalogs next to the executable
/// (`<exe>/share/locale/...` — the default localedir GTK's intl
/// resolves relative to its dll) when writable, and always to a
/// per-user data dir (`LOCALAPPDATA`/`USERPROFILE`). Returns the data
/// dir to bind.
#[cfg(not(unix))]
fn extract_translations() -> PathBuf {
    let base = env::var("LOCALAPPDATA")
        .or_else(|_| env::var("USERPROFILE"))
        .unwrap_or_else(|_| env::temp_dir().to_string_lossy().into_owned());
    let data_root = PathBuf::from(base).join("lan-mouse").join("locale");
    let exe_root = env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("share").join("locale")));
    for (lang, bytes) in LANGUAGES {
        let rel = PathBuf::from(lang).join("LC_MESSAGES").join("lan-mouse.mo");
        for root in [Some(&data_root), exe_root.as_ref()].into_iter().flatten() {
            let path = root.join(&rel);
            // skip only when the bytes on disk already match — a stale
            // catalog from an older version must be overwritten
            if fs::read(&path).is_ok_and(|existing| existing == *bytes) {
                continue;
            }
            match fs::create_dir_all(path.parent().unwrap_or(root))
                .and_then(|_| fs::write(&path, bytes))
            {
                Ok(()) => log::info!("extracted translation catalog to {}", path.display()),
                Err(e) => log::debug!("cannot extract catalog to {path:?}: {e}"),
            }
        }
    }
    data_root
}

/// Register `lan-mouse` (and UTF-8 codeset) with the intl implementation
/// GTK actually uses — found by probing the dlls GTK links. Without a
/// bound domain, `.ui` translatable strings fall back to the untranslated
/// msgid even though the `.mo` file exists.
#[cfg(windows)]
fn bind_platform_domain(root: &Path) {
    use std::ffi::{OsStr, OsString};
    use std::os::windows::ffi::OsStrExt;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LoadLibraryW(name: *const u16) -> isize;
        fn GetProcAddress(module: isize, name: *const u8) -> usize;
    }
    // proxy-libintl's wbindtextdomain keeps the domain narrow — only
    // the directory argument is UTF-16
    type BindTextdomainW = unsafe extern "C" fn(*const u8, *const u16) -> usize;
    type BindTextdomainMb = unsafe extern "C" fn(*const u8, *const u8) -> usize;
    type Codeset = unsafe extern "C" fn(*const u8, *const u8) -> usize;

    use std::ffi::CStr;

    let wide = |s: &OsStr| -> Vec<u16> { s.encode_wide().chain(Some(0)).collect() };
    let dir_wide = wide(root.as_os_str());
    let domain_mb = c"lan-mouse";

    // intl providers GTK4 windows builds may carry: gvsbuild ships the
    // proxy-libintl inside glib-2.0-0.dll / intl-8.dll exporting
    // `g_libintl_*` prefixed symbols; MinGW distributions ship
    // libglib-2.0-0.dll / libintl-8.dll with the unprefixed names.
    // Probe every dll × symbol-name combination, binding wherever the
    // export exists (binding an unused provider is harmless).
    const DLLS: &[&str] = &[
        "intl-8.dll",
        "glib-2.0-0.dll",
        "libglib-2.0-0.dll",
        "intl.dll",
        "libintl.dll",
        "libintl-8.dll",
    ];
    const W_BIND: &[&CStr] = &[c"g_libintl_wbindtextdomain", c"wbindtextdomain"];
    const MB_BIND: &[&CStr] = &[c"g_libintl_bindtextdomain", c"bindtextdomain"];
    const CODESET: &[&CStr] = &[
        c"g_libintl_bind_textdomain_codeset",
        c"bind_textdomain_codeset",
    ];

    let sym = |handle: isize, names: &[&'static CStr]| -> Option<(&'static CStr, usize)> {
        names.iter().copied().find_map(|name| {
            let addr = unsafe { GetProcAddress(handle, name.as_ptr().cast()) };
            (addr != 0).then_some((name, addr))
        })
    };

    let mut bound = false;
    for dll in DLLS {
        let handle = unsafe { LoadLibraryW(wide(&OsString::from(*dll)).as_ptr()) };
        if handle == 0 {
            continue;
        }
        if let Some((name, addr)) = sym(handle, W_BIND) {
            let f: BindTextdomainW = unsafe { std::mem::transmute(addr) };
            unsafe { f(domain_mb.as_ptr().cast(), dir_wide.as_ptr()) };
            bound = true;
            log::info!(
                "bound gettext domain in {dll} via {}",
                name.to_string_lossy()
            );
        } else if let Some((name, addr)) = sym(handle, MB_BIND) {
            // bindtextdomain takes the dir in the platform encoding —
            // UTF-8 in proxy-libintl
            let f: BindTextdomainMb = unsafe { std::mem::transmute(addr) };
            let mut dir = root.to_string_lossy().into_owned().into_bytes();
            dir.push(0);
            unsafe { f(domain_mb.as_ptr().cast(), dir.as_ptr()) };
            bound = true;
            log::info!(
                "bound gettext domain in {dll} via {}",
                name.to_string_lossy()
            );
        }
        if let Some((name, addr)) = sym(handle, CODESET) {
            let f: Codeset = unsafe { std::mem::transmute(addr) };
            unsafe { f(domain_mb.as_ptr().cast(), c"UTF-8".as_ptr().cast()) };
            log::debug!(
                "set gettext codeset in {dll} via {}",
                name.to_string_lossy()
            );
        }
    }
    if !bound {
        log::warn!(
            "no libintl provider found — .ui strings stay untranslated; \
             runtime strings still translate via the embedded catalog"
        );
    }
}

#[cfg(all(not(unix), not(windows)))]
fn bind_platform_domain(_root: &Path) {}

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

/// Pick a non-"C" base locale so gettext honors `LANGUAGE`.
///
/// gettext only consults LANGUAGE outside the "C"/"POSIX" locale, and
/// since glibc 2.36 "C.UTF-8" counts as "C", so it cannot serve as the
/// neutral base on current systems. When the environment already
/// resolves to a real locale we keep it; otherwise probe common UTF-8
/// locales — the configured language's own first — ending on C.UTF-8,
/// which still works as a base on older glibc. On a system with no
/// generated UTF-8 locale at all and glibc >= 2.36, the override is
/// unreachable (documented limitation).
#[cfg(unix)]
unsafe fn apply_language_base_locale() {
    let env_locale = env::var("LC_ALL")
        .or_else(|_| env::var("LC_MESSAGES"))
        .or_else(|_| env::var("LANG"))
        .unwrap_or_default();
    let c_like = env_locale.is_empty()
        || env_locale == "C"
        || env_locale == "POSIX"
        || env_locale.starts_with("C.");
    if !c_like {
        // environment already gives a non-C base — honor it
        setlocale(LocaleCategory::LcAll, "");
        return;
    }
    for cand in candidate_locales() {
        if setlocale(LocaleCategory::LcAll, cand.as_str()).is_some() {
            return;
        }
    }
    log::warn!("no usable non-C locale found; language override may not apply");
}

/// Candidate base locales: the configured language's own UTF-8 locale
/// first (matches LANGUAGE exactly), then widely installed UTF-8
/// locales, C.UTF-8 last for older glibc where it still counts non-C.
#[cfg(unix)]
fn candidate_locales() -> Vec<String> {
    let mut v: Vec<String> = Vec::new();
    if let Some(lang) = configured_language() {
        v.push(format!("{lang}.UTF-8"));
    }
    for c in [
        "en_US.UTF-8",
        "en_US.utf8",
        "de_DE.UTF-8",
        "fr_FR.UTF-8",
        "zh_CN.UTF-8",
        "zh_CN.utf8",
        "C.UTF-8",
    ] {
        let s = c.to_string();
        if !v.contains(&s) {
            v.push(s);
        }
    }
    v
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
#[cfg(unix)]
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
        // exercise the same base-locale path as production: glibc >=
        // 2.36 treats C.UTF-8 as "C" and would silently ignore
        // LANGUAGE — the test caught this on ubuntu-24.04
        env::set_var("LANGUAGE", "zh_CN");
        #[cfg(unix)]
        unsafe {
            apply_language_base_locale();
            let dir = extract_translations();
            bindtextdomain(DOMAIN, &dir).unwrap();
            bind_textdomain_codeset(DOMAIN, "UTF-8").unwrap();
            textdomain(DOMAIN).unwrap();
        }
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
