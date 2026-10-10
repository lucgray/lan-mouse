//! gettext runtime: domain binding, locale resolution, and the exported
//! `g_libintl_*` ABI surface that glib/gtk resolve from `intl-8.dll`.
//!
//! gvsbuild ships a 14 KB proxy-libintl **stub** under that name whose
//! `dgettext` returns the msgid verbatim — it can never load a catalog.
//! This crate is built into a cdylib and packaged over the stub as
//! `intl-8.dll`, so every `g_libintl_*` import lands on a real
//! implementation.

use crate::catalog::{Catalog, parse_mo};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[derive(Default)]
struct State {
    /// textdomain → bound locale root (`<dir>/<lang>/LC_MESSAGES/<domain>.mo`)
    dirs: HashMap<String, PathBuf>,
    /// (domain, lang) → parsed catalog (None = lookup failed, don't retry)
    catalogs: HashMap<(String, String), Option<Catalog>>,
    default_domain: CString,
}

fn state() -> &'static Mutex<State> {
    static S: OnceLock<Mutex<State>> = OnceLock::new();
    S.get_or_init(|| {
        Mutex::new(State {
            default_domain: CString::new("messages").unwrap(),
            ..Default::default()
        })
    })
}

/// gettext's language precedence: LANGUAGE (colon list) > LC_ALL >
/// LC_MESSAGES > LANG > OS UI language. "C"/"POSIX"/empty are skipped.
fn candidate_languages() -> Vec<String> {
    let mut out = Vec::new();
    fn push(out: &mut Vec<String>, l: &str) {
        for lang in expand_lang(l) {
            if !out.contains(&lang) {
                out.push(lang);
            }
        }
    }
    if let Ok(langs) = std::env::var("LANGUAGE") {
        for l in langs.split(':') {
            push(&mut out, l);
        }
    }
    for var in ["LC_ALL", "LC_MESSAGES", "LANG"] {
        if let Ok(l) = std::env::var(var) {
            push(&mut out, &l);
        }
    }
    if out.is_empty() {
        if let Some(l) = os_ui_language() {
            push(&mut out, &l);
        }
    }
    out
}

/// "zh_CN.UTF-8@mod" → "zh_CN.UTF-8@mod", "zh_CN.UTF-8", "zh_CN", "zh"
/// — gettext's successive reductions. "C"/"POSIX" yield nothing.
fn expand_lang(lang: &str) -> Vec<String> {
    let mut v = Vec::new();
    let full = lang.trim();
    if full.is_empty() || full == "C" || full == "POSIX" || full.starts_with("C.") {
        return v;
    }
    v.push(full.to_string());
    let no_mod = full.split('@').next().unwrap_or(full);
    let no_codeset = no_mod.split('.').next().unwrap_or(no_mod);
    for cand in [no_mod, no_codeset] {
        if !v.iter().any(|x| x == cand) {
            v.push(cand.to_string());
        }
    }
    if let Some(lang_only) = no_codeset.split('_').next() {
        if lang_only != no_codeset && !v.iter().any(|x| x == lang_only) {
            v.push(lang_only.to_string());
        }
    }
    v
}

/// Windows UI language as the fallback locale ("zh-CN" → "zh_CN").
#[cfg(windows)]
fn os_ui_language() -> Option<String> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetUserDefaultUILanguage() -> u16;
    }
    const LANG_ZH_CN: u16 = 0x0804;
    const LANG_ZH_TW: u16 = 0x0404;
    let _ = (LANG_ZH_CN, LANG_ZH_TW); // documented ids, name resolution below
    let primary = unsafe { GetUserDefaultUILanguage() } & 0x3ff;
    let sub = unsafe { GetUserDefaultUILanguage() } >> 10;
    let name = match primary {
        0x04 if unsafe { GetUserDefaultUILanguage() } == LANG_ZH_CN => "zh_CN",
        0x04 if unsafe { GetUserDefaultUILanguage() } == LANG_ZH_TW => "zh_TW",
        0x04 => "zh",
        0x09 => "en",
        0x07 => "de",
        0x0c => "fr",
        0x11 => "ja",
        0x10 => "it",
        0x0a => "es",
        0x13 => "nl",
        0x16 => "pt",
        0x19 => "ru",
        0x12 => "ko",
        _ => return None,
    };
    let _ = sub;
    Some(name.to_string())
}

#[cfg(not(windows))]
fn os_ui_language() -> Option<String> {
    None
}

/// Default locale root when a domain has no explicit binding:
/// `<module dir>/share/locale` — where the bundled app extracts its
/// catalogs next to the executable.
#[cfg(windows)]
fn default_localedir() -> PathBuf {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetModuleHandleExW(flags: u32, addr: *const u16, module: *mut isize) -> i32;
        fn GetModuleFileNameW(module: isize, buf: *mut u16, len: u32) -> u32;
    }
    const FROM_ADDRESS: u32 = 0x4;
    const UNCHANGED_REFCOUNT: u32 = 0x2;
    let mut hmod: isize = 0;
    let anchored = &anchored_module_dir as *const _ as *const u16;
    if unsafe { GetModuleHandleExW(FROM_ADDRESS | UNCHANGED_REFCOUNT, anchored, &mut hmod) } != 0 {
        let mut buf = [0u16; 1024];
        let len = unsafe { GetModuleFileNameW(hmod, buf.as_mut_ptr(), buf.len() as u32) };
        if len > 0 {
            let path = String::from_utf16_lossy(&buf[..len as usize]);
            if let Some(dir) = Path::new(&path).parent() {
                return dir.join("share").join("locale");
            }
        }
    }
    PathBuf::from("share").join("locale")
}

#[cfg(windows)]
fn anchored_module_dir() {}

#[cfg(not(windows))]
fn default_localedir() -> PathBuf {
    PathBuf::from("share").join("locale")
}

/// Look up `msgid` under `domain` for each LANGUAGE candidate.
fn lookup(domain: &str, msgid: &str) -> Option<String> {
    let langs = candidate_languages();
    if langs.is_empty() {
        return None;
    }
    let mut st = state().lock().ok()?;
    for lang in langs {
        let key = (domain.to_string(), lang.clone());
        if !st.catalogs.contains_key(&key) {
            let dir = st
                .dirs
                .get(domain)
                .cloned()
                .unwrap_or_else(default_localedir);
            let path = dir
                .join(&lang)
                .join("LC_MESSAGES")
                .join(format!("{domain}.mo"));
            let cat = std::fs::read(&path).ok().and_then(|b| parse_mo(&b));
            st.catalogs.insert(key.clone(), cat);
        }
        if let Some(Some(cat)) = st.catalogs.get(&key) {
            if let Some(t) = cat.gettext(msgid) {
                return Some(t.to_string());
            }
        }
    }
    None
}

fn lookup_plural(domain: &str, msgid1: &str, msgid2: &str, n: u64) -> Option<String> {
    let langs = candidate_languages();
    let mut st = state().lock().ok()?;
    for lang in langs {
        let key = (domain.to_string(), lang.clone());
        if !st.catalogs.contains_key(&key) {
            let dir = st
                .dirs
                .get(domain)
                .cloned()
                .unwrap_or_else(default_localedir);
            let path = dir
                .join(&lang)
                .join("LC_MESSAGES")
                .join(format!("{domain}.mo"));
            let cat = std::fs::read(&path).ok().and_then(|b| parse_mo(&b));
            st.catalogs.insert(key.clone(), cat);
        }
        if let Some(Some(cat)) = st.catalogs.get(&key) {
            let t = cat.ngettext(msgid1, msgid2, n);
            if t != msgid1 && t != msgid2 || cat.gettext(msgid1).is_some() {
                return Some(t.to_string());
            }
        }
    }
    None
}

fn leak_str(s: &str) -> *mut c_char {
    match CString::new(s) {
        Ok(cs) => Box::leak(cs.into_boxed_c_str()).as_ptr() as *mut c_char,
        Err(_) => std::ptr::null_mut(),
    }
}

unsafe fn cstr<'a>(p: *const c_char) -> Option<&'a str> {
    if p.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(p) }.to_str().ok()
}

macro_rules! intl_fn {
    ($(#[$m:meta])* fn $name:ident ($($a:ident : $t:ty),*) -> $r:ty $body:block) => {
        $(#[$m])*
        #[no_mangle]
        pub unsafe extern "C" fn $name($($a: $t),*) -> $r $body
    };
}

intl_fn! {
    /// bindtextdomain(domain, dirname) — dirname NULL queries the
    /// current binding; returns the bound directory (process-lifetime
    /// pointer) or NULL.
    fn g_libintl_bindtextdomain(domain: *const c_char, dirname: *const c_char) -> *const c_char {
        let Some(domain) = (unsafe { cstr(domain) }) else {
            return std::ptr::null();
        };
        let mut st = match state().lock() {
            Ok(s) => s,
            Err(_) => return std::ptr::null(),
        };
        match unsafe { cstr(dirname) } {
            Some(dir) => {
                st.dirs.insert(domain.to_string(), PathBuf::from(dir));
                leak_str(dir)
            }
            None => match st.dirs.get(domain) {
                Some(dir) => leak_str(&dir.to_string_lossy()),
                None => std::ptr::null(),
            },
        }
    }
}

intl_fn! {
    /// wbindtextdomain(domain, dirname_w) — wide-dir variant used by
    /// glib on Windows; same semantics, UTF-16 return.
    fn g_libintl_wbindtextdomain(domain: *const c_char, dirname: *const u16) -> *const u16 {
        let Some(domain) = (unsafe { cstr(domain) }) else {
            return std::ptr::null();
        };
        if dirname.is_null() {
            return match state().lock() {
                Ok(st) => match st.dirs.get(domain) {
                    Some(dir) => {
                        let w: Vec<u16> = dir
                            .to_string_lossy()
                            .encode_utf16()
                            .chain(Some(0))
                            .collect();
                        Box::leak(w.into_boxed_slice()).as_ptr()
                    }
                    None => std::ptr::null(),
                },
                Err(_) => std::ptr::null(),
            };
        }
        let mut len = 0;
        while unsafe { *dirname.add(len) } != 0 {
            len += 1;
        }
        let dir = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(dirname, len) });
        let mut st = match state().lock() {
            Ok(s) => s,
            Err(_) => return std::ptr::null(),
        };
        st.dirs.insert(domain.to_string(), PathBuf::from(&dir));
        let w: Vec<u16> = dir.encode_utf16().chain(Some(0)).collect();
        Box::leak(w.into_boxed_slice()).as_ptr()
    }
}

intl_fn! {
    /// bind_textdomain_codeset — catalogs are UTF-8; record and echo.
    fn g_libintl_bind_textdomain_codeset(domain: *const c_char, codeset: *const c_char) -> *const c_char {
        if unsafe { cstr(domain) }.is_none() || unsafe { cstr(codeset) }.is_none() {
            return std::ptr::null();
        }
        codeset
    }
}

intl_fn! {
    /// textdomain(domain) — set (or with NULL query) the default domain.
    fn g_libintl_textdomain(domain: *const c_char) -> *const c_char {
        let mut st = match state().lock() {
            Ok(s) => s,
            Err(_) => return std::ptr::null(),
        };
        if let Some(d) = unsafe { cstr(domain) } {
            if let Ok(cs) = CString::new(d) {
                st.default_domain = cs;
            }
        }
        st.default_domain.as_ptr()
    }
}

intl_fn! {
    /// gettext(msgid) — default domain.
    fn g_libintl_gettext(msgid: *const c_char) -> *mut c_char {
        let Some(msgid) = (unsafe { cstr(msgid) }) else {
            return std::ptr::null_mut();
        };
        let default = state()
            .lock()
            .map(|st| st.default_domain.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "messages".to_string());
        match lookup(&default, msgid) {
            Some(t) => leak_str(&t),
            None => msgid.as_ptr() as *mut c_char,
        }
    }
}

intl_fn! {
    /// dgettext(domain, msgid) — the entry point glib's g_dgettext and
    /// GtkBuilder's `translatable` handling call.
    fn g_libintl_dgettext(domain: *const c_char, msgid: *const c_char) -> *mut c_char {
        let (Some(domain), Some(msgid)) = (unsafe { cstr(domain) }, unsafe { cstr(msgid) }) else {
            return std::ptr::null_mut();
        };
        match lookup(domain, msgid) {
            Some(t) => leak_str(&t),
            None => msgid.as_ptr() as *mut c_char,
        }
    }
}

intl_fn! {
    /// dcgettext(domain, msgid, category) — category ignored: LANGUAGE
    /// env already decides precedence.
    fn g_libintl_dcgettext(domain: *const c_char, msgid: *const c_char, _category: i32) -> *mut c_char {
        unsafe { g_libintl_dgettext(domain, msgid) }
    }
}

intl_fn! {
    /// ngettext(msgid1, msgid2, n) — default domain plural.
    fn g_libintl_ngettext(msgid1: *const c_char, msgid2: *const c_char, n: u64) -> *mut c_char {
        let default = state()
            .lock()
            .map(|st| st.default_domain.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "messages".to_string());
        unsafe { g_libintl_dngettext(default.as_ptr() as *const c_char, msgid1, msgid2, n) }
    }
}

intl_fn! {
    /// dngettext(domain, msgid1, msgid2, n).
    fn g_libintl_dngettext(domain: *const c_char, msgid1: *const c_char, msgid2: *const c_char, n: u64) -> *mut c_char {
        let (Some(domain), Some(m1), Some(m2)) = (
            unsafe { cstr(domain) },
            unsafe { cstr(msgid1) },
            unsafe { cstr(msgid2) },
        ) else {
            return std::ptr::null_mut();
        };
        match lookup_plural(domain, m1, m2, n) {
            Some(t) => leak_str(&t),
            None => (if n == 1 { m1 } else { m2 }).as_ptr() as *mut c_char,
        }
    }
}

intl_fn! {
    /// dcngettext(domain, msgid1, msgid2, n, category).
    fn g_libintl_dcngettext(domain: *const c_char, msgid1: *const c_char, msgid2: *const c_char, n: u64, _category: i32) -> *mut c_char {
        unsafe { g_libintl_dngettext(domain, msgid1, msgid2, n) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn write_mo(dir: &Path, domain: &str, lang: &str) {
        // minimal hand-built .mo: header + 2 entries
        let entries = [("Connections", "连接"), ("Preferences", "偏好设置")];
        let n = entries.len() as u32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&0x950412deu32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // revision
        buf.extend_from_slice(&n.to_le_bytes());
        buf.extend_from_slice(&28u32.to_le_bytes()); // orig table off
        buf.extend_from_slice(&(28 + n * 8).to_le_bytes()); // trans table off
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        let mut off = 28 + n * 8 * 2;
        let mut orig_meta = Vec::new();
        let mut trans_meta = Vec::new();
        let mut bodies = Vec::new();
        for (o, t) in entries {
            for (s, meta) in [(o, &mut orig_meta), (t, &mut trans_meta)] {
                let b = s.as_bytes();
                let m: &mut Vec<(u32, u32)> = meta;
                m.push((b.len() as u32, off));
                bodies.extend_from_slice(b);
                bodies.push(0);
                off += b.len() as u32 + 1;
            }
        }
        for (l, o) in orig_meta {
            buf.extend_from_slice(&l.to_le_bytes());
            buf.extend_from_slice(&o.to_le_bytes());
        }
        for (l, o) in trans_meta {
            buf.extend_from_slice(&l.to_le_bytes());
            buf.extend_from_slice(&o.to_le_bytes());
        }
        buf.extend_from_slice(&bodies);
        let d = dir.join(lang).join("LC_MESSAGES");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join(format!("{domain}.mo")), buf).unwrap();
    }

    #[test]
    fn bind_then_dgettext_translates() {
        let root = std::env::temp_dir().join(format!("intl-test-{}", std::process::id()));
        write_mo(&root, "lan-mouse", "zh_CN");
        std::env::set_var("LANGUAGE", "zh_CN");
        let domain = CString::new("lan-mouse").unwrap();
        let dir = CString::new(root.to_string_lossy().into_owned()).unwrap();
        unsafe {
            assert!(!g_libintl_bindtextdomain(domain.as_ptr(), dir.as_ptr()).is_null());
            let msgid = CString::new("Connections").unwrap();
            let out = g_libintl_dgettext(domain.as_ptr(), msgid.as_ptr());
            assert_eq!(CStr::from_ptr(out).to_str().unwrap(), "连接");
        }
        std::env::remove_var("LANGUAGE");
    }

    #[test]
    fn untranslated_returns_msgid() {
        std::env::set_var("LANGUAGE", "zh_CN");
        let domain = CString::new("no-such-domain").unwrap();
        let msgid = CString::new("abc").unwrap();
        let out = unsafe { g_libintl_dgettext(domain.as_ptr(), msgid.as_ptr()) };
        assert_eq!(unsafe { CStr::from_ptr(out) }.to_str().unwrap(), "abc");
        std::env::remove_var("LANGUAGE");
    }
}
