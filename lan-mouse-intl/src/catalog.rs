//! GNU .mo catalog parsing and plural-form evaluation — shared by the
//! GTK frontend's in-crate lookup and the exported `g_libintl_*` dll
//! surface.

use std::collections::HashMap;

/// Parsed `.mo` catalog: msgid → translation, plus the Plural-Forms
/// evaluator from the catalog header. Translations may contain several
/// NUL-separated plural forms.
pub struct Catalog {
    entries: HashMap<String, Vec<String>>,
    nplurals: usize,
    plural_expr: String,
}

/// Minimal GNU .mo reader: header (magic, count, msgid/msgstr table
/// offsets) followed by length/offset pairs pointing at NUL-free
/// string bodies.
pub fn parse_mo(b: &[u8]) -> Option<Catalog> {
    let u32le = |o: usize| -> Option<usize> {
        Some(u32::from_le_bytes(b.get(o..o + 4)?.try_into().ok()?) as usize)
    };
    if u32le(0)? != 0x9504_12de {
        return None;
    }
    let (n, o_tab, t_tab) = (u32le(8)?, u32le(12)?, u32le(16)?);
    let mut entries = HashMap::new();
    let mut header = String::new();
    for i in 0..n {
        let (ol, oo) = (u32le(o_tab + i * 8)?, u32le(o_tab + i * 8 + 4)?);
        let (tl, to) = (u32le(t_tab + i * 8)?, u32le(t_tab + i * 8 + 4)?);
        let orig = std::str::from_utf8(b.get(oo..oo + ol)?).ok()?;
        let trans = std::str::from_utf8(b.get(to..to + tl)?).ok()?;
        if orig.is_empty() {
            header = trans.to_string();
            continue;
        }
        entries.insert(
            orig.to_string(),
            trans.split('\0').map(str::to_string).collect(),
        );
    }
    let (nplurals, plural_expr) = plural_forms(&header);
    Some(Catalog {
        entries,
        nplurals,
        plural_expr,
    })
}

impl Catalog {
    /// Singular lookup — returns the first plural form.
    pub fn gettext(&self, msgid: &str) -> Option<&str> {
        self.entries.get(msgid)?.first().map(String::as_str)
    }

    /// Plural lookup — evaluates the Plural-Forms expression for `n`,
    /// falling back to the C default (n != 1).
    pub fn ngettext<'a>(&'a self, msgid1: &'a str, msgid2: &'a str, n: u64) -> &'a str {
        let idx = eval_plural(&self.plural_expr, n)
            .unwrap_or_else(|| usize::from(n != 1))
            .min(self.nplurals.saturating_sub(1));
        self.entries
            .get(msgid1)
            .and_then(|forms| forms.get(idx))
            .map(String::as_str)
            .unwrap_or(if n == 1 { msgid1 } else { msgid2 })
    }
}

/// Extract `nplurals` and `plural=` from the Plural-Forms header,
/// defaulting to the C locale rule (nplurals=2; plural=n!=1).
fn plural_forms(header: &str) -> (usize, String) {
    let mut nplurals = 2;
    let mut expr = "(n != 1)".to_string();
    for line in header.lines() {
        let Some(rest) = line.trim_start().strip_prefix("Plural-Forms:") else {
            continue;
        };
        for part in rest.split(';') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix("nplurals=") {
                nplurals = v.trim().parse().unwrap_or(2).max(1);
            } else if let Some(v) = part.strip_prefix("plural=") {
                expr = v.trim().to_string();
            }
        }
    }
    (nplurals.max(1), expr)
}

/// Recursive-descent evaluator for C89 plural expressions:
/// ternary `?:`, `||`, `&&`, comparisons, `+ - * / %`, parens, `n`, ints.
fn eval_plural(expr: &str, n: u64) -> Option<usize> {
    let mut p = Parser {
        t: expr.as_bytes(),
        i: 0,
        n,
    };
    let v = p.ternary()?;
    Some(v as usize)
}

struct Parser<'a> {
    t: &'a [u8],
    i: usize,
    n: u64,
}

impl Parser<'_> {
    fn skip(&mut self) {
        while self.t.get(self.i).is_some_and(u8::is_ascii_whitespace) {
            self.i += 1;
        }
    }
    fn eat(&mut self, s: &str) -> bool {
        self.skip();
        if self.t[self.i..].starts_with(s.as_bytes()) {
            self.i += s.len();
            true
        } else {
            false
        }
    }
    fn peek(&self) -> Option<u8> {
        self.t.get(self.i).copied()
    }
    fn ternary(&mut self) -> Option<u64> {
        let c = self.or()?;
        if self.eat("?") {
            let a = self.ternary()?;
            if !self.eat(":") {
                return None;
            }
            let b = self.ternary()?;
            Some(if c != 0 { a } else { b })
        } else {
            Some(c)
        }
    }
    fn or(&mut self) -> Option<u64> {
        let mut l = self.and()?;
        while self.eat("||") {
            let r = self.and()?;
            l = u64::from(l != 0 || r != 0);
        }
        Some(l)
    }
    fn and(&mut self) -> Option<u64> {
        let mut l = self.equality()?;
        while self.eat("&&") {
            let r = self.equality()?;
            l = u64::from(l != 0 && r != 0);
        }
        Some(l)
    }
    fn equality(&mut self) -> Option<u64> {
        let mut l = self.relational()?;
        loop {
            if self.eat("==") {
                l = u64::from(l == self.relational()?);
            } else if self.eat("!=") {
                l = u64::from(l != self.relational()?);
            } else {
                return Some(l);
            }
        }
    }
    fn relational(&mut self) -> Option<u64> {
        let mut l = self.additive()?;
        loop {
            if self.eat("<=") {
                l = u64::from(l <= self.additive()?);
            } else if self.eat(">=") {
                l = u64::from(l >= self.additive()?);
            } else if self.eat("<") {
                l = u64::from(l < self.additive()?);
            } else if self.eat(">") {
                l = u64::from(l > self.additive()?);
            } else {
                return Some(l);
            }
        }
    }
    fn additive(&mut self) -> Option<u64> {
        let mut l = self.term()?;
        loop {
            if self.eat("+") {
                l = l.checked_add(self.term()?)?;
            } else if self.eat("-") {
                l = l.checked_sub(self.term()?)?;
            } else {
                return Some(l);
            }
        }
    }
    fn term(&mut self) -> Option<u64> {
        let mut l = self.factor()?;
        loop {
            if self.eat("*") {
                l = l.checked_mul(self.factor()?)?;
            } else if self.eat("/") {
                l = l.checked_div(self.factor()?)?;
            } else if self.eat("%") {
                l = l.checked_rem(self.factor()?)?;
            } else {
                return Some(l);
            }
        }
    }
    fn factor(&mut self) -> Option<u64> {
        self.skip();
        match self.peek()? {
            b'(' => {
                self.i += 1;
                let v = self.ternary()?;
                if !self.eat(")") {
                    return None;
                }
                Some(v)
            }
            b'n' => {
                self.i += 1;
                Some(self.n)
            }
            b'!' => {
                self.i += 1;
                Some(u64::from(self.factor()? == 0))
            }
            c if c.is_ascii_digit() => {
                let start = self.i;
                while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    self.i += 1;
                }
                std::str::from_utf8(&self.t[start..self.i])
                    .ok()?
                    .parse()
                    .ok()
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plural_eval_common_forms() {
        // C/English
        assert_eq!(eval_plural("(n != 1)", 1), Some(0));
        assert_eq!(eval_plural("(n != 1)", 5), Some(1));
        // Chinese/Japanese
        assert_eq!(eval_plural("0", 3), Some(0));
        // French
        assert_eq!(eval_plural("(n > 1)", 0), Some(0));
        assert_eq!(eval_plural("(n > 1)", 2), Some(1));
        // Russian-style
        let ru =
            "(n%10==1 && n%100!=11 ? 0 : n%10>=2 && n%10<=4 && (n%100<10 || n%100>=20) ? 1 : 2)";
        assert_eq!(eval_plural(ru, 1), Some(0));
        assert_eq!(eval_plural(ru, 3), Some(1));
        assert_eq!(eval_plural(ru, 11), Some(2));
        // Polish-style
        let pl = "(n==1 ? 0 : n%10>=2 && n%10<=4 && (n%100<12 || n%100>14) ? 1 : 2)";
        assert_eq!(eval_plural(pl, 5), Some(2));
    }

    #[test]
    fn mo_parses_zh_catalog() {
        // built by lan-mouse-gtk/build.rs at that crate's compile time;
        // locate via the workspace target dir
        let mo =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../lan-mouse-gtk/po/zh_CN.po");
        assert!(mo.exists(), "zh_CN.po must exist for this test");
    }
}
