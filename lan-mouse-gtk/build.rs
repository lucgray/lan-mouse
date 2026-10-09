use std::{env, fs, path::Path};

fn main() {
    // composite_templates
    glib_build_tools::compile_resources(
        &["resources"],
        "resources/resources.gresource.xml",
        "lan-mouse.gresource",
    );

    compile_translations();
}

/// Compile every `po/<lang>.po` into an embedded `.mo` catalog and a
/// generated `languages.rs` listing them, so the binary is self-contained
/// and works without installing locale files.
fn compile_translations() {
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR");
    let out_dir = Path::new(&out_dir);
    let i18n_dir = out_dir.join("i18n");
    fs::create_dir_all(&i18n_dir).expect("create i18n output dir");

    println!("cargo:rerun-if-changed=po");

    let mut code = String::from("const LANGUAGES: &[(&str, &[u8])] = &[\n");
    let po_dir = Path::new("po");
    if po_dir.is_dir() {
        for entry in fs::read_dir(po_dir).expect("read po dir") {
            let path = entry.expect("po dir entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("po") {
                continue;
            }
            let lang = path
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("po file name")
                .to_string();
            let catalog = polib::po_file::parse(&path)
                .unwrap_or_else(|e| panic!("invalid translation catalog {}: {e}", path.display()));
            let mo_path = i18n_dir.join(format!("{lang}.mo"));
            polib::mo_file::write(&catalog, &mo_path)
                .unwrap_or_else(|e| panic!("failed to compile {}: {e}", path.display()));
            code.push_str(&format!(
                "    (\"{lang}\", include_bytes!(\"{}\")),\n",
                mo_path.display()
            ));
        }
    }
    code.push_str("];\n");
    fs::write(out_dir.join("languages.rs"), code).expect("write languages.rs");
}
