#[cfg(feature = "native")]
fn native() {
    use sha2::{Digest as _, Sha256};
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};

    fn digest(path: &Path) -> String {
        Sha256::digest(fs::read(path).expect("read native provenance input"))
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned();
    let target = env::var("TARGET").unwrap();
    assert_eq!(
        target,
        env::var("HOST").unwrap(),
        "BitTorrent needs a native build for the target ABI"
    );
    let override_dir = env::var_os("ARIAX_BT_NATIVE_DIR");
    let prefix = override_dir.clone().map(PathBuf::from).unwrap_or_else(|| {
        root.join("toolchains/bt-native")
            .join(&target)
            .join("install")
    });
    println!("cargo:rerun-if-env-changed=ARIAX_BT_NATIVE_DIR");
    for path in [
        "native/libtorrent/sources.json",
        "native/libtorrent/ariax.patch",
        "scripts/bt_native.py",
    ] {
        println!("cargo:rerun-if-changed={}", root.join(path).display());
    }
    // Provisioning is explicit in CI. Cargo must never trigger an unexpected
    // native source download or a full dependency rebuild on a workstation.
    let manifest_path = prefix.join("ariax-native.json");
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&manifest_path).expect("provision native dependencies with scripts/bt_native.py"),
    )
    .expect("valid native provenance manifest");
    assert_eq!(manifest["target"], target, "native ABI mismatch");
    for (key, path) in [
        ("sourcesSha256", "native/libtorrent/sources.json"),
        ("patchSha256", "native/libtorrent/ariax.patch"),
        ("builderSha256", "scripts/bt_native.py"),
    ] {
        assert_eq!(
            manifest["inputs"][key],
            digest(&root.join(path)),
            "stale native build: {key}"
        );
    }
    for (name, expected) in manifest["files"]
        .as_object()
        .expect("native file inventory")
    {
        assert!(
            !name.contains(['\\', ':'])
                && Path::new(name)
                    .components()
                    .all(|part| matches!(part, std::path::Component::Normal(_))),
            "unsafe native inventory path"
        );
        let path = prefix.join(name);
        assert_eq!(
            expected.as_str().unwrap(),
            digest(&path),
            "modified native file: {name}"
        );
        println!("cargo:rerun-if-changed={}", path.display());
    }
    println!("cargo:rerun-if-changed={}", manifest_path.display());
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/bridge.cc");
    println!("cargo:rerun-if-changed=include/bridge.h");
    let mut bridge = cxx_build::bridge("src/lib.rs");
    bridge
        .file("src/bridge.cc")
        .include("include")
        .include(prefix.join("include"))
        .std("c++17")
        .define("TORRENT_ABI_VERSION", "2")
        .define("TORRENT_USE_ASSERTS", "0")
        .define("BOOST_ASIO_ENABLE_CANCELIO", None)
        .define("BOOST_ASIO_NO_DEPRECATED", None)
        .define("BOOST_SYSTEM_USE_UTF8", None)
        .define("TORRENT_USE_OPENSSL", None)
        .define("TORRENT_USE_LIBCRYPTO", None)
        .define("TORRENT_SSL_PEERS", None)
        .define("OPENSSL_NO_SSL2", None)
        .define("OPENSSL_NO_SSL3", None)
        .define("OPENSSL_NO_TLS1", None)
        .define("OPENSSL_NO_TLS1_1", None)
        .define("OPENSSL_NO_DTLS1", None)
        .define("TORRENT_USE_I2P", "0")
        .define("TORRENT_USE_RTC", "0")
        .define("TORRENT_DISABLE_LOGGING", None)
        .define("TORRENT_DISABLE_MUTABLE_TORRENTS", None);
    if target.contains("windows") {
        bridge
            .define("_WIN32_WINNT", "0x0A00")
            .define("WIN32_LEAN_AND_MEAN", None);
    }
    if target.ends_with("msvc") {
        bridge
            .define("BOOST_ALL_NO_LIB", None)
            .flag("/EHsc")
            .flag("/bigobj");
    }
    bridge.compile("ariax_bt_bridge");
    println!(
        "cargo:rustc-link-search=native={}",
        prefix.join("lib").display()
    );
    println!("cargo:rustc-link-lib=static=torrent-rasterbar");
    if target.ends_with("msvc") {
        println!("cargo:rustc-link-lib=static=libssl");
        println!("cargo:rustc-link-lib=static=libcrypto");
    } else {
        println!("cargo:rustc-link-lib=static=ssl");
        println!("cargo:rustc-link-lib=static=crypto");
    }
    if target.contains("windows") {
        for library in [
            "bcrypt", "mswsock", "ws2_32", "iphlpapi", "crypt32", "advapi32", "user32",
        ] {
            println!("cargo:rustc-link-lib={library}");
        }
    } else if target.contains("linux") {
        println!("cargo:rustc-link-lib=dl");
        println!("cargo:rustc-link-lib=pthread");
    }
}

fn main() {
    #[cfg(feature = "native")]
    native();
}
