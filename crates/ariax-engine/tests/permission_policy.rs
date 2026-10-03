#![forbid(unsafe_code)]

use ariax_engine::{Engine, HttpControlError, HttpProcessResources};
use ariax_runtime::RuntimeProfile;
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "../benches/rpc_active_profile/setup.rs"]
mod setup;

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "ariax-engine-permissions-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o777)).unwrap();
        }
        Self(root)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedding_defaults_to_shared_state_and_strict_builder_rejects_it() {
    let root = Root::new();
    let control = root.0.join("control");
    let journals = control.join("http-journals");
    fs::create_dir_all(&journals).unwrap();
    #[cfg(unix)]
    for directory in [&control, &journals] {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o777)).unwrap();
    }
    let engine = Engine::builder()
        .output_root(root.0.join("output"))
        .control_directory(&control)
        .database_path(root.0.join("session.db"))
        .build()
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    #[cfg(unix)]
    for directory in [&control, &journals] {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(directory).unwrap().permissions().mode() & 0o777,
            0o777
        );
    }
    #[cfg(windows)]
    for directory in [&control, &journals] {
        assert!(ariax_windows_security::verify_private_directory(directory).is_err());
    }
    let strict = Engine::builder()
        .output_root(root.0.join("output"))
        .control_directory(root.0.join("control"))
        .database_path(root.0.join("session.db"))
        .require_private_permissions(true)
        .build()
        .await;
    assert!(strict.is_err());
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_builder_rejects_linked_control_directory() {
    let root = Root::new();
    let target = root.0.join("target");
    let control = root.0.join("control");
    fs::create_dir(&target).unwrap();
    std::os::unix::fs::symlink(&target, &control).unwrap();
    let result = Engine::builder()
        .output_root(root.0.join("output"))
        .control_directory(control)
        .build()
        .await;
    assert!(result.is_err());
    assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn permission_policy_cannot_be_changed_by_rpc_or_persisted_as_task_option() {
    let root = Root::new();
    let resources = HttpProcessResources::for_profile(RuntimeProfile::Concurrency).unwrap();
    let (mut plane, _) = setup::build_control_plane(&root.0, &resources, 32, false).unwrap();
    for value in [false, true] {
        let error = plane
            .call(
                "aria2.changeGlobalOption",
                json!([{"require-private-permissions": value}]),
            )
            .unwrap_err();
        assert!(matches!(error, HttpControlError::OptionPatchRejected(_)));
    }
    assert!(!ariax_config::persisted_option_is_safe(
        "require-private-permissions"
    ));
    plane.shutdown_async().await.unwrap();
}
