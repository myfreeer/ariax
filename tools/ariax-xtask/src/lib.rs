#![forbid(unsafe_code)]

//! Repository maintenance tasks that must remain deterministic and testable.

mod config_contracts;
mod core_contracts;
mod inventory;
mod journal_contracts;
mod runtime_contracts;
mod storage_contracts;

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use config_contracts::generate_config_contracts;
use core_contracts::generate_core_contracts;
use inventory::{GenerationMode, generate_aria2_inventory};
use journal_contracts::generate_journal_contracts;
use runtime_contracts::generate_runtime_contracts;
use storage_contracts::generate_storage_contracts;

/// Canonical upstream repository for the compatibility reference.
pub const ARIA2_REPOSITORY: &str = "https://github.com/aria2/aria2";

/// aria2 files required by the first compatibility inventory generator.
pub const REQUIRED_ARIA2_PATHS: &[&str] = &[
    "src/OptionHandlerFactory.cc",
    "src/prefs.cc",
    "src/OptionHandlerImpl.h",
    "src/OptionHandlerImpl.cc",
    "src/usage_text.h",
    "src/help_tags.h",
    "src/help_tags.cc",
    "src/RpcMethodFactory.cc",
    "src/RpcMethodImpl.h",
    "src/RpcMethodImpl.cc",
    "src/RpcMethod.cc",
    "doc/manual-src/en/aria2c.rst",
    "test/RpcMethodTest.cc",
    "test/RpcResponseTest.cc",
    "test/RpcHelperTest.cc",
];

const PIN_PATH: &str = "compat/aria2-reference.pin";

/// The immutable aria2 source reference used for compatibility generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Aria2Reference {
    /// Pin-file schema version.
    pub schema: u32,
    /// Canonical source repository URL.
    pub repository: String,
    /// Exact source commit.
    pub commit: String,
}

impl Aria2Reference {
    /// Parses the strict bootstrap `key=value` pin format.
    pub fn parse(input: &str) -> Result<Self, String> {
        let mut schema = None;
        let mut repository = None;
        let mut commit = None;

        for (index, line) in input.lines().enumerate() {
            if line.is_empty() {
                continue;
            }
            if line.trim() != line {
                return Err(format!(
                    "line {} contains surrounding whitespace",
                    index + 1
                ));
            }
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("line {} is not a key=value pair", index + 1))?;
            if key.is_empty() || value.is_empty() || value.contains('=') {
                return Err(format!("line {} has a malformed key or value", index + 1));
            }
            match key {
                "schema" => set_once(
                    &mut schema,
                    value
                        .parse::<u32>()
                        .map_err(|_| format!("line {} has an invalid schema", index + 1))?,
                    key,
                )?,
                "repository" => set_once(&mut repository, value.to_owned(), key)?,
                "commit" => set_once(&mut commit, value.to_owned(), key)?,
                _ => return Err(format!("line {} has unknown key {key}", index + 1)),
            }
        }

        let reference = Self {
            schema: schema.ok_or_else(|| "missing schema".to_owned())?,
            repository: repository.ok_or_else(|| "missing repository".to_owned())?,
            commit: commit.ok_or_else(|| "missing commit".to_owned())?,
        };
        if reference.schema != 1 {
            return Err(format!("unsupported schema {}", reference.schema));
        }
        if reference.repository != ARIA2_REPOSITORY {
            return Err(format!(
                "repository must be the canonical upstream {ARIA2_REPOSITORY}"
            ));
        }
        if reference.commit.len() != 40
            || !reference
                .commit
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("commit must be exactly 40 lowercase hexadecimal characters".to_owned());
        }
        Ok(reference)
    }

    /// Loads and parses a reference pin.
    pub fn load(path: &Path) -> Result<Self, String> {
        let input = fs::read_to_string(path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        Self::parse(&input)
    }
}

/// Executes an xtask command and returns its standard-output line.
pub fn execute<I, S>(arguments: I, workspace_root: &Path) -> Result<String, String>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let command = parse_command(arguments)?;
    let reference = Aria2Reference::load(&workspace_root.join(PIN_PATH))?;

    match command {
        XtaskCommand::VerifyAria2 { source_dir } => {
            let source_dir = source_dir.unwrap_or_else(|| default_aria2_source(workspace_root));
            verify_aria2_checkout(&reference, &source_dir)?;
            Ok(format!(
                "verified aria2 {} at {}",
                reference.commit,
                source_dir.display()
            ))
        }
        XtaskCommand::Generate { source_dir, check } => {
            let source_dir = source_dir.unwrap_or_else(|| default_aria2_source(workspace_root));
            verify_aria2_checkout(&reference, &source_dir)?;
            let mode = if check {
                GenerationMode::Check
            } else {
                GenerationMode::Write
            };
            let (inventory, upstream_options) =
                generate_aria2_inventory(workspace_root, &source_dir, &reference, mode)?;
            let core = generate_core_contracts(workspace_root, mode)?;
            let config =
                generate_config_contracts(workspace_root, &reference, &upstream_options, mode)?;
            let storage = generate_storage_contracts(workspace_root, mode)?;
            let journal = generate_journal_contracts(workspace_root, mode)?;
            let runtime = generate_runtime_contracts(workspace_root, mode)?;
            Ok(format!(
                "{inventory}; {core}; {config}; {storage}; {journal}; {runtime}"
            ))
        }
        XtaskCommand::VerifyContracts { source_dir } => {
            let source_dir = source_dir.unwrap_or_else(|| default_aria2_source(workspace_root));
            verify_aria2_checkout(&reference, &source_dir)?;
            let (inventory, upstream_options) = generate_aria2_inventory(
                workspace_root,
                &source_dir,
                &reference,
                GenerationMode::Check,
            )?;
            let core = generate_core_contracts(workspace_root, GenerationMode::Check)?;
            let config = generate_config_contracts(
                workspace_root,
                &reference,
                &upstream_options,
                GenerationMode::Check,
            )?;
            let storage = generate_storage_contracts(workspace_root, GenerationMode::Check)?;
            let journal = generate_journal_contracts(workspace_root, GenerationMode::Check)?;
            let runtime = generate_runtime_contracts(workspace_root, GenerationMode::Check)?;
            Ok(format!(
                "{inventory}; {core}; {config}; {storage}; {journal}; {runtime}"
            ))
        }
        XtaskCommand::PrintAria2Pin => Ok(reference.commit),
        XtaskCommand::PrintAria2Repository => Ok(reference.repository),
    }
}

fn default_aria2_source(workspace_root: &Path) -> PathBuf {
    workspace_root
        .parent()
        .unwrap_or(workspace_root)
        .join("aria2")
}

/// Verifies that a checkout matches the pin and contains every extraction input.
pub fn verify_aria2_checkout(reference: &Aria2Reference, source_dir: &Path) -> Result<(), String> {
    let actual_head = git_output(source_dir, &["rev-parse", "HEAD"])?;
    if actual_head != reference.commit {
        return Err(format!(
            "aria2 HEAD {actual_head} does not match pinned commit {}",
            reference.commit
        ));
    }

    let object_type = git_output(source_dir, &["cat-file", "-t", &reference.commit])?;
    if object_type != "commit" {
        return Err(format!(
            "pinned aria2 object {} is {object_type}, not a commit",
            reference.commit
        ));
    }
    for path in REQUIRED_ARIA2_PATHS {
        git_success(
            source_dir,
            &["cat-file", "-e", &format!("{}:{path}", reference.commit)],
        )?;
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum XtaskCommand {
    VerifyAria2 {
        source_dir: Option<PathBuf>,
    },
    Generate {
        source_dir: Option<PathBuf>,
        check: bool,
    },
    VerifyContracts {
        source_dir: Option<PathBuf>,
    },
    PrintAria2Pin,
    PrintAria2Repository,
}

fn parse_command<I, S>(arguments: I) -> Result<XtaskCommand, String>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let mut arguments = arguments.into_iter().map(Into::into);
    let command = arguments
        .next()
        .ok_or_else(|| "expected an xtask command".to_owned())?;
    match command.to_str() {
        Some("verify-aria2") => {
            let source_dir = arguments.next().map(PathBuf::from);
            if arguments.next().is_some() {
                return Err("verify-aria2 accepts at most one source path".to_owned());
            }
            Ok(XtaskCommand::VerifyAria2 { source_dir })
        }
        Some("generate") => parse_generate(arguments),
        Some("verify-contracts") => {
            let source_dir = arguments.next().map(PathBuf::from);
            if arguments.next().is_some() {
                return Err("verify-contracts accepts at most one source path".to_owned());
            }
            Ok(XtaskCommand::VerifyContracts { source_dir })
        }
        Some("print-aria2-pin") => parse_argumentless(arguments, XtaskCommand::PrintAria2Pin),
        Some("print-aria2-repository") => {
            parse_argumentless(arguments, XtaskCommand::PrintAria2Repository)
        }
        Some(other) => Err(format!("unknown command {other}")),
        None => Err("command is not valid UTF-8".to_owned()),
    }
}

fn parse_generate(mut arguments: impl Iterator<Item = OsString>) -> Result<XtaskCommand, String> {
    let mut check = false;
    let mut source_dir = None;
    for argument in arguments.by_ref() {
        if argument == "--check" {
            if check {
                return Err("generate accepts --check only once".to_owned());
            }
            check = true;
        } else if source_dir.replace(PathBuf::from(argument)).is_some() {
            return Err("generate accepts at most one source path".to_owned());
        }
    }
    Ok(XtaskCommand::Generate { source_dir, check })
}

fn parse_argumentless(
    mut arguments: impl Iterator<Item = OsString>,
    command: XtaskCommand,
) -> Result<XtaskCommand, String> {
    if arguments.next().is_some() {
        return Err("print commands accept no arguments".to_owned());
    }
    Ok(command)
}

fn set_once<T>(slot: &mut Option<T>, value: T, key: &str) -> Result<(), String> {
    if slot.replace(value).is_some() {
        return Err(format!("duplicate key {key}"));
    }
    Ok(())
}

fn git_output(source_dir: &Path, arguments: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(source_dir)
        .args(arguments)
        .output()
        .map_err(|error| format!("failed to run git: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout)
        .map(|output| output.trim().to_owned())
        .map_err(|error| format!("git output was not UTF-8: {error}"))
}

fn git_success(source_dir: &Path, arguments: &[&str]) -> Result<(), String> {
    git_output(source_dir, arguments).map(|_| ())
}

fn git_blob(source_dir: &Path, commit: &str, path: &str) -> Result<String, String> {
    let object = format!("{commit}:{path}");
    let output = Command::new("git")
        .arg("-C")
        .arg(source_dir)
        .args(["show", &object])
        .output()
        .map_err(|error| format!("failed to run git: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git show {object} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| format!("git show {object} output was not UTF-8: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        ARIA2_REPOSITORY, Aria2Reference, REQUIRED_ARIA2_PATHS, execute, verify_aria2_checkout,
    };
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

    const VALID: &str = "schema=1\nrepository=https://github.com/aria2/aria2\ncommit=9e7273583f83e881e3ec067b523ba88724088d2f\n";

    #[test]
    fn reference_pin_parses() {
        let reference = Aria2Reference::parse(VALID).expect("valid reference");
        assert_eq!(reference.schema, 1);
        assert_eq!(reference.repository, ARIA2_REPOSITORY);
        assert_eq!(reference.commit, "9e7273583f83e881e3ec067b523ba88724088d2f");
    }

    #[test]
    fn reference_pin_rejects_invalid_commits() {
        let uppercase = VALID.replace("9e727", "9E727");
        assert!(Aria2Reference::parse(&uppercase).is_err());
        let short = VALID.replace("9e7273583f83e881e3ec067b523ba88724088d2f", "9e727358");
        assert!(Aria2Reference::parse(&short).is_err());
    }

    #[test]
    fn reference_pin_rejects_schema_and_key_errors() {
        assert!(Aria2Reference::parse(&VALID.replace("schema=1\n", "")).is_err());
        assert!(Aria2Reference::parse(&VALID.replace("schema=1", "schema=2")).is_err());
        assert!(Aria2Reference::parse(&format!("{VALID}unknown=value\n")).is_err());
        assert!(Aria2Reference::parse(&format!("{VALID}schema=1\n")).is_err());
    }

    #[test]
    fn reference_pin_rejects_malformed_values_and_repository() {
        assert!(Aria2Reference::parse(&VALID.replace("schema=1", " schema=1")).is_err());
        assert!(Aria2Reference::parse(&VALID.replace("schema=1", "schema=1=extra")).is_err());
        assert!(
            Aria2Reference::parse(
                &VALID.replace(ARIA2_REPOSITORY, "https://example.invalid/aria2")
            )
            .is_err()
        );
    }

    #[test]
    fn extraction_paths_are_unique() {
        let mut paths = REQUIRED_ARIA2_PATHS.to_vec();
        paths.sort_unstable();
        paths.dedup();
        assert_eq!(paths.len(), REQUIRED_ARIA2_PATHS.len());
    }

    #[test]
    fn verifier_accepts_matching_checkout_and_default_path() {
        let fixture = Fixture::new(None);
        let reference = fixture.reference();
        verify_aria2_checkout(&reference, &fixture.aria2).expect("matching checkout");

        let output = execute(["verify-aria2"], &fixture.workspace).expect("default checkout");
        assert!(output.contains(&fixture.commit));
    }

    #[test]
    fn verifier_rejects_wrong_head() {
        let fixture = Fixture::new(None);
        let reference = fixture.reference();
        fs::write(fixture.aria2.join("later-change"), "later").expect("write later change");
        fixture.commit_all("later commit");
        let error = verify_aria2_checkout(&reference, &fixture.aria2).expect_err("wrong HEAD");
        assert!(error.contains("does not match pinned commit"));
    }

    #[test]
    fn verifier_rejects_missing_extraction_path() {
        let fixture = Fixture::new(Some(REQUIRED_ARIA2_PATHS[0]));
        let error = verify_aria2_checkout(&fixture.reference(), &fixture.aria2)
            .expect_err("missing extraction path");
        assert!(error.contains(REQUIRED_ARIA2_PATHS[0]));
    }

    #[test]
    fn verifier_rejects_non_repository() {
        let fixture = Fixture::new(None);
        let error = verify_aria2_checkout(&fixture.reference(), &fixture.root.join("missing"))
            .expect_err("missing repository");
        assert!(error.contains("git rev-parse HEAD failed"));
    }

    #[test]
    fn commands_print_pin_and_repository() {
        let fixture = Fixture::new(None);
        assert_eq!(
            execute(["print-aria2-pin"], &fixture.workspace).expect("print pin"),
            fixture.commit
        );
        assert_eq!(
            execute(["print-aria2-repository"], &fixture.workspace).expect("print repository"),
            ARIA2_REPOSITORY
        );
    }

    #[test]
    fn commands_reject_missing_unknown_and_extra_arguments() {
        let fixture = Fixture::new(None);
        assert!(execute(Vec::<OsString>::new(), &fixture.workspace).is_err());
        assert!(execute(["unknown"], &fixture.workspace).is_err());
        assert!(execute(["print-aria2-pin", "extra"], &fixture.workspace).is_err());
        assert!(execute(["verify-aria2", "one", "two"], &fixture.workspace).is_err());
        assert!(execute(["verify-contracts", "one", "two"], &fixture.workspace).is_err());
        assert!(execute(["generate", "--check", "--check"], &fixture.workspace).is_err());
        assert!(execute(["generate", "one", "two"], &fixture.workspace).is_err());
    }

    #[test]
    fn generation_writes_checks_and_uses_pinned_git_objects() {
        let fixture = Fixture::new(None);
        let source = fixture.aria2.as_os_str().to_owned();
        let output = execute(
            [OsString::from("generate"), source.clone()],
            &fixture.workspace,
        )
        .expect("generate inventories");
        assert!(output.contains("2 preferences, 2 handlers, 2 manual directives"));

        let options_path = fixture.workspace.join("generated/aria2_options.json");
        let rpc_path = fixture.workspace.join("generated/aria2_rpc.json");
        let options = fs::read_to_string(&options_path).expect("read option inventory");
        let rpc = fs::read_to_string(&rpc_path).expect("read RPC inventory");
        assert!(options.contains("\"name\": \"dir\""));
        assert!(rpc.contains("\"name\": \"aria2.addUri\""));

        execute(
            [
                OsString::from("generate"),
                OsString::from("--check"),
                source.clone(),
            ],
            &fixture.workspace,
        )
        .expect("check generated inventories");

        fs::write(fixture.aria2.join("src/prefs.cc"), "dirty worktree input\n")
            .expect("dirty source worktree");
        execute(
            [OsString::from("verify-contracts"), source],
            &fixture.workspace,
        )
        .expect("check reads committed object rather than worktree");

        fs::write(&options_path, "stale\n").expect("make generated output stale");
        let error = execute(["verify-contracts"], &fixture.workspace)
            .expect_err("stale generated output must fail");
        assert!(error.contains("aria2_options.json is out of date"));
    }

    struct Fixture {
        root: PathBuf,
        workspace: PathBuf,
        aria2: PathBuf,
        commit: String,
    }

    impl Fixture {
        fn new(missing_path: Option<&str>) -> Self {
            let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
            let root =
                std::env::temp_dir().join(format!("ariax-xtask-test-{}-{id}", std::process::id()));
            let workspace = root.join("ariax");
            let aria2 = root.join("aria2");
            fs::create_dir_all(workspace.join("compat")).expect("create workspace fixture");
            fs::create_dir_all(&aria2).expect("create aria2 fixture");
            run_git(&aria2, &["init", "--quiet"]);

            for path in REQUIRED_ARIA2_PATHS {
                if missing_path.is_some_and(|missing| missing == *path) {
                    continue;
                }
                let destination = aria2.join(path);
                fs::create_dir_all(destination.parent().expect("fixture parent"))
                    .expect("create source parent");
                fs::write(destination, fixture_content(path)).expect("write source");
            }
            let mut fixture = Self {
                root,
                workspace,
                aria2,
                commit: String::new(),
            };
            fixture.commit_all("fixture commit");
            fixture.write_pin();
            fixture
        }

        fn reference(&self) -> Aria2Reference {
            Aria2Reference::load(&self.workspace.join("compat/aria2-reference.pin"))
                .expect("load fixture reference")
        }

        fn commit_all(&self, message: &str) {
            run_git(&self.aria2, &["add", "--all"]);
            run_git(
                &self.aria2,
                &[
                    "-c",
                    "user.name=ariax tests",
                    "-c",
                    "user.email=ariax-tests@example.invalid",
                    "commit",
                    "--quiet",
                    "--message",
                    message,
                ],
            );
        }

        fn write_pin(&mut self) {
            self.commit = run_git(&self.aria2, &["rev-parse", "HEAD"]);
            let pin = format!(
                "schema=1\nrepository={ARIA2_REPOSITORY}\ncommit={}\n",
                self.commit
            );
            fs::write(self.workspace.join("compat/aria2-reference.pin"), pin)
                .expect("write fixture pin");
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn run_git(directory: &Path, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(arguments)
            .output()
            .expect("run fixture git");
        assert!(
            output.status.success(),
            "git {} failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("git output UTF-8")
            .trim()
            .to_owned()
    }

    fn fixture_content(path: &str) -> String {
        match path {
            "src/prefs.cc" => r#"
PrefPtr PREF_DIR = makePref("dir");
PrefPtr PREF_SPLIT =
    makePref("split");
"#
            .to_owned(),
            "src/OptionHandlerFactory.cc" => r#"
{
  OptionHandler* op(new LocalFilePathOptionHandler(PREF_DIR, TEXT_DIR));
  op->addTag(TAG_BASIC);
  op->setInitialOption(true);
  handlers.push_back(op);
}
#ifdef ENABLE_SPLIT
{
  OptionHandler* op(new NumberOptionHandler(PREF_SPLIT, TEXT_SPLIT, "5", 1));
  op->setChangeGlobalOption(true);
  handlers.push_back(op);
}
#endif
"#
            .to_owned(),
            "doc/manual-src/en/aria2c.rst" => {
                ".. option:: -d, --dir=<DIR>\n\n.. option:: -s, --split=<N>\n".to_owned()
            }
            "src/RpcMethodFactory.cc" => r#"
std::vector<std::string> rpcMethodNames = {
    "aria2.addUri",
#ifdef ENABLE_BITTORRENT
    "aria2.addTorrent",
#endif
};
std::vector<std::string> rpcNotificationsNames = {
    "aria2.onDownloadStart",
};
"#
            .to_owned(),
            _ => format!("fixture for {path}\n"),
        }
    }
}
