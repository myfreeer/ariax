//! Startup arguments shared by the dedicated RPC command forms.

use ariax_config::{OptionValue, SecretString, builtin_registry, parse_option_value};
use ariax_engine::RpcAuthPolicy;
use ariax_runtime::RuntimeProfile;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Default)]
pub(crate) struct StartupOptions {
    pub profile: Option<RuntimeProfile>,
    pub session_export: Option<ariax_engine::SessionExportConfig>,
    pub input_file: Option<(PathBuf, ariax_engine::SessionFormat)>,
    credentials: BTreeMap<&'static str, SecretString>,
}

impl StartupOptions {
    pub fn parse(arguments: &[OsString]) -> Result<(Self, &[OsString]), String> {
        let mut options = Self::default();
        let mut cursor = 0;
        let mut session = BTreeMap::new();
        let mut session_names = BTreeSet::new();
        while let Some(argument) = arguments.get(cursor).and_then(|arg| arg.to_str()) {
            let (local_name, inline) = argument
                .split_once('=')
                .map_or((argument, None), |(name, value)| (name, Some(value)));
            if matches!(
                local_name,
                "--save-session"
                    | "--save-session-format"
                    | "--save-session-interval"
                    | "--input-file"
                    | "--input-file-format"
            ) {
                if !session_names.insert(local_name) {
                    return Err(format!("duplicate {local_name}"));
                }
                cursor += 1;
                let value = match inline {
                    Some(value) => value,
                    None => {
                        let value = arguments
                            .get(cursor)
                            .and_then(|arg| arg.to_str())
                            .filter(|value| !value.starts_with("--"))
                            .ok_or_else(|| format!("{local_name} requires a value"))?;
                        cursor += 1;
                        value
                    }
                };
                session.insert(local_name, value);
                continue;
            }
            if let Some(value) = argument.strip_prefix("--profile=") {
                if options.profile.is_some() {
                    return Err("duplicate --profile".to_owned());
                }
                options.profile = Some(RuntimeProfile::parse(value).map_err(|_| {
                    "invalid runtime profile; expected auto, concurrency, throughput, latency, or compact".to_owned()
                })?);
                cursor += 1;
                continue;
            }
            if argument == "--profile" {
                return Err(
                    "--profile requires =auto|concurrency|throughput|latency|compact".to_owned(),
                );
            }
            let (name, inline) = argument
                .split_once('=')
                .map_or((argument, None), |(name, value)| (name, Some(value)));
            let name = match name {
                "--rpc-secret" => "rpc-secret",
                "--rpc-user" => "rpc-user",
                "--rpc-passwd" => "rpc-passwd",
                _ => break,
            };
            if options.credentials.contains_key(name) {
                return Err(format!("duplicate --{name}"));
            }
            cursor += 1;
            let value = match inline {
                Some(value) => value,
                None => {
                    let value = arguments
                        .get(cursor)
                        .and_then(|arg| arg.to_str())
                        .filter(|value| !value.starts_with("--"))
                        .ok_or_else(|| format!("--{name} requires a Unicode value"))?;
                    cursor += 1;
                    value
                }
            };
            options
                .credentials
                .insert(name, parse_credential(name, value)?);
        }
        let absolute = |value: &str| -> Result<PathBuf, String> {
            if value.is_empty() {
                return Err("session path must not be empty".to_owned());
            }
            let path = PathBuf::from(value);
            if path.is_absolute() {
                Ok(path)
            } else {
                env::current_dir()
                    .map(|base| base.join(path))
                    .map_err(|_| "cannot resolve local session path".to_owned())
            }
        };
        let format = |name: &str| -> Result<ariax_engine::SessionFormat, String> {
            ariax_engine::SessionFormat::parse(session.get(name).copied().unwrap_or("aria2"))
                .map_err(|_| format!("{name} requires aria2 or json"))
        };
        if let Some(path) = session.get("--save-session") {
            let interval = session
                .get("--save-session-interval")
                .copied()
                .unwrap_or("0")
                .parse::<u64>()
                .ok()
                .filter(|seconds| *seconds <= 86_400)
                .ok_or_else(|| {
                    "--save-session-interval must be between 0 and 86400 seconds".to_owned()
                })?;
            options.session_export = Some(ariax_engine::SessionExportConfig {
                path: absolute(path)?,
                format: format("--save-session-format")?,
                interval: (interval != 0).then(|| Duration::from_secs(interval)),
            });
        } else if session.contains_key("--save-session-format")
            || session.contains_key("--save-session-interval")
        {
            return Err("session format and interval require --save-session".to_owned());
        }
        if let Some(path) = session.get("--input-file") {
            options.input_file = Some((absolute(path)?, format("--input-file-format")?));
        } else if session.contains_key("--input-file-format") {
            return Err("input format requires --input-file".to_owned());
        }
        Ok((options, &arguments[cursor..]))
    }

    pub fn has_rpc_arguments(&self) -> bool {
        !self.credentials.is_empty()
    }

    pub fn auth_from_environment(&self) -> Result<RpcAuthPolicy, String> {
        self.auth_with_environment(|name| env::var(name))
    }

    fn auth_with_environment(
        &self,
        mut lookup: impl FnMut(&str) -> Result<String, env::VarError>,
    ) -> Result<RpcAuthPolicy, String> {
        let mut resolve = |name, variable| -> Result<Option<SecretString>, String> {
            if let Some(value) = self.credentials.get(name) {
                return Ok(Some(value.clone()));
            }
            match lookup(variable) {
                Ok(value) => parse_credential(name, &value).map(Some),
                Err(env::VarError::NotPresent) => Ok(None),
                Err(env::VarError::NotUnicode(_)) => {
                    Err(format!("{variable} must be valid Unicode"))
                }
            }
        };
        let secret = resolve("rpc-secret", "ARIAX_RPC_SECRET")?;
        let user = resolve("rpc-user", "ARIAX_RPC_USER")?;
        let password = resolve("rpc-passwd", "ARIAX_RPC_PASSWD")?;
        let auth = match secret {
            Some(secret) if !secret.is_empty() => {
                RpcAuthPolicy::with_secret(secret.expose_secret())
            }
            _ => RpcAuthPolicy::default(),
        };
        match (user, password) {
            (None, None) => Ok(auth),
            (Some(user), Some(password)) => auth
                .with_http_basic(
                    user.expose_secret().to_owned(),
                    password.expose_secret().to_owned(),
                )
                .map_err(|_| "invalid RPC Basic credentials".to_owned()),
            _ => Err("rpc-user and rpc-passwd must be configured together".to_owned()),
        }
    }
}

fn parse_credential(name: &'static str, value: &str) -> Result<SecretString, String> {
    if name == "rpc-secret" && value.is_empty() {
        return Err("rpc-secret must not be empty".to_owned());
    }
    let definition = builtin_registry()
        .find(name)
        .expect("registered RPC credential");
    match parse_option_value(definition, value, None) {
        Ok(OptionValue::Secret(value)) => Ok(value),
        _ => Err(format!("invalid or oversized --{name}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_startup_options_validate_formats_intervals_and_required_paths() {
        let arguments = [
            "--save-session=export.txt",
            "--save-session-interval=5",
            "--input-file=input.txt",
            "--input-file-format=json",
            "--rpc-stdio",
        ]
        .map(OsString::from);
        let (options, rest) = StartupOptions::parse(&arguments).expect("session options");
        let save = options.session_export.expect("export");
        assert!(save.path.is_absolute());
        assert_eq!(save.format, ariax_engine::SessionFormat::Aria2);
        assert_eq!(save.interval, Some(Duration::from_secs(5)));
        assert_eq!(
            options.input_file.expect("input").1,
            ariax_engine::SessionFormat::Json
        );
        assert_eq!(rest, &[OsString::from("--rpc-stdio")]);
        for invalid in [
            vec!["--input-file-format=json"],
            vec!["--save-session-format=aria2"],
            vec!["--save-session-interval=1"],
            vec!["--input-file="],
            vec!["--input-file=x", "--input-file-format=yaml"],
            vec!["--save-session=x", "--save-session-interval=86401"],
            vec!["--save-session=x", "--save-session=y"],
        ] {
            assert!(
                StartupOptions::parse(&invalid.into_iter().map(OsString::from).collect::<Vec<_>>())
                    .is_err()
            );
        }
    }

    #[test]
    fn rpc_startup_credentials_override_environment_without_exposure() {
        let arguments = [
            "--rpc-user=cli-user-canary",
            "--profile=compact",
            "--rpc-passwd",
            "cli-pass-canary",
            "--rpc-secret=cli-token-canary",
            "--rpc-http",
        ]
        .map(OsString::from);
        let (options, rest) = StartupOptions::parse(&arguments).expect("parse startup");
        assert_eq!(rest, &[OsString::from("--rpc-http")]);
        assert_eq!(options.profile, Some(RuntimeProfile::Compact));
        let auth = options
            .auth_with_environment(|_| panic!("CLI values must override environment"))
            .expect("auth policy");
        assert!(auth.is_required());
        assert!(!format!("{options:?} {auth:?}").contains("canary"));
        let (empty, _) = StartupOptions::parse(&[]).expect("empty options");
        let auth = empty
            .auth_with_environment(|name| {
                Ok(match name {
                    "ARIAX_RPC_SECRET" => "token-canary",
                    "ARIAX_RPC_USER" => "user-canary",
                    "ARIAX_RPC_PASSWD" => "",
                    _ => unreachable!(),
                }
                .to_owned())
            })
            .expect("environment defaults and empty password");
        assert!(auth.is_required());
        assert!(!format!("{auth:?}").contains("canary"));
    }

    #[test]
    fn rpc_startup_rejects_missing_duplicate_invalid_and_oversized_credentials() {
        for arguments in [
            vec!["--rpc-passwd".to_owned()],
            vec!["--rpc-secret=".to_owned()],
            vec![
                "--rpc-secret=secret-canary".to_owned(),
                "--rpc-secret=duplicate-canary".to_owned(),
            ],
            vec![format!("--rpc-passwd={}secret-canary", "x".repeat(4096))],
            vec!["--profile=compact".to_owned(), "--profile=auto".to_owned()],
        ] {
            let arguments = arguments
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>();
            let error = StartupOptions::parse(&arguments).expect_err("invalid arguments");
            assert!(!error.contains("canary"));
        }
        for arguments in [
            vec!["--rpc-user=user-canary"],
            vec!["--rpc-passwd=pass-canary"],
            vec!["--rpc-user=", "--rpc-passwd=pass-canary"],
            vec!["--rpc-user=user:canary", "--rpc-passwd=pass-canary"],
            vec!["--rpc-user=user-canary", "--rpc-passwd=pass\ncanary"],
        ] {
            let arguments = arguments
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>();
            let (options, _) = StartupOptions::parse(&arguments).expect("bounded option values");
            let error = options
                .auth_with_environment(|_| Err(env::VarError::NotPresent))
                .expect_err("invalid credentials");
            assert!(!error.contains("canary"));
        }
    }
}
