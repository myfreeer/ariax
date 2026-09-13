//! Startup arguments shared by the dedicated RPC command forms.

use ariax_config::{OptionValue, SecretString, builtin_registry, parse_option_value};
use ariax_engine::RpcAuthPolicy;
use ariax_runtime::RuntimeProfile;
use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;

#[derive(Debug, Default)]
pub(crate) struct StartupOptions {
    pub profile: Option<RuntimeProfile>,
    credentials: BTreeMap<&'static str, SecretString>,
}

impl StartupOptions {
    pub fn parse(arguments: &[OsString]) -> Result<(Self, &[OsString]), String> {
        let mut options = Self::default();
        let mut cursor = 0;
        while let Some(argument) = arguments.get(cursor).and_then(|arg| arg.to_str()) {
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
