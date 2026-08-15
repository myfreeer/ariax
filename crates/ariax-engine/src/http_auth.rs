//! Bounded HTTP Basic credentials and a permission-checked project netrc parser.

use hyper::header::HeaderValue;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

pub const MAX_HTTP_NETRC_BYTES: usize = 1024 * 1024;
pub const MAX_HTTP_NETRC_ENTRIES: usize = 1024;
pub const MAX_HTTP_NETRC_TOKEN_BYTES: usize = 4096;
pub const MAX_HTTP_BASIC_USERNAME_BYTES: usize = 1024;
pub const MAX_HTTP_BASIC_PASSWORD_BYTES: usize = 4096;

#[derive(Clone, Eq, PartialEq)]
pub struct HttpBasicCredentials {
    username: Arc<str>,
    password: Arc<str>,
}

impl HttpBasicCredentials {
    pub fn new(username: String, password: String) -> Result<Self, HttpAuthError> {
        if username.is_empty()
            || username.len() > MAX_HTTP_BASIC_USERNAME_BYTES
            || password.len() > MAX_HTTP_BASIC_PASSWORD_BYTES
            || username.bytes().any(is_forbidden_secret_byte)
            || password.bytes().any(is_forbidden_secret_byte)
        {
            return Err(HttpAuthError::InvalidCredentials);
        }
        Ok(Self {
            username: username.into(),
            password: password.into(),
        })
    }

    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn authorization_header(&self) -> Result<HttpAuthorization, HttpAuthError> {
        let mut plaintext = Vec::new();
        plaintext
            .try_reserve_exact(self.username.len() + 1 + self.password.len())
            .map_err(|_| HttpAuthError::AllocationFailed)?;
        plaintext.extend_from_slice(self.username.as_bytes());
        plaintext.push(b':');
        plaintext.extend_from_slice(self.password.as_bytes());
        let encoded = encode_base64(&plaintext);
        plaintext.fill(0);
        let value = HeaderValue::from_str(&format!("Basic {encoded}"))
            .map_err(|_| HttpAuthError::InvalidCredentials)?;
        Ok(HttpAuthorization(value))
    }
}

impl fmt::Debug for HttpBasicCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpBasicCredentials")
            .field("username", &"<redacted>")
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct HttpAuthorization(HeaderValue);

impl HttpAuthorization {
    #[must_use]
    pub const fn as_header_value(&self) -> &HeaderValue {
        &self.0
    }
}

impl fmt::Debug for HttpAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HttpAuthorization(<redacted>)")
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HttpNetrc {
    machines: BTreeMap<Arc<str>, HttpBasicCredentials>,
    default: Option<HttpBasicCredentials>,
}

impl HttpNetrc {
    pub fn load(path: &Path) -> Result<Self, HttpAuthError> {
        let file = open_private_netrc(path)?;
        let metadata = file.metadata().map_err(|_| HttpAuthError::NetrcIo)?;
        if !metadata.is_file() {
            return Err(HttpAuthError::NetrcNotRegular);
        }
        if metadata.len() > MAX_HTTP_NETRC_BYTES as u64 {
            return Err(HttpAuthError::NetrcTooLarge);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(
                usize::try_from(metadata.len()).map_err(|_| HttpAuthError::NetrcTooLarge)?,
            )
            .map_err(|_| HttpAuthError::AllocationFailed)?;
        file.take((MAX_HTTP_NETRC_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| HttpAuthError::NetrcIo)?;
        if bytes.len() > MAX_HTTP_NETRC_BYTES {
            return Err(HttpAuthError::NetrcTooLarge);
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| HttpAuthError::NetrcSyntax)?;
        Self::parse(text)
    }

    pub fn parse(input: &str) -> Result<Self, HttpAuthError> {
        if input.len() > MAX_HTTP_NETRC_BYTES {
            return Err(HttpAuthError::NetrcTooLarge);
        }
        let tokens = tokenize_netrc(input)?;
        let mut machines = BTreeMap::new();
        let mut default = None;
        let mut index = 0;
        while index < tokens.len() {
            let selector = tokens[index].as_str();
            index += 1;
            let machine = match selector {
                "machine" => {
                    let name = tokens.get(index).ok_or(HttpAuthError::NetrcSyntax)?;
                    index += 1;
                    Some(normalize_machine(name)?)
                }
                "default" => None,
                "macdef" => return Err(HttpAuthError::NetrcUnsupported),
                _ => return Err(HttpAuthError::NetrcSyntax),
            };
            let mut login = None;
            let mut password = None;
            while index < tokens.len()
                && !matches!(tokens[index].as_str(), "machine" | "default" | "macdef")
            {
                let keyword = tokens[index].as_str();
                index += 1;
                let value = tokens.get(index).ok_or(HttpAuthError::NetrcSyntax)?;
                index += 1;
                match keyword {
                    "login" | "user" => set_once(&mut login, value.clone())?,
                    "password" | "passwd" => set_once(&mut password, value.clone())?,
                    "account" => {}
                    _ => return Err(HttpAuthError::NetrcSyntax),
                }
            }
            let credentials = HttpBasicCredentials::new(
                login.ok_or(HttpAuthError::NetrcMissingLogin)?,
                password.ok_or(HttpAuthError::NetrcMissingPassword)?,
            )?;
            if let Some(machine) = machine {
                if machines.len() == MAX_HTTP_NETRC_ENTRIES {
                    return Err(HttpAuthError::TooManyNetrcEntries);
                }
                if machines.insert(machine, credentials).is_some() {
                    return Err(HttpAuthError::DuplicateNetrcMachine);
                }
            } else if default.replace(credentials).is_some() {
                return Err(HttpAuthError::DuplicateNetrcDefault);
            }
        }
        Ok(Self { machines, default })
    }

    #[must_use]
    pub fn credentials_for(&self, host: &str) -> Option<&HttpBasicCredentials> {
        let normalized = host.trim_end_matches('.').to_ascii_lowercase();
        self.machines
            .get(normalized.as_str())
            .or(self.default.as_ref())
    }

    #[must_use]
    pub fn machine_count(&self) -> usize {
        self.machines.len()
    }
}

#[derive(Clone, Debug, Default)]
pub struct HttpAuthPolicy {
    explicit: BTreeMap<Arc<str>, HttpBasicCredentials>,
    netrc: Option<HttpNetrc>,
}

impl HttpAuthPolicy {
    pub fn new(
        explicit: impl IntoIterator<Item = (String, HttpBasicCredentials)>,
        netrc: Option<HttpNetrc>,
    ) -> Result<Self, HttpAuthError> {
        let mut entries = BTreeMap::new();
        for (host, credentials) in explicit {
            if entries.len() == MAX_HTTP_NETRC_ENTRIES {
                return Err(HttpAuthError::TooManyNetrcEntries);
            }
            let host = normalize_machine(&host)?;
            if entries.insert(host, credentials).is_some() {
                return Err(HttpAuthError::DuplicateNetrcMachine);
            }
        }
        Ok(Self {
            explicit: entries,
            netrc,
        })
    }

    pub fn authorization_for(
        &self,
        host: &str,
        challenge_scheme: Option<&str>,
    ) -> Result<Option<HttpAuthorization>, HttpAuthError> {
        if let Some(scheme) = challenge_scheme
            && !scheme.eq_ignore_ascii_case("basic")
        {
            return Err(HttpAuthError::UnsupportedScheme);
        }
        let normalized = host.trim_end_matches('.').to_ascii_lowercase();
        self.explicit
            .get(normalized.as_str())
            .or_else(|| {
                self.netrc
                    .as_ref()
                    .and_then(|netrc| netrc.credentials_for(&normalized))
            })
            .map(HttpBasicCredentials::authorization_header)
            .transpose()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpAuthError {
    InvalidCredentials,
    UnsupportedScheme,
    AllocationFailed,
    NetrcIo,
    NetrcNotRegular,
    NetrcInsecurePermissions,
    NetrcTooLarge,
    NetrcSyntax,
    NetrcUnsupported,
    NetrcMissingLogin,
    NetrcMissingPassword,
    DuplicateNetrcMachine,
    DuplicateNetrcDefault,
    TooManyNetrcEntries,
}

impl HttpAuthError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidCredentials => "invalid_basic_credentials",
            Self::UnsupportedScheme => "unsupported_http_auth_scheme",
            Self::AllocationFailed => "auth_allocation_failed",
            Self::NetrcIo => "netrc_io",
            Self::NetrcNotRegular => "netrc_not_regular",
            Self::NetrcInsecurePermissions => "netrc_insecure_permissions",
            Self::NetrcTooLarge => "netrc_too_large",
            Self::NetrcSyntax => "netrc_syntax",
            Self::NetrcUnsupported => "netrc_unsupported",
            Self::NetrcMissingLogin => "netrc_missing_login",
            Self::NetrcMissingPassword => "netrc_missing_password",
            Self::DuplicateNetrcMachine => "duplicate_netrc_machine",
            Self::DuplicateNetrcDefault => "duplicate_netrc_default",
            Self::TooManyNetrcEntries => "too_many_netrc_entries",
        }
    }
}

impl fmt::Display for HttpAuthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for HttpAuthError {}

#[cfg(unix)]
fn open_private_netrc(path: &Path) -> Result<File, HttpAuthError> {
    use rustix::fs::{Mode, OFlags, open};
    use std::os::unix::fs::PermissionsExt as _;

    let fd = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|_| HttpAuthError::NetrcIo)?;
    let file = File::from(fd);
    let metadata = file.metadata().map_err(|_| HttpAuthError::NetrcIo)?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(HttpAuthError::NetrcInsecurePermissions);
    }
    Ok(file)
}

#[cfg(windows)]
fn open_private_netrc(path: &Path) -> Result<File, HttpAuthError> {
    ariax_windows_security::verify_private_file(path)
        .map_err(|_| HttpAuthError::NetrcInsecurePermissions)?;
    File::open(path).map_err(|_| HttpAuthError::NetrcIo)
}

#[cfg(not(any(unix, windows)))]
fn open_private_netrc(_path: &Path) -> Result<File, HttpAuthError> {
    Err(HttpAuthError::NetrcInsecurePermissions)
}

fn tokenize_netrc(input: &str) -> Result<Vec<String>, HttpAuthError> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut comment = false;
    for character in input.chars() {
        if comment {
            if character == '\n' {
                comment = false;
            }
            continue;
        }
        if escaped {
            token.push(character);
            escaped = false;
        } else if quoted && character == '\\' {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if !quoted && character == '#' {
            finish_token(&mut tokens, &mut token)?;
            comment = true;
        } else if !quoted && character.is_whitespace() {
            finish_token(&mut tokens, &mut token)?;
        } else {
            if character == '\0' || character.is_control() {
                return Err(HttpAuthError::NetrcSyntax);
            }
            token.push(character);
            if token.len() > MAX_HTTP_NETRC_TOKEN_BYTES {
                return Err(HttpAuthError::NetrcSyntax);
            }
        }
    }
    if quoted || escaped {
        return Err(HttpAuthError::NetrcSyntax);
    }
    finish_token(&mut tokens, &mut token)?;
    Ok(tokens)
}

fn finish_token(tokens: &mut Vec<String>, token: &mut String) -> Result<(), HttpAuthError> {
    if !token.is_empty() {
        tokens
            .try_reserve(1)
            .map_err(|_| HttpAuthError::AllocationFailed)?;
        tokens.push(std::mem::take(token));
    }
    Ok(())
}

fn normalize_machine(input: &str) -> Result<Arc<str>, HttpAuthError> {
    let host = input.trim_end_matches('.');
    if host.is_empty()
        || host.len() > 253
        || !host.is_ascii()
        || host
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(HttpAuthError::NetrcSyntax);
    }
    Ok(host.to_ascii_lowercase().into())
}

fn set_once(slot: &mut Option<String>, value: String) -> Result<(), HttpAuthError> {
    if slot.replace(value).is_some() {
        return Err(HttpAuthError::NetrcSyntax);
    }
    Ok(())
}

const fn is_forbidden_secret_byte(byte: u8) -> bool {
    byte == 0 || byte == b'\r' || byte == b'\n'
}

fn encode_base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(TABLE[(first >> 2) as usize] as char);
        output.push(TABLE[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(third & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exact_and_default_credentials_without_debug_secret_disclosure() {
        let netrc = HttpNetrc::parse(
            "machine Example.TEST login alice password \"s e c r e t\"\n\
             default login fallback password fallback-secret\n",
        )
        .expect("netrc");
        assert_eq!(netrc.machine_count(), 1);
        assert_eq!(
            netrc
                .credentials_for("example.test.")
                .expect("exact")
                .username(),
            "alice"
        );
        assert_eq!(
            netrc
                .credentials_for("other.test")
                .expect("default")
                .username(),
            "fallback"
        );
        let debug = format!(
            "{:?}",
            netrc.credentials_for("example.test").expect("exact")
        );
        assert!(!debug.contains("alice"));
        assert!(!debug.contains("s e c r e t"));
    }

    #[test]
    fn explicit_credentials_win_and_only_basic_is_supported() {
        let explicit = HttpBasicCredentials::new("explicit".to_owned(), "secret".to_owned())
            .expect("credentials");
        let netrc = HttpNetrc::parse("default login fallback password other").expect("netrc");
        let policy = HttpAuthPolicy::new([("example.test".to_owned(), explicit)], Some(netrc))
            .expect("policy");
        let authorization = policy
            .authorization_for("example.test", Some("Basic"))
            .expect("auth")
            .expect("credentials");
        assert_eq!(
            authorization.as_header_value(),
            "Basic ZXhwbGljaXQ6c2VjcmV0"
        );
        assert_eq!(
            policy.authorization_for("example.test", Some("Digest")),
            Err(HttpAuthError::UnsupportedScheme)
        );
        assert!(!format!("{authorization:?}").contains("ZXhwbGljaXQ"));
    }

    #[test]
    fn rejects_ambiguous_or_incomplete_netrc_records() {
        assert_eq!(
            HttpNetrc::parse("machine a.example login user"),
            Err(HttpAuthError::NetrcMissingPassword)
        );
        assert_eq!(
            HttpNetrc::parse(
                "machine a.example login one password x machine a.example login two password y"
            ),
            Err(HttpAuthError::DuplicateNetrcMachine)
        );
        assert_eq!(
            HttpNetrc::parse("macdef init echo unsafe"),
            Err(HttpAuthError::NetrcUnsupported)
        );
        assert_eq!(
            HttpBasicCredentials::new("bad\nname".to_owned(), "secret".to_owned()),
            Err(HttpAuthError::InvalidCredentials)
        );
    }

    #[cfg(unix)]
    #[test]
    fn netrc_file_requires_private_permissions_and_rejects_symlinks() {
        use std::fs;
        use std::os::unix::fs::{PermissionsExt as _, symlink};
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("ariax-netrc-{}-{nonce}", std::process::id()));
        fs::create_dir(&directory).expect("directory");
        let path = directory.join("netrc");
        fs::write(&path, "machine example.test login user password secret").expect("write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("chmod");
        assert!(HttpNetrc::load(&path).is_ok());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("chmod");
        assert_eq!(
            HttpNetrc::load(&path),
            Err(HttpAuthError::NetrcInsecurePermissions)
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("chmod");
        let link = directory.join("link");
        symlink(&path, &link).expect("symlink");
        assert_eq!(HttpNetrc::load(&link), Err(HttpAuthError::NetrcIo));
        fs::remove_file(link).expect("remove link");
        fs::remove_file(path).expect("remove file");
        fs::remove_dir(directory).expect("remove directory");
    }
}
