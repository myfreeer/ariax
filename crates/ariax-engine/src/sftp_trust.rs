//! Bounded OpenSSH host matching. No network peer grants credential authority.
use crate::{ContentChecksum, ContentHasher, ProtocolFailure, TransferOptions};
use ariax_core::{
    Generation, Gid, HostKeyChallenge, HostKeyChallengeId, HostKeyFingerprint,
    PresentedHostKeyChallenge,
};
use hmac::{Hmac, KeyInit, Mac};
use russh::keys::ssh_key::{
    PublicKey,
    known_hosts::{Entry, HostPatterns, Marker},
};
use sha2::{Digest, Sha256};
use std::{
    io::{BufRead, BufReader},
    path::Path,
};

pub(crate) const MAX_KNOWN_HOSTS_BYTES: usize = 8 * 1024 * 1024;
const MAX_KNOWN_HOSTS_LINES: usize = 65_536;
const MAX_LINE: usize = 64 * 1024;
pub(crate) const MAX_RETAINED_KNOWN_HOSTS_BYTES: usize = 1024 * 1024;
type KnownHostKeys = Vec<(Option<Marker>, Vec<u8>)>;

pub(crate) struct HostTrust {
    exact: Option<Vec<u8>>,
    fingerprint: Option<[u8; 32]>,
    known: KnownHostKeys,
    legacy: Option<ContentChecksum>,
    check: bool,
}
impl HostTrust {
    pub(crate) fn load(
        options: &TransferOptions,
        host: &str,
        port: u16,
    ) -> Result<Self, ProtocolFailure> {
        let exact = options
            .sftp_host_key
            .as_deref()
            .map(|text| {
                PublicKey::from_openssh(text)
                    .map_err(|_| ProtocolFailure::HostKeyMismatch)
                    .and_then(|key| key.to_bytes().map_err(|_| ProtocolFailure::Malformed))
            })
            .transpose()?;
        let fingerprint = options
            .sftp_host_key_sha256
            .as_deref()
            .map(crate::transfer_task::parse_host_key_fingerprint)
            .transpose()
            .map_err(|_| ProtocolFailure::HostKeyMismatch)?;
        let legacy = options
            .ssh_host_key_md
            .as_deref()
            .map(ContentChecksum::parse)
            .transpose()
            .map_err(|_| ProtocolFailure::HostKeyMismatch)?;
        let default_path = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
            .map(|home| {
                std::path::PathBuf::from(home)
                    .join(".ssh")
                    .join("known_hosts")
            });
        let known = if let Some(path) = &options.sftp_known_hosts {
            load_known_hosts(path, host, port)?
        } else if let Some(path) = default_path.filter(|path| path.exists()) {
            load_known_hosts(&path, host, port)?
        } else {
            Vec::new()
        };
        Ok(Self {
            exact,
            fingerprint,
            known,
            legacy,
            check: options.sftp_check_host_key,
        })
    }
    pub(crate) fn accepts(&self, blob: &[u8]) -> Result<bool, ProtocolFailure> {
        // Revocation is authoritative even when another matching line/pin exists.
        if self
            .known
            .iter()
            .any(|(marker, key)| *marker == Some(Marker::Revoked) && key == blob)
        {
            return Err(ProtocolFailure::HostKeyMismatch);
        }
        let fingerprint: [u8; 32] = Sha256::digest(blob).into();
        if self.exact.as_ref().is_some_and(|key| key != blob)
            || self.fingerprint.is_some_and(|key| key != fingerprint)
        {
            return Err(ProtocolFailure::HostKeyMismatch);
        }
        if self.exact.is_some() || self.fingerprint.is_some() {
            return Ok(true);
        }
        if !self.known.is_empty() {
            if self
                .known
                .iter()
                .any(|(marker, key)| marker.is_none() && key == blob)
            {
                return Ok(true);
            }
            return Err(ProtocolFailure::HostKeyMismatch);
        }
        if let Some(expected) = &self.legacy {
            let mut hash = ContentHasher::new(expected.algorithm());
            hash.update(blob);
            return if &hash.finalize() == expected {
                Ok(true)
            } else {
                Err(ProtocolFailure::HostKeyMismatch)
            };
        }
        Ok(!self.check)
    }
}

pub(crate) fn challenge(
    gid: Gid,
    generation: Generation,
    host: String,
    port: u16,
    key: &PublicKey,
    blob: Vec<u8>,
) -> Result<PresentedHostKeyChallenge, ProtocolFailure> {
    let mut digest = Sha256::new();
    digest.update(b"ariax/sftp-challenge/v1\0");
    digest.update(gid.to_string());
    digest.update(generation.get().to_le_bytes());
    digest.update(host.as_bytes());
    digest.update(port.to_le_bytes());
    digest.update(&blob);
    let hash = digest.finalize();
    let id = HostKeyChallengeId::new(hash[..16].try_into().expect("hash prefix"));
    PresentedHostKeyChallenge::new(
        HostKeyChallenge {
            id,
            canonical_host: host,
            port,
            algorithm: key.algorithm().to_string(),
            fingerprint_sha256: HostKeyFingerprint::for_presented_key(&blob),
        },
        blob,
    )
    .map_err(|_| ProtocolFailure::Malformed)
}

fn load_known_hosts(path: &Path, host: &str, port: u16) -> Result<KnownHostKeys, ProtocolFailure> {
    let file =
        crate::http_auth::open_private_netrc(path).map_err(|_| ProtocolFailure::HostKeyMismatch)?;
    if !file
        .metadata()
        .map_err(|_| ProtocolFailure::HostKeyMismatch)?
        .is_file()
    {
        return Err(ProtocolFailure::HostKeyMismatch);
    }
    parse_known_hosts(BufReader::new(file), host, port)
}
fn parse_known_hosts(
    mut input: impl BufRead,
    host: &str,
    port: u16,
) -> Result<KnownHostKeys, ProtocolFailure> {
    let name = if port == 22 {
        host.to_ascii_lowercase()
    } else {
        format!("[{}]:{port}", host.to_ascii_lowercase())
    };
    let mut keys = Vec::new();
    let mut total = 0usize;
    let mut lines = 0;
    let mut retained = 0usize;
    loop {
        let mut line = Vec::new();
        loop {
            let available = input
                .fill_buf()
                .map_err(|_| ProtocolFailure::HostKeyMismatch)?;
            if available.is_empty() {
                break;
            }
            let count = available
                .iter()
                .position(|b| *b == b'\n')
                .map_or(available.len(), |index| index + 1);
            if line.len() + count > MAX_LINE || total + count > MAX_KNOWN_HOSTS_BYTES {
                return Err(ProtocolFailure::ResourceLimit);
            }
            line.extend_from_slice(&available[..count]);
            total += count;
            input.consume(count);
            if line.last() == Some(&b'\n') {
                break;
            }
        }
        if line.is_empty() {
            break;
        }
        lines += 1;
        if lines > MAX_KNOWN_HOSTS_LINES {
            return Err(ProtocolFailure::ResourceLimit);
        }
        let line = std::str::from_utf8(&line)
            .map_err(|_| ProtocolFailure::Malformed)?
            .trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let first = fields.next().ok_or(ProtocolFailure::Malformed)?;
        let marker = first.starts_with('@');
        let pattern = if marker {
            fields.next().ok_or(ProtocolFailure::Malformed)?
        } else {
            first
        };
        let patterns: HostPatterns = match pattern.parse() {
            Ok(patterns) => patterns,
            Err(_) => return Err(ProtocolFailure::HostKeyMismatch),
        };
        if !matches_host(&patterns, &name) {
            continue;
        }
        let algorithm = fields.next().ok_or(ProtocolFailure::HostKeyMismatch)?;
        let encoded = fields.next().ok_or(ProtocolFailure::HostKeyMismatch)?;
        let normalized = if marker {
            format!("{first} {pattern} {algorithm} {encoded}")
        } else {
            format!("{pattern} {algorithm} {encoded}")
        };
        let entry: Entry = normalized
            .parse()
            .map_err(|_| ProtocolFailure::HostKeyMismatch)?;
        let key = entry
            .public_key()
            .to_bytes()
            .map_err(|_| ProtocolFailure::Malformed)?;
        if key.len() > ariax_core::MAX_PRESENTED_HOST_KEY_BYTES {
            return Err(ProtocolFailure::ResourceLimit);
        }
        retained = retained
            .saturating_add(key.capacity())
            .saturating_add(2 * std::mem::size_of::<(Option<Marker>, Vec<u8>)>());
        if retained > MAX_RETAINED_KNOWN_HOSTS_BYTES {
            return Err(ProtocolFailure::ResourceLimit);
        }
        keys.push((entry.marker().copied(), key));
    }
    Ok(keys)
}

fn matches_host(patterns: &HostPatterns, host: &str) -> bool {
    match patterns {
        HostPatterns::HashedName { salt, hash } => Hmac::<sha1::Sha1>::new_from_slice(salt)
            .is_ok_and(|mut mac| {
                mac.update(host.as_bytes());
                mac.verify_slice(hash).is_ok()
            }),
        HostPatterns::Patterns(patterns) => {
            let mut matched = false;
            for pattern in patterns {
                let (negative, pattern) = pattern
                    .strip_prefix('!')
                    .map_or((false, pattern.as_str()), |value| (true, value));
                if wildcard(pattern.as_bytes(), host.as_bytes()) {
                    if negative {
                        return false;
                    }
                    matched = true;
                }
            }
            matched
        }
    }
}

// Iterative glob matching uses constant space and has a bounded hostname (253
// bytes); it never builds a regex from metadata or recurses on wildcard input.
fn wildcard(pattern: &[u8], name: &[u8]) -> bool {
    let (mut p, mut n, mut star, mut retry) = (0, 0, None, 0);
    while n < name.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p].eq_ignore_ascii_case(&name[n])) {
            p += 1;
            n += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            p += 1;
            retry = n;
        } else if let Some(previous) = star {
            retry += 1;
            n = retry;
            p = previous + 1;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

#[cfg(test)]
pub(crate) struct TestKnownHosts(pub(crate) std::path::PathBuf);
#[cfg(test)]
impl TestKnownHosts {
    pub(crate) fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ariax-known-hosts-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&path)
                .unwrap();
        }
        #[cfg(windows)]
        ariax_windows_security::create_private_file(&path).unwrap();
        Self(path)
    }
}
#[cfg(test)]
impl Drop for TestKnownHosts {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64ct::Encoding;
    const KEY: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIB9dG4kjRhQTtWTVzd2t27+t0DEHBPW7iOD23TUiYLio";
    #[test]
    fn known_hosts_matches_patterns_hashes_revocation_and_bounds() {
        let blob = PublicKey::from_openssh(KEY).unwrap().to_bytes().unwrap();
        let input = format!("*.example.test,!blocked.example.test {KEY}\n");
        assert_eq!(
            parse_known_hosts(input.as_bytes(), "good.example.test", 22)
                .unwrap()
                .len(),
            1
        );
        assert!(
            parse_known_hosts(input.as_bytes(), "blocked.example.test", 22)
                .unwrap()
                .is_empty()
        );
        let mut mac = Hmac::<sha1::Sha1>::new_from_slice(b"salt").unwrap();
        mac.update(b"[example.test]:2222");
        let input = format!(
            "|1|{}|{} {KEY}\n",
            base64ct::Base64::encode_string(b"salt"),
            base64ct::Base64::encode_string(&mac.finalize().into_bytes())
        );
        assert_eq!(
            parse_known_hosts(input.as_bytes(), "example.test", 2222)
                .unwrap()
                .len(),
            1
        );
        let known = parse_known_hosts(
            format!("@revoked example.test {KEY}\n").as_bytes(),
            "example.test",
            22,
        )
        .unwrap();
        let trust = HostTrust {
            exact: Some(blob.clone()),
            fingerprint: None,
            known,
            legacy: None,
            check: true,
        };
        assert_eq!(trust.accepts(&blob), Err(ProtocolFailure::HostKeyMismatch));
        assert!(
            parse_known_hosts(
                b"example.test ssh-ed25519 invalid\n".as_slice(),
                "example.test",
                22
            )
            .is_err()
        );
        assert!(
            parse_known_hosts(
                b"other.test ssh-ed25519 invalid\n".as_slice(),
                "example.test",
                22
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            parse_known_hosts(vec![b'x'; MAX_LINE + 1].as_slice(), "example.test", 22).is_err()
        );
    }
    #[test]
    fn pins_are_exact_and_unknown_challenges_are_generation_bound() {
        let key = PublicKey::from_openssh(KEY).unwrap();
        let blob = key.to_bytes().unwrap();
        let known_hosts = TestKnownHosts::new();
        let mut options = TransferOptions {
            sftp_known_hosts: Some(known_hosts.0.clone()),
            ..Default::default()
        };
        let trust = HostTrust::load(&options, "example.test", 22).unwrap();
        assert_eq!(trust.accepts(&blob), Ok(false));
        let a = challenge(
            Gid::new(1).unwrap(),
            Generation::INITIAL,
            "example.test".into(),
            22,
            &key,
            blob.clone(),
        )
        .unwrap();
        assert_eq!(
            a.summary().fingerprint_sha256,
            HostKeyFingerprint::for_presented_key(&blob)
        );
        options.sftp_host_key_sha256 = Some(ariax_storage::session_host_key_pin_value(
            a.summary().fingerprint_sha256,
        ));
        assert_eq!(
            HostTrust::load(&options, "example.test", 22)
                .unwrap()
                .accepts(&blob),
            Ok(true)
        );
        options.sftp_host_key_sha256 = Some("00".repeat(32));
        assert_eq!(
            HostTrust::load(&options, "example.test", 22)
                .unwrap()
                .accepts(&blob),
            Err(ProtocolFailure::HostKeyMismatch)
        );
    }
}
