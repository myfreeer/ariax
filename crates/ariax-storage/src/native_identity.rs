use crate::PathPlatform;
use std::error::Error;
use std::fmt;

pub const NATIVE_IDENTITY_VERSION: u8 = 1;
pub const NATIVE_IDENTITY_UNIX_BYTES: usize = 18;
pub const NATIVE_IDENTITY_WINDOWS_BYTES: usize = 26;

/// Stable platform-native file identity persisted inside root bindings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeIdentityV1 {
    Unix {
        device: u64,
        inode: u64,
    },
    Windows {
        volume_serial: u64,
        file_id: [u8; 16],
    },
}

impl NativeIdentityV1 {
    #[must_use]
    pub const fn platform(self) -> PathPlatform {
        match self {
            Self::Unix { .. } => PathPlatform::Unix,
            Self::Windows { .. } => PathPlatform::Windows,
        }
    }

    #[must_use]
    pub fn encode(self) -> Box<[u8]> {
        let mut bytes = Vec::with_capacity(match self {
            Self::Unix { .. } => NATIVE_IDENTITY_UNIX_BYTES,
            Self::Windows { .. } => NATIVE_IDENTITY_WINDOWS_BYTES,
        });
        bytes.push(NATIVE_IDENTITY_VERSION);
        bytes.push(self.platform() as u8);
        match self {
            Self::Unix { device, inode } => {
                bytes.extend_from_slice(&device.to_le_bytes());
                bytes.extend_from_slice(&inode.to_le_bytes());
            }
            Self::Windows {
                volume_serial,
                file_id,
            } => {
                bytes.extend_from_slice(&volume_serial.to_le_bytes());
                bytes.extend_from_slice(&file_id);
            }
        }
        bytes.into_boxed_slice()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, NativeIdentityError> {
        let (&version, rest) = bytes
            .split_first()
            .ok_or(NativeIdentityError::WrongLength)?;
        if version != NATIVE_IDENTITY_VERSION {
            return Err(NativeIdentityError::UnsupportedVersion(version));
        }
        let (&platform, payload) = rest.split_first().ok_or(NativeIdentityError::WrongLength)?;
        match platform {
            value if value == PathPlatform::Unix as u8 => {
                let payload: &[u8; 16] = payload
                    .try_into()
                    .map_err(|_| NativeIdentityError::WrongLength)?;
                Ok(Self::Unix {
                    device: u64::from_le_bytes(
                        payload[..8]
                            .try_into()
                            .expect("the Unix identity device slice is fixed"),
                    ),
                    inode: u64::from_le_bytes(
                        payload[8..]
                            .try_into()
                            .expect("the Unix identity inode slice is fixed"),
                    ),
                })
            }
            value if value == PathPlatform::Windows as u8 => {
                let payload: &[u8; 24] = payload
                    .try_into()
                    .map_err(|_| NativeIdentityError::WrongLength)?;
                Ok(Self::Windows {
                    volume_serial: u64::from_le_bytes(
                        payload[..8]
                            .try_into()
                            .expect("the Windows identity volume slice is fixed"),
                    ),
                    file_id: payload[8..]
                        .try_into()
                        .expect("the Windows file identity slice is fixed"),
                })
            }
            value => Err(NativeIdentityError::UnknownPlatform(value)),
        }
    }

    pub fn decode_for_current(bytes: &[u8]) -> Result<Self, NativeIdentityError> {
        let identity = Self::decode(bytes)?;
        if identity.platform() != PathPlatform::current() {
            return Err(NativeIdentityError::PlatformMismatch {
                encoded: identity.platform(),
                current: PathPlatform::current(),
            });
        }
        Ok(identity)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeIdentityError {
    WrongLength,
    UnsupportedVersion(u8),
    UnknownPlatform(u8),
    PlatformMismatch {
        encoded: PathPlatform,
        current: PathPlatform,
    },
}

impl NativeIdentityError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::WrongLength => "wrong_length",
            Self::UnsupportedVersion(_) => "unsupported_version",
            Self::UnknownPlatform(_) => "unknown_platform",
            Self::PlatformMismatch { .. } => "platform_mismatch",
        }
    }
}

pub const ALL_NATIVE_IDENTITY_ERROR_CODES: [&str; 4] = [
    "wrong_length",
    "unsupported_version",
    "unknown_platform",
    "platform_mismatch",
];

impl fmt::Display for NativeIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength => formatter.write_str("native identity has the wrong length"),
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "native identity version {version} is unsupported"
                )
            }
            Self::UnknownPlatform(platform) => {
                write!(
                    formatter,
                    "native identity platform tag {platform} is unknown"
                )
            }
            Self::PlatformMismatch { encoded, current } => write!(
                formatter,
                "native identity platform {} does not match current platform {}",
                encoded.code(),
                current.code()
            ),
        }
    }
}

impl Error for NativeIdentityError {}

#[cfg(test)]
mod tests {
    use super::{NativeIdentityError, NativeIdentityV1};
    use crate::PathPlatform;

    #[test]
    fn unix_identity_round_trips_exact_bytes() {
        let identity = NativeIdentityV1::Unix {
            device: 0x0102_0304_0506_0708,
            inode: 0x1112_1314_1516_1718,
        };
        let encoded = identity.encode();
        assert_eq!(encoded.len(), 18);
        assert_eq!(NativeIdentityV1::decode(&encoded), Ok(identity));
    }

    #[test]
    fn windows_identity_round_trips_exact_bytes() {
        let identity = NativeIdentityV1::Windows {
            volume_serial: 0x0102_0304_0506_0708,
            file_id: [0xa5; 16],
        };
        let encoded = identity.encode();
        assert_eq!(encoded.len(), 26);
        assert_eq!(NativeIdentityV1::decode(&encoded), Ok(identity));
    }

    #[test]
    fn malformed_and_cross_platform_identities_are_rejected() {
        assert_eq!(
            NativeIdentityV1::decode(&[1, PathPlatform::Unix as u8]),
            Err(NativeIdentityError::WrongLength)
        );
        assert_eq!(
            NativeIdentityV1::decode(&[2, PathPlatform::Unix as u8]),
            Err(NativeIdentityError::UnsupportedVersion(2))
        );
        assert_eq!(
            NativeIdentityV1::decode(&[1, 99]),
            Err(NativeIdentityError::UnknownPlatform(99))
        );

        let foreign = if cfg!(windows) {
            NativeIdentityV1::Unix {
                device: 1,
                inode: 2,
            }
        } else {
            NativeIdentityV1::Windows {
                volume_serial: 1,
                file_id: [2; 16],
            }
        };
        assert!(matches!(
            NativeIdentityV1::decode_for_current(&foreign.encode()),
            Err(NativeIdentityError::PlatformMismatch { .. })
        ));
    }
}
