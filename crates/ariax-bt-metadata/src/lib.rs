#![forbid(unsafe_code)]

//! Bounded, native-independent BitTorrent metadata validation.

mod bencode;
mod metadata;
pub use metadata::{
    BtIdentity, Magnet, MetadataFile, MetadataLimits, TorrentMetadata, parse_info, parse_magnet,
    parse_torrent,
};
pub use metadata::{
    info_section, magnet_with_trackers, torrent_from_info, validate_resume, with_trackers,
    with_web_seeds,
};

/// Errors contain stable classifications and never untrusted metadata or URLs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BtError {
    InvalidMetadata,
    MetadataLimit,
    InvalidMagnet,
    UnsafePath,
    Symlink,
    Collision,
    Selection,
    IdentityMismatch,
    Credentials,
    Destination,
    UnprotectedRoot,
    Overloaded,
    Closed,
    Native,
    StaleCompletion,
    CheckpointFailed,
    CheckpointTimeout,
    UnsupportedOption,
    RequiresRestart,
}

impl std::fmt::Display for BtError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "BitTorrent {}",
            match self {
                Self::InvalidMetadata => "metadata is invalid",
                Self::MetadataLimit => "metadata exceeds its limit",
                Self::InvalidMagnet => "magnet is invalid",
                Self::UnsafePath => "path is unsafe",
                Self::Symlink => "symlink entries are forbidden",
                Self::Collision => "file mapping is not unique",
                Self::Selection => "file selection is invalid",
                Self::IdentityMismatch => "identity does not match",
                Self::Credentials => "metadata contains credentials",
                Self::Destination => "destination is forbidden",
                Self::UnprotectedRoot => "output root is not protected",
                Self::Overloaded => "capacity is exhausted",
                Self::Closed => "adapter is closed",
                Self::Native => "native operation failed",
                Self::StaleCompletion => "completion is stale",
                Self::CheckpointFailed => "checkpoint failed",
                Self::CheckpointTimeout => "checkpoint timed out",
                Self::UnsupportedOption => "option is unsupported",
                Self::RequiresRestart => "option requires an explicit restart",
            }
        )
    }
}

impl std::error::Error for BtError {}
