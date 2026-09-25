#![forbid(unsafe_code)]

//! Safe BitTorrent admission, resource ownership, and isolated native execution.

mod bridge_budget;
mod mapping;
mod root;

#[cfg(feature = "libtorrent")]
mod native;

pub use ariax_bt_metadata::{
    BtError, BtIdentity, Magnet, MetadataFile, MetadataLimits, TorrentMetadata, parse_info,
    parse_magnet, parse_torrent,
};
pub use ariax_bt_metadata::{
    info_section, magnet_with_trackers, torrent_from_info, validate_resume, validate_tracker,
    with_trackers, with_web_seeds,
};
pub use bridge_budget::{
    BandwidthAllocation, BandwidthGroup, BandwidthUpdate, BridgeBudget, BridgeLease, BridgeLimits,
    OwnedBlob, bandwidth_updates, split_bandwidth,
};
pub use mapping::{FileMapping, MappingOptions, map_files};
pub use root::ProtectedRoot;

#[cfg(feature = "libtorrent")]
pub use native::{
    BtAdapter, BtAdapterConfig, BtAdmission, BtCommand, BtEvent, BtHandle, BtPeer, BtPending,
    BtReply, BtResources, BtSnapshot, BtTaskSettings,
};
