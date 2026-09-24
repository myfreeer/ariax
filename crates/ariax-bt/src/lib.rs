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
