#![forbid(unsafe_code)]

//! Portable storage contracts shared by safe-open and disk backends.

mod layout;
mod path;
mod root_binding;

pub use layout::{
    ALL_LAYOUT_ERRORS, ALL_MAP_SPAN_ERRORS, FileEntry, FileLayout, FileSpan, GlobalOffsetMapper,
    GlobalSpan, LAYOUT_HASH_DOMAIN, LayoutError, LayoutHash, MAX_LAYOUT_BYTES, MAX_LAYOUT_ENTRIES,
    MapSpanError,
};
pub use path::{
    ALL_PATH_VALIDATION_ERRORS, MAX_SAFE_RELATIVE_BYTES, PathPlatform, PathValidationError,
    SafePathBuilder, SafeRelativePath,
};
pub use root_binding::{
    ALL_ROOT_BINDING_ERROR_CLASSES, FileIdentity, MAX_IDENTITY_BYTES, MAX_PLATFORM_PATH_BYTES,
    PlatformPath, ROOT_BINDING_HASH_DOMAIN, RootBinding, RootBindingError, RootBindingHash,
    RootIdentity,
};
