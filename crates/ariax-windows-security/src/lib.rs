//! Safe, narrow wrappers around native Windows operating-system APIs.
//!
//! The rest of the workspace forbids unsafe Rust. This crate isolates the
//! Win32 FFI required to create private filesystem objects atomically, verify
//! the exact security descriptor expected by Ariax, and query bounded process
//! resource diagnostics.

#![cfg_attr(not(windows), forbid(unsafe_code))]

#[cfg(windows)]
mod windows;

#[cfg(windows)]
pub use windows::{
    NativeFileInformation, apply_private_file_acl, create_private_directory, create_private_file,
    create_relative_file_no_reparse, current_process_working_set_bytes, directory_names,
    link_relative_no_replace, open_absolute_directory_no_reparse,
    open_relative_directory_no_reparse, open_relative_regular_file_no_reparse,
    query_native_file_information, remove_relative_file_no_reparse, rename_relative_replace,
    verify_private_directory, verify_private_file, verify_private_file_allow_alias,
    verify_single_link_regular_file,
};
