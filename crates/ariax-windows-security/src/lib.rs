//! Safe, narrow wrappers around native Windows filesystem ACL operations.
//!
//! The rest of the workspace forbids unsafe Rust. This crate isolates the
//! Win32 FFI required to create private filesystem objects atomically and to
//! verify the exact security descriptor expected by Ariax.

#![cfg_attr(not(windows), forbid(unsafe_code))]

#[cfg(windows)]
mod windows;

#[cfg(windows)]
pub use windows::{
    apply_private_file_acl, create_private_directory, create_private_file,
    verify_private_directory, verify_private_file, verify_single_link_regular_file,
};
