use std::ffi::c_void;
use std::fs::File;
use std::io;
use std::mem::{offset_of, size_of};
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::path::Path;
use std::ptr::{self, NonNull};

use windows_sys::Win32::Foundation::{
    GENERIC_READ, GENERIC_WRITE, INVALID_HANDLE_VALUE, LocalFree,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SDDL_REVISION_1, SE_FILE_OBJECT, SetSecurityInfo,
};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
    DACL_SECURITY_INFORMATION, GetAce, GetAclInformation, GetLengthSid,
    GetSecurityDescriptorControl, GetSecurityDescriptorDacl, GetSecurityDescriptorOwner,
    GetTokenInformation, INHERITED_ACE, IsValidAcl, IsValidSid, OWNER_SECURITY_INFORMATION,
    PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
    SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, CREATE_NEW, CreateDirectoryW, CreateFileW, FILE_ALL_ACCESS,
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, GetFileInformationByHandle,
    OPEN_EXISTING, READ_CONTROL, WRITE_DAC,
};
use windows_sys::Win32::System::SystemServices::ACCESS_ALLOWED_ACE_TYPE;
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

const MAX_SID_STRING_UNITS: usize = 1_024;

/// Creates one directory with a protected ACL installed by the kernel at creation.
///
/// The operation has create-new semantics and never modifies an existing path.
pub fn create_private_directory(path: &Path) -> io::Result<()> {
    let descriptor = PrivateSecurityDescriptor::new(ObjectKind::Directory)?;
    let path = wide_path(path)?;
    let attributes = descriptor.security_attributes();

    // SAFETY: `path` is NUL-terminated, `attributes` points to a live, valid,
    // self-relative descriptor, and neither pointer escapes the call.
    if unsafe { CreateDirectoryW(path.as_ptr(), &attributes) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Creates one regular file with a protected ACL and create-new semantics.
pub fn create_private_file(path: &Path) -> io::Result<File> {
    let descriptor = PrivateSecurityDescriptor::new(ObjectKind::File)?;
    let path = wide_path(path)?;
    let attributes = descriptor.security_attributes();

    // SAFETY: `path` and `attributes` satisfy the Win32 lifetime and layout
    // contracts. CREATE_NEW prevents replacement or traversal of an existing
    // final-path object. The returned handle is transferred to `File` below.
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            &attributes,
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: successful CreateFileW returns a unique owned HANDLE, and File
    // assumes exactly that ownership and closes it once.
    Ok(unsafe { File::from_raw_handle(handle) })
}

/// Replaces a current-user-owned regular file's DACL with the Ariax private ACL.
pub fn apply_private_file_acl(path: &Path) -> io::Result<()> {
    let opened = open_path(path, ObjectKind::File, READ_CONTROL | WRITE_DAC)?;
    let expected = PrivateSecurityDescriptor::new(ObjectKind::File)?;
    let actual = handle_security_descriptor(opened.handle.as_raw_handle())?;
    verify_current_owner(actual.as_ptr(), expected.as_ptr())?;
    let (_, dacl) = descriptor_owner_dacl(expected.as_ptr())?;
    let security_information = DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION;

    // SAFETY: the no-follow handle remains live, the expected DACL borrows from
    // a live descriptor, and null owner/group/SACL pointers match the requested
    // information bits. Ownership was verified before this mutation.
    let status = unsafe {
        SetSecurityInfo(
            opened.handle.as_raw_handle(),
            SE_FILE_OBJECT,
            security_information,
            ptr::null_mut(),
            ptr::null_mut(),
            dacl,
            ptr::null(),
        )
    };
    if status != 0 {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    let tightened = handle_security_descriptor(opened.handle.as_raw_handle())?;
    verify_security_descriptor(tightened.as_ptr(), expected.as_ptr())
}

/// Verifies a directory without changing its owner or ACL.
pub fn verify_private_directory(path: &Path) -> io::Result<()> {
    verify_private_path(path, ObjectKind::Directory)
}

/// Verifies a regular file without changing its owner or ACL.
pub fn verify_private_file(path: &Path) -> io::Result<()> {
    verify_private_path(path, ObjectKind::File)
}

/// Verifies a uniquely linked regular file through a no-follow handle.
///
/// This operation reads only file attributes. It neither requires a private
/// ACL nor changes the file's owner or security descriptor.
pub fn verify_single_link_regular_file(path: &Path) -> io::Result<()> {
    open_path(path, ObjectKind::File, 0).map(drop)
}

#[derive(Clone, Copy)]
enum ObjectKind {
    Directory,
    File,
}

impl ObjectKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Directory => "directory",
            Self::File => "regular file",
        }
    }
}

fn verify_private_path(path: &Path, kind: ObjectKind) -> io::Result<()> {
    let opened = open_path(path, kind, READ_CONTROL)?;
    let actual = handle_security_descriptor(opened.handle.as_raw_handle())?;
    let expected = PrivateSecurityDescriptor::new(kind)?;
    verify_security_descriptor(actual.as_ptr(), expected.as_ptr())
}

struct OpenedPath {
    handle: OwnedHandle,
}

fn open_path(path: &Path, kind: ObjectKind, additional_access: u32) -> io::Result<OpenedPath> {
    let path = wide_path(path)?;
    // SAFETY: `path` is NUL-terminated. OPEN_EXISTING plus
    // FILE_FLAG_OPEN_REPARSE_POINT opens the final object itself without
    // following a final reparse point. BACKUP_SEMANTICS permits directory
    // handles, and the returned handle is uniquely owned.
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            FILE_READ_ATTRIBUTES | additional_access,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateFileW returned a unique owned HANDLE.
    let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
    let mut information = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: `handle` is live and `information` has the required layout.
    if unsafe { GetFileInformationByHandle(handle.as_raw_handle(), &mut information) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(permission_denied("private path is a Windows reparse point"));
    }
    let is_directory = information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0;
    if is_directory != matches!(kind, ObjectKind::Directory) {
        return Err(permission_denied(kind.label()));
    }
    if matches!(kind, ObjectKind::File) && information.nNumberOfLinks != 1 {
        return Err(permission_denied(
            "private files must have exactly one hard link",
        ));
    }
    Ok(OpenedPath { handle })
}

struct PrivateSecurityDescriptor {
    allocation: LocalAllocation,
}

impl PrivateSecurityDescriptor {
    fn new(kind: ObjectKind) -> io::Result<Self> {
        let user = current_user_sid_string()?;
        let flags = match kind {
            ObjectKind::Directory => "OICI",
            ObjectKind::File => "",
        };
        let sddl =
            format!("O:{user}D:P(A;{flags};FA;;;{user})(A;{flags};FA;;;SY)(A;{flags};FA;;;BA)");
        Self::from_sddl(&sddl)
    }

    fn from_sddl(sddl: &str) -> io::Result<Self> {
        let mut encoded: Vec<u16> = sddl.encode_utf16().collect();
        encoded.push(0);
        let mut descriptor = ptr::null_mut();

        // SAFETY: the SDDL input is NUL-terminated UTF-16 and the output slot is
        // valid. On success Windows returns a LocalAlloc allocation we own.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                encoded.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let allocation = LocalAllocation::new(descriptor)?;
        Ok(Self { allocation })
    }

    fn as_ptr(&self) -> PSECURITY_DESCRIPTOR {
        self.allocation.as_ptr()
    }

    fn security_attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.as_ptr(),
            bInheritHandle: 0,
        }
    }
}

struct LocalAllocation(NonNull<c_void>);

impl LocalAllocation {
    fn new(pointer: *mut c_void) -> io::Result<Self> {
        NonNull::new(pointer)
            .map(Self)
            .ok_or_else(|| io::Error::other("Win32 returned a null allocation"))
    }

    fn as_ptr(&self) -> *mut c_void {
        self.0.as_ptr()
    }
}

impl Drop for LocalAllocation {
    fn drop(&mut self) {
        // SAFETY: the pointer came from a Win32 API documented to allocate with
        // LocalAlloc, and this owner frees it exactly once.
        unsafe {
            LocalFree(self.as_ptr());
        }
    }
}

fn current_user_sid_string() -> io::Result<String> {
    let mut token = ptr::null_mut();
    // SAFETY: the output pointer is valid, and GetCurrentProcess returns the
    // documented pseudo-handle accepted by OpenProcessToken.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: OpenProcessToken returned a unique real handle on success.
    let token = unsafe { OwnedHandle::from_raw_handle(token) };

    let mut required = 0_u32;
    // SAFETY: a null buffer with length zero is the documented sizing query.
    unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            ptr::null_mut(),
            0,
            &mut required,
        );
    }
    if required < size_of::<TOKEN_USER>() as u32 {
        return Err(io::Error::last_os_error());
    }

    let word_count = (required as usize).div_ceil(size_of::<usize>());
    let mut storage = vec![0_usize; word_count];
    let mut written = required;
    // SAFETY: `storage` is pointer-aligned and has at least `required` writable
    // bytes. The token and output-length pointers remain valid for the call.
    if unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            storage.as_mut_ptr().cast(),
            required,
            &mut written,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if written < size_of::<TOKEN_USER>() as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "TokenUser response is truncated",
        ));
    }

    // SAFETY: GetTokenInformation initialized a TOKEN_USER at the aligned start
    // of `storage`, which remains alive while its SID is converted.
    let user = unsafe { &*storage.as_ptr().cast::<TOKEN_USER>() };
    // SAFETY: the TOKEN_USER buffer remains live and Win32 supplied its SID pointer.
    let user_sid_is_valid = !user.User.Sid.is_null() && unsafe { IsValidSid(user.User.Sid) } != 0;
    if !user_sid_is_valid {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "current token contains an invalid user SID",
        ));
    }

    let mut string_sid = ptr::null_mut();
    // SAFETY: the token buffer owns a valid SID for the duration of this call;
    // the output slot receives a LocalAlloc UTF-16 string on success.
    if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut string_sid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let string_sid = LocalAllocation::new(string_sid.cast())?;
    wide_pointer_to_string(string_sid.as_ptr().cast())
}

fn wide_pointer_to_string(pointer: *const u16) -> io::Result<String> {
    if pointer.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Win32 returned a null string",
        ));
    }
    let mut length = 0;
    while length < MAX_SID_STRING_UNITS {
        // SAFETY: ConvertSidToStringSidW returns a NUL-terminated allocation.
        // The defensive cap prevents an unbounded scan if that contract breaks.
        if unsafe { *pointer.add(length) } == 0 {
            // SAFETY: the scan established that `length` initialized code units
            // precede the terminator in the live allocation.
            let units = unsafe { std::slice::from_raw_parts(pointer, length) };
            return String::from_utf16(units).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "SID string is not valid UTF-16")
            });
        }
        length += 1;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "SID string exceeds the defensive length limit",
    ))
}

fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
    let mut encoded = Vec::new();
    for unit in path.as_os_str().encode_wide() {
        if unit == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows path contains an interior NUL",
            ));
        }
        encoded.push(unit);
    }
    encoded.push(0);
    Ok(encoded)
}

fn handle_security_descriptor(handle: *mut c_void) -> io::Result<LocalAllocation> {
    let mut descriptor = ptr::null_mut();
    // SAFETY: `handle` is a live filesystem handle opened with READ_CONTROL,
    // all unused component outputs are null, and the descriptor output slot is
    // valid. The returned allocation is owned by the caller.
    let status = unsafe {
        GetSecurityInfo(
            handle,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    LocalAllocation::new(descriptor)
}

fn descriptor_owner_dacl(descriptor: PSECURITY_DESCRIPTOR) -> io::Result<(PSID, *mut ACL)> {
    let owner = descriptor_owner(descriptor)?;
    let mut dacl_present = 0;
    let mut dacl = ptr::null_mut();
    let mut dacl_defaulted = 0;
    // SAFETY: same descriptor lifetime and valid output slots as above.
    if unsafe {
        GetSecurityDescriptorDacl(
            descriptor,
            &mut dacl_present,
            &mut dacl,
            &mut dacl_defaulted,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful descriptor query returned `dacl`, which borrows
    // from the live descriptor allocation.
    let dacl_is_valid = !dacl.is_null() && unsafe { IsValidAcl(dacl) } != 0;
    if dacl_present == 0 || !dacl_is_valid {
        return Err(permission_denied("security descriptor has no valid DACL"));
    }
    Ok((owner, dacl))
}

fn verify_security_descriptor(
    actual: PSECURITY_DESCRIPTOR,
    expected: PSECURITY_DESCRIPTOR,
) -> io::Result<()> {
    let mut control = 0;
    let mut revision = 0;
    // SAFETY: `actual` points to a live descriptor and both output slots are valid.
    if unsafe { GetSecurityDescriptorControl(actual, &mut control, &mut revision) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if control & SE_DACL_PROTECTED == 0 {
        return Err(permission_denied("DACL inheritance is not protected"));
    }

    verify_current_owner(actual, expected)?;
    let (_, actual_dacl) = descriptor_owner_dacl(actual)?;
    let (_, expected_dacl) = descriptor_owner_dacl(expected)?;

    let mut actual_entries = acl_entries(actual_dacl)?;
    let mut expected_entries = acl_entries(expected_dacl)?;
    actual_entries.sort_unstable();
    expected_entries.sort_unstable();
    if actual_entries != expected_entries {
        return Err(permission_denied(
            "DACL does not exactly grant FullControl to the current user, SYSTEM, and Administrators",
        ));
    }
    Ok(())
}

fn verify_current_owner(
    actual: PSECURITY_DESCRIPTOR,
    expected: PSECURITY_DESCRIPTOR,
) -> io::Result<()> {
    let actual_owner = descriptor_owner(actual)?;
    let expected_owner = descriptor_owner(expected)?;
    if sid_bytes(actual_owner)? != sid_bytes(expected_owner)? {
        return Err(permission_denied(
            "filesystem object owner is not the current user",
        ));
    }
    Ok(())
}

fn descriptor_owner(descriptor: PSECURITY_DESCRIPTOR) -> io::Result<PSID> {
    let mut owner = ptr::null_mut();
    let mut owner_defaulted = 0;
    // SAFETY: `descriptor` points to a live descriptor and both output slots are
    // valid. The owner pointer borrows from the descriptor allocation.
    if unsafe { GetSecurityDescriptorOwner(descriptor, &mut owner, &mut owner_defaulted) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful descriptor query returned `owner`, which borrows
    // from the live descriptor allocation.
    let owner_is_valid = !owner.is_null() && unsafe { IsValidSid(owner) } != 0;
    if !owner_is_valid {
        return Err(permission_denied("security descriptor has no valid owner"));
    }
    Ok(owner)
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct AceEntry {
    flags: u8,
    mask: u32,
    sid: Vec<u8>,
}

fn acl_entries(dacl: *const ACL) -> io::Result<Vec<AceEntry>> {
    // SAFETY: callers supply a DACL borrowed from a live security descriptor.
    let dacl_is_valid = !dacl.is_null() && unsafe { IsValidAcl(dacl) } != 0;
    if !dacl_is_valid {
        return Err(permission_denied("DACL is invalid"));
    }
    let mut information = ACL_SIZE_INFORMATION::default();
    // SAFETY: `dacl` is valid and `information` has the exact required layout.
    if unsafe {
        GetAclInformation(
            dacl,
            (&mut information as *mut ACL_SIZE_INFORMATION).cast(),
            size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }

    let mut entries = Vec::with_capacity(information.AceCount as usize);
    for index in 0..information.AceCount {
        let mut raw_ace = ptr::null_mut();
        // SAFETY: GetAclInformation established the index range and GetAce
        // returns a pointer borrowed from the live ACL.
        if unsafe { GetAce(dacl, index, &mut raw_ace) } == 0 {
            return Err(io::Error::last_os_error());
        }
        if raw_ace.is_null() {
            return Err(permission_denied("DACL contains a null ACE"));
        }
        // SAFETY: GetAce returns at least an ACE_HEADER for a valid ACL.
        let header = unsafe { &*raw_ace.cast::<ACE_HEADER>() };
        let sid_offset = offset_of!(ACCESS_ALLOWED_ACE, SidStart);
        if u32::from(header.AceType) != ACCESS_ALLOWED_ACE_TYPE
            || usize::from(header.AceSize) < size_of::<ACCESS_ALLOWED_ACE>()
        {
            return Err(permission_denied("DACL contains an unexpected ACE"));
        }
        // SAFETY: the type and size checks establish the fixed fields of an
        // ACCESS_ALLOWED_ACE are present in the ACL-owned allocation.
        let ace = unsafe { &*raw_ace.cast::<ACCESS_ALLOWED_ACE>() };
        if ace.Header.AceFlags & INHERITED_ACE as u8 != 0 || ace.Mask != FILE_ALL_ACCESS {
            return Err(permission_denied("DACL contains an unexpected ACE"));
        }
        // SAFETY: SidStart is the documented first byte of the SID embedded in
        // an ACCESS_ALLOWED_ACE, and AceSize bounds are checked below.
        let sid = unsafe { ptr::addr_of!((*raw_ace.cast::<ACCESS_ALLOWED_ACE>()).SidStart) }
            .cast_mut()
            .cast();
        let sid = sid_bytes_bounded(sid, usize::from(header.AceSize) - sid_offset)?;
        entries.push(AceEntry {
            flags: ace.Header.AceFlags,
            mask: ace.Mask,
            sid,
        });
    }
    Ok(entries)
}

fn sid_bytes(sid: PSID) -> io::Result<Vec<u8>> {
    // SAFETY: callers supply a SID borrowed from a live descriptor or ACE.
    let sid_is_valid = !sid.is_null() && unsafe { IsValidSid(sid) } != 0;
    if !sid_is_valid {
        return Err(permission_denied(
            "security descriptor contains an invalid SID",
        ));
    }
    // SAFETY: IsValidSid accepted the SID and GetLengthSid only reads its header.
    let length = unsafe { GetLengthSid(sid) } as usize;
    // SAFETY: a valid SID occupies exactly GetLengthSid initialized bytes.
    Ok(unsafe { std::slice::from_raw_parts(sid.cast::<u8>(), length) }.to_vec())
}

fn sid_bytes_bounded(sid: PSID, maximum: usize) -> io::Result<Vec<u8>> {
    const SID_HEADER_BYTES: usize = 8;
    if sid.is_null() || maximum < SID_HEADER_BYTES {
        return Err(permission_denied("ACE contains a truncated SID"));
    }
    // SAFETY: the enclosing validated ACE provides at least the eight-byte SID
    // header. Its second byte is the sub-authority count.
    let sub_authorities = unsafe { *sid.cast::<u8>().add(1) } as usize;
    let length = SID_HEADER_BYTES
        .checked_add(
            sub_authorities
                .checked_mul(size_of::<u32>())
                .ok_or_else(|| permission_denied("ACE SID length overflowed"))?,
        )
        .ok_or_else(|| permission_denied("ACE SID length overflowed"))?;
    if length > maximum {
        return Err(permission_denied("ACE SID exceeds its record boundary"));
    }
    // SAFETY: the length was derived from the SID header and checked against
    // the containing ACE before asking Win32 to validate the structure.
    if unsafe { IsValidSid(sid) } == 0 {
        return Err(permission_denied("ACE contains an invalid SID"));
    }
    // SAFETY: the containing ACE owns `length` initialized SID bytes.
    Ok(unsafe { std::slice::from_raw_parts(sid.cast::<u8>(), length) }.to_vec())
}

fn permission_denied(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDirectory(std::path::PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "ariax-windows-security-{}-{timestamp}-{nonce}",
                std::process::id()
            ));
            fs::create_dir(&path).expect("create test parent");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn creates_and_verifies_private_objects() {
        let root = TestDirectory::new();
        let directory = root.path().join("private 🔒");
        create_private_directory(&directory).expect("create private directory");
        verify_private_directory(&directory).expect("verify private directory");

        let file_path = directory.join("session.db");
        drop(create_private_file(&file_path).expect("create private file"));
        verify_private_file(&file_path).expect("verify private file");
    }

    #[test]
    fn creation_never_replaces_existing_objects() {
        let root = TestDirectory::new();
        let directory = root.path().join("private");
        create_private_directory(&directory).expect("create private directory");
        assert_eq!(
            create_private_directory(&directory)
                .expect_err("existing directory must be rejected")
                .kind(),
            io::ErrorKind::AlreadyExists
        );

        let file_path = directory.join("session.db");
        drop(create_private_file(&file_path).expect("create private file"));
        assert_eq!(
            create_private_file(&file_path)
                .expect_err("existing file must be rejected")
                .kind(),
            io::ErrorKind::AlreadyExists
        );
    }

    #[test]
    fn apply_replaces_an_inherited_file_acl() {
        let root = TestDirectory::new();
        let file_path = root.path().join("sidecar-wal");
        fs::write(&file_path, b"wal").expect("create inherited file");
        verify_single_link_regular_file(&file_path).expect("verify file identity");
        assert!(verify_private_file(&file_path).is_err());

        apply_private_file_acl(&file_path).expect("apply private ACL");
        verify_private_file(&file_path).expect("verify tightened file");
        assert_eq!(fs::read(&file_path).expect("read sidecar"), b"wal");
    }

    #[test]
    fn rejects_wrong_kind_and_reparse_points() {
        let require_reparse_coverage = matches!(
            std::env::var("ARIAX_REQUIRE_WINDOWS_REPARSE_TEST").as_deref(),
            Ok("1")
        );
        let root = TestDirectory::new();
        let directory = root.path().join("private");
        create_private_directory(&directory).expect("create private directory");
        let file_path = directory.join("file");
        drop(create_private_file(&file_path).expect("create private file"));

        assert_eq!(
            verify_private_file(&directory)
                .expect_err("directory is not a file")
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            verify_private_directory(&file_path)
                .expect_err("file is not a directory")
                .kind(),
            io::ErrorKind::PermissionDenied
        );

        let link = directory.join("file-link");
        match std::os::windows::fs::symlink_file(&file_path, &link) {
            Ok(()) => assert_eq!(
                verify_private_file(&link)
                    .expect_err("reparse point must be rejected")
                    .kind(),
                io::ErrorKind::PermissionDenied
            ),
            Err(error)
                if error.kind() == io::ErrorKind::PermissionDenied && !require_reparse_coverage =>
            {
                eprintln!(
                    "skipping Windows reparse-point coverage because symlink creation is not permitted; set ARIAX_REQUIRE_WINDOWS_REPARSE_TEST=1 to require it"
                );
                return;
            }
            Err(error) => panic!("create file symlink: {error}"),
        }

        let directory_target = directory.join("directory-target");
        create_private_directory(&directory_target).expect("create target directory");
        let directory_link = directory.join("directory-link");
        match std::os::windows::fs::symlink_dir(&directory_target, &directory_link) {
            Ok(()) => assert_eq!(
                verify_private_directory(&directory_link)
                    .expect_err("directory reparse point must be rejected")
                    .kind(),
                io::ErrorKind::PermissionDenied
            ),
            Err(error)
                if error.kind() == io::ErrorKind::PermissionDenied && !require_reparse_coverage =>
            {
                eprintln!(
                    "skipping Windows directory reparse-point coverage because symlink creation is not permitted; set ARIAX_REQUIRE_WINDOWS_REPARSE_TEST=1 to require it"
                );
            }
            Err(error) => panic!("create directory symlink: {error}"),
        }
    }

    #[test]
    fn rejects_hard_link_aliases_before_verification_or_mutation() {
        let root = TestDirectory::new();
        let directory = root.path().join("private");
        create_private_directory(&directory).expect("create private directory");
        let file_path = directory.join("session.db.ariax-owner-lock");
        drop(create_private_file(&file_path).expect("create private file"));
        let alias = directory.join("owner-lock-alias");
        fs::hard_link(&file_path, &alias).expect("create hard-link alias");

        for path in [&file_path, &alias] {
            assert_eq!(
                verify_single_link_regular_file(path)
                    .expect_err("hard-linked file identity must be rejected")
                    .kind(),
                io::ErrorKind::PermissionDenied
            );
            assert_eq!(
                verify_private_file(path)
                    .expect_err("hard-linked file must be rejected")
                    .kind(),
                io::ErrorKind::PermissionDenied
            );
            assert_eq!(
                apply_private_file_acl(path)
                    .expect_err("hard-linked file must not be mutated")
                    .kind(),
                io::ErrorKind::PermissionDenied
            );
        }
    }

    #[test]
    fn descriptor_validation_rejects_wrong_owner_foreign_ace_and_inheritance() {
        let expected = PrivateSecurityDescriptor::new(ObjectKind::File).expect("expected ACL");
        let current = current_user_sid_string().expect("current user SID");
        let base = format!("(A;;FA;;;{current})(A;;FA;;;SY)(A;;FA;;;BA)");

        let wrong_owner = PrivateSecurityDescriptor::from_sddl(&format!("O:SYD:P{base}"))
            .expect("wrong-owner descriptor");
        assert!(verify_current_owner(wrong_owner.as_ptr(), expected.as_ptr()).is_err());
        assert!(verify_security_descriptor(wrong_owner.as_ptr(), expected.as_ptr()).is_err());

        let foreign =
            PrivateSecurityDescriptor::from_sddl(&format!("O:{current}D:P{base}(A;;FR;;;WD)"))
                .expect("foreign descriptor");
        assert!(verify_security_descriptor(foreign.as_ptr(), expected.as_ptr()).is_err());

        let inherited = PrivateSecurityDescriptor::from_sddl(&format!("O:{current}D:{base}"))
            .expect("inherited descriptor");
        assert!(verify_security_descriptor(inherited.as_ptr(), expected.as_ptr()).is_err());
    }

    #[test]
    fn verification_rejects_broad_objects_without_mutating_them() {
        let root = TestDirectory::new();
        let directory_before = descriptor_snapshot(root.path(), ObjectKind::Directory);
        assert!(verify_private_directory(root.path()).is_err());
        assert_eq!(
            descriptor_snapshot(root.path(), ObjectKind::Directory),
            directory_before
        );

        let file_path = root.path().join("inherited-file");
        fs::write(&file_path, b"unchanged").expect("create inherited file");
        let file_before = descriptor_snapshot(&file_path, ObjectKind::File);
        assert!(verify_private_file(&file_path).is_err());
        assert_eq!(
            descriptor_snapshot(&file_path, ObjectKind::File),
            file_before
        );
        assert_eq!(fs::read(&file_path).expect("read file"), b"unchanged");
    }

    fn descriptor_snapshot(path: &Path, kind: ObjectKind) -> Vec<u8> {
        let opened = open_path(path, kind, READ_CONTROL).expect("open path without following");
        let descriptor = handle_security_descriptor(opened.handle.as_raw_handle())
            .expect("read security descriptor");
        // SAFETY: `descriptor` is a live self-relative security descriptor
        // returned by GetSecurityInfo.
        let length = unsafe {
            windows_sys::Win32::Security::GetSecurityDescriptorLength(descriptor.as_ptr())
        } as usize;
        assert!(length > 0);
        // SAFETY: GetSecurityDescriptorLength reports the allocation's complete
        // initialized descriptor length while `descriptor` remains live.
        unsafe { std::slice::from_raw_parts(descriptor.as_ptr().cast::<u8>(), length) }.to_vec()
    }
}
