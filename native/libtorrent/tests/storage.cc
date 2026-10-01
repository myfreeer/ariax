#include <libtorrent/aux_/file.hpp>
#include <libtorrent/aux_/part_file.hpp>
#include <libtorrent/aux_/path.hpp>
#include <libtorrent/aux_/posix_part_file.hpp>
#include <libtorrent/aux_/posix_storage.hpp>
#include <libtorrent/aux_/session_settings.hpp>
#include <array>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#ifdef _WIN32
#include <windows.h>
#include <aclapi.h>
#include <sddl.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace lt = libtorrent;
namespace fs = std::filesystem;
using namespace lt::flags;

void require(bool value, char const* message) {
    if (!value) throw std::runtime_error(message);
}

struct Fixture {
#ifdef _WIN32
    fs::path path = fs::temp_directory_path() / ("ariax-native-storage-" + std::to_string(GetCurrentProcessId()));
#else
    mode_t previous_umask = ::umask(0);
    fs::path path = fs::temp_directory_path() / ("ariax-native-storage-" + std::to_string(::getpid()));
#endif
    Fixture() {
        require(!fs::exists(path), "fresh storage fixture");
        lt::error_code error;
        lt::create_directories(path.string(), error);
        require(!error, "private storage fixture creation");
    }
    ~Fixture() {
        std::error_code error;
        fs::remove_all(path, error);
#ifndef _WIN32
        ::umask(previous_umask);
#endif
    }
};

#ifdef _WIN32
struct Security {
    PSECURITY_DESCRIPTOR descriptor = nullptr;
    PSID owner = nullptr;
    PACL acl = nullptr;
    explicit Security(fs::path const& path) {
        require(GetNamedSecurityInfoW(const_cast<wchar_t*>(path.c_str()), SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION, &owner, nullptr, &acl,
            nullptr, &descriptor) == ERROR_SUCCESS, "read native storage security");
    }
    ~Security() { LocalFree(descriptor); }
    Security(Security const&) = delete;
    Security& operator=(Security const&) = delete;
    std::wstring text() const {
        LPWSTR value = nullptr;
        require(ConvertSecurityDescriptorToStringSecurityDescriptorW(descriptor, SDDL_REVISION_1,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION, &value, nullptr), "encode ACL");
        std::wstring result(value);
        LocalFree(value);
        return result;
    }
};

void permissions(fs::path const& path, unsigned) {
    Security security(path);
    SECURITY_DESCRIPTOR_CONTROL control = 0;
    DWORD revision = 0;
    require(GetSecurityDescriptorControl(security.descriptor, &control, &revision)
        && (control & SE_DACL_PROTECTED), "native object must protect its own DACL");
    HANDLE token = nullptr;
    require(OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token), "read test user token");
    std::array<std::uintptr_t, 128> buffer{};
    DWORD bytes = sizeof(buffer);
    BOOL const loaded = GetTokenInformation(token, TokenUser, buffer.data(), bytes, &bytes);
    CloseHandle(token);
    require(loaded, "read test user SID");
    PSID user = reinterpret_cast<TOKEN_USER*>(buffer.data())->User.Sid;
    require(EqualSid(user, security.owner), "native object owner is the current user");
    ACL_SIZE_INFORMATION info{};
    require(security.acl && GetAclInformation(security.acl, &info, sizeof(info), AclSizeInformation)
        && info.AceCount == 3, "native DACL has exactly three principals");
    unsigned seen = 0;
    for (DWORD index = 0; index < info.AceCount; ++index) {
        void* raw = nullptr;
        require(GetAce(security.acl, index, &raw), "read native DACL entry");
        auto const* ace = static_cast<ACCESS_ALLOWED_ACE const*>(raw);
        require(ace->Header.AceType == ACCESS_ALLOWED_ACE_TYPE
            && !(ace->Header.AceFlags & INHERITED_ACE) && ace->Mask == FILE_ALL_ACCESS,
            "native DACL has explicit full-control grants");
        PSID sid = const_cast<DWORD*>(&ace->SidStart);
        unsigned const principal = EqualSid(sid, user) ? 1
            : IsWellKnownSid(sid, WinLocalSystemSid) ? 2
            : IsWellKnownSid(sid, WinBuiltinAdministratorsSid) ? 4 : 0;
        require(principal && !(seen & principal), "native DACL admits only expected principals");
        seen |= principal;
    }
    require(seen == 7, "native DACL retains every expected principal");
}
#else
void permissions(fs::path const& path, unsigned expected) {
    struct stat metadata{};
    require(::stat(path.c_str(), &metadata) == 0, "storage entry exists");
    require((metadata.st_mode & 0777) == expected, "private native creation mode");
}
#endif

void native_files(fs::path const& root) {
    auto const directory = root / "native/nested";
    lt::error_code error;
    lt::create_directories(directory.string(), error);
    require(!error, "nested native directory creation");
    permissions(root / "native", 0700);
    permissions(directory, 0700);
    for (bool executable : {false, true}) {
        auto const path = directory / (executable ? "executable" : "payload");
        auto mode = lt::aux::open_mode::write;
        if (executable) mode |= lt::aux::open_mode::executable;
        { lt::aux::file_handle file(path.string(), 4, mode); }
        permissions(path, executable ? 0700 : 0600);
#ifdef _WIN32
        Security before(path);
        require(SetNamedSecurityInfoW(const_cast<wchar_t*>(path.c_str()), SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | UNPROTECTED_DACL_SECURITY_INFORMATION,
            nullptr, nullptr, before.acl, nullptr) == ERROR_SUCCESS,
            "existing inherited ACL fixture");
        auto const existing = Security(path).text();
        { lt::aux::file_handle file(path.string(), 4, mode); }
        require(Security(path).text() == existing, "opening an existing file preserves its ACL");
#else
        require(::chmod(path.c_str(), 0640) == 0, "existing permission fixture");
        { lt::aux::file_handle file(path.string(), 4, mode); }
        permissions(path, 0640);
#endif
    }
    bool rejected = false;
    try {
        lt::aux::file_handle file((directory / "missing").string(), 0, lt::aux::open_mode::read_only);
    } catch (lt::storage_error const&) {
        rejected = true;
    }
    require(rejected && !fs::exists(directory / "missing"), "read-only open cannot create payload");
}

template <typename PartFile>
void part_file(fs::path const& directory) {
    std::array<char, 4> const payload{{'p', 'a', 'r', 't'}};
    lt::error_code error;
    {
        PartFile file(directory.string(), "payload.parts", 1, 16384);
        require(file.write(payload, lt::piece_index_t{0}, 0, error) == 4 && !error, "part-file write");
        file.flush_metadata(error);
        require(!error, "part-file metadata flush");
    }
    permissions(directory, 0700);
    permissions(directory / "payload.parts", 0600);
    {
        PartFile restored(directory.string(), "payload.parts", 1, 16384);
        std::array<char, 4> read{};
        require(restored.read(read, lt::piece_index_t{0}, 0, error) == 4 && !error, "part-file reopen");
        require(read == payload, "restored part-file bytes");
    }
}

void stdio_files(fs::path const& root) {
    lt::file_storage files;
    files.add_file("stdio/nested/payload", 4);
    files.add_file("stdio/nested/executable", 4, lt::file_storage::flag_executable);
    files.set_piece_length(16384);
    files.set_num_pieces(1);
    lt::renamed_files renamed;
    lt::aux::vector<lt::download_priority_t, lt::file_index_t> priorities;
    auto const path = root.string();
    lt::storage_params params{files, renamed, path, {}, lt::storage_mode_sparse,
        priorities, lt::sha1_hash{}, true, false};
    lt::aux::posix_storage storage(params);
    lt::aux::session_settings settings;
    lt::storage_error error;
    storage.initialize(settings, error);
    require(!error, "stdio storage initialization");
    std::array<char, 8> const payload{{'p', 'a', 'y', 'l', 'o', 'a', 'd', '!'}};
    require(storage.write(settings, payload, lt::piece_index_t{0}, 0, error) == 8 && !error,
        "stdio payload write");
    permissions(root / "stdio", 0700);
    permissions(root / "stdio/nested", 0700);
    permissions(root / "stdio/nested/payload", 0600);
    permissions(root / "stdio/nested/executable", 0700);
    std::array<char, 8> read{};
    require(storage.read(settings, read, lt::piece_index_t{0}, 0, error) == 8 && !error,
        "stdio payload reopen");
    require(read == payload, "stdio payload bytes");
}

int main() try {
    // A separate single-threaded process avoids changing an application's umask.
    Fixture fixture;
    permissions(fixture.path, 0700);
    native_files(fixture.path);
    part_file<lt::aux::part_file>(fixture.path / "native-parts");
    part_file<lt::aux::posix_part_file>(fixture.path / "stdio-parts");
    stdio_files(fixture.path);
    std::cout << "Native private storage passed.\n";
} catch (std::exception const& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
