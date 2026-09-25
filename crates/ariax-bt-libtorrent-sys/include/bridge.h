#pragma once

#include "rust/cxx.h"
#include <cstdint>
#include <memory>

namespace ariax::bt {
struct NativeOptions;
struct NativeMetadata;
struct NativeStatus;
struct NativePeer;
struct NativeCheckpoint;

class NativeSession {
public:
    explicit NativeSession(NativeOptions const& options);
    ~NativeSession();
    void add(std::uint64_t gid, rust::Slice<std::uint8_t const> torrent,
        rust::Str magnet, rust::Str root, rust::Slice<std::uint8_t const> resume);
    NativeMetadata metadata(std::uint64_t gid) const;
    void approve(std::uint64_t gid, rust::Vec<rust::String> const& paths,
        rust::Slice<std::uint8_t const> priorities);
    void resume(std::uint64_t gid);
    void remove(std::uint64_t gid);
    NativeStatus status(std::uint64_t gid) const;
    rust::Vec<NativePeer> peers(std::uint64_t gid, std::uint32_t limit) const;
    void connect_peer(std::uint64_t gid, rust::Str address, std::uint16_t port);
    void checkpoint(std::uint64_t gid, std::uint64_t request, std::uint32_t limit);
    NativeCheckpoint poll_checkpoint(std::uint64_t gid, std::uint64_t request);
    void set_rates(std::uint32_t download, std::uint32_t upload, bool suspended);
    void set_task_options(std::uint64_t gid, std::uint32_t download,
        std::uint32_t upload, std::uint32_t peers, bool dht, bool pex);
    std::uint64_t drain_alerts();
    std::uint16_t listen_port() const;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

std::unique_ptr<NativeSession> new_session(NativeOptions const& options);
rust::String native_version();
}
