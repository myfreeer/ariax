#include "ariax-bt-libtorrent-sys/src/lib.rs.h"
#include "bridge.h"
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/bencode.hpp>
#include <libtorrent/ip_filter.hpp>
#include <libtorrent/load_torrent.hpp>
#include <libtorrent/magnet_uri.hpp>
#include <libtorrent/peer_info.hpp>
#include <libtorrent/read_resume_data.hpp>
#include <libtorrent/session.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/settings_pack.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_status.hpp>
#include <libtorrent/version.hpp>
#include <libtorrent/write_resume_data.hpp>
#include <algorithm>
#include <climits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace ariax::bt {
namespace lt = libtorrent;
namespace {
void require(bool condition) {
    if (!condition) throw std::runtime_error("bt/native-contract-rejected");
}

std::string string(rust::Str value) { return {value.data(), value.size()}; }
std::string string(rust::String const& value) { return {value.data(), value.size()}; }
lt::span<char const> bytes(rust::Slice<std::uint8_t const> value) {
    return {reinterpret_cast<char const*>(value.data()), static_cast<std::ptrdiff_t>(value.size())};
}
rust::Vec<std::uint8_t> owned_bytes(char const* data, std::size_t size) {
    rust::Vec<std::uint8_t> result;
    result.reserve(size);
    for (std::size_t i = 0; i < size; ++i) result.push_back(static_cast<std::uint8_t>(data[i]));
    return result;
}
std::uint64_t unsigned_count(std::int64_t value) {
    return static_cast<std::uint64_t>(std::max(std::int64_t(0), value));
}

// bencode checks its limit before every output growth. The bounded metadata,
// peer and file counts separately bound the intermediate entry tree.
struct BoundedOutput {
    using difference_type = std::ptrdiff_t;
    using value_type = void;
    using pointer = void;
    using reference = void;
    using iterator_category = std::output_iterator_tag;
    std::vector<char>& output;
    std::size_t limit;
    BoundedOutput& operator*() { return *this; }
    BoundedOutput& operator++() { return *this; }
    BoundedOutput operator++(int) { return *this; }
    BoundedOutput& operator=(char value) {
        require(output.size() < limit);
        if (output.size() == output.capacity()) {
            output.reserve(std::min(limit, std::max(std::size_t(256), output.capacity() * 2)));
        }
        output.push_back(value);
        return *this;
    }
};

struct CheckpointState {
    std::mutex mutex;
    std::uint64_t request;
    std::uint8_t state = 0;
    std::vector<char> bytes;
    explicit CheckpointState(std::uint64_t id) : request(id) {}
};

lt::load_torrent_limits limits(NativeOptions const& options) {
    lt::load_torrent_limits result;
    result.max_buffer_size = int(options.metadata_bytes);
    result.max_pieces = int(options.max_pieces);
    result.max_decode_depth = int(options.decode_depth);
    result.max_decode_tokens = int(options.decode_tokens);
    result.max_directory_depth = int(options.decode_depth);
    result.max_duplicate_filenames = 64;
    return result;
}

void validate_info(lt::torrent_info const& info, NativeOptions const& options) {
    require(info.num_files() > 0 && info.num_files() <= int(options.max_files));
    require(info.num_pieces() > 0 && info.num_pieces() <= int(options.max_pieces));
    require(info.info_section().size() <= int(options.metadata_bytes));
    for (auto index : info.layout().file_range()) {
        require(!(info.layout().file_flags(index) & lt::file_storage::flag_symlink));
    }
}

void filter_private(lt::session& session) {
    lt::ip_filter filter;
    // The first full-build lane denies special-use destinations for untrusted
    // callers, including tracker resolutions and web seeds. Explicit trusted
    // local admission may use private peers for LAN transfers and fixtures.
    for (auto const& range : {
            std::pair{"0.0.0.0", "0.255.255.255"},
            std::pair{"10.0.0.0", "10.255.255.255"},
            std::pair{"100.64.0.0", "100.127.255.255"},
            std::pair{"127.0.0.0", "127.255.255.255"},
            std::pair{"169.254.0.0", "169.254.255.255"},
            std::pair{"172.16.0.0", "172.31.255.255"},
            std::pair{"192.0.0.0", "192.0.0.255"},
            std::pair{"192.0.2.0", "192.0.2.255"},
            std::pair{"192.88.99.0", "192.88.99.255"},
            std::pair{"192.168.0.0", "192.168.255.255"},
            std::pair{"198.18.0.0", "198.19.255.255"},
            std::pair{"198.51.100.0", "198.51.100.255"},
            std::pair{"203.0.113.0", "203.0.113.255"},
            std::pair{"224.0.0.0", "255.255.255.255"},
            std::pair{"::", "::ffff:ffff:ffff"},
            std::pair{"64:ff9b::", "64:ff9b:ffff:ffff:ffff:ffff:ffff:ffff"},
            std::pair{"100::", "100:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
            std::pair{"2001::", "2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff"},
            std::pair{"2001:db8::", "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"},
            std::pair{"2002::", "2002:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
            std::pair{"3fff::", "3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff"},
            std::pair{"fc00::", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"}}) {
        filter.add_rule(lt::make_address(range.first), lt::make_address(range.second), lt::ip_filter::blocked);
    }
    session.set_ip_filter(filter);
}
}

struct NativeSession::Impl {
    NativeOptions options;
    std::unique_ptr<lt::session> session;
    std::map<std::uint64_t, lt::torrent_handle> torrents;
    std::map<std::uint64_t, std::shared_ptr<CheckpointState>> checkpoints;

    lt::torrent_handle handle(std::uint64_t gid) const {
        auto found = torrents.find(gid);
        require(found != torrents.end() && found->second.is_valid());
        return found->second;
    }
};

NativeSession::NativeSession(NativeOptions const& options) try : impl_(std::make_unique<Impl>()) {
    require(options.max_torrents > 0 && options.max_torrents <= 4096);
    require(options.connections >= 2 && options.connections <= 16384);
    require(options.files >= 2 && options.files <= 16384);
    require(options.disk_threads >= 1 && options.disk_threads <= 16);
    require(options.metadata_bytes > 0 && options.metadata_bytes <= 64 * 1024 * 1024);
    require(options.max_files > 0 && options.max_files <= 100000);
    require(options.max_pieces > 0 && options.max_pieces <= 2 * 1024 * 1024);
    require(options.decode_depth > 0 && options.decode_depth <= 100);
    require(options.decode_tokens > 0 && options.decode_tokens <= 1000000);
    require(options.alert_items > 0 && options.alert_items <= 4096);
    require(options.download_limit <= INT_MAX && options.upload_limit <= INT_MAX);
    require(options.encryption <= 2 && options.listen.size() <= 1024);
    impl_->options = options;
    lt::settings_pack pack;
    pack.set_str(lt::settings_pack::listen_interfaces, string(options.listen));
    pack.set_str(lt::settings_pack::user_agent, "ariax/0.1");
    // Keep the pinned release's public bootstrap defaults when DHT is enabled.
    if (!options.dht) pack.set_str(lt::settings_pack::dht_bootstrap_nodes, "");
    pack.set_bool(lt::settings_pack::enable_dht, options.dht);
    pack.set_bool(lt::settings_pack::enable_lsd, false);
    pack.set_bool(lt::settings_pack::enable_upnp, false);
    pack.set_bool(lt::settings_pack::enable_natpmp, false);
    pack.set_bool(lt::settings_pack::ssrf_mitigation, true);
    pack.set_bool(lt::settings_pack::apply_ip_filter_to_trackers, true);
    pack.set_bool(lt::settings_pack::validate_https_trackers, true);
    pack.set_bool(lt::settings_pack::allow_idna, false);
    pack.set_int(lt::settings_pack::connections_limit, int(options.connections));
    pack.set_int(lt::settings_pack::file_pool_size, int(options.files));
    pack.set_int(lt::settings_pack::aio_threads, int(options.disk_threads));
    pack.set_int(lt::settings_pack::hashing_threads, 0);
    pack.set_int(lt::settings_pack::max_peerlist_size, int(options.connections));
    pack.set_int(lt::settings_pack::max_paused_peerlist_size, int(options.connections));
    pack.set_int(lt::settings_pack::max_metadata_size, int(options.metadata_bytes));
    pack.set_int(lt::settings_pack::metadata_token_limit, int(options.decode_tokens));
    pack.set_int(lt::settings_pack::ariax_metadata_depth, int(options.decode_depth));
    pack.set_int(lt::settings_pack::ariax_metadata_files, int(options.max_files));
    pack.set_int(lt::settings_pack::max_piece_count, int(options.max_pieces));
    pack.set_int(lt::settings_pack::alert_queue_size, int(options.alert_items));
    pack.set_int(lt::settings_pack::alert_mask, int(std::uint32_t(lt::alert_category::error | lt::alert_category::status | lt::alert_category::storage)));
    pack.set_int(lt::settings_pack::max_queued_disk_bytes, 1024 * 1024);
    pack.set_int(lt::settings_pack::send_buffer_watermark, 64 * 1024);
    pack.set_int(lt::settings_pack::send_buffer_low_watermark, 16 * 1024);
    pack.set_int(lt::settings_pack::max_peer_recv_buffer_size, 64 * 1024);
    pack.set_int(lt::settings_pack::max_http_recv_buffer_size, 64 * 1024);
    pack.set_int(lt::settings_pack::max_web_seed_connections, 4);
    pack.set_int(lt::settings_pack::download_rate_limit, int(options.download_limit));
    pack.set_int(lt::settings_pack::upload_rate_limit, int(options.upload_limit));
    pack.set_int(lt::settings_pack::out_enc_policy, int(options.encryption));
    pack.set_int(lt::settings_pack::in_enc_policy, int(options.encryption));
    impl_->session = std::make_unique<lt::session>(lt::session_params(pack));
    if (!options.allow_private) filter_private(*impl_->session);
} catch (...) { throw std::runtime_error("bt/native-session-rejected"); }

NativeSession::~NativeSession() = default;

void NativeSession::add(std::uint64_t gid, rust::Slice<std::uint8_t const> torrent,
    rust::Str magnet, rust::Str root, rust::Slice<std::uint8_t const> resume_data) try {
    require(gid != 0 && impl_->torrents.count(gid) == 0 && impl_->torrents.size() < impl_->options.max_torrents);
    require(torrent.size() <= impl_->options.metadata_bytes && magnet.size() <= 65536);
    require(resume_data.size() <= 64 * 1024 * 1024 && root.size() > 0 && root.size() <= 32768);
    require(torrent.empty() != magnet.empty());
    lt::add_torrent_params params;
    auto cfg = limits(impl_->options);
    if (!torrent.empty()) {
        params = lt::load_torrent_buffer(bytes(torrent), cfg);
        require(bool(params.ti));
        validate_info(*params.ti, impl_->options);
    } else {
        params = lt::parse_magnet_uri(string(magnet));
    }
    if (!resume_data.empty()) {
        auto restored = lt::read_resume_data(bytes(resume_data), cfg);
        auto expected = params.ti ? params.ti->info_hashes() : params.info_hashes;
        auto actual = restored.ti ? restored.ti->info_hashes() : restored.info_hashes;
        require(expected == actual);
        // Only progress and counters cross the recovery boundary. Paths, peer
        // addresses, credentials, flags and settings are admitted afresh.
        // A recovered root is rechecked against payload bytes. Imported cached
        // completion bits and file timestamps never establish verified progress.
        params.have_pieces.clear();
        params.verified_pieces.clear();
        params.merkle_trees = std::move(restored.merkle_trees);
        params.merkle_tree_mask = std::move(restored.merkle_tree_mask);
        params.verified_leaf_hashes = std::move(restored.verified_leaf_hashes);
        params.total_downloaded = restored.total_downloaded;
        params.total_uploaded = restored.total_uploaded;
    }
    params.save_path = string(root);
    params.flags &= ~(lt::torrent_flags::auto_managed | lt::torrent_flags::seed_mode | lt::torrent_flags::no_verify_files);
    params.flags |= lt::torrent_flags::ariax_hold_metadata | lt::torrent_flags::apply_ip_filter
        | lt::torrent_flags::duplicate_is_error | lt::torrent_flags::disable_lsd;
    if (!impl_->options.pex) params.flags |= lt::torrent_flags::disable_pex;
    if (!impl_->options.dht) params.flags |= lt::torrent_flags::disable_dht;
    if (torrent.empty()) params.flags &= ~lt::torrent_flags::paused;
    else params.flags |= lt::torrent_flags::paused;
    auto handle = impl_->session->add_torrent(std::move(params));
    impl_->torrents.emplace(gid, handle);
} catch (...) { throw std::runtime_error("bt/native-add-rejected"); }

NativeMetadata NativeSession::metadata(std::uint64_t gid) const try {
    auto info = impl_->handle(gid).torrent_file();
    require(bool(info));
    validate_info(*info, impl_->options);
    NativeMetadata result;
    auto data = info->info_section();
    result.info = owned_bytes(data.data(), std::size_t(data.size()));
    auto hashes = info->info_hashes();
    if (hashes.has_v1()) result.v1 = owned_bytes(hashes.v1.data(), 20);
    if (hashes.has_v2()) result.v2 = owned_bytes(hashes.v2.data(), 32);
    for (auto index : info->layout().file_range()) {
        result.file_sizes.push_back(unsigned_count(info->layout().file_size(index)));
        result.padding.push_back(info->layout().pad_file_at(index) ? 1 : 0);
        result.symlinks.push_back(bool(info->layout().file_flags(index) & lt::file_storage::flag_symlink) ? 1 : 0);
    }
    result.piece_length = std::uint32_t(info->piece_length());
    result.pieces = std::uint32_t(info->num_pieces());
    return result;
} catch (...) { throw std::runtime_error("bt/native-metadata-rejected"); }

void NativeSession::approve(std::uint64_t gid, rust::Vec<rust::String> const& paths,
    rust::Slice<std::uint8_t const> priorities) try {
    require(paths.size() == priorities.size() && paths.size() <= impl_->options.max_files);
    std::vector<std::string> names;
    std::vector<lt::download_priority_t> selected;
    names.reserve(paths.size());
    selected.reserve(priorities.size());
    std::size_t bytes_used = 0;
    for (std::size_t i = 0; i < paths.size(); ++i) {
        bytes_used += paths[i].size();
        require(bytes_used <= impl_->options.metadata_bytes && priorities[i] <= 7);
        names.push_back(string(paths[i]));
        selected.emplace_back(priorities[i]);
    }
    require(impl_->handle(gid).ariax_approve_metadata(names, selected));
} catch (...) { throw std::runtime_error("bt/native-approval-rejected"); }

void NativeSession::resume(std::uint64_t gid) try {
    auto h = impl_->handle(gid);
    require(!h.ariax_metadata_held());
    h.resume();
    h.status({}); // Wait for the session-thread ordering boundary.
} catch (...) { throw std::runtime_error("bt/native-resume-rejected"); }

void NativeSession::remove(std::uint64_t gid) try {
    auto h = impl_->handle(gid);
    impl_->session->remove_torrent(h);
    impl_->session->get_torrents(); // Session-thread acknowledgement after removal.
    impl_->torrents.erase(gid);
    impl_->checkpoints.erase(gid);
} catch (...) { throw std::runtime_error("bt/native-remove-rejected"); }

NativeStatus NativeSession::status(std::uint64_t gid) const try {
    auto h = impl_->handle(gid);
    auto status = h.status({});
    NativeStatus result;
    result.metadata = status.has_metadata;
    result.held = h.ariax_metadata_held();
    result.paused = bool(status.flags & lt::torrent_flags::paused);
    result.checking = status.state == lt::torrent_status::checking_files || status.state == lt::torrent_status::checking_resume_data;
    result.finished = status.is_finished;
    result.seeding = status.is_seeding;
    result.error = std::uint32_t(std::max(0, status.errc.value()));
    result.total_bytes = unsigned_count(status.total_wanted);
    result.done_bytes = unsigned_count(status.total_wanted_done);
    result.downloaded = unsigned_count(status.all_time_download);
    result.uploaded = unsigned_count(status.all_time_upload);
    result.download_rate = std::uint32_t(std::max(0, status.download_payload_rate));
    result.upload_rate = std::uint32_t(std::max(0, status.upload_payload_rate));
    result.peers = std::uint32_t(std::max(0, status.num_peers));
    result.seeds = std::uint32_t(std::max(0, status.num_seeds));
    result.progress_ppm = std::uint32_t(std::max(0, status.progress_ppm));
    if (status.has_metadata && !result.held) {
        auto progress = h.file_progress();
        require(progress.size() <= impl_->options.max_files);
        for (auto value : progress) result.file_progress.push_back(unsigned_count(value));
    }
    return result;
} catch (...) { throw std::runtime_error("bt/native-status-failed"); }

rust::Vec<NativePeer> NativeSession::peers(std::uint64_t gid, std::uint32_t limit) const try {
    require(limit > 0 && limit <= impl_->options.connections);
    std::vector<lt::peer_info> peers;
    impl_->handle(gid).get_peer_info(peers);
    require(peers.size() <= limit);
    rust::Vec<NativePeer> result;
    result.reserve(peers.size());
    char const* hex = "0123456789abcdef";
    for (auto const& peer : peers) {
        NativePeer value;
        value.address = peer.remote_endpoint().address().to_string();
        value.port = peer.remote_endpoint().port();
        std::string id;
        for (unsigned char byte : peer.pid) { id.push_back(hex[byte >> 4]); id.push_back(hex[byte & 15]); }
        value.peer_id = id;
        value.download_rate = std::uint32_t(std::max(0, peer.down_speed));
        value.upload_rate = std::uint32_t(std::max(0, peer.up_speed));
        value.seeder = bool(peer.flags & lt::peer_info::seed);
        value.am_choking = bool(peer.flags & lt::peer_info::choked);
        value.peer_choking = bool(peer.flags & lt::peer_info::remote_choked);
        result.push_back(std::move(value));
    }
    return result;
} catch (...) { throw std::runtime_error("bt/native-peer-query-failed"); }

void NativeSession::connect_peer(std::uint64_t gid, rust::Str address, std::uint16_t port) try {
    require(address.size() <= 64 && port != 0);
    auto endpoint = lt::tcp::endpoint(lt::make_address(string(address)), port);
    if (!impl_->options.allow_private) {
        require(!(impl_->session->get_ip_filter().access(endpoint.address()) & lt::ip_filter::blocked));
    }
    impl_->handle(gid).connect_peer(endpoint);
} catch (...) { throw std::runtime_error("bt/native-peer-rejected"); }

void NativeSession::checkpoint(std::uint64_t gid, std::uint64_t request, std::uint32_t limit) try {
    require(request != 0 && limit > 0 && limit <= 64 * 1024 * 1024);
    require(impl_->checkpoints.count(gid) == 0 && impl_->checkpoints.size() < 64);
    auto h = impl_->handle(gid);
    auto state = std::make_shared<CheckpointState>(request);
    impl_->checkpoints.emplace(gid, state);
    h.ariax_checkpoint([state, limit](lt::error_code error, lt::add_torrent_params params) noexcept {
        std::vector<char> data;
        std::uint8_t outcome = 2;
        try {
            if (!error) {
                params.trackers.clear();
                params.tracker_tiers.clear();
                params.url_seeds.clear();
                params.peers.clear();
                params.banned_peers.clear();
                params.renamed_files.clear();
                params.save_path.clear();
                params.part_file_dir.clear();
                params.root_certificate.clear();
                params.comment.clear();
                params.created_by.clear();
                lt::bencode(BoundedOutput{data, limit}, lt::write_resume_data(params));
                outcome = 1;
            }
        } catch (...) { outcome = 2; data.clear(); }
        std::lock_guard<std::mutex> lock(state->mutex);
        state->bytes = std::move(data);
        state->state = outcome;
    });
} catch (...) { throw std::runtime_error("bt/native-checkpoint-rejected"); }

NativeCheckpoint NativeSession::poll_checkpoint(std::uint64_t gid, std::uint64_t request) try {
    auto found = impl_->checkpoints.find(gid);
    require(found != impl_->checkpoints.end());
    auto state = found->second;
    std::lock_guard<std::mutex> lock(state->mutex);
    require(state->request == request);
    NativeCheckpoint result;
    result.request = request;
    result.state = state->state;
    if (state->state != 0) {
        if (state->state == 1) result.data = owned_bytes(state->bytes.data(), state->bytes.size());
        impl_->checkpoints.erase(found);
    }
    return result;
} catch (...) { throw std::runtime_error("bt/native-checkpoint-poll-rejected"); }

void NativeSession::set_rates(std::uint32_t download, std::uint32_t upload, bool suspended) try {
    require(download <= INT_MAX && upload <= INT_MAX);
    lt::settings_pack settings;
    settings.set_int(lt::settings_pack::download_rate_limit, int(download));
    settings.set_int(lt::settings_pack::upload_rate_limit, int(upload));
    if (suspended) impl_->session->pause();
    impl_->session->apply_settings(settings);
    if (!suspended) impl_->session->resume();
    auto applied = impl_->session->get_settings();
    require(impl_->session->is_paused() == suspended);
    require(applied.get_int(lt::settings_pack::download_rate_limit) == int(download)
        && applied.get_int(lt::settings_pack::upload_rate_limit) == int(upload));
} catch (...) { throw std::runtime_error("bt/native-rate-update-rejected"); }

void NativeSession::set_task_options(std::uint64_t gid, std::uint32_t download,
    std::uint32_t upload, std::uint32_t peers, bool dht, bool pex) try {
    require(download <= INT_MAX && upload <= INT_MAX && peers >= 2 && peers <= impl_->options.connections);
    auto h = impl_->handle(gid);
    h.set_download_limit(int(download));
    h.set_upload_limit(int(upload));
    h.set_max_connections(int(peers));
    auto mask = lt::torrent_flags::disable_dht | lt::torrent_flags::disable_pex;
    lt::torrent_flags_t flags{};
    if (!dht) flags |= lt::torrent_flags::disable_dht;
    if (!pex) flags |= lt::torrent_flags::disable_pex;
    h.set_flags(flags, mask);
    require(std::max(0, h.download_limit()) == int(download) && std::max(0, h.upload_limit()) == int(upload)
        && h.max_connections() == int(peers) && (h.flags() & mask) == flags);
} catch (...) { throw std::runtime_error("bt/native-option-update-rejected"); }

std::uint64_t NativeSession::drain_alerts() try {
    std::vector<lt::alert*> alerts;
    impl_->session->pop_alerts(&alerts);
    std::uint64_t dropped = 0;
    for (auto const* alert : alerts) if (lt::alert_cast<lt::alerts_dropped_alert>(alert)) ++dropped;
    return dropped;
} catch (...) { throw std::runtime_error("bt/native-alert-poll-failed"); }

std::uint16_t NativeSession::listen_port() const { return impl_->session->listen_port(); }
std::unique_ptr<NativeSession> new_session(NativeOptions const& options) { return std::make_unique<NativeSession>(options); }
rust::String native_version() { return "libtorrent/" LIBTORRENT_VERSION " ariax-patch/1"; }
}
