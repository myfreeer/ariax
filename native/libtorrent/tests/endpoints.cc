#include <libtorrent/alert_types.hpp>
#include <libtorrent/aux_/path.hpp>
#include <libtorrent/load_torrent.hpp>
#include <libtorrent/session.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/torrent_status.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <string>
#include <thread>

namespace lt = libtorrent;
namespace fs = std::filesystem;
using namespace std::chrono_literals;
void require(bool, char const*);

namespace {

// Nonblocking socket operations and a joined worker keep both failure and
// shutdown bounded. An empty handler is a probe that closes after accept().
class Origin {
    boost::asio::io_context io;
    lt::tcp::acceptor listener;
    std::function<std::string(std::string const&, unsigned short)> handler;
    std::atomic<bool> stop{false};
    std::thread worker;
    unsigned short const port;

    void serve(lt::tcp::socket& socket) {
        if (!handler) return;
        socket.non_blocking(true);
        std::string input;
        auto const deadline = std::chrono::steady_clock::now() + 2s;
        while (input.find("\r\n\r\n") == std::string::npos) {
            if (stop) return;
            require(std::chrono::steady_clock::now() < deadline, "fixture header deadline");
            std::array<char, 2048> bytes{};
            lt::error_code ec;
            auto const size = socket.read_some(boost::asio::buffer(bytes), ec);
            if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) {
                std::this_thread::sleep_for(1ms);
                continue;
            }
            if (ec) return;
            input.append(bytes.data(), size);
            require(input.size() <= 16384, "fixture header limit");
        }
        ++requests;
        auto const output = handler(input, port);
        std::size_t sent = 0;
        while (sent != output.size()) {
            if (stop) return;
            require(std::chrono::steady_clock::now() < deadline, "fixture response deadline");
            lt::error_code ec;
            sent += socket.write_some(boost::asio::buffer(output.data() + sent, output.size() - sent), ec);
            if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) {
                std::this_thread::sleep_for(1ms);
            } else if (ec) return;
        }
    }

public:
    std::atomic<int> connections{0};
    std::atomic<int> requests{0};
    std::atomic<bool> failed{false};

    explicit Origin(char const* address,
        std::function<std::string(std::string const&, unsigned short)> respond = {})
        : listener(io, lt::tcp::endpoint(lt::make_address(address), 0))
        , handler(std::move(respond)), port(listener.local_endpoint().port()) {
        listener.non_blocking(true);
        worker = std::thread([this] {
            try {
                while (!stop) {
                    lt::tcp::socket socket(io);
                    lt::error_code ec;
                    listener.accept(socket, ec);
                    if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) {
                        std::this_thread::sleep_for(1ms);
                        continue;
                    }
                    require(!ec, "fixture accept");
                    require(++connections <= 128, "fixture connection limit");
                    serve(socket);
                }
            } catch (...) { failed = true; }
        });
    }

    ~Origin() { stop = true; worker.join(); }
    unsigned short listen_port() const { return port; }
    std::string url(char const* host, std::string const& path) const {
        return "http://" + std::string(host) + ":" + std::to_string(port) + path;
    }
};

struct Directory {
    fs::path path = fs::temp_directory_path() / ("ariax-native-endpoints-"
        + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    Directory() {
        lt::error_code error;
        lt::create_directory(path.string(), error);
        require(!error, "private endpoint fixture directory");
    }
    ~Directory() { std::error_code ec; fs::remove_all(path, ec); }
};

std::string read(fs::path const& path) {
    std::ifstream input(path, std::ios::binary);
    require(bool(input), "native fixture input");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

enum class Mode { direct, redirect, secret, credentials, dns, blocked, chain, chain_allowed, peer_allowed, peer_blocked };

void endpoint(bool web, Mode mode) {
    char const* step = "read payload fixture";
    try {
        auto const payload = read(fs::path(ARIAX_FIXTURE_DIR) / "payload.bin");
        step = "forbidden listener";
        Origin forbidden("127.0.0.1");
        std::atomic<int> final_requests{0};
        auto const base = web ? std::string("/payload") : std::string("/announce");
        bool const chain = mode == Mode::chain || mode == Mode::chain_allowed;
        step = "source listener";
        Origin source("127.0.0.2", [&](std::string const& request, unsigned short port) {
            auto const first_space = request.find(' ');
            auto const second_space = request.find(' ', first_space + 1);
            require(first_space != std::string::npos && second_space != std::string::npos, "fixture request line");
            auto const uri = request.substr(first_space + 1, second_space - first_space - 1);
            auto const query_start = uri.find('?');
            auto const query = query_start == std::string::npos ? std::string() : uri.substr(query_start);
            auto const path = uri.substr(0, query_start);
            if (path == base + "/final" || (mode == Mode::chain_allowed && path == base + "/20")) {
                ++final_requests;
                std::string body;
                std::string extra;
                if (web) {
                    // The one-piece fixture is requested as one exact byte range.
                    require(request.find("bytes=0-4999") != std::string::npos, "web-seed range");
                    body = payload;
                    extra = "Content-Range: bytes 0-4999/5000\r\n";
                } else {
                    body = "d8:intervali3600e5:peers";
                    if (mode == Mode::peer_allowed || mode == Mode::peer_blocked) {
                        body += "6:";
                        body.append("\x7f\0\0\1", 4);
                        body.push_back(char(forbidden.listen_port() >> 8));
                        body.push_back(char(forbidden.listen_port() & 255));
                    } else body += "0:";
                    body += "e";
                }
                return std::string(web ? "HTTP/1.1 206 Partial Content\r\n" : "HTTP/1.1 200 OK\r\n")
                    + extra + "Content-Length: " + std::to_string(body.size())
                    + "\r\nConnection: close\r\n\r\n" + body;
            }
            std::string target = "http://127.0.0.2:" + std::to_string(port) + base + "/final";
            if (mode == Mode::secret) target += "?%74oken=canary";
            else if (mode == Mode::credentials) target.insert(7, "user:canary@");
            else if (mode == Mode::dns) target = forbidden.url("localhost", base + "/final");
            else if (chain) {
                auto const hop = std::stoi(path.substr(base.size() + 1));
                target = "http://127.0.0.2:" + std::to_string(port) + base + "/" + std::to_string(hop + 1);
            }
            if (!web && mode != Mode::secret) target += query;
            return "HTTP/1.1 302 Found\r\nLocation: " + target + "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        });
        step = "private directory";
        Directory directory;
        bool const peer = mode == Mode::peer_allowed || mode == Mode::peer_blocked;
        bool const allowed = mode == Mode::direct || mode == Mode::redirect
            || mode == Mode::chain_allowed || mode == Mode::peer_allowed;
        bool const filtered = mode == Mode::dns || mode == Mode::blocked || mode == Mode::peer_blocked;
        {
            lt::settings_pack settings;
            settings.set_str(lt::settings_pack::listen_interfaces, "127.0.0.1:0");
            settings.set_bool(lt::settings_pack::enable_dht, false);
            settings.set_bool(lt::settings_pack::enable_lsd, false);
            settings.set_bool(lt::settings_pack::enable_upnp, false);
            settings.set_bool(lt::settings_pack::enable_natpmp, false);
            settings.set_bool(lt::settings_pack::ssrf_mitigation, true);
            settings.set_bool(lt::settings_pack::apply_ip_filter_to_trackers, true);
            settings.set_int(lt::settings_pack::stop_tracker_timeout, 1);
            settings.set_int(lt::settings_pack::alert_mask, int(std::uint32_t(lt::alert_category::error | lt::alert_category::tracker
                | lt::alert_category::peer | lt::alert_category::ip_block | lt::alert_category::connect)));
            lt::session_params params(settings);
            if (filtered) {
                params.ip_filter.add_rule(lt::make_address("127.0.0.1"), lt::make_address("127.0.0.1"), lt::ip_filter::blocked);
                params.ip_filter.add_rule(lt::make_address("::1"), lt::make_address("::1"), lt::ip_filter::blocked);
            }
            step = "native session";
            lt::session session(std::move(params));
            step = "load torrent fixture";
            auto add = lt::load_torrent_file((fs::path(ARIAX_FIXTURE_DIR) / "v1.torrent").string());
            add.save_path = directory.path.string();
            add.flags &= ~lt::torrent_flags::auto_managed;
            add.flags |= lt::torrent_flags::ariax_hold_metadata | lt::torrent_flags::paused | lt::torrent_flags::apply_ip_filter;
            auto const url = mode == Mode::blocked ? forbidden.url("localhost", base + "/final")
                : source.url("127.0.0.2", base + (mode == Mode::direct || peer ? "/final" : chain ? "/0" : "/start"));
            if (web) add.url_seeds.push_back(url);
            else add.trackers.push_back(url);
            step = "add torrent fixture";
            auto handle = session.add_torrent(std::move(add));
            require(handle.ariax_metadata_held(), "endpoint metadata hold");
            require(handle.ariax_approve_metadata({"payload.bin"}, {lt::download_priority_t(4)}), "endpoint metadata approval");
            handle.resume();
            step = "transfer";
            bool rejected = false;
            bool tracker_reply = false;
            auto const deadline = std::chrono::steady_clock::now() + (chain ? 45s : 8s);
            for (;;) {
                std::vector<lt::alert*> alerts;
                session.pop_alerts(&alerts);
                for (auto const* alert : alerts) {
                    auto matches = [filtered](lt::error_code const& error) {
                        return error == (filtered ? lt::errors::banned_by_ip_filter : lt::errors::ssrf_mitigation);
                    };
                    if (auto const* error = lt::alert_cast<lt::tracker_error_alert>(alert)) rejected |= matches(error->error);
                    if (auto const* error = lt::alert_cast<lt::url_seed_alert>(alert)) rejected |= matches(error->error);
                    if (auto const* error = lt::alert_cast<lt::peer_error_alert>(alert)) rejected |= matches(error->error);
                    if (auto const* error = lt::alert_cast<lt::peer_disconnected_alert>(alert)) rejected |= matches(error->error);
                    if (auto const* blocked = lt::alert_cast<lt::peer_blocked_alert>(alert)) {
                        rejected |= filtered && blocked->reason == lt::peer_blocked_alert::ip_filter;
                    }
                    tracker_reply |= lt::alert_cast<lt::tracker_reply_alert>(alert) != nullptr;
                }
                require(!source.failed && !forbidden.failed, "native endpoint fixture failed");
                bool const completed = peer ? forbidden.connections > 0
                    : web ? final_requests > 0 && handle.status().is_seeding : tracker_reply;
                if ((allowed && completed) || (!allowed && rejected)) break;
                if (std::chrono::steady_clock::now() >= deadline) {
                    std::cerr << "Native endpoint deadline: web=" << web << " mode=" << int(mode)
                        << " origin_requests=" << source.requests.load()
                        << " final_requests=" << final_requests.load()
                        << " forbidden_requests=" << forbidden.connections.load()
                        << " tracker_reply=" << tracker_reply << " rejected=" << rejected << '\n';
                    require(false, "native endpoint acceptance deadline");
                }
                std::this_thread::sleep_for(5ms);
            }
            require(allowed != rejected, "unexpected native endpoint disposition");
        }
        // Session shutdown drains disk writes before inspecting the downloaded file.
        if (allowed && web) require(read(directory.path / "payload.bin") == payload, "web-seed payload bytes");
        require(mode == Mode::peer_allowed ? forbidden.connections > 0 : forbidden.connections == 0,
            "forbidden destination received a connection");
        if (mode == Mode::blocked) require(source.requests == 0, "blocked initial destination escaped filtering");
        else require(source.requests > 0, "endpoint test never contacted its origin");
        if (chain && source.requests != 21) {
            std::cerr << "Native redirect chain: mode=" << int(mode)
                << " origin_connections=" << source.connections.load()
                << " origin_requests=" << source.requests.load()
                << " final_requests=" << final_requests.load() << '\n';
            require(false, "web-seed redirect limit changed");
        }
        if (mode == Mode::secret || mode == Mode::credentials) require(final_requests == 0, "unsafe redirect was followed");
    } catch (std::exception const& error) {
        throw std::runtime_error(std::string(step) + ": " + error.what());
    }
}

} // namespace

void endpoint_policy() {
    auto checked = [](bool web, Mode mode) {
        try { endpoint(web, mode); }
        catch (std::exception const& error) {
            throw std::runtime_error(std::string(web ? "web-seed" : "tracker")
                + " mode " + std::to_string(static_cast<int>(mode)) + ": " + error.what());
        }
    };
    for (bool web : {false, true}) {
        for (auto mode : {Mode::direct, Mode::redirect, Mode::secret, Mode::credentials, Mode::dns, Mode::blocked}) {
            checked(web, mode);
        }
    }
    checked(true, Mode::chain_allowed);
    checked(true, Mode::chain);
    checked(false, Mode::peer_allowed);
    checked(false, Mode::peer_blocked);
}
