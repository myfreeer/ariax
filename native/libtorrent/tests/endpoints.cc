#include <libtorrent/alert_types.hpp>
#include <libtorrent/load_torrent.hpp>
#include <libtorrent/session.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/torrent_status.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
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
                    require(++requests <= 128, "fixture request limit");
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
        fs::create_directory(path);
        fs::permissions(path, fs::perms::owner_all);
    }
    ~Directory() { std::error_code ec; fs::remove_all(path, ec); }
};

std::string read(fs::path const& path) {
    std::ifstream input(path, std::ios::binary);
    require(bool(input), "native fixture input");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

enum class Mode { direct, redirect, secret, credentials, dns, blocked, chain, peer_allowed, peer_blocked };

void endpoint(bool web, Mode mode) {
    auto const payload = read(fs::path(ARIAX_FIXTURE_DIR) / "payload.bin");
    Origin forbidden("127.0.0.1");
    std::atomic<int> final_requests{0};
    auto const base = web ? std::string("/payload") : std::string("/announce");
    Origin source("127.0.0.2", [&](std::string const& request, unsigned short port) {
        auto const first_space = request.find(' ');
        auto const second_space = request.find(' ', first_space + 1);
        require(first_space != std::string::npos && second_space != std::string::npos, "fixture request line");
        auto const uri = request.substr(first_space + 1, second_space - first_space - 1);
        auto const query_start = uri.find('?');
        auto const query = query_start == std::string::npos ? std::string() : uri.substr(query_start);
        auto const path = uri.substr(0, query_start);
        if (path == base + "/final") {
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
        else if (mode == Mode::chain) {
            auto const hop = std::stoi(path.substr(base.size() + 1));
            target = "http://127.0.0.2:" + std::to_string(port) + base + "/" + std::to_string(hop + 1);
        }
        if (!web && mode != Mode::secret) target += query;
        return "HTTP/1.1 302 Found\r\nLocation: " + target + "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    });
    Directory directory;
    bool const peer = mode == Mode::peer_allowed || mode == Mode::peer_blocked;
    bool const allowed = mode == Mode::direct || mode == Mode::redirect || mode == Mode::peer_allowed;
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
        settings.set_int(lt::settings_pack::alert_mask, int(lt::alert_category::error | lt::alert_category::tracker
            | lt::alert_category::peer | lt::alert_category::ip_block | lt::alert_category::connect));
        lt::session_params params(settings);
        if (filtered) {
            params.ip_filter.add_rule(lt::make_address("127.0.0.1"), lt::make_address("127.0.0.1"), lt::ip_filter::blocked);
            params.ip_filter.add_rule(lt::make_address("::1"), lt::make_address("::1"), lt::ip_filter::blocked);
        }
        lt::session session(std::move(params));
        auto add = lt::load_torrent_file((fs::path(ARIAX_FIXTURE_DIR) / "v1.torrent").string());
        add.save_path = directory.path.string();
        add.flags &= ~lt::torrent_flags::auto_managed;
        add.flags |= lt::torrent_flags::ariax_hold_metadata | lt::torrent_flags::paused | lt::torrent_flags::apply_ip_filter;
        auto const url = mode == Mode::blocked ? forbidden.url("localhost", base + "/final")
            : source.url("127.0.0.2", base + (mode == Mode::direct || peer ? "/final" : mode == Mode::chain ? "/0" : "/start"));
        if (web) add.url_seeds.push_back(url);
        else add.trackers.push_back(url);
        auto handle = session.add_torrent(std::move(add));
        require(handle.ariax_metadata_held(), "endpoint metadata hold");
        require(handle.ariax_approve_metadata({"payload.bin"}, {lt::download_priority_t(4)}), "endpoint metadata approval");
        handle.resume();
        bool rejected = false;
        bool tracker_reply = false;
        auto const deadline = std::chrono::steady_clock::now() + (mode == Mode::chain ? 45s : 8s);
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
            bool const completed = peer ? forbidden.requests > 0 : web ? handle.status().is_seeding : tracker_reply;
            if ((allowed && completed) || (!allowed && rejected)) break;
            require(std::chrono::steady_clock::now() < deadline, "native endpoint acceptance deadline");
            std::this_thread::sleep_for(5ms);
        }
        if (allowed && web) require(read(directory.path / "payload.bin") == payload, "web-seed payload bytes");
        require(allowed != rejected, "unexpected native endpoint disposition");
    }
    require(mode == Mode::peer_allowed ? forbidden.requests > 0 : forbidden.requests == 0,
        "forbidden destination received a connection");
    if (mode == Mode::blocked) require(source.requests == 0, "blocked initial destination escaped filtering");
    else require(source.requests > 0, "endpoint test never contacted its origin");
    if (mode == Mode::chain) require(source.requests == 21, "web-seed redirect limit changed");
    if (mode == Mode::secret || mode == Mode::credentials) require(final_requests == 0, "unsafe redirect was followed");
}

} // namespace

void endpoint_policy() {
    for (bool web : {false, true}) {
        for (auto mode : {Mode::direct, Mode::redirect, Mode::secret, Mode::credentials, Mode::dns, Mode::blocked}) {
            endpoint(web, mode);
        }
    }
    endpoint(true, Mode::chain);
    endpoint(false, Mode::peer_allowed);
    endpoint(false, Mode::peer_blocked);
}
