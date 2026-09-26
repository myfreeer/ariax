#include <libtorrent/alert_types.hpp>
#include <libtorrent/aux_/parse_url.hpp>
#include <libtorrent/entry.hpp>
#include <libtorrent/session.hpp>
#include <libtorrent/session_params.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/udp.hpp>
#include <array>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace lt = libtorrent;
using namespace std::chrono_literals;

void require(bool value, char const* message) {
    if (!value) throw std::runtime_error(message);
}

void endpoint_policy();

void redirects() {
    for (auto const& target : {"https://seed.example/file", "https://other.example/file?q=1"}) {
        require(lt::aux::ariax_redirect_allowed("https://seed.example/start", target), "allowed redirect");
    }
    for (auto const& target : {
            "http://seed.example/file", "https://user:canary@seed.example/file",
            "https://user@seed.example/file", "https://seed.example/file#fragment",
            "https://seed.example/file?passkey=canary", "https://seed.example/file?%74oken=canary",
            "https://seed.example/file?KEY=canary", "https://seed.example/file?access_token=canary",
            "https://seed.example/file\r\nInjected: yes", "file:///payload"}) {
        require(!lt::aux::ariax_redirect_allowed("https://seed.example/start", target), "rejected redirect");
    }
    require(lt::aux::ariax_redirect_allowed("http://tracker.example/start?key=123&port=1",
        "https://tracker.example/announce?key=123&port=1"), "native tracker key");
    require(!lt::aux::ariax_redirect_allowed("http://tracker.example/start?key=123",
        "https://tracker.example/announce?key=456"), "changed tracker key");
    require(!lt::aux::ariax_redirect_allowed("https://seed.example/", "https://seed.example/" + std::string(4096, 'x')),
        "bounded redirect");
}

void dht_destination(bool blocked) {
    boost::asio::io_context io;
    lt::udp::socket peer(io, lt::udp::endpoint(lt::make_address("127.0.0.1"), 0));
    peer.non_blocking(true);
    lt::settings_pack settings;
    settings.set_str(lt::settings_pack::listen_interfaces, "127.0.0.1:0");
    settings.set_str(lt::settings_pack::dht_bootstrap_nodes, "");
    settings.set_bool(lt::settings_pack::enable_dht, true);
    settings.set_bool(lt::settings_pack::enable_lsd, false);
    settings.set_bool(lt::settings_pack::enable_upnp, false);
    settings.set_bool(lt::settings_pack::enable_natpmp, false);
    settings.set_bool(lt::settings_pack::apply_filter_to_dht, true);
    lt::session_params params(settings);
    if (blocked) params.ip_filter.add_rule(lt::make_address("127.0.0.1"), lt::make_address("127.0.0.1"), lt::ip_filter::blocked);
    lt::session session(std::move(params));
    auto const deadline = std::chrono::steady_clock::now() + 5s;
    while (!session.is_dht_running() || session.listen_port() == 0) {
        require(std::chrono::steady_clock::now() < deadline, "DHT startup deadline");
        std::this_thread::sleep_for(5ms);
    }
    lt::entry query;
    query["q"] = "ping";
    query["y"] = "q";
    query["a"]["id"] = std::string(20, 'a');
    session.dht_direct_request(peer.local_endpoint(), query);
    session.post_dht_stats();
    bool processed = false;
    bool received = false;
    auto const started = std::chrono::steady_clock::now();
    do {
        std::vector<lt::alert*> alerts;
        session.pop_alerts(&alerts);
        for (auto const* alert : alerts) processed |= lt::alert_cast<lt::dht_stats_alert>(alert) != nullptr;
        std::array<char, 4096> bytes{};
        lt::udp::endpoint sender;
        lt::error_code error;
        auto const size = peer.receive_from(boost::asio::buffer(bytes), sender, 0, error);
        received |= !error && size > 0;
        require(std::chrono::steady_clock::now() < deadline, "DHT dispatch deadline");
        std::this_thread::sleep_for(5ms);
    } while (!processed || (!received && std::chrono::steady_clock::now() - started < 500ms));
    require(received != blocked, "DHT destination filter");
}

int main() try {
    redirects();
    dht_destination(false);
    dht_destination(true);
    endpoint_policy();
    std::cout << "Native destination policy passed.\n";
} catch (std::exception const& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
