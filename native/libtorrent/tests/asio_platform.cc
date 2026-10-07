#include "asio_platform.h"
// Later POSIX declarations must not change the configured Windows ABI.
#if defined(__MINGW32__)
#include <unistd.h>
#endif
#include <boost/asio/detail/thread.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <type_traits>

#ifdef _WIN32
static_assert(std::is_same_v<boost::asio::detail::thread,
    boost::asio::detail::win_thread>);
#endif

int main() {
    for (int i = 0; i < 8; ++i) {
        boost::asio::io_context context;
        boost::asio::steady_timer timer(context, std::chrono::milliseconds(1));
        bool completed = false;
        timer.async_wait([&](boost::system::error_code error) {
            completed = !error;
        });
        context.run();
        if (!completed) return 1;
    }
}
