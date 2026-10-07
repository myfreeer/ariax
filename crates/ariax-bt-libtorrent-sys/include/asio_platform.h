#pragma once

// Configure Asio before MinGW headers expose _POSIX_THREADS. The pinned
// Windows library creates Win32 threads; joining those as pthreads frees
// their state while they are still running.
#ifdef _WIN32
#include <boost/asio/detail/config.hpp>
#ifdef BOOST_ASIO_HAS_PTHREADS
#error "Ariax requires Asio Win32 threads; include asio_platform.h before other headers"
#endif
#endif
