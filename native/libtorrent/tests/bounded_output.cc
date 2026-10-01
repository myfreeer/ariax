#include "bounded_output.h"
#include <libtorrent/bencode.hpp>
#include <iostream>
#include <string>
#include <utility>

using ariax::bt::detail::BoundedOutput;

namespace {
void check(bool condition) {
    if (!condition) throw std::logic_error("bounded output regression");
}

template <typename Operation>
void rejects(Operation operation) {
    bool rejected = false;
    try { operation(); } catch (std::runtime_error const&) { rejected = true; }
    check(rejected);
}

void iterator_assignment() {
    std::vector<char> first;
    BoundedOutput output{first, 3};
    *output++ = 'a';
    auto copied = output;
    *copied++ = 'b';
    std::vector<char> second{'!'};
    BoundedOutput reassigned{second, 1};
    reassigned = copied;
    std::string suffix = "c";
    reassigned = std::copy(suffix.begin(), suffix.end(), reassigned);
    check(first == std::vector<char>({'a', 'b', 'c'}));
    check(second == std::vector<char>({'!'}));
    rejects([&] { *reassigned++ = 'd'; });
    check(first.size() == 3);
    output = BoundedOutput{second, 2};
    *output++ = '?';
    check(second == std::vector<char>({'!', '?'}));
    check(first == std::vector<char>({'a', 'b', 'c'}));
    output = std::move(reassigned);
    rejects([&] { *output++ = 'e'; });
}

void byte_limits() {
    for (std::size_t limit : {0, 1, 255, 256, 257, 1024}) {
        std::vector<char> bytes;
        std::vector<char> input(limit, 'x');
        BoundedOutput sink{bytes, limit};
        sink = std::copy(input.begin(), input.end(), sink);
        check(bytes == input);
        rejects([&] { *sink++ = 'y'; });
        check(bytes == input);
        bytes.clear();
        input.push_back('z');
        rejects([&] { sink = std::copy(input.begin(), input.end(), sink); });
        check(bytes.size() == limit);
        check(std::all_of(bytes.begin(), bytes.end(), [](char c) { return c == 'x'; }));
    }
}

void preformatted_bencode() {
    using libtorrent::entry;
    entry value(entry::list_type{entry(std::string("text")),
        entry(entry::preformatted_type{'i', '7', 'e'})});
    std::string expected = "l4:texti7ee";
    std::vector<char> bytes;
    libtorrent::bencode(BoundedOutput{bytes, expected.size()}, value);
    check(std::string(bytes.begin(), bytes.end()) == expected);
    bytes.clear();
    rejects([&] { libtorrent::bencode(BoundedOutput{bytes, expected.size()-1}, value); });
    check(std::string(bytes.begin(), bytes.end()) == expected.substr(0, expected.size()-1));
    bytes.clear();
    libtorrent::bencode(BoundedOutput{bytes, 0}, entry(entry::preformatted_type{}));
    check(bytes.empty());
    rejects([&] { libtorrent::bencode(BoundedOutput{bytes, 0}, value); });
    check(bytes.empty());
}
}

int main() try {
    iterator_assignment();
    byte_limits();
    preformatted_bencode();
    std::cout << "Bounded output regression passed.\n";
} catch (std::exception const& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
