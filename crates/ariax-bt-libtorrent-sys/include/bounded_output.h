#pragma once

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace ariax::bt::detail {

// Check the cap before every output growth. Metadata, peer and file counts
// separately bound the intermediate entry tree.
struct BoundedOutput {
    using difference_type = std::ptrdiff_t;
    using value_type = void;
    using pointer = void;
    using reference = void;
    using iterator_category = std::output_iterator_tag;
    std::reference_wrapper<std::vector<char>> output;
    std::size_t limit;
    BoundedOutput& operator*() { return *this; }
    BoundedOutput& operator++() { return *this; }
    BoundedOutput operator++(int) { return *this; }
    BoundedOutput& operator=(char value) {
        auto& destination = output.get();
        if (destination.size() >= limit) throw std::runtime_error("bt/native-contract-rejected");
        if (destination.size() == destination.capacity()) {
            destination.reserve(std::min(limit, std::max(std::size_t(256), destination.capacity() * 2)));
        }
        destination.push_back(value);
        return *this;
    }
};

// MSVC's std::copy assigns its wrapped output iterator before returning it.
static_assert(std::is_copy_constructible_v<BoundedOutput>);
static_assert(std::is_copy_assignable_v<BoundedOutput>);

} // namespace ariax::bt::detail
