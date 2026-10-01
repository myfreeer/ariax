#pragma once
#include <iostream>

// Declare after a native session so this runs immediately before its
// destructor, including when an assertion throws during the test.
struct NativeShutdownTrace {
    char const* name;
    ~NativeShutdownTrace() {
        std::cerr << "Native shutdown begins: " << name << std::endl;
    }
};
