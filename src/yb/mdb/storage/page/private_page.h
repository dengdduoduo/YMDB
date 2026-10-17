#pragma once

#include <cstdint>

class PPage {
public:
    static constexpr uint64_t SIZE = 4096;
    uint8_t bytes[SIZE];
};
