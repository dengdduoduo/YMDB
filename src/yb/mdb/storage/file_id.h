#pragma once

#include <cstdint>

class TmpFileId {
public:
    uint32_t id;

    TmpFileId(uint32_t id) : id(id) { }

    bool operator<(const TmpFileId other) const {
        return this->id < other.id;
    }

    bool operator==(const TmpFileId other) const {
        return this->id == other.id;
    }
};
