#pragma once

#include <iostream>
#include <sstream>

class OStream {
    friend class Logger;

private:
    bool enabled;
    std::stringstream stream;

    OStream(bool enabled) :
        enabled(enabled) { }

public:
    ~OStream()
    {
        if (enabled && !stream.str().empty()) {
            std::cerr << stream.str() << std::endl;
        }
    }

    OStream(const OStream&) = delete;
    OStream& operator=(const OStream&) = delete;
    OStream(OStream&&) = delete;
    OStream& operator=(OStream&&) = delete;

    template<typename T>
    OStream& operator<<(T&& obj)
    {
        if (enabled)
            stream << obj;
        return *this;
    }

    template<typename T>
    OStream& operator<<(T& obj)
    {
        if (enabled)
            stream << obj;
        return *this;
    }
};

class Logger {
public:
    bool debug_enabled = true;
    bool error_enabled = true;
    bool info_enabled = false;

    OStream debug() { return OStream(debug_enabled); }
    OStream error() { return OStream(error_enabled); }
    OStream info() { return OStream(info_enabled); }
};

inline Logger logger;
