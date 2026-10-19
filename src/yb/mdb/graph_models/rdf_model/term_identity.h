#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

namespace SPARQL {

// A versioned, length-delimited dictionary key. The leading NUL cannot be part
// of an RDF IRI. Never serialize a process-local pointer or registration order.
struct RdfTermIdentity {
    char kind;  // IRI, literal, language literal, or scoped blank node.
    std::string lexical;
    std::string qualifier;

    static constexpr const char* kXsd = "http://www.w3.org/2001/XMLSchema#";

    std::string Key() const {
        return std::string("\0MDB1", 5) + kind + std::to_string(qualifier.size()) + ":" +
               qualifier + lexical;
    }

    static bool IsKey(std::string_view key) {
        return key.substr(0, 5) == std::string_view("\0MDB1", 5);
    }

    static RdfTermIdentity Decode(std::string_view key) {
        if (!IsKey(key) || key.size() < 8) throw std::runtime_error("Invalid RDF term key");
        const char kind = key[5];
        if (kind != 'I' && kind != 'L' && kind != 'G' && kind != 'B') {
            throw std::runtime_error("Unknown RDF term kind");
        }
        size_t length = 0, pos = 6;
        if (key[pos] < '0' || key[pos] > '9') throw std::runtime_error("Invalid RDF term length");
        for (; pos < key.size() && key[pos] != ':'; ++pos) {
            if (key[pos] < '0' || key[pos] > '9' || length > key.size() / 10) {
                throw std::runtime_error("Invalid RDF term length");
            }
            length = length * 10 + static_cast<size_t>(key[pos] - '0');
        }
        if (pos == key.size() || length > key.size() - pos - 1) {
            throw std::runtime_error("Truncated RDF term key");
        }
        return {kind, std::string(key.substr(pos + 1 + length)),
                std::string(key.substr(pos + 1, length))};
    }

    static std::string NormalizeLanguage(std::string language) {
        if (language.empty()) throw std::runtime_error("Empty RDF language tag");
        bool initial = true, segment = false;
        for (auto& c : language) {
            if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
            if (c == '-') {
                if (!segment) throw std::runtime_error("Invalid RDF language tag");
                initial = false;
                segment = false;
            } else if ((c >= 'a' && c <= 'z') || (!initial && c >= '0' && c <= '9')) {
                segment = true;
            } else {
                throw std::runtime_error("Invalid RDF language tag");
            }
        }
        if (!segment) throw std::runtime_error("Invalid RDF language tag");
        return language;
    }
};

class RdfTermRegistry {
public:
    static uint64_t Pack(uint64_t mask, const RdfTermIdentity& term) {
        const auto key = term.Key();
        uint64_t hash = 14695981039346656037ULL;
        for (unsigned char c : key) hash = (hash ^ c) * 1099511628211ULL;
        const auto id = mask | (hash & 0x0000FFFFFFFFFFFFULL);
        std::lock_guard<std::mutex> lock(Mutex());
        auto [it, inserted] = Terms().emplace(id, term);
        if (!inserted && it->second.Key() != key) {
            throw std::runtime_error("RDF packed term hash collision");
        }
        return id;
    }

    static std::optional<RdfTermIdentity> Find(uint64_t id) {
        std::lock_guard<std::mutex> lock(Mutex());
        auto found = Terms().find(id);
        if (found == Terms().end()) return std::nullopt;
        return found->second;
    }

private:
    static std::mutex& Mutex() { static std::mutex value; return value; }
    static std::unordered_map<uint64_t, RdfTermIdentity>& Terms() {
        static std::unordered_map<uint64_t, RdfTermIdentity> value;
        return value;
    }
};

}  // namespace SPARQL
