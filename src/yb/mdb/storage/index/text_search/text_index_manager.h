#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <tuple>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>

namespace TextSearch {

class TextIndex {
public:
    virtual ~TextIndex() = default;
};

class TextIndexManager {
public:
    enum class TOKENIZE_TYPE : uint8_t {
        IDENTITY = 0,
        SPLIT_WHITESPACE = 1,
    };

    enum class NORMALIZE_TYPE : uint8_t {
        IDENTITY = 0,
        LOWERCASE = 1,
    };

    struct TextIndexMetadata {
        TOKENIZE_TYPE tokenization_type;
        NORMALIZE_TYPE normalization_type;
        uint64_t predicate_id;
        std::string predicate;

        friend std::ostream& operator<<(std::ostream& os, const TextIndexMetadata& metadata)
        {
            os << "{\"tokenization\": " << '"' << static_cast<int>(metadata.tokenization_type) << '"';
            os << ", \"normalization\": " << '"' << static_cast<int>(metadata.normalization_type) << '"';
            os << ", \"predicate\": \"" << metadata.predicate << "\"}";
            return os;
        }
    };

    static constexpr char TEXT_SEARCH_DIR[] = "text_search_index";

    void init() {}

    void load_text_index(const std::string& name, const TextIndexMetadata& metadata) {}

    TextIndex* get_text_index(const std::string& name) { return nullptr; }

    std::tuple<uint64_t, uint64_t> create_text_search_index(
        const std::string& name,
        const std::string& predicate,
        NORMALIZE_TYPE normalization_type,
        TOKENIZE_TYPE tokenization_type)
    {
        return {0, 0};
    }

    const boost::unordered_flat_map<std::string, TextIndexMetadata>& get_name2metadata() const
    {
        return name2metadata;
    }

    const boost::unordered_flat_map<std::string, std::vector<std::string>>& get_predicate2names() const
    {
        return predicate2names;
    }

    std::vector<std::string> get_index_names() { return {}; }

    bool has_changes() const { return has_changes_; }

    std::size_t num_text_search_indexes() const { return 0; }

private:
    bool has_changes_ = false;
    std::mutex name2text_search_mutex;
    boost::unordered_flat_map<std::string, std::unique_ptr<TextIndex>> name2text_search_index;
    boost::unordered_flat_map<std::string, TextIndexMetadata> name2metadata;
    boost::unordered_flat_map<std::string, std::vector<std::string>> predicate2names;
};

} // namespace TextSearch
