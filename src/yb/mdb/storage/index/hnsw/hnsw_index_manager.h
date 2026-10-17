#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>

namespace HNSW {

class HNSWIndex {
public:
    virtual ~HNSWIndex() = default;
};

class HNSWIndexManager {
public:
    enum class MetricType : uint8_t {
        L2 = 0,
        IP = 1,
        COSINE = 2,
    };

    struct HNSWIndexMetadata {
        MetricType metric_type;
        std::string predicate;

        friend std::ostream& operator<<(std::ostream& os, const HNSWIndexMetadata& metadata)
        {
            os << "{\"metric_type\": " << static_cast<int>(metadata.metric_type);
            os << ", \"predicate\": " << metadata.predicate << "}";
            return os;
        }
    };

    static constexpr char HNSW_INDEX_DIR[] = "hnsw_index";

    void init() {}

    ~HNSWIndexManager() {}

    void load_hnsw_index(const std::string& name, const HNSWIndexMetadata& metadata) {}

    void write_to_disk(const std::string& name, HNSWIndex* index) {}

    HNSWIndex* get_hnsw_index(const std::string& name) { return nullptr; }

    template<uint64_t model_id>
    uint64_t create_hnsw_index(
        const std::string& name,
        const std::string& predicate,
        uint64_t dimension,
        uint64_t max_edges,
        uint64_t num_candidates,
        MetricType metric_type)
    {
        return 0;
    }

    const boost::unordered_flat_map<std::string, HNSWIndexMetadata>& get_name2metadata() const
    {
        return name2metadata;
    }

    bool has_changes() const { return has_changes_; }

    const boost::unordered_flat_map<std::string, std::vector<std::string>>& get_predicate2names() const
    {
        return predicate2names;
    }

    std::vector<std::string> get_index_names()
    {
        return {};
    }

    std::size_t num_hnsw_indexes() const { return 0; }

private:
    bool has_changes_ = false;
    std::mutex name2hnsw_index_mutex;
    boost::unordered_flat_map<std::string, std::unique_ptr<HNSWIndex>> name2hnsw_index;
    boost::unordered_flat_map<std::string, HNSWIndexMetadata> name2metadata;
    boost::unordered_flat_map<std::string, std::vector<std::string>> predicate2names;
};

} // namespace HNSW
