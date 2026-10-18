#pragma once
#include <chrono>
#include <cstdint>
#include <map>
#include <ostream>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "query/var_id.h"

class ObjectId;
using uint64_t = std::uint64_t;

struct ThreadInfo {
    bool interruption_requested = false;
    uint_fast32_t worker_index = 0;
    std::chrono::system_clock::time_point timeout;
    std::chrono::system_clock::time_point time_start;
};

class QueryContext {
public:
    ThreadInfo thread_info;

    std::string get_var_name(uint64_t id) const {
        for (const auto& [name, var] : var_names_) {
            if (var.id == id) return name;
        }
        return "v" + std::to_string(id);
    }
    std::size_t get_var_size() const { return var_names_.size(); }
    std::size_t get_var_size(uint64_t) const { return var_names_.size(); }
    std::size_t get_var_size(const VarId&) const { return var_names_.size(); }
    void _debug_print(std::ostream&, uint64_t) const {}
    bool is_internal(const VarId& var) const {
        for (const auto& [name, id] : var_names_) {
            if (id == var) return !name.empty() && name[0] == '.';
        }
        return false;
    }
    bool is_blank(const VarId&) const { return false; }
    uint64_t get_thread_info_worker_index() const { return 0; }
    bool cancellation_requested() const { return thread_info.interruption_requested; }
    void clear_graph_object_map() {}

    double get_rand() {
        return distribution_(rand_generator_);
    }

    std::set<VarId> get_non_internal_vars() {
        std::set<VarId> res;
        for (const auto& [name, var] : var_names_) {
            if (!name.empty() && name[0] != '.') {
                res.emplace(var);
            }
        }
        return res;
    }

    VarId get_or_create_var(const std::string& name) {
        auto it = var_names_.find(name);
        if (it == var_names_.end()) {
            auto [new_it, _] = var_names_.try_emplace(name, VarId(static_cast<uint_fast32_t>(var_names_.size())));
            return new_it->second;
        }
        return it->second;
    }

    VarId get_internal_var() {
        auto name = "." + std::to_string(internal_var_counter_++);
        auto id = VarId(static_cast<uint_fast32_t>(var_names_.size()));
        var_names_.emplace(std::move(name), id);
        return id;
    }

    VarId get_internal_var(const std::string& name) {
        return get_or_create_var(name);
    }

    uint64_t get_new_blank_node() { return next_blank_node_id_++; }
    std::unordered_map<std::string, uint64_t> blank_node_ids;

private:
    std::map<VarId, uint64_t> graph_object;
    std::map<VarId, std::vector<uint64_t>> path_object;
    mutable std::map<std::string, VarId> var_names_;
    uint64_t internal_var_counter_ = 0;
    uint64_t next_blank_node_id_ = 0;
    std::mt19937 rand_generator_{42};
    std::uniform_real_distribution<double> distribution_{0.0, 1.0};
};

inline QueryContext& get_query_ctx() {
    static QueryContext ctx;
    return ctx;
}
