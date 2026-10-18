// Portions Copyright (c) YugabyteDB, Inc.

#pragma once

#include <memory>
#include <memory_resource>
#include <new>
#include <set>
#include <unordered_map>
#include <vector>

#include "query/executor/binding_iter.h"

// Each child is evaluated against an isolated copy of the outer binding.
// Unbound join keys use compatibility matching, as required by SPARQL joins.
class HashJoinBindingIter : public BindingIter {
 public:
  static constexpr size_t kDefaultMaxBuildRows = 65536;
  static constexpr size_t kDefaultMaxBuildBytes = 16 * 1024 * 1024;
  static size_t DefaultMaxBuildBytes();

  HashJoinBindingIter(
      std::unique_ptr<BindingIter> lhs, std::unique_ptr<BindingIter> rhs, std::set<VarId> lhs_vars,
      std::set<VarId> rhs_vars, size_t max_build_rows = kDefaultMaxBuildRows,
      size_t max_build_bytes = DefaultMaxBuildBytes());

  struct BuildMemory {
    size_t rows = 0;
    size_t key_buckets = 0;
    size_t payload_bytes = 0;
    size_t accounted_bytes = 0;
    size_t tracked_bytes = 0;
    size_t peak_tracked_bytes = 0;
    size_t byte_limit = 0;
  };

  // accounted_bytes is the existing capacity lower bound (excluding node/allocator overhead).
  // tracked_bytes/peak_tracked_bytes cover all PMR requests, including hash nodes and growth.
  // Call after begin for the live build, or after draining for the peak; children are excluded.
  BuildMemory build_memory() const;

  void assign_nulls() override;
  void print(std::ostream& os, int indent, bool stats) const override;

 protected:
  void _begin(Binding& parent_binding) override;
  bool _next() override;
  void _reset() override;

 private:
  // Tracks requested container bytes, including simultaneous old/new buffers on growth.
  // Fixed plan metadata, isolated Bindings and child iterators are outside this budget.
  class BuildResource final : public std::pmr::memory_resource {
   public:
    struct LimitExceeded : std::bad_alloc {};
    explicit BuildResource(size_t limit) : limit_(limit) {}
    size_t bytes = 0;
    size_t peak = 0;
    const size_t limit_;

   private:
    void* do_allocate(size_t size, size_t alignment) override;
    void do_deallocate(void* ptr, size_t size, size_t alignment) override;
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
      return this == &other;
    }
  };

  using Key = std::pmr::vector<uint64_t>;
  using Row = std::pmr::vector<ObjectId>;
  struct KeyHash {
    size_t operator()(const Key& key) const;
  };

  void Start();
  void ClearBuild();
  Key MakeKey(const Binding& binding);
  bool Compatible(const Row& row) const;

  std::unique_ptr<BindingIter> lhs_;
  std::unique_ptr<BindingIter> rhs_;
  std::vector<VarId> lhs_vars_;
  std::vector<VarId> rhs_vars_;
  std::vector<VarId> join_vars_;
  std::vector<size_t> join_positions_;
  size_t max_build_rows_;
  Binding* parent_binding_ = nullptr;
  std::unique_ptr<Binding> lhs_binding_;
  std::unique_ptr<Binding> rhs_binding_;
  BuildResource memory_;
  std::pmr::vector<Row> rows_;
  std::pmr::unordered_map<Key, std::pmr::vector<size_t>, KeyHash> buckets_;
  std::pmr::vector<size_t> unbound_rows_;
  std::pmr::vector<size_t> candidates_;
  size_t candidate_index_ = 0;
  bool exhausted_ = true;
};
