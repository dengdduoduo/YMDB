#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <vector>

#include "query/exceptions.h"
#include "query/executor/batch_operator.h"
#include "query/executor/hash_join.h"
#include "yb/mdb/model/query_statistics.h"
#include "yb/util/test_macros.h"

namespace yb::mdb {
namespace {

class TestRows : public BindingIter {
 public:
  explicit TestRows(size_t rows, size_t fail = SIZE_MAX) : count_(rows), fail_(fail) {}
  void _begin(Binding& binding) override { binding_ = &binding; position_ = 0; }
  bool _next() override {
    if (position_ == fail_) throw QueryException("injected row read failure");
    if (position_ == count_) return false;
    binding_->add(VarId(0), ::ObjectId(++position_));
    return true;
  }
  void _reset() override { position_ = 0; }
  void assign_nulls() override { binding_->add(VarId(0), ::ObjectId::get_null()); }
  void print(std::ostream&, int, bool) const override {}
 private:
  Binding* binding_ = nullptr;
  size_t count_, fail_, position_ = 0;
};

class EmptyThenRow : public BatchOperator {
 public:
  explicit EmptyThenRow(bool fail) : fail_(fail) {}
  Status Open(const Binding&) override { step_ = 0; return Status::OK(); }
  Result<BindingBatch> NextBatch() override {
    BindingBatch batch;
    RETURN_NOT_OK(batch.Init({VarId(0)}, 1, 512));
    if (step_++ == 0) return batch;  // A filtered empty batch is not EOF.
    if (step_ == 2) {
      Binding binding(1); binding.add(VarId(0), ::ObjectId(42));
      RETURN_NOT_OK(batch.Append(binding));
      return batch;
    }
    if (fail_) return STATUS(IOError, "injected batch page failure");
    batch.eof = true;
    return batch;
  }
  void Close() override {}
 private:
  bool fail_;
  size_t step_ = 0;
};

}  // namespace

TEST(MdbBindingBatchTest, FixedLayoutNullBitmapAndOrigins) {
  Binding binding(3), restored(3);
  binding.add(VarId(0), ::ObjectId(11));
  binding.add(VarId(2), ::ObjectId(22));
  BindingBatch batch;
  ASSERT_OK(batch.Init({VarId(2), VarId(1), VarId(0)}, 100, 512, true));
  ASSERT_LT(batch.capacity(), 100);
  ASSERT_LE(batch.Bytes(), 512);
  ASSERT_OK(batch.Append(binding, 99));
  ASSERT_TRUE(batch.IsBound(0, 0));
  ASSERT_FALSE(batch.IsBound(0, 1));
  ASSERT_EQ(batch.Value(0, 2), ::ObjectId(11));
  ASSERT_EQ(batch.Origin(0), 99);
  ASSERT_OK(batch.CopyRow(0, &restored));
  ASSERT_EQ(binding, restored);
}

TEST(MdbBindingBatchTest, RejectsInsufficientBudgetAndInvalidLayouts) {
  BindingBatch batch;
  EXPECT_FALSE(batch.Init({VarId(0)}, 1, 8).ok());
  EXPECT_FALSE(batch.Init({VarId(0), VarId(0)}, 1, 512).ok());
  EXPECT_FALSE(batch.Init({VarId(0)}, 0, 512).ok());
  ASSERT_OK(batch.Init({VarId(0)}, 1, 512));
  Binding binding(1);
  ASSERT_OK(batch.Append(binding));
  EXPECT_FALSE(batch.Append(binding).ok());
  EXPECT_FALSE(batch.CopyRow(1, &binding).ok());
}

TEST(MdbBindingBatchTest, SourceSplitsRowsAndPreservesInputOnReopen) {
  Binding input(2), row(2);
  input.add(VarId(1), ::ObjectId(71));
  BindingIterBatchSource source(std::make_unique<TestRows>(5), 2, 1024);
  ASSERT_OK(source.Open(input));
  std::vector<uint64_t> actual;
  for (;;) {
    auto batch = ASSERT_RESULT(source.NextBatch());
    ASSERT_LE(batch.rows(), 2);
    for (size_t i = 0; i < batch.rows(); ++i) {
      ASSERT_OK(batch.CopyRow(i, &row));
      actual.push_back(row[VarId(0)].id);
      ASSERT_EQ(row[VarId(1)].id, 71);
      ASSERT_EQ(batch.Origin(i), actual.size() - 1);
    }
    if (batch.eof) break;
  }
  ASSERT_EQ(actual, (std::vector<uint64_t>{1,2,3,4,5}));
  source.Close();
  EXPECT_FALSE(source.NextBatch().ok());
  input.add(VarId(1), ::ObjectId(72));
  ASSERT_OK(source.Open(input));
  auto batch = ASSERT_RESULT(source.NextBatch());
  ASSERT_OK(batch.CopyRow(0, &row));
  ASSERT_EQ(row[VarId(1)].id, 72);
}

TEST(MdbBindingBatchTest, RowFailureIsStickyAndNeverBecomesEof) {
  Binding input(1);
  BindingIterBatchSource source(std::make_unique<TestRows>(8, 3), 2, 512);
  ASSERT_OK(source.Open(input));
  ASSERT_EQ(ASSERT_RESULT(source.NextBatch()).rows(), 2);
  auto failed = source.NextBatch();
  ASSERT_FALSE(failed.ok());
  EXPECT_NE(failed.status().ToString().find("injected row read failure"), std::string::npos);
  EXPECT_FALSE(source.NextBatch().ok());
}

TEST(MdbBindingBatchTest, LegacyAdapterContinuesAfterEmptyBatchAndResets) {
  Binding input(1);
  BatchBindingIter iter(std::make_unique<EmptyThenRow>(false), {VarId(0)});
  iter.begin(input);
  ASSERT_TRUE(iter.next());
  ASSERT_EQ(input[VarId(0)].id, 42);
  ASSERT_FALSE(iter.next());
  iter.assign_nulls();
  ASSERT_TRUE(input[VarId(0)].is_null());
  iter.reset();
  ASSERT_TRUE(iter.next());
  ASSERT_EQ(input[VarId(0)].id, 42);
}

TEST(MdbBindingBatchTest, LegacyAdapterThrowsAfterLaterPageFailure) {
  Binding input(1);
  BatchBindingIter iter(std::make_unique<EmptyThenRow>(true), {VarId(0)});
  iter.begin(input);
  ASSERT_TRUE(iter.next());
  EXPECT_THROW(iter.next(), QueryException);
  EXPECT_THROW(iter.next(), QueryException);
}

}  // namespace yb::mdb

namespace yb::mdb {
namespace {
class HashRows : public BindingIter {
 public:
  HashRows(std::vector<std::vector<uint64_t>> rows, std::vector<VarId> vars)
      : rows_(std::move(rows)), vars_(std::move(vars)) {}
  void _begin(Binding& input) override { input_ = &input; position_ = 0; }
  bool _next() override {
    if (position_ == rows_.size()) return false;
    for (size_t col = 0; col < vars_.size(); ++col)
      input_->add(vars_[col], ::ObjectId(rows_[position_][col]));
    ++position_; return true;
  }
  void _reset() override { position_ = 0; }
  void assign_nulls() override {}
  void print(std::ostream&, int, bool) const override {}
 private:
  std::vector<std::vector<uint64_t>> rows_;
  std::vector<VarId> vars_;
  Binding* input_ = nullptr;
  size_t position_ = 0;
};
std::unique_ptr<BindingIter> HashInput(std::vector<std::vector<uint64_t>> rows,
                                     std::vector<VarId> vars) {
  return std::make_unique<HashRows>(std::move(rows), std::move(vars));
}
}

TEST(MdbHashJoinTest, CompositeKeysDuplicatesEmptyAndReopen) {
  BatchJoinOptions options; options.input_rows = 1; options.output_rows = 1;
  options.batch_bytes = 512; options.memory_bytes = 32768;
  HashJoin join(HashInput({{1,2,10},{1,2,10},{1,3,11}}, {VarId(0),VarId(1),VarId(2)}),
                HashInput({{1,2,20},{1,2,21},{1,4,22}}, {VarId(0),VarId(1),VarId(3)}),
                {VarId(0),VarId(1)}, {VarId(0),VarId(1),VarId(2)}, nullptr, options);
  Binding input(5), row(5); input.add(VarId(4), ::ObjectId(77));
  for (int pass = 0; pass < 2; ++pass) {
    ASSERT_OK(join.Open(input));
    size_t count = 0;
    for (;;) {
      auto batch = ASSERT_RESULT(join.NextBatch());
      for (size_t i = 0; i < batch.rows(); ++i) {
        ASSERT_OK(batch.CopyRow(i, &row));
        ASSERT_EQ(row[VarId(2)].id, 10);
        ASSERT_TRUE(row[VarId(3)].id == 20 || row[VarId(3)].id == 21);
        ASSERT_EQ(row[VarId(4)].id, 77 + pass); ++count;
      }
      if (batch.eof) break;
    }
    ASSERT_EQ(count, 4);
    input.add(VarId(4), ::ObjectId(78));
  }
  join.Close(); ASSERT_FALSE(join.NextBatch().ok());
  HashJoin empty(HashInput({}, {VarId(0)}), HashInput({{1}}, {VarId(0)}),
                 {VarId(0)}, {VarId(0)}, nullptr, options);
  ASSERT_OK(empty.Open(input));
  auto batch = ASSERT_RESULT(empty.NextBatch());
  ASSERT_TRUE(batch.eof); ASSERT_EQ(batch.rows(), 0);
}

TEST(MdbHashJoinTest, BudgetFailureIsStickyAndNullKeysAreRejected) {
  BatchJoinOptions options; options.batch_bytes = 512; options.memory_bytes = 16384;
  std::vector<std::vector<uint64_t>> large(2000, {1});
  HashJoin join(HashInput(large, {VarId(0)}), HashInput({{1}}, {VarId(0)}),
                {VarId(0)}, {VarId(0)}, nullptr, options);
  Binding input(1);
  ASSERT_OK(join.Open(input));
  auto failed = join.NextBatch(); ASSERT_FALSE(failed.ok());
  ASSERT_NE(failed.status().ToString().find("memory limit"), std::string::npos);
  ASSERT_FALSE(join.NextBatch().ok());
  HashJoin nulls(HashInput({{::ObjectId::get_null().id}}, {VarId(0)}),
                 HashInput({{1}}, {VarId(0)}), {VarId(0)}, {VarId(0)}, nullptr, options);
  ASSERT_OK(nulls.Open(input));
  failed = nulls.NextBatch(); ASSERT_FALSE(failed.ok());
  ASSERT_NE(failed.status().ToString().find("unbound"), std::string::npos);
}

TEST(MdbQueryStatisticsTest, BoundedCountsSketchAndMissingMetadata) {
  QueryStatistics stats;
  for (uint64_t i = 0; i < 100; ++i) stats.Add(i / 10, 7, i);
  ASSERT_EQ(stats.triples, 100);
  ASSERT_EQ(stats.predicates.at(7), 100);
  ASSERT_NEAR(stats.subjects.Estimate(), 10, 1);
  ASSERT_NEAR(stats.objects.Estimate(), 100, 5);
  ASSERT_NE(stats.ToJson().find("full_snapshot_scan"), std::string::npos);
  for (uint64_t i = 0; i < 10000; ++i) stats.Add(i, i, i);
  ASSERT_EQ(stats.predicates.size(), QueryStatistics::kMaxKeys);
  ASSERT_EQ(stats.subject_degrees.size(), QueryStatistics::kMaxKeys);
  ASSERT_FALSE(stats.predicates_complete); ASSERT_FALSE(stats.degrees_complete);
}

TEST(MdbHashJoinTest, BuildReadFailureNeverPublishesPartialResults) {
  BatchJoinOptions options; options.input_rows = 2; options.output_rows = 1;
  options.batch_bytes = 512; options.memory_bytes = 32768;
  HashJoin join(std::make_unique<TestRows>(10, 5), HashInput({{1}}, {VarId(0)}),
                {VarId(0)}, {VarId(0)}, nullptr, options);
  Binding input(1); ASSERT_OK(join.Open(input));
  auto failed = join.NextBatch(); ASSERT_FALSE(failed.ok());
  ASSERT_NE(failed.status().ToString().find("injected row read failure"), std::string::npos);
  ASSERT_FALSE(join.NextBatch().ok());
}

}  // namespace yb::mdb
