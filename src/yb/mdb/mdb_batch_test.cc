#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <vector>

#include "query/exceptions.h"
#include "query/executor/batch_operator.h"
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
