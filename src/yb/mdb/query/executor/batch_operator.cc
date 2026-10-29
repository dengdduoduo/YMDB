#include "query/executor/batch_operator.h"

#include <exception>
#include "query/exceptions.h"
#include "query/executor/batch_options.h"
#include "yb/mdb/storage/docdb_read_context.h"

namespace yb::mdb {

Status BindingIterBatchSource::Open(const Binding& input) {
  Close();
  binding_ = std::make_unique<Binding>(input.size);
  binding_->add_all(input);
  try { iter_->begin(*binding_); }
  catch (const std::exception& e) { failure_ = STATUS(RuntimeError, e.what()); }
  return failure_;
}

Result<BindingBatch> BindingIterBatchSource::NextBatch() {
  if (!binding_) return STATUS(IllegalState, "Binding batch source is closed");
  RETURN_NOT_OK(failure_);
  BindingBatch batch;
  RETURN_NOT_OK(batch.Init(BindingBatch::DenseLayout(binding_->size), row_limit_, byte_limit_, true));
  try {
    while (!batch.full() && !eof_) {
      if (!iter_->next()) { eof_ = true; break; }
      RETURN_NOT_OK(batch.Append(*binding_, emitted_++));
    }
  } catch (const std::exception& e) {
    failure_ = STATUS(RuntimeError, e.what());
    return failure_;
  }
  batch.eof = eof_;
  return batch;
}

void BindingIterBatchSource::Close() {
  binding_.reset();
  eof_ = false;
  emitted_ = 0;
  failure_ = Status::OK();
}

void BatchBindingIter::_begin(Binding& input) {
  binding_ = &input;
  _reset();
}

void BatchBindingIter::_reset() {
  source_->Close();
  batch_ = BindingBatch();
  position_ = 0;
  failure_ = Status::OK();
  const auto status = source_->Open(*binding_);
  if (!status.ok()) throw QueryException(status.ToString());
}

bool BatchBindingIter::_next() {
  if (!failure_.ok()) throw QueryException(failure_.ToString());
  if (auto* context = DocDBReadContextScope::Current()) {
    if (context->cancelled.load()) failure_ = STATUS(Aborted, "MDB batch query cancelled");
    else if (CoarseMonoClock::Now() >= context->deadline)
      failure_ = STATUS(TimedOut, "MDB batch query deadline exceeded");
    if (!failure_.ok()) throw QueryException(failure_.ToString());
  }
  while (position_ == batch_.rows()) {
    if (batch_.eof) return false;
    batch_ = BindingBatch();
    auto next = source_->NextBatch();
    if (!next.ok()) {
      failure_ = next.status();
      throw QueryException(failure_.ToString());
    }
    batch_ = std::move(*next);
    position_ = 0;
  }
  const auto status = batch_.CopyRow(position_++, binding_);
  if (!status.ok()) {
    failure_ = status;
    throw QueryException(status.ToString());
  }
  return true;
}

void BatchBindingIter::assign_nulls() {
  for (auto var : output_vars_) binding_->add(var, ::ObjectId::get_null());
}

void BatchBindingIter::print(std::ostream& os, int indent, bool stats) const {
  if (stats) print_generic_stats(os, indent);
  os << std::string(indent, ' ') << name_ << "()\n";
}

std::string BatchQueryStats::ToJson() const {
  std::string result = "{";
#define MDB_STAT(name) result += "\"" #name "\":" + std::to_string(name) + ","
  MDB_STAT(adapter_parse_us);
  MDB_STAT(adapter_read_view_us);
  MDB_STAT(adapter_dictionary_us);
  MDB_STAT(adapter_plan_us);
  MDB_STAT(adapter_execute_us);
  MDB_STAT(adapter_total_us);
  MDB_STAT(storage_flush_us);
  MDB_STAT(storage_decode_us);
  MDB_STAT(remote_spawn_us);
  MDB_STAT(remote_open_wait_us);
  MDB_STAT(remote_pull_wait_us);
  MDB_STAT(remote_sink_us);
  MDB_STAT(remote_cleanup_us);
  MDB_STAT(read_operations); MDB_STAT(flush_rounds); MDB_STAT(response_payload_bytes);
  MDB_STAT(decoded_rows); MDB_STAT(join_operators); MDB_STAT(outer_rows);
  MDB_STAT(unique_probes); MDB_STAT(duplicate_rows); MDB_STAT(negative_cache_hits);
  MDB_STAT(tablet_groups); MDB_STAT(output_rows); MDB_STAT(peak_buffered_bytes);
  MDB_STAT(max_in_flight);
  MDB_STAT(hash_join_operators); MDB_STAT(hash_build_rows); MDB_STAT(hash_probe_rows);
  MDB_STAT(hash_output_rows); MDB_STAT(hash_peak_reserved_bytes);
  MDB_STAT(optimizer_hash_choices); MDB_STAT(optimizer_index_choices);
  MDB_STAT(optimizer_missing_stats);
  MDB_STAT(hash_build_right_operators); MDB_STAT(estimated_hash_build_rows);
  MDB_STAT(estimated_hash_build_bytes);
  MDB_STAT(remote_read_operations);
  MDB_STAT(remote_fragments); MDB_STAT(remote_max_workers); MDB_STAT(remote_batches);
  MDB_STAT(remote_rows); MDB_STAT(remote_response_bytes); MDB_STAT(remote_peak_frame_bytes);
  MDB_STAT(remote_coordinator_fallbacks);
  MDB_STAT(global_count_having_operators);
  MDB_STAT(global_count_having_hidden_counts);
  MDB_STAT(global_count_distinct_operators);
  MDB_STAT(global_count_distinct_sets);
  MDB_STAT(global_count_distinct_entries);
  MDB_STAT(global_count_operators);
  MDB_STAT(global_count_groups);
  MDB_STAT(global_distinct_operators);
  MDB_STAT(global_distinct_retained_rows);
  MDB_STAT(global_slice_operators);
  MDB_STAT(global_slice_capacity);
  MDB_STAT(global_slice_retained_rows);
  MDB_STAT(global_topk_operators);
  MDB_STAT(global_topk_capacity);
  MDB_STAT(global_topk_retained_rows);
  MDB_STAT(global_input_rows);
  MDB_STAT(global_reserved_bytes);
  MDB_STAT(global_finalize_operators);
  MDB_STAT(exchange_input_us);
  MDB_STAT(exchange_interleaved_operators);
  MDB_STAT(exchange_total_us);
  MDB_STAT(exchange_setup_us);
  MDB_STAT(exchange_left_input_us);
  MDB_STAT(exchange_right_input_us);
  MDB_STAT(exchange_partition_us);
  MDB_STAT(exchange_consume_us);
  MDB_STAT(exchange_sink_us);
  MDB_STAT(exchange_cleanup_us);
  MDB_STAT(exchange_operators); MDB_STAT(exchange_left_rows); MDB_STAT(exchange_right_rows);
  MDB_STAT(exchange_output_rows); MDB_STAT(exchange_broadcast_rows); MDB_STAT(exchange_spill_files);
  MDB_STAT(exchange_live_files); MDB_STAT(exchange_spill_bytes); MDB_STAT(exchange_spill_read_rows);
  MDB_STAT(exchange_peak_reserved_bytes); MDB_STAT(exchange_max_partition_rows);
  MDB_STAT(exchange_join_passes); MDB_STAT(exchange_chunked_passes);
  MDB_STAT(exchange_skew_partitions); MDB_STAT(exchange_cleanup_failures);
#undef MDB_STAT
  result += "\"remote_worker_pids\":\"" + remote_worker_pids + "\",";
  result += "\"join_decisions\":\"" + join_decisions + "\"}";
  return result;
}

}  // namespace yb::mdb
