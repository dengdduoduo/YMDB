// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/distributed/exchange.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <set>
#include <sys/stat.h>
#include <unistd.h>

#include "query/exceptions.h"
#include "query/executor/hash_join.h"

namespace yb::mdb {
namespace {

using Layout = std::vector<uint32_t>;
Layout Variables(const FragmentTriplePB& triple) {
  std::set<uint32_t> vars;
  for (const auto& term : triple.terms()) if (term.has_variable()) vars.insert(term.variable());
  return {vars.begin(), vars.end()};
}

Status Check(const ExchangeControl& c) {
  if (c.cancelled && c.cancelled->load()) return STATUS(Aborted, "MDB exchange cancelled");
  if (CoarseMonoClock::Now() >= (c.live_deadline ? *c.live_deadline : c.deadline))
    return STATUS(TimedOut, "MDB exchange deadline expired");
  return Status::OK();
}
Status IoError(const char* operation) {
  return STATUS(IOError, std::string("MDB exchange ") + operation + ": " + std::strerror(errno));
}
uint64_t Decode(const uint8_t* bytes) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i) value |= uint64_t(bytes[i]) << (i * 8);
  return value;
}
void Encode(uint64_t value, uint8_t* bytes) {
  for (size_t i = 0; i < 8; ++i) bytes[i] = value >> (i * 8);
}
uint64_t Hash(const std::vector<uint64_t>& values) {
  uint64_t hash = 14695981039346656037ULL;
  for (auto value : values) for (size_t byte = 0; byte < 8; ++byte) {
    hash = (hash ^ (value & 255)) * 1099511628211ULL;
    value >>= 8;
  }
  return hash;
}
size_t Overhead(const FragmentOpenPB& input, const WorkerOptions& options) {
  return 8192 + options.exchange_partitions * 2048 + input.variable_count() * 128;
}

struct File {
  std::string path;
  int fd = -1;
  size_t width = 0;
  uint64_t rows = 0;
};

class Spill {
 public:
  Spill(const WorkerOptions& options, ExchangeControl control) : options_(options), c_(control) {}
  ~Spill() { (void)Cleanup(); }
  std::array<std::vector<File>, 2> files;

  Status Init(const std::array<Layout, 2>& layouts) {
    std::string pattern = options_.exchange_spill_directory + "/mdb-exchange-XXXXXX";
    std::vector<char> name(pattern.begin(), pattern.end()); name.push_back(0);
    if (!mkdtemp(name.data())) return IoError("create private directory");
    directory_ = name.data();
    for (size_t side = 0; side < 2; ++side) {
      files[side].reserve(options_.exchange_partitions);
      for (size_t p = 0; p < options_.exchange_partitions; ++p) {
        RETURN_NOT_OK(Check(c_));
        files[side].push_back({directory_ + "/" + std::to_string(side) + "-" + std::to_string(p),
                               -1, layouts[side].size(), 0});
        auto& file = files[side].back();
        file.fd = open(file.path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
        if (file.fd < 0) return IoError("create spill file");
        ++c_.stats->exchange_spill_files; ++c_.stats->exchange_live_files;
        std::array<uint8_t, 16> header;
        std::memcpy(header.data(), "MDBX0001", 8); Encode(file.width, header.data() + 8);
        RETURN_NOT_OK(Write(file, header.data(), header.size()));
      }
    }
    return Status::OK();
  }

  Status Append(size_t side, size_t p, const std::vector<uint64_t>& row) {
    RETURN_NOT_OK(Check(c_));
    auto& file = files[side][p];
    if (row.size() != file.width || std::find(row.begin(), row.end(), 0) != row.end())
      return STATUS(Corruption, "MDB exchange input has an invalid layout or unbound ID");
    std::vector<uint8_t> bytes((row.size() + 1) * 8);
    for (size_t col = 0; col < row.size(); ++col) Encode(row[col], bytes.data() + 8 * col);
    Encode(Hash(row), bytes.data() + row.size() * 8);
    RETURN_NOT_OK(Write(file, bytes.data(), bytes.size()));
    ++file.rows;
    c_.stats->exchange_max_partition_rows = std::max(c_.stats->exchange_max_partition_rows, file.rows);
    return Status::OK();
  }

  Status Validate(const File& file) const {
    struct stat info;
    if (fstat(file.fd, &info)) return IoError("stat spill file");
    if (uint64_t(info.st_size) != 16 + file.rows * (file.width + 1) * 8)
      return STATUS(Corruption, "MDB exchange truncated or oversized spill file");
    std::array<uint8_t, 16> header;
    RETURN_NOT_OK(Read(file, 0, header.data(), header.size()));
    if (std::memcmp(header.data(), "MDBX0001", 8) || Decode(header.data() + 8) != file.width)
      return STATUS(Corruption, "MDB exchange spill header mismatch");
    return Status::OK();
  }
  Status Row(const File& file, uint64_t index, std::vector<uint64_t>* row) const {
    RETURN_NOT_OK(Check(c_));
    std::vector<uint8_t> bytes((file.width + 1) * 8);
    RETURN_NOT_OK(Read(file, 16 + index * bytes.size(), bytes.data(), bytes.size()));
    row->resize(file.width);
    for (size_t col = 0; col < file.width; ++col) (*row)[col] = Decode(bytes.data() + col * 8);
    if (Hash(*row) != Decode(bytes.data() + file.width * 8) ||
        std::find(row->begin(), row->end(), 0) != row->end())
      return STATUS(Corruption, "MDB exchange spill checksum or ID mismatch");
    ++c_.stats->exchange_spill_read_rows;
    return Status::OK();
  }
  Status Cleanup() {
    Status failure;
    for (auto& side : files) for (auto& file : side) {
      if (file.fd >= 0) { close(file.fd); file.fd = -1; }
      if (!file.path.empty()) {
        if (unlink(file.path.c_str()) && errno != ENOENT) {
          ++c_.stats->exchange_cleanup_failures; failure = IoError("unlink spill file");
        } else {
          if (c_.stats->exchange_live_files) --c_.stats->exchange_live_files;
          file.path.clear();
        }
      }
    }
    if (!directory_.empty()) {
      if (rmdir(directory_.c_str()) && errno != ENOENT) {
        ++c_.stats->exchange_cleanup_failures; failure = IoError("remove private directory");
      } else directory_.clear();
    }
    return failure;
  }
 private:
  Status Write(const File& file, const uint8_t* data, size_t size) {
    if (size > options_.exchange_spill_bytes - written_)
      return STATUS(RuntimeError, "MDB exchange disk budget exceeded");
    written_ += size;
    c_.stats->exchange_spill_bytes += size;
    while (size) {
      RETURN_NOT_OK(Check(c_));
      const auto n = write(file.fd, data, size);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) return IoError("write spill file");
      data += n; size -= n;
    }
    return Status::OK();
  }
  Status Read(const File& file, uint64_t offset, uint8_t* data, size_t size) const {
    while (size) {
      RETURN_NOT_OK(Check(c_));
      const auto n = pread(file.fd, data, size, offset);
      if (n < 0 && errno == EINTR) continue;
      if (n < 0) return IoError("read spill file");
      if (n == 0) return STATUS(Corruption, "MDB exchange unexpected spill EOF");
      data += n; size -= n; offset += n;
    }
    return Status::OK();
  }
  const WorkerOptions& options_;
  ExchangeControl c_;
  std::string directory_;
  uint64_t written_ = 0;
};

// Disk-backed, range-limited binding source for the existing P5 HashJoin.
class DiskIter : public BindingIter {
 public:
  DiskIter(const Spill& spill, const File& file, Layout layout, uint64_t offset, uint64_t limit)
      : spill_(spill), file_(file), layout_(std::move(layout)), offset_(offset), limit_(limit) {}
  void SetLimit(uint64_t limit) { limit_ = limit; }
  void assign_nulls() override { for (auto var : layout_) binding_->add(VarId(var), ::ObjectId::get_null()); }
  void print(std::ostream& os, int indent, bool) const override { os << std::string(indent, ' ') << "ExchangeDisk\n"; }
 protected:
  void _begin(Binding& binding) override { binding_ = &binding; _reset(); }
  void _reset() override {
    position_ = 0;
    const auto status = spill_.Validate(file_);
    if (!status.ok()) throw QueryException(status.ToString());
  }
  bool _next() override {
    if (position_ == limit_) return false;
    const auto status = spill_.Row(file_, offset_ + position_++, &row_);
    if (!status.ok()) throw QueryException(status.ToString());
    for (size_t col = 0; col < layout_.size(); ++col) binding_->add(VarId(layout_[col]), ::ObjectId(row_[col]));
    return true;
  }
 private:
  const Spill& spill_;
  const File& file_;
  Layout layout_;
  uint64_t offset_, limit_, position_ = 0;
  Binding* binding_ = nullptr;
  std::vector<uint64_t> row_;
};

}  // namespace

Status ValidateExchange(const FragmentOpenPB& input, const WorkerOptions& options) {
  if (input.triples_size() != 2) return STATUS(NotSupported, "MDB exchange requires two BGP triples");
  if (options.exchange != ExchangeMode::Hash && options.exchange != ExchangeMode::Broadcast)
    return STATUS(InvalidArgument, "Invalid MDB exchange mode");
  if (!input.variable_count() || input.variable_count() > 128 || !input.projection_size() ||
      input.projection_size() > 128 || options.exchange_partitions < 2 ||
      options.exchange_partitions > 64 || options.exchange_skew_factor < 1 || options.exchange_skew_factor > 16 ||
      options.exchange_memory_bytes < 65536 || options.exchange_memory_bytes > 256 * 1024 * 1024 ||
      options.exchange_spill_bytes < options.exchange_partitions * 32 || options.exchange_spill_bytes > (1ULL << 40) ||
      options.exchange_spill_directory.empty() || options.exchange_spill_directory.front() != '/' ||
      options.exchange_spill_directory.size() > 512 || options.exchange_spill_directory.find('\0') != std::string::npos)
    return STATUS(InvalidArgument, "MDB exchange resource configuration is invalid");
  if (Overhead(input, options) + 32768 + input.variable_count() * 64 > options.exchange_memory_bytes)
    return STATUS(InvalidArgument, "MDB exchange metadata and hash state exceed memory budget");
  std::array<Layout, 2> layouts = {Variables(input.triples(0)), Variables(input.triples(1))};
  if (layouts[0].empty() || layouts[1].empty()) return STATUS(NotSupported, "Constant-only exchange side");
  std::set<uint32_t> bound(layouts[0].begin(), layouts[0].end());
  bool shared = false;
  for (auto var : layouts[1]) { shared |= bound.count(var); bound.insert(var); }
  if (!shared) return STATUS(NotSupported, "Cartesian exchange remains at coordinator");
  for (const auto& triple : input.triples()) {
    if (triple.terms_size() != 3) return STATUS(InvalidArgument, "Invalid exchange triple");
    for (const auto& term : triple.terms()) if (term.has_variable() && term.variable() >= input.variable_count())
      return STATUS(InvalidArgument, "Exchange variable outside binding");
  }
  for (auto var : input.projection()) if (!bound.count(var)) return STATUS(NotSupported, "Unbound exchange projection");
  struct stat info;
  if (stat(options.exchange_spill_directory.c_str(), &info) || !S_ISDIR(info.st_mode))
    return STATUS(IOError, "MDB exchange spill root is not an existing directory");
  return Status::OK();
}

Status ExchangeRows(const FragmentOpenPB& input, const WorkerOptions& options,
                    ExchangeControl control, const ExchangeInput& producer, const FragmentRowSink& sink) {
  RETURN_NOT_OK(ValidateExchange(input, options));
  if (!control.stats) return STATUS(InvalidArgument, "MDB exchange requires query statistics");
  RETURN_NOT_OK(Check(control));
  auto& stats = *control.stats;
  ++stats.exchange_operators;
  const std::array<Layout, 2> layouts = {Variables(input.triples(0)), Variables(input.triples(1))};
  Layout keys;
  std::set_intersection(layouts[0].begin(), layouts[0].end(), layouts[1].begin(), layouts[1].end(),
                        std::back_inserter(keys));
  std::array<std::vector<size_t>, 2> key_columns;
  for (size_t side = 0; side < 2; ++side) for (auto key : keys)
    key_columns[side].push_back(std::find(layouts[side].begin(), layouts[side].end(), key) - layouts[side].begin());
  try {
    Spill spill(options, control);
    RETURN_NOT_OK(spill.Init(layouts));
    std::array<uint64_t, 2> totals = {0, 0};
    for (size_t side = 0; side < 2; ++side) {
      Status failure;
      const auto load = producer(side, [&](const std::vector<uint64_t>& row) -> Status {
        if (!failure.ok()) return failure;
        auto append = [&]() -> Status {
          RETURN_NOT_OK(Check(control));
          if (row.size() != layouts[side].size()) return STATUS(Corruption, "Exchange producer layout mismatch");
          std::vector<uint64_t> values;
          for (auto col : key_columns[side]) values.push_back(row[col]);
          const size_t p = options.exchange == ExchangeMode::Hash
              ? Hash(values) % options.exchange_partitions : totals[side] % options.exchange_partitions;
          if (side == 1 && options.exchange == ExchangeMode::Broadcast) {
            for (size_t bucket = 0; bucket < options.exchange_partitions; ++bucket) {
              RETURN_NOT_OK(spill.Append(side, bucket, row));
              ++stats.exchange_broadcast_rows;
            }
          } else RETURN_NOT_OK(spill.Append(side, p, row));
          ++totals[side];
          if (side) ++stats.exchange_right_rows; else ++stats.exchange_left_rows;
          return Status::OK();
        };
        failure = append();
        return failure;
      });
      RETURN_NOT_OK(failure); RETURN_NOT_OK(load);
    }
    // One consumer at a time. Oversized/hot build partitions are range-chunked;
    // each left partition is rescanned for every disjoint right chunk. Duplicate
    // keys keep their complete Cartesian bag without recursive repartitioning.
    for (size_t p = 0; p < options.exchange_partitions; ++p) {
      RETURN_NOT_OK(Check(control));
      const auto& left = spill.files[0][p]; const auto& right = spill.files[1][p];
      RETURN_NOT_OK(spill.Validate(left)); RETURN_NOT_OK(spill.Validate(right));
      if (!left.rows || !right.rows) continue;
      const uint64_t average = options.exchange == ExchangeMode::Broadcast ? totals[1] :
          (totals[1] + options.exchange_partitions - 1) / options.exchange_partitions;
      if (right.rows > average * options.exchange_skew_factor) ++stats.exchange_skew_partitions;
      for (uint64_t offset = 0; offset < right.rows;) {
        auto build = std::make_unique<DiskIter>(spill, right, layouts[1], offset, right.rows - offset);
        auto* range = build.get();
        auto probe = std::make_unique<DiskIter>(spill, left, layouts[0], 0, left.rows);
        std::vector<VarId> join_keys, build_vars;
        for (auto key : keys) join_keys.emplace_back(key);
        for (auto var : layouts[1]) build_vars.emplace_back(var);
        BatchJoinOptions hash_options;
        hash_options.memory_bytes = options.exchange_memory_bytes - Overhead(input, options);
        hash_options.batch_bytes = 4096; hash_options.input_rows = 32; hash_options.output_rows = 64;
        // Disk sources perform the explicit query checks. No storage pages or
        // shared parent transaction session are opened by this local HashJoin.
        HashJoin join(std::move(build), std::move(probe), std::move(join_keys), std::move(build_vars), nullptr, hash_options);
        Binding binding(input.variable_count());
        RETURN_NOT_OK(join.Open(binding));
        const auto count = std::min<uint64_t>(right.rows - offset, join.build_capacity());
        range->SetLimit(count);
        stats.exchange_peak_reserved_bytes = std::max<uint64_t>(stats.exchange_peak_reserved_bytes,
            join.reserved_bytes() + Overhead(input, options));
        ++stats.exchange_join_passes;
        if (count < right.rows) ++stats.exchange_chunked_passes;
        while (true) {
          RETURN_NOT_OK(Check(control));
          auto batch = VERIFY_RESULT(join.NextBatch());
          for (size_t row = 0; row < batch.rows(); ++row) {
            RETURN_NOT_OK(Check(control));
            RETURN_NOT_OK(batch.CopyRow(row, &binding));
            std::vector<uint64_t> output;
            for (auto var : input.projection()) output.push_back(binding[VarId(var)].id);
            RETURN_NOT_OK(sink(output));
            ++stats.exchange_output_rows;
          }
          if (batch.eof) break;
        }
        offset += count;
      }
    }
    RETURN_NOT_OK(Check(control));
    return spill.Cleanup();
  } catch (const std::exception& e) {
    return STATUS(RuntimeError, std::string("MDB exchange failed: ") + e.what());
  }
}

Status GatherExchangeFragments(const FragmentPlan& plan, const WorkerOptions& options,
                               DocDBReadContext& context, const FragmentRowSink& sink) {
  ExchangeControl control{&context.cancelled, context.deadline, &context.stats, &context.deadline};
  return ExchangeRows(plan.physical, options, control,
      [&](size_t side, const FragmentRowSink& input) -> Status {
        FragmentPlan child = plan;
        child.physical.clear_triples();
        *child.physical.add_triples() = plan.physical.triples(side);
        child.physical.clear_projection();
        for (auto var : Variables(child.physical.triples(0))) child.physical.add_projection(var);
        return GatherFragments(child, options, context, input);
      }, sink);
}

}  // namespace yb::mdb
