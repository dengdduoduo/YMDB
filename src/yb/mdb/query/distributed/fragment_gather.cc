// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/distributed/fragment_gather.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <unistd.h>

#include "yb/mdb/query/distributed/fragment_transport.h"
#include "yb/util/subprocess.h"
#include "yb/util/uuid.h"

namespace yb::mdb {
namespace {

bool SafeToken(const std::string& token, bool path) {
  if (token.empty() || token.front() == '-') return false;
  for (unsigned char c : token) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.' ||
        (path ? c == '/' : c == '@' || c == ':')) continue;
    return false;
  }
  return !path || token.front() == '/';
}

struct Stream {
  // Destruction kills/reaps the local process. SSH channel EOF also ends the
  // remote pull loop; an in-progress remote read remains bounded by its deadline.
  std::unique_ptr<Subprocess> process;
  FragmentRequestPB request;
};

Status CheckResponse(const FragmentResponsePB& response, const FragmentRequestPB& request) {
  if (response.version() != request.version() || response.query_id() != request.query_id() ||
      response.fragment_id() != request.fragment_id() || response.attempt_id() != request.attempt_id() ||
      response.stream_id() != request.stream_id() || response.batch_seq() != request.batch_seq())
    return STATUS(Corruption, "MDB worker response belongs to another stream or batch");
  if (response.has_error()) return STATUS(RuntimeError, response.error());
  return Status::OK();
}

}  // namespace

Status GatherFragments(const FragmentPlan& plan, const WorkerOptions& options,
                       DocDBReadContext& context, const FragmentRowSink& sink) {
  if (options.targets.empty() || options.targets.size() > 32 || !options.max_workers ||
      options.max_workers > 16 || !SafeToken(options.binary, true))
    return STATUS(InvalidArgument, "MDB worker configuration is invalid");
  if (!options.flagfile.empty() && !SafeToken(options.flagfile, true))
    return STATUS(InvalidArgument, "MDB worker flagfile path contains unsupported characters");
  if (!options.ssh_config.empty() && !SafeToken(options.ssh_config, true))
    return STATUS(InvalidArgument, "MDB worker SSH config path contains unsupported characters");
  for (const auto& target : options.targets) if (!SafeToken(target, false))
    return STATUS(InvalidArgument, "MDB SSH target contains unsupported characters");
  if (context.cancelled.load()) return STATUS(Aborted, "MDB query cancelled");
  if (plan.empty) return Status::OK();
  RETURN_NOT_OK(FragmentPlan::ValidateCoverage(plan.tasks));
  const auto child = VERIFY_RESULT(context.ExportChild());
  const auto query_id = Uuid::Generate().ToString();
  std::vector<std::unique_ptr<Stream>> active;
  size_t next_task = 0;
  auto start = [&]() -> Status {
    auto stream = std::make_unique<Stream>();
    auto& request = stream->request;
    request.set_version(kFragmentVersion);
    request.set_query_id(query_id);
    request.set_fragment_id(static_cast<uint32_t>(next_task));
    request.set_attempt_id(1);
    request.set_stream_id(static_cast<uint32_t>(next_task));
    request.set_batch_seq(0);
    auto* open = request.mutable_open();
    *open = plan.physical;
    *open->mutable_transaction() = child;
    open->set_partition_start(plan.tasks[next_task].partition_start);
    open->set_partition_end(plan.tasks[next_task].partition_end);
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        context.deadline - CoarseMonoClock::Now()).count();
    if (remaining <= 0) return STATUS(TimedOut, "MDB query expired before worker start");
    open->set_timeout_ms(static_cast<uint32_t>(std::min<int64_t>(remaining, 30000)));
    RETURN_NOT_OK(ValidateFragmentOpen(*open));
    const auto& target = options.targets[next_task % options.targets.size()];
    std::vector<std::string> args;
    if (target == "local") args = {options.binary, "--logtostderr=true"};
    else args = {"/usr/bin/ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                 "-o", "ConnectTimeout=5", "-o", "ConnectionAttempts=1", "-o", "ServerAliveInterval=2",
                 "-o", "ServerAliveCountMax=2", target, options.binary, "--logtostderr=true"};
    if (!options.flagfile.empty()) args.push_back("--flagfile=" + options.flagfile);
    if (target != "local" && !options.ssh_config.empty())
      args.insert(args.begin() + 1, {"-F", options.ssh_config});
    stream->process = std::make_unique<Subprocess>(args.front(), args);
    stream->process->PipeParentStdin();
    stream->process->PipeParentStdout();
    RETURN_NOT_OK(stream->process->Start());
    RETURN_NOT_OK(SetFragmentNonblocking(stream->process->to_child_stdin_fd()));
    RETURN_NOT_OK(SetFragmentNonblocking(stream->process->from_child_stdout_fd()));
    RETURN_NOT_OK(WriteFragmentFrame(stream->process->to_child_stdin_fd(), request,
                                    context.deadline, &context.cancelled));
    active.push_back(std::move(stream));
    ++next_task;
    ++context.stats.remote_fragments;
    context.stats.remote_max_workers = std::max<uint64_t>(context.stats.remote_max_workers, active.size());
    return Status::OK();
  };
  while (next_task < plan.tasks.size() && active.size() < options.max_workers) RETURN_NOT_OK(start());
  // Read OPEN acknowledgements, then let all workers compute concurrently.
  auto open_ack = [&](Stream& stream) -> Status {
    FragmentResponsePB response;
    RETURN_NOT_OK(ReadFragmentFrame(stream.process->from_child_stdout_fd(), &response,
                                   context.deadline, &context.cancelled));
    RETURN_NOT_OK(CheckResponse(response, stream.request));
    if (response.rows() || response.values_size() || response.eof())
      return STATUS(Corruption, "Unexpected MDB OPEN acknowledgement payload");
    if (!context.stats.remote_worker_pids.empty()) context.stats.remote_worker_pids += ',';
    context.stats.remote_worker_pids += std::to_string(response.worker_pid());
    stream.request.clear_open();
    stream.request.set_batch_seq(1);
    return WriteFragmentFrame(stream.process->to_child_stdin_fd(), stream.request,
                              context.deadline, &context.cancelled);
  };
  for (auto& stream : active) RETURN_NOT_OK(open_ack(*stream));
  size_t current = 0;
  while (!active.empty()) {
    auto& stream = *active[current];
    FragmentResponsePB response;
    RETURN_NOT_OK(ReadFragmentFrame(stream.process->from_child_stdout_fd(), &response,
                                   context.deadline, &context.cancelled));
    RETURN_NOT_OK(CheckResponse(response, stream.request));
    const auto width = plan.physical.projection_size();
    if (response.rows() > options.batch_rows ||
        static_cast<uint64_t>(response.values_size()) != uint64_t(response.rows()) * width ||
        (!response.eof() && response.has_transaction_result()))
      return STATUS(Corruption, "MDB worker batch schema or row count mismatch");
    ++context.stats.remote_batches;
    context.stats.remote_response_bytes += response.ByteSizeLong();
    context.stats.remote_peak_frame_bytes = std::max<uint64_t>(
        context.stats.remote_peak_frame_bytes, response.ByteSizeLong());
    std::vector<uint64_t> row(width);
    for (uint32_t r = 0; r < response.rows(); ++r) {
      if (context.cancelled.load()) return STATUS(Aborted, "MDB Gather cancelled by consumer");
      if (CoarseMonoClock::Now() >= context.deadline) return STATUS(TimedOut, "MDB Gather deadline expired");
      for (int c = 0; c < width; ++c) {
        row[c] = response.values(r * width + c);
        if (!row[c]) return STATUS(Corruption, "MDB worker returned an unbound RDF ID");
      }
      RETURN_NOT_OK(sink(row));
      ++context.stats.remote_rows;
    }
    if (response.eof()) {
      if (!response.has_transaction_result()) return STATUS(Corruption, "Missing MDB child read result");
      RETURN_NOT_OK(context.ApplyChild(response.transaction_result()));
      active.erase(active.begin() + current);
      if (next_task < plan.tasks.size()) {
        RETURN_NOT_OK(start());
        RETURN_NOT_OK(open_ack(*active.back()));
      }
    } else {
      stream.request.set_batch_seq(stream.request.batch_seq() + 1);
      // One outstanding credit per stream. No prefetch after an error/cancel.
      RETURN_NOT_OK(WriteFragmentFrame(stream.process->to_child_stdin_fd(), stream.request,
                                      context.deadline, &context.cancelled));
      ++current;
    }
    if (!active.empty()) current %= active.size();
  }
  return Status::OK();
}

}  // namespace yb::mdb
