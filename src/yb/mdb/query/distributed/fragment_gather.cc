// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/distributed/fragment_gather.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <limits>
#include <memory>
#include <poll.h>
#include <unistd.h>

#include "yb/mdb/query/distributed/fragment_transport.h"
#include "yb/util/subprocess.h"
#include "yb/util/uuid.h"
#include "yb/mdb/query/executor/wall_time_scope.h"

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
  FragmentFrameReader reader;
  uint64_t read_operations = 0;
  size_t input = 0;
  bool opening = true;
};

Status FinishWorker(Subprocess& process, DocDBReadContext& context) {
  // EOF completes the data stream, not the process. A cleanup crash must fail
  // the query instead of being hidden by Subprocess's SIGKILL fallback.
  close(process.ReleaseChildStdinFd());
  for (;;) {
    if (context.cancelled.load(std::memory_order_relaxed))
      return STATUS(Aborted, "MDB worker exit cancelled");
    if (CoarseMonoClock::Now() >= context.deadline)
      return STATUS(TimedOut, "MDB worker exit deadline expired");
    int exit_status;
    const auto status = process.WaitNoBlock(&exit_status);
    if (status.ok()) return process.Wait();  // Checks the cached exit status.
    if (!status.IsTimedOut()) return status;
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
}

Status CheckResponse(const FragmentResponsePB& response, const FragmentRequestPB& request) {
  if (response.version() != request.version() || response.query_id() != request.query_id() ||
      response.fragment_id() != request.fragment_id() || response.attempt_id() != request.attempt_id() ||
      response.stream_id() != request.stream_id() || response.batch_seq() != request.batch_seq())
    return STATUS(Corruption, "MDB worker response belongs to another stream or batch");
  if (response.has_error()) return STATUS(RuntimeError, response.error());
  return Status::OK();
}

Status GatherInputs(const std::array<const FragmentPlan*, 2>& plans, size_t input_count,
                    const WorkerOptions& options, DocDBReadContext& context,
                    const FragmentInputSink& sink) {
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
  std::array<size_t, 2> next_tasks = {0, 0};
  size_t task_count = 0;
  for (size_t side = 0; side < input_count; ++side) {
    const auto& plan = *plans[side];
    if (plan.empty) continue;
    RETURN_NOT_OK(FragmentPlan::ValidateCoverage(plan.tasks));
    if (plan.tasks.size() > std::numeric_limits<uint32_t>::max() - task_count)
      return STATUS(InvalidArgument, "MDB fragment identity space exhausted");
    task_count += plan.tasks.size();
    const auto& first = plans[0]->physical;
    const auto& open = plan.physical;
    if (open.master_addresses() != first.master_addresses() || open.keyspace() != first.keyspace() ||
        open.id_namespace() != first.id_namespace() || open.table_ids_size() != first.table_ids_size())
      return STATUS(InvalidArgument, "MDB inputs have different storage identities");
    for (int i = 0; i < open.table_ids_size(); ++i) if (open.table_ids(i) != first.table_ids(i))
      return STATUS(InvalidArgument, "MDB inputs have different table identities");
  }
  if (!task_count) return Status::OK();
  const auto child = VERIFY_RESULT(context.ExportChild());
  // Reject either physical input before launching any worker.
  for (size_t side = 0; side < input_count; ++side) {
    if (plans[side]->empty) continue;
    auto open = plans[side]->physical;
    *open.mutable_transaction() = child;
    open.set_timeout_ms(1);
    RETURN_NOT_OK(ValidateFragmentOpen(open));
  }
  const auto query_id = Uuid::Generate().ToString();
  std::vector<std::unique_ptr<Stream>> active;
  size_t next_input = 0;
  uint32_t fragment = 0;
  auto start = [&]() -> Status {
    WallTimeScope timer(&context.stats.remote_spawn_us);
    while (plans[next_input]->empty || next_tasks[next_input] == plans[next_input]->tasks.size())
      next_input = (next_input + 1) % input_count;
    const auto side = next_input;
    const auto& plan = *plans[side];
    const auto& task = plan.tasks[next_tasks[side]];
    auto stream = std::make_unique<Stream>();
    stream->input = side;
    auto& request = stream->request;
    request.set_version(kFragmentVersion);
    request.set_query_id(query_id);
    request.set_fragment_id(fragment);
    request.set_attempt_id(1);
    request.set_stream_id(fragment);
    request.set_batch_seq(0);
    auto* open = request.mutable_open();
    *open = plan.physical;
    *open->mutable_transaction() = child;
    open->set_partition_start(task.partition_start);
    open->set_partition_end(task.partition_end);
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        context.deadline - CoarseMonoClock::Now()).count();
    if (remaining <= 0) return STATUS(TimedOut, "MDB query expired before worker start");
    open->set_timeout_ms(static_cast<uint32_t>(std::min<int64_t>(remaining, 30000)));
    RETURN_NOT_OK(ValidateFragmentOpen(*open));
    const auto& target = options.targets[fragment % options.targets.size()];
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
    ++next_tasks[side];
    next_input = (side + 1) % input_count;
    ++fragment;
    ++context.stats.remote_fragments;
    context.stats.remote_max_workers = std::max<uint64_t>(context.stats.remote_max_workers, active.size());
    return Status::OK();
  };
  while (fragment < task_count && active.size() < options.max_workers) RETURN_NOT_OK(start());
  auto open_ack = [&](Stream& stream, const FragmentResponsePB& response) -> Status {
    if (response.rows() || response.values_size() || response.eof())
      return STATUS(Corruption, "Unexpected MDB OPEN acknowledgement payload");
    if (!context.stats.remote_worker_pids.empty()) context.stats.remote_worker_pids += ',';
    context.stats.remote_worker_pids += std::to_string(response.worker_pid());
    stream.opening = false;
    stream.request.clear_open();
    stream.request.set_batch_seq(1);
    return WriteFragmentFrame(stream.process->to_child_stdin_fd(), stream.request,
                              context.deadline, &context.cancelled);
  };
  size_t current = 0;
  auto read_ready = [&](FragmentResponsePB* response) -> Status {
    std::vector<pollfd> fds;
    for (const auto& stream : active)
      fds.push_back({stream->process->from_child_stdout_fd(), POLLIN, 0});
    for (;;) {
      if (context.cancelled.load()) return STATUS(Aborted, "MDB Gather cancelled");
      if (CoarseMonoClock::Now() >= context.deadline)
        return STATUS(TimedOut, "MDB Gather deadline expired");
      const auto ready = poll(fds.data(), fds.size(), 20);
      if (ready < 0 && errno == EINTR) continue;
      if (ready < 0) return STATUS(IOError, "MDB Gather pipe poll failed");
      // Rotate preference after each complete frame. A partial frame never
      // reserves the consumer while another credited stream is ready.
      for (size_t offset = 0; offset < active.size(); ++offset) {
        const auto index = (current + offset) % active.size();
        if (!fds[index].revents) continue;
        if (fds[index].revents & POLLNVAL)
          return STATUS(IOError, "MDB Gather invalid worker pipe");
        bool complete;
        RETURN_NOT_OK(active[index]->reader.ReadAvailable(
            fds[index].fd, response, &complete, context.deadline, &context.cancelled));
        if (complete) { current = index; return Status::OK(); }
      }
    }
  };
  while (!active.empty()) {
    FragmentResponsePB response;
    const auto wait_start = WallMicros();
    const auto ready = read_ready(&response);
    // Attribute pooled waits by the frame that ended them, not worker lifetime.
    auto& wait_counter = ready.ok() && active[current]->opening
        ? context.stats.remote_open_wait_us : context.stats.remote_pull_wait_us;
    wait_counter += WallMicros() - wait_start;
    RETURN_NOT_OK(ready);
    auto& stream = *active[current];
    RETURN_NOT_OK(CheckResponse(response, stream.request));
    if (stream.opening) {
      RETURN_NOT_OK(open_ack(stream, response));
      current = (current + 1) % active.size();
      continue;
    }
    const auto width = plans[stream.input]->physical.projection_size();
    if (response.rows() > options.batch_rows ||
        static_cast<uint64_t>(response.values_size()) != uint64_t(response.rows()) * width ||
        (!response.eof() && response.has_transaction_result()))
      return STATUS(Corruption, "MDB worker batch schema or row count mismatch");
    // Responses report cumulative reads for this fragment, not a per-batch count.
    const auto reads = std::max(stream.read_operations, response.read_operations());
    context.stats.remote_read_operations += reads - stream.read_operations;
    stream.read_operations = reads;
    ++context.stats.remote_batches;
    context.stats.remote_response_bytes += response.ByteSizeLong();
    context.stats.remote_peak_frame_bytes = std::max<uint64_t>(
        context.stats.remote_peak_frame_bytes, response.ByteSizeLong());
    {
      WallTimeScope timer(&context.stats.remote_sink_us);
      std::vector<uint64_t> row(width);
      for (uint32_t r = 0; r < response.rows(); ++r) {
        if (context.cancelled.load()) return STATUS(Aborted, "MDB Gather cancelled by consumer");
        if (CoarseMonoClock::Now() >= context.deadline) return STATUS(TimedOut, "MDB Gather deadline expired");
        for (int c = 0; c < width; ++c) {
          row[c] = response.values(r * width + c);
          if (!row[c]) return STATUS(Corruption, "MDB worker returned an unbound RDF ID");
        }
        RETURN_NOT_OK(sink(stream.input, row));
        ++context.stats.remote_rows;
      }
    }
    if (response.eof()) {
      if (!response.has_transaction_result()) return STATUS(Corruption, "Missing MDB child read result");
      {
        WallTimeScope timer(&context.stats.remote_cleanup_us);
        RETURN_NOT_OK(FinishWorker(*stream.process, context));
        RETURN_NOT_OK(context.ApplyChild(response.transaction_result()));
        active.erase(active.begin() + current);
      }
      if (fragment < task_count) {
        RETURN_NOT_OK(start());
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

}  // namespace

Status GatherFragments(const FragmentPlan& plan, const WorkerOptions& options,
                       DocDBReadContext& context, const FragmentRowSink& sink) {
  return GatherInputs({&plan, nullptr}, 1, options, context,
      [&](size_t, const auto& row) { return sink(row); });
}

Status GatherFragmentPair(const std::array<FragmentPlan, 2>& plans, const WorkerOptions& options,
                          DocDBReadContext& context, const FragmentInputSink& sink) {
  return GatherInputs({&plans[0], &plans[1]}, 2, options, context, sink);
}

}  // namespace yb::mdb
