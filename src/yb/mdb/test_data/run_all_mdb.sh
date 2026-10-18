#!/bin/bash
# Run all MDB benchmarks sequentially
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="python3"
BENCH="$SCRIPT_DIR/run_single_benchmark.py"

DATASETS="ds100 ds200 ds500 ds1000 ds2000"
QUERY_COUNTS="10 50 100 200 500 1000"

LOGFILE="$SCRIPT_DIR/bench_results/mdb_all.log"
mkdir -p "$SCRIPT_DIR/bench_results"

echo "===== MDB ALL BENCHMARKS START: $(date) =====" | tee -a "$LOGFILE"

for ds in $DATASETS; do
  for qn in $QUERY_COUNTS; do
    OUTFILE="$SCRIPT_DIR/bench_results/single_mdb_${ds}_q${qn}.json"
    if [ -f "$OUTFILE" ]; then
      echo "[SKIP] mdb $ds q=$qn — already exists" | tee -a "$LOGFILE"
      continue
    fi
    echo ">>> Running: mdb $ds q=$qn  $(date)" | tee -a "$LOGFILE"
    $PYTHON "$BENCH" --db mdb --dataset "$ds" --num-queries "$qn" 2>&1 | tee -a "$LOGFILE"
    echo "<<< Done: mdb $ds q=$qn  $(date)" | tee -a "$LOGFILE"

    # Kill any leftover MDB server
    pkill -f "mdb server" 2>/dev/null || true
    rm -rf "/tmp/mdb_single_${ds}" 2>/dev/null || true
    sleep 1
  done
done

echo "===== MDB ALL BENCHMARKS DONE: $(date) =====" | tee -a "$LOGFILE"
