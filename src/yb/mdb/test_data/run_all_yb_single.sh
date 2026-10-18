#!/bin/bash
# Run all YB single-node benchmarks sequentially
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="python3"
BENCH="$SCRIPT_DIR/run_single_benchmark.py"

DATASETS="ds100 ds200 ds500 ds1000 ds2000"
QUERY_COUNTS="10 50 100 200 500 1000"

LOGFILE="$SCRIPT_DIR/bench_results/yb_single_all.log"
mkdir -p "$SCRIPT_DIR/bench_results"

echo "===== YB SINGLE-NODE ALL BENCHMARKS START: $(date) =====" | tee -a "$LOGFILE"

for ds in $DATASETS; do
  for qn in $QUERY_COUNTS; do
    OUTFILE="$SCRIPT_DIR/bench_results/single_yb_${ds}_q${qn}.json"
    if [ -f "$OUTFILE" ]; then
      echo "[SKIP] yb $ds q=$qn — already exists" | tee -a "$LOGFILE"
      continue
    fi
    echo ">>> Running: yb $ds q=$qn  $(date)" | tee -a "$LOGFILE"
    $PYTHON "$BENCH" --db yb --dataset "$ds" --num-queries "$qn" --tservers 1 2>&1 | tee -a "$LOGFILE"
    echo "<<< Done: yb $ds q=$qn  $(date)" | tee -a "$LOGFILE"

    sleep 2
  done
done

echo "===== YB SINGLE-NODE ALL BENCHMARKS DONE: $(date) =====" | tee -a "$LOGFILE"
