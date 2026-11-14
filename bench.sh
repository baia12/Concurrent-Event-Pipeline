#!/usr/bin/env bash
# Performance study: 1,000,000 records, Q=8, B=256, P=1,2,4(,8), 3 runs each, median.
# Run from the project root:  bash bench.sh
set -eu
cc -std=c11 -O2 -Wall -Wextra -pthread event_pipeline.c -o event_pipeline
Q=8; B=256; RUNS=3
PS="1 2 4"; [ "$(nproc 2>/dev/null || echo 1)" -ge 8 ] && PS="1 2 4 8"

python3 - <<'PY'
with open("bench.csv", "w", encoding="utf-8", newline="\n") as f:
    for i in range(1, 1_000_001):
        level = ("INFO", "WARN", "ERROR")[i % 3]
        f.write(f"{i},{level},{(i % 2001) - 1000}\n")
PY

echo "Validating once with --verify (not timed)..."
./event_pipeline --mode parallel --input bench.csv --output bench_out.csv \
  --threads 4 --queue $Q --batch $B --verify

# system information for the report
{
  echo "CPU:      $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ //') ($(nproc) logical CPUs)"
  echo "OS:       $(uname -sr) $(. /etc/os-release 2>/dev/null && echo "($PRETTY_NAME)")"
  echo "Compiler: $(cc --version | head -1)"
  echo "Storage:  $(df -P . | tail -1 | awk '{print $1}')  rotational=$(cat /sys/block/$(lsblk -no pkname "$(df -P . | tail -1 | awk '{print $1}')" 2>/dev/null | head -1)/queue/rotational 2>/dev/null || echo unknown) (0 = SSD/NVMe, 1 = HDD)"
  echo "Cache:    input was read by the validation run first, so it is almost certainly in the page cache"
} | tee bench_system.txt

: > bench_raw.csv; echo "P,run,seconds" >> bench_raw.csv
export PIPELINE_BENCH=1      # harmless if your build prints the timer unconditionally
for p in $PS; do
  for r in $(seq 1 $RUNS); do
    t=$(./event_pipeline --mode parallel --input bench.csv --output bench_out.csv \
          --threads "$p" --queue $Q --batch $B 2>&1 >/dev/null | sed -n 's/^BENCHMARK_SECONDS=//p')
    [ -n "$t" ] || { echo "no BENCHMARK_SECONDS line from the program"; exit 1; }
    echo "$p,$r,$t" | tee -a bench_raw.csv
  done
done
python3 plot_bench.py
rm -f bench.csv bench_out.csv      # do not submit the 1M-record input
