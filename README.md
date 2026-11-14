# Concurrent Event Pipeline

A concurrent event-processing pipeline implemented with C11 and POSIX threads (Pthreads).
It supports serial and parallel processing, two bounded queues, ordered result collection,
coordinated error handling, atomic output publication, and verification against an independent
serial implementation.

**Pipeline:** the main thread is the producer and reads batches of lines; `P` persistent worker
threads parse and classify them; one collector thread commits results in input order and writes the
alerts file. The number of uncommitted tasks is bounded by `W = 2P + Q`.

## Project Files

```
Concurrent_Event_Pipeline/
├── event_pipeline.c    complete C11/Pthreads source
├── tests.sh            automated functional tests (cases A-F and error cases)
├── bench.sh            performance benchmark (1,000,000 records)
├── plot_bench.py       builds the benchmark table and graph
├── README.md

```

Generated when you run the scripts (do not submit): the `event_pipeline` binary,
`tests_results.txt`, `bench_raw.csv`, `bench_table.md`, `bench_table.csv`, `bench_graph.png`,
`bench_system.txt`. `bench.sh` deletes the 1,000,000-record `bench.csv` when it finishes.

## Supported Platform and Requirements

* Linux / POSIX. Tested on Ubuntu 26.04.1 LTS under WSL2 with GCC 15.2.0 (also built on Windows 11
  with MSYS2 UCRT64, GCC 16.2.0).
* GCC (or another C11 compiler), Pthreads, Python 3, and Matplotlib (`pip install matplotlib`)
  for the benchmark graph.
* Use Ubuntu/WSL2 for the official tests and benchmark. On native Windows, `rename()` may not
  replace an existing output file, so publishing over an existing file can fail there.

## Compilation

From the directory containing `event_pipeline.c`:

```
cc -std=c11 -O2 -Wall -Wextra -pthread event_pipeline.c -o event_pipeline
```

(`gcc` can be used instead of `cc`.)

## Running the Program

Serial mode:

```
./event_pipeline --mode serial --input events.csv --output serial_alerts.csv
```

Parallel mode:

```
./event_pipeline --mode parallel --input events.csv --output parallel_alerts.csv \
    --threads 4 --queue 8 --batch 256 --verify
```

### Command-Line Options

| Option | Description |
| --- | --- |
| `--mode serial\|parallel` | Select the processing mode (required) |
| `--input PATH` | Input CSV file (required) |
| `--output PATH` | Output alerts file (required) |
| `--threads P` | Number of worker threads, 1-32 (parallel only) |
| `--queue Q` | Capacity of each queue, 1-64 (parallel only) |
| `--batch B` | Records per task, 1-1024 (parallel only) |
| `--verify` | Compare the parallel result with a serial run (parallel only) |

Parallel mode requires `--threads`, `--queue` and `--batch`. Each option may appear once; missing,
repeated, unknown or out-of-range options are rejected with a nonzero exit status. No defaults are
substituted.

### Output

The final line on standard output is:

```
records=… info=… warn=… error=… amount_sum=… alerts=… verify=PASS|FAIL|SKIP
```

`PASS` means the independent serial comparison succeeded; `SKIP` is printed in serial mode or when
`--verify` was not given. Errors go to standard error and the exit status is nonzero. On a
successful parallel run the timer (from just before thread creation to the successful close of the
temporary file) is also printed on standard error as `BENCHMARK_SECONDS=…`.

The alerts file contains `id,amount` followed by the ERROR records in original input order. It is
written to a temporary file in the output directory and renamed to `--output` only after every
stage, the close, and the optional verification have succeeded. On failure the temporary file is
removed and an existing output file is left unchanged.

## Input Format

Each record is one line: `id,level,amount`

* `id` is a positive integer equal to its one-based line number (maximum 10000000).
* `level` is exactly `INFO`, `WARN` or `ERROR` (case-sensitive).
* `amount` is an integer in [-1000000, 1000000]; no spaces or extra fields.
* Lines end with LF; the last line may omit it. An empty file is valid; a blank line is not.
  A record longer than 128 bytes (excluding LF) is rejected.

Example (`events.csv`, 8 records):

```
1,INFO,100
2,ERROR,250
3,WARN,150
4,ERROR,-50
5,INFO,200
6,WARN,300
7,ERROR,500
8,INFO,-100
```

Expected summary and alerts file for this input:

```
records=8 info=3 warn=2 error=3 amount_sum=1350 alerts=3 verify=PASS
```

```
id,amount
2,250
4,-50
7,500
```

The assignment's 5-record hand-check file (`1,INFO,5 / 2,WARN,7 / 3,ERROR,11 / 4,INFO,-3 /
5,ERROR,4`) gives `records=5 info=2 warn=1 error=2 amount_sum=24 alerts=2` and the alerts file
`id,amount / 3,11 / 5,4`. `tests.sh` uses this file for cases A and B.

## Automated Tests

```
chmod +x tests.sh
./tests.sh
```

`tests.sh` builds the program, creates its own inputs in a temporary directory, and runs:

| Case | Input and settings | What it checks |
| --- | --- | --- |
| A | hand-check file; P=1, Q=2, B=2 | exact counts and alerts file |
| B | same file; P=4, Q=1, B=1 | backpressure and ordered output |
| C | empty file; P=4, Q=1, B=1 | close and join with no tasks; header-only output |
| D | one ERROR record without final LF; P=3, Q=1, B=4 | final-line handling |
| E | valid prefix then `3,DEBUG,9`; P=4, Q=1, B=1 | abort, nonzero exit, existing output unchanged, no temp file left |
| F | 20000 valid records; P=8, Q=2, B=17, five repeats | many uneven batches, deterministic merge |
| CLI | `--threads 0` | invalid option rejected |
| Long line | a 5000-byte line, parallel and serial | rejected without hanging |

Every valid case must report `verify=PASS` and its alerts file is compared byte-for-byte with serial
mode (`cmp`). Results are saved in `tests_results.txt`; the script prints `ALL TESTS PASSED` when
every check succeeds.

## Manual Test Reproduction

Create the sample input (see "Input Format" above) as `events.csv`, then run the commands below
(`bash` is needed for the `{1..5}` loop).

**Test A - single worker**

```
./event_pipeline --mode parallel --input events.csv --output caseA.csv --threads 1 --queue 2 --batch 2 --verify
```

**Test B - small queue and batch**

```
./event_pipeline --mode parallel --input events.csv --output caseB.csv --threads 4 --queue 1 --batch 1 --verify
```

**Test C - empty input**

```
: > empty.csv
./event_pipeline --mode parallel --input empty.csv --output caseC.csv --threads 4 --queue 1 --batch 1 --verify
```

The output file contains only `id,amount`.

**Test D - final line without newline**

```
printf '1,ERROR,11' > caseD.csv
./event_pipeline --mode parallel --input caseD.csv --output caseD_alerts.csv --threads 3 --queue 1 --batch 4 --verify
```

**Test E - invalid record**

```
printf '1,INFO,5\n2,WARN,7\n3,DEBUG,9\n' > invalid.csv
./event_pipeline --mode parallel --input invalid.csv --output caseE.csv --threads 4 --queue 1 --batch 1 --verify
echo "exit=$?"
```

The program rejects the `DEBUG` level, exits nonzero and does not create `caseE.csv`.

**Output protection (an existing output file survives a failure)**

```
echo "DO NOT CHANGE THIS FILE" > protected.csv
./event_pipeline --mode parallel --input invalid.csv --output protected.csv --threads 4 --queue 1 --batch 1
cat protected.csv
```

`protected.csv` still contains `DO NOT CHANGE THIS FILE`.

**Long record (over 128 bytes)**

```
python3 -c "open('long.csv','w').write('1,INFO,' + '1'*130 + '\n')"
./event_pipeline --mode serial --input long.csv --output long_alerts.csv
```

The oversized record is rejected with a nonzero exit status.

**Invalid command-line option**

```
./event_pipeline --mode parallel --input events.csv --output test.csv --threads 4 --queue 8 --batch 256 --unknown
```

The unsupported option is rejected with a nonzero exit status.

**Test F - 10,001 records, repeated five times**

```
python3 -c "with open('large.csv','w') as f:
 for i in range(1,10002):
  level=['INFO','WARN','ERROR'][(i-1)%3]
  amount=(i%101)-50
  f.write(f'{i},{level},{amount}\n')"

for i in {1..5}; do
    ./event_pipeline --mode parallel --input large.csv --output caseF_$i.csv --threads 8 --queue 2 --batch 17 --verify || break
done
```

All five runs finish with `verify=PASS`
(`records=10001 info=3334 warn=3334 error=3333 amount_sum=-97 alerts=3333`).

## Performance Benchmark

```
chmod +x bench.sh
./bench.sh
```

`bench.sh`:

1. compiles the program with `-O2`;
2. generates a deterministic 1,000,000-record input file;
3. validates the parallel output once with `--verify` (not timed);
4. times worker counts 1, 2 and 4 (and 8 if at least 8 logical CPUs are available), three runs each,
   with Q=8 and B=256 for every run;
5. records CPU, OS, compiler and storage information in `bench_system.txt`;
6. runs `plot_bench.py`, which writes `bench_table.md`, `bench_table.csv` and `bench_graph.png`
   (median runtime and speedup relative to P=1).

The timer starts immediately before thread creation and stops after the temporary output file is
closed successfully; CLI parsing, input generation and the `--verify` pass are excluded. The
benchmark results and their discussion are in `report.pdf`.

## Notes

* `--verify` runs an independent serial implementation after the pipeline and compares every
  summary field and every byte of the alerts file. A mismatch prevents publication and returns
  nonzero.
* ERROR alerts are always written in input order, even when tasks finish out of order.
* Ownership rule: exactly one stage owns and frees each batch, task and result; it is stated in
  the comments of `event_pipeline.c` and in `report.pdf`.
