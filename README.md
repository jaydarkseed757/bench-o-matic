# bench-o-matic

A disk I/O benchmarking tool that measures sequential and random read/write performance with multi-threaded workloads and detailed latency statistics.

Available in two flavours that produce identical output:

| Implementation | Source |
|---|---|
| Rust (primary) | `src/` + `Cargo.toml` |
| Python (reference) | `bench_io.py` |

---

## Table of Contents

- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Command-line options](#command-line-options)
- [Output explained](#output-explained)
- [Usage examples](#usage-examples)
- [Building (Rust)](#building-rust)
- [Design notes](#design-notes)
- [Platform notes](#platform-notes)

> **New in this version:** `--duration`, `--read-pct`, `--interval`, `--queue-depth`, `--engine`, `--num-files` — see [the new flags](#new-flags) section.

---

## Quick start

```bash
# Rust — build once, then run
cargo build --release
./target/release/bench_io

# Python — no build step needed
python3 bench_io.py
```

With no arguments both tools run a 256 MB, 4 K-block, sequential + random, write + read benchmark on a single thread and clean up after themselves.

---

## New flags

Five new capabilities were added on top of the original design. Each is opt-in and backwards-compatible — existing invocations are unchanged.

### `--duration <TIME>` — time-bounded execution

Run each phase for a wall-clock duration instead of a fixed operation count. Accepts `"10s"`, `"1m"`, `"2m30s"`, `"1h"`. If both `--duration` and `--num-ops` are supplied, whichever limit fires first stops the worker.

```bash
# Run every phase for exactly 30 seconds
./target/release/bench_io --duration 30s --mode random
```

This is essential for detecting **thermal throttling**: drives (especially laptop NVMe) often sustain peak throughput for only 20–60 seconds before the controller drops to a lower gear. A fixed operation count may complete before throttling kicks in and report a misleadingly high number. A fixed duration catches it.

Warmup always uses `--warmup` operation count, not the duration limit.

---

### `--read-pct <0–100>` — interleaved mixed ratio

Only meaningful with `--workload mixed`. When supplied, runs a **single interleaved phase** where each operation is independently drawn as a read (with probability `read-pct/100`) or a write (with probability `(100−read-pct)/100`). Without `--read-pct`, `--workload mixed` runs two separate sequential phases (write then read) as before.

```bash
# 70% reads / 30% writes — classic OLTP ratio
./target/release/bench_io \
  --mode random --workload mixed --read-pct 70 \
  --file-size 4G --num-ops 20000
```

The result key in JSON is `sequential_mixed` / `random_mixed`; the display label includes the ratio: "Random Mixed (70R/30W)".

The randomness is per-operation (using the thread-local RNG), so the actual ratio in a short run may deviate slightly from the requested percentage — this is intentional and matches real workload variance.

---

### `--interval <TIME>` — per-interval time-series

Print a live throughput/IOPS/p99 line every N seconds during each phase, then show a summary table in the results block. Same time format as `--duration`.

```bash
# Watch per-second numbers during a 60-second run
./target/release/bench_io \
  --duration 60s --interval 1s \
  --mode sequential --workload write --file-size 1G
```

Live output appears inline as the phase runs:

```
  Working ...   [   1s]  4821.3 MB/s  75332 iops  p99: 0.018 ms
  [   2s]  4799.1 MB/s  74986 iops  p99: 0.019 ms
  [   3s]  3102.4 MB/s  48475 iops  p99: 0.041 ms  ← throttling visible here
```

The summary table is printed after the per-phase result block:

```
     Time-series intervals:
     Elapsed    Throughput        IOPS         p99 lat
         1s      4821.3 MB/s     75332        0.018 ms
         2s      4799.1 MB/s     74986        0.019 ms
         3s      3102.4 MB/s     48475        0.041 ms
```

Each row covers exactly one interval window. Throughput and IOPS are computed over that window's duration, not the total elapsed time — a drop in one row is unambiguous evidence of throttling.

A final partial snapshot is captured after the last complete interval, so no data is lost if the phase duration isn't an exact multiple of the interval.

---

### `--queue-depth <N>` and `--engine <sync|io-uring>`

#### Queue depth

Controls how many I/O operations each thread keeps in flight simultaneously. The default (`--queue-depth 1`) is the existing synchronous model: issue one operation, wait for it to complete, then issue the next.

Higher queue depths expose the drive's internal parallelism. Modern NVMe SSDs have hardware queues of 32–128 entries; a depth-1 benchmark will not saturate them and will under-report peak IOPS.

```bash
# Simulate 32 in-flight requests per thread
./target/release/bench_io \
  --mode random --workload read \
  --queue-depth 32 --threads 4 \
  --file-size 4G --duration 30s
```

#### Engines

`--engine sync` (default): thread-simulation. Each queue-depth slot gets its own OS thread and file handle. The OS can coalesce these into parallel NCQ/NVMe commands, so results approach true async I/O for random workloads. Total threads = `--threads × --queue-depth`.

`--engine io-uring` (Linux 5.1+ only): true async I/O via the `io_uring` subsystem. Uses the completion-refill pattern: submits `queue-depth` SQEs at startup, then immediately resubmits each slot as its CQE arrives, maintaining constant depth. This is the most accurate way to saturate NVMe drives and is equivalent to what `fio` measures with `ioengine=io_uring`.

```bash
# True async, depth 64, Linux only
./target/release/bench_io \
  --engine io-uring --queue-depth 64 \
  --mode random --workload read \
  --file-size 8G --duration 30s
```

If `io_uring` initialisation fails (kernel too old, seccomp restrictions, etc.), the tool falls back to the sync engine with a warning rather than aborting.

---

### `--num-files <N>` — file-set mode

By default `num-files = threads` (one file per thread). Setting a larger value creates a pool of smaller files that workers rotate through, stressing **directory and metadata operations** in addition to raw data throughput.

```bash
# 1000 × 4 MB files — simulates a mail server or web cache
./target/release/bench_io \
  --num-files 1000 --file-size 4M \
  --block-size 4K --mode random \
  --workload mixed --threads 8
```

With `num-files > threads`, each operation picks a file round-robin (sequential access) or at random (random access). Multiple threads can access the same file; this is intentional — it tests the filesystem's concurrent inode handling.

With `num-files < threads`, files are reused round-robin across threads. This matches the pre-existing behaviour when `num-files` is not specified.

---

## How it works

### Phases

Each run is broken into one or more **phases**. A phase is a single combination of access pattern × operation:

```
mode: both      →  sequential write, sequential read, random write, random read
mode: sequential →  sequential write, sequential read   (or just one if --workload write/read)
mode: random     →  random write, random read
```

`--workload mixed` (default) runs write **then** read as separate phases, giving clean independent numbers. It does **not** interleave reads and writes in the same phase.

### Execution model

1. **Preallocate** — one file per thread is written with non-zero random data before any measurement begins. This forces the OS to allocate real disk extents and defeats sparse-file / copy-on-write shortcuts that would make writes artificially fast.

2. **Warm-up** — before each measured phase, every thread performs `--warmup` operations into a throwaway metrics collector. This fills OS/disk caches and brings the hardware into a steady state so the first measured operation isn't an outlier.

3. **Measured phase** — threads run concurrently. Each operation is timed individually with a monotonic high-resolution clock (`Instant::now()` / `time.perf_counter()`). The wall-clock time spans from before the first thread is spawned to after the last thread finishes.

4. **Metrics aggregation** — per-operation latencies are collected in a thread-safe structure (a mutex-guarded `Vec<f64>` in Rust; a `threading.Lock`-protected list in Python). Byte counts use lock-free atomics in Rust.

5. **Cleanup** — test files are deleted unless `--no-cleanup` is passed.

### I/O model

| Mode | Behaviour |
|---|---|
| Sequential write | Opens the preallocated file in read-write mode, writes one block at a time advancing forward, wraps to the beginning when the end is reached. |
| Sequential read | Opens the file read-only, reads one block at a time advancing forward, wraps at EOF. |
| Random write | Seeks to a uniformly random block-aligned offset before each write. |
| Random read | Seeks to a uniformly random block-aligned offset before each read. |

**File-per-thread:** each thread exclusively owns one file for the entire phase. This avoids inode-level lock contention and lets the OS scheduler give each thread a distinct I/O queue, which is important for measuring parallel throughput accurately.

### Metrics computed

From the raw per-operation latency list (sorted):

| Metric | Formula |
|---|---|
| **IOPS** | `operations ÷ wall_time_s` |
| **Throughput** | `total_bytes ÷ wall_time_s ÷ 1 MiB` |
| **Latency avg** | arithmetic mean of per-op durations |
| **Latency min/max** | first/last element of sorted list |
| **p50 / p95 / p99** | linear-interpolation percentiles |
| **stddev** | sample standard deviation |

---

## Command-line options

### File and I/O sizing

| Option | Default | Description |
|---|---|---|
| `--dir <PATH>` | `/tmp/bench_io` | Directory where test files are created. Must be writable. The tool refuses to run if the required space exceeds 80% of available disk. |
| `--file-size <SIZE>` | `256M` | Size of each test file. Total disk used = `file-size × num-files`. Accepts suffixes: `B`, `K`/`KB`, `M`/`MB`, `G`/`GB` (all 1024-based). |
| `--block-size <SIZE>` | `4K` | Size of each individual I/O operation. Same suffix rules as `--file-size`. Must be ≤ `--file-size`. Common values: `4K` (database random I/O), `64K`, `1M` (streaming). |
| `--num-ops <N>` | `1000` | Number of measured I/O operations per thread. Ignored if `--duration` is set and fires first. |
| `--num-files <N>` | `= threads` | Number of test files to preallocate. Set higher than `--threads` to benchmark across a pool of many small files (simulates mail servers, web caches, object stores). |

### Workload shape

| Option | Default | Description |
|---|---|---|
| `--workload <TYPE>` | `mixed` | `write` — writes only. `read` — reads only. `mixed` — separate write phase then read phase. With `--read-pct`, a single interleaved phase instead. |
| `--mode <PATTERN>` | `both` | `sequential` — advance linearly through the file, wrapping at end. `random` — uniform random block-aligned offset each op. `both` — run sequential then random. |
| `--threads <N>` | `1` | Number of concurrent worker threads. Each thread is independent; wall time covers the full parallel span. |
| `--read-pct <0–100>` | off | **Only with `--workload mixed`.** Switches from two separate phases to one interleaved phase where each operation is a read with this probability and a write otherwise. `--read-pct 70` = 70% reads, 30% writes. |
| `--duration <TIME>` | off | Time-bound each phase instead of (or in addition to) `--num-ops`. Format: `"10s"`, `"1m"`, `"2m30s"`, `"1h"`. Whichever limit fires first stops the worker. Essential for detecting thermal throttling. |

### Concurrency and I/O engine

| Option | Default | Description |
|---|---|---|
| `--queue-depth <N>` | `1` | In-flight I/O operations per thread. `1` = synchronous (default). Higher values expose drive-level parallelism; NVMe drives benefit from depths of 32–128. |
| `--engine <TYPE>` | `sync` | `sync` — thread-simulation: each queue-depth slot gets its own OS thread and file handle. `io-uring` — Linux 5.1+ only: true async I/O via `io_uring`, completion-refill pattern. Falls back to `sync` with a warning if `io_uring` init fails. |

### I/O behaviour

| Option | Default | Description |
|---|---|---|
| `--fsync` | off | Call `fsync(2)` after every write — measures **durable write throughput**. Much lower than buffered throughput. Use to simulate databases or journalled filesystems. |
| `--unbuffered` | off | Bypass OS page cache. Linux: `O_DIRECT` (block size must be a multiple of 512 B; alignment is handled automatically). macOS: `F_NOCACHE` via `fcntl`. Silently ignored elsewhere. |
| `--warmup <N>` | `50` | Operations before measurement (results discarded). Fills caches and reaches steady state. Set to `0` to disable. |

### Output control

| Option | Default | Description |
|---|---|---|
| `--interval <TIME>` | off | Print a live throughput/IOPS/p99 line every N seconds during each phase, then show a summary table in the results block. Same format as `--duration`. Best combined with `--duration`. |
| `--histogram` | off | Print a 16-bucket ASCII latency histogram after each result block. Useful for seeing bimodal distributions (cache hits vs. misses) that averages hide. |
| `--json` | off | Emit results as a JSON object after the human-readable output. Suitable for `jq` or automated comparison. Raw latencies are excluded from JSON. |
| `--no-cleanup` | off | Retain test files after the run. Useful for back-to-back read benchmarks without re-preallocating. |

---

## Output explained

```
  ▶  Sequential Write
     Operations  :          200       ← total ops across all threads
     Transferred :       0.78 MB      ← total bytes moved
     Wall time   :      0.003 s       ← span from first thread spawn to last join
     Throughput  :     278.09 MB/s    ← total_bytes ÷ wall_time
     IOPS        :      71190.2       ← operations ÷ wall_time
     Latency avg :      0.009 ms      ← mean per-op duration
     Latency min :      0.000 ms
     Latency max :      0.094 ms
     p50/p95/p99 : 0.006 ms / 0.026 ms / 0.049 ms
```

**Throughput vs. IOPS:** throughput scales with block size (large blocks → high MB/s, same IOPS). IOPS is the storage-independent measure of how many operations per second the system can sustain.

**Wall time vs. latency:** wall time measures the whole phase including thread management overhead. Per-operation latency is measured inside the hot loop with a monotonic clock, so it excludes that overhead.

### Time-series output (with `--interval`)

```
     Time-series intervals:
     Elapsed    Throughput        IOPS         p99 lat
         1s      4821.3 MB/s     75332        0.018 ms
         2s      4799.1 MB/s     74986        0.019 ms
         3s      3102.4 MB/s     48475        0.041 ms   ← throttle event
```

Each row covers exactly one interval window. Throughput and IOPS are computed over that window, not cumulative — a drop is an unambiguous throttle event.

### JSON schema

```json
{
  "config": {
    "dir": "/tmp/bench_io",
    "file_size_bytes": 268435456,
    "block_size_bytes": 4096,
    "num_ops": 1000,
    "threads": 1,
    "num_files": 1,
    "queue_depth": 1,
    "workload": "mixed",
    "mode": "both",
    "fsync": false,
    "unbuffered": false,
    "warmup_ops": 50
  },
  "results": {
    "sequential_write": {
      "operations": 1000,
      "errors": 0,
      "total_bytes": 4096000,
      "total_mb": 3.906,
      "wall_time_s": 0.0142,
      "throughput_mb_s": 275.1,
      "iops": 70414.8,
      "latency_ms": {
        "avg": 0.0087,
        "min": 0.0004,
        "max": 0.341,
        "p50": 0.006,
        "p95": 0.027,
        "p99": 0.051,
        "stddev": 0.012
      }
    },
    "random_mixed": { "..." : "..." }
  }
}
```

Result keys are `{mode}_{operation}`: `sequential_write`, `random_read`, `sequential_mixed`, `random_mixed`.

---

## Usage examples

### Baseline — defaults

```bash
./target/release/bench_io
```

Runs all four phases (sequential write/read, random write/read) with 256 MB files, 4 K blocks, 1000 ops, single thread.

### Simulate OLTP database random I/O

```bash
./target/release/bench_io \
  --dir /mnt/nvme \
  --file-size 4G \
  --block-size 4K \
  --workload mixed \
  --read-pct 70 \
  --mode random \
  --threads 8 \
  --queue-depth 32 \
  --duration 60s \
  --interval 5s \
  --fsync
```

70/30 interleaved read/write, queue depth 32, 8 threads, durable writes — matches a realistic OLTP pattern. `--interval 5s` reveals any throughput drop caused by thermal throttling after 20–30 s.

### NVMe saturation test (Linux, io_uring)

```bash
./target/release/bench_io \
  --engine io-uring \
  --queue-depth 64 \
  --mode random \
  --workload read \
  --file-size 8G \
  --block-size 4K \
  --duration 30s \
  --interval 1s
```

True async I/O via `io_uring` at depth 64. This is the closest equivalent to `fio --ioengine=io_uring --iodepth=64` and will saturate modern NVMe drives that depth-1 tests cannot.

### Detect thermal throttling

```bash
./target/release/bench_io \
  --mode sequential \
  --workload write \
  --file-size 1G \
  --block-size 1M \
  --duration 120s \
  --interval 5s
```

Run sequential writes for 2 minutes with 5-second snapshots. A healthy drive holds throughput flat; a throttling drive shows a step-down in the time-series table at the 30–60 second mark.

### File-set mode (metadata-heavy workload)

```bash
./target/release/bench_io \
  --num-files 500 \
  --file-size 1M \
  --block-size 4K \
  --mode random \
  --workload mixed \
  --threads 4
```

500 × 1 MB files instead of one large file. Stresses inode cache, directory indexing, and `open()`/`close()` overhead — relevant for mail servers, web caches, and object storage backends.

### Large sequential throughput (streaming)

```bash
./target/release/bench_io \
  --file-size 2G \
  --block-size 1M \
  --mode sequential \
  --workload mixed \
  --duration 30s
```

1 MiB blocks give the OS maximum opportunity to coalesce I/O and reflect real streaming (video, backup, log-shipping) throughput.

### Bypass page cache (raw device speed)

```bash
./target/release/bench_io \
  --file-size 1G \
  --block-size 4K \
  --mode random \
  --unbuffered \
  --duration 30s
```

`--unbuffered` removes the OS buffer layer so results reflect actual device latency rather than DRAM speed. On Linux, block size must be a multiple of 512 B (4 K satisfies this).

### Capture results for CI / scripting

```bash
./target/release/bench_io \
  --mode random \
  --workload read \
  --num-ops 2000 \
  --json \
  | tee bench_results.json \
  | jq '.results.random_read | {iops, throughput_mb_s}'
```

### Keep files between runs (avoid re-preallocating)

```bash
# First run — preallocate and write
./target/release/bench_io --no-cleanup --workload write --mode sequential

# Second run — read from the already-preallocated files
./target/release/bench_io --no-cleanup --workload read --mode sequential

# Clean up manually
rm /tmp/bench_io/bench_t*.dat
```

### Latency histogram

```bash
./target/release/bench_io \
  --block-size 4K \
  --num-ops 2000 \
  --mode random \
  --histogram
```

The histogram reveals tail behaviour — a bimodal distribution (e.g. a spike at < 0.1 ms and another at 1–5 ms) indicates cache hits mixed with physical I/O.

---

## Building (Rust)

**Requirements:** Rust 1.70+ (uses `let-else`, `format!` with capture syntax).

```bash
# Development build (fast compile, no optimisations)
cargo build

# Release build (LTO, strip — recommended for actual benchmarks)
cargo build --release

# Run directly without a separate build step
cargo run --release -- --file-size 512M --block-size 64K
```

The binary is placed at `target/release/bench_io` (or `target/debug/bench_io` for dev builds).

**Dependencies** (`Cargo.toml`):

| Crate | Version | Platforms | Purpose |
|---|---|---|---|
| `clap` | 4.x | all | Argument parsing with derive macros |
| `rand` | 0.8 | all | Thread-local RNG for random offsets and buffer fill |
| `serde_json` | 1.x | all | JSON output via `json!` macro |
| `libc` | 0.2 | all | `O_DIRECT`, `F_NOCACHE`, `statvfs` platform calls |
| `io-uring` | 0.6 | Linux only | `io_uring` async I/O backend (`--engine io-uring`) |

---

## Design notes

**Why one file per thread?** Sharing a single file across threads would serialize `seek + read/write` pairs through the OS inode lock on most filesystems, destroying parallelism. Separate files let each thread own an independent I/O stream and let the storage controller see truly concurrent requests.

**Why pre-fill files with non-zero data?** Most filesystems (ext4 with `extent` allocation, APFS, XFS) will delay actual block allocation until the first write (sparse files / copy-on-write). Pre-filling forces real extent allocation so write benchmarks measure actual I/O rather than metadata operations.

**Why discard warm-up results?** The first few operations after opening a file often hit OS scheduler jitter, TLB cold starts, and disk rotational latency (on HDDs). The warm-up phase absorbs these outliers so they don't skew p99/max latency.

**Why wall time for throughput but per-op timers for latency?** Wall time correctly accounts for the parallelism of multi-thread runs — if 4 threads each transfer 1 GB in 2 seconds, the system throughput is 2 GB/s, not 500 MB/s. Per-op latency is measured inside the hot loop so it isn't inflated by thread startup or join overhead.

**Why `O_DIRECT` alignment matters on Linux?** The kernel requires the file offset, buffer address, and transfer length to each be a multiple of the logical block size (512 B minimum, 4 KiB for NVMe). The tool allocates buffers with a 4 KiB-aligned start offset inside an over-sized `Vec` — no `unsafe` allocator required.

**Why a time-series reporter thread?** Draining per-interval metrics from a background thread, rather than trying to checkpoint them in the hot I/O loop, keeps the I/O path free of extra synchronisation. The reporter sleeps for the interval duration, then atomically swaps the interval counters (bytes/ops) to zero and takes the latency vec with `mem::take`. Workers never block on the reporter.

**Why thread-simulation for queue depth?** The OS can coalesce independent file I/O from multiple threads into parallel NCQ commands the same way true async I/O does. For NVMe the practical IOPS difference between thread-simulation and `io_uring` is small at depth > 8. `io_uring` has the edge at very high depths (64+) and very low latencies (< 20 µs) because it eliminates per-syscall overhead entirely.

**Why does `--read-pct` produce a single phase instead of two?** Separate write and read phases let caches warm between them, which inflates read numbers and hides write-read interference. An interleaved phase with `--read-pct` forces both request types to share the queue simultaneously, which is what actually happens under database load.

---

## Platform notes

| Feature | Linux | macOS | Windows |
|---|---|---|---|
| `--unbuffered` | `O_DIRECT` (requires 512 B-aligned blocks) | `F_NOCACHE` via `fcntl` (no alignment needed) | Not implemented (ignored) |
| `--engine io-uring` | ✅ Linux 5.1+ | ❌ (rejected at startup) | ❌ (rejected at startup) |
| Disk space check | `statvfs(2)` | `statvfs(2)` | Skipped |
| `--fsync` | `fsync(2)` | `fsync(2)` | `FlushFileBuffers` (via std) |

On macOS, `O_DIRECT` does not exist. `F_NOCACHE` disables the unified buffer cache for a single file descriptor without the alignment requirements of `O_DIRECT`, making it easier to use but with slightly different semantics (data may still be in device-level caches).

The `io-uring` crate is compiled only on Linux (`[target.'cfg(target_os = "linux")'.dependencies]`), so the binary on macOS/Windows has no dependency on it and `src/uring.rs` is excluded from compilation entirely.
