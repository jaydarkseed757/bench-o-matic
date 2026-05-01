#!/usr/bin/env python3
"""
bench-o-matic: Disk I/O Performance Benchmarking Tool

Measures sequential and random read/write performance with configurable
parameters, multi-threaded workloads, and detailed latency statistics.

Design notes:
- One file per thread avoids lock contention on the filesystem level.
- Files are preallocated with non-zero data to defeat sparse-file / zero-page
  optimizations that would otherwise make writes artificially fast.
- time.perf_counter() is used for sub-microsecond timing resolution.
- fsync() is optional; omitting it measures OS-buffered throughput.
"""

import argparse
import json
import os
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Size helpers
# ---------------------------------------------------------------------------

_SIZE_UNITS: Dict[str, int] = {
    "B": 1,
    "K": 1024, "KB": 1024, "KIB": 1024,
    "M": 1024 ** 2, "MB": 1024 ** 2, "MIB": 1024 ** 2,
    "G": 1024 ** 3, "GB": 1024 ** 3, "GIB": 1024 ** 3,
}


def parse_size(s: str) -> int:
    """Convert a human-readable size string to bytes.

    Accepts: '4K', '64KB', '1M', '256MB', '1G', '2GB', or raw integer string.
    """
    s = s.strip().upper()
    for suffix in sorted(_SIZE_UNITS, key=len, reverse=True):
        if s.endswith(suffix):
            numeric = s[: -len(suffix)].strip()
            return int(float(numeric) * _SIZE_UNITS[suffix])
    return int(s)  # bare integer → bytes


def human_size(n: int) -> str:
    """Format bytes as a human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


# ---------------------------------------------------------------------------
# Thread-safe metrics collector
# ---------------------------------------------------------------------------

@dataclass
class Metrics:
    """Accumulates per-operation latencies and byte counts across threads."""

    _latencies: List[float] = field(default_factory=list, repr=False)
    _bytes: int = 0
    _errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def record(self, latency_s: float, nbytes: int) -> None:
        with self._lock:
            self._latencies.append(latency_s * 1000.0)  # keep in milliseconds
            self._bytes += nbytes

    def record_error(self) -> None:
        with self._lock:
            self._errors += 1

    def summarise(self, wall_time_s: float) -> dict:
        """Return a result dict; includes raw latencies for optional histogram."""
        lats = sorted(self._latencies)
        n = len(lats)
        if n == 0:
            return {"operations": 0, "errors": self._errors}
        mb = self._bytes / (1024 ** 2)
        return {
            "operations": n,
            "errors": self._errors,
            "total_bytes": self._bytes,
            "total_mb": round(mb, 3),
            "wall_time_s": round(wall_time_s, 4),
            "throughput_mb_s": round(mb / wall_time_s, 3) if wall_time_s > 0 else 0,
            "iops": round(n / wall_time_s, 1) if wall_time_s > 0 else 0,
            "latency_ms": {
                "avg": round(statistics.mean(lats), 4),
                "min": round(lats[0], 4),
                "max": round(lats[-1], 4),
                "p50": round(_pct(lats, 50), 4),
                "p95": round(_pct(lats, 95), 4),
                "p99": round(_pct(lats, 99), 4),
                "stddev": round(statistics.stdev(lats), 4) if n > 1 else 0.0,
            },
            "_latencies": lats,  # retained for histogram; stripped from JSON output
        }


def _pct(sorted_data: List[float], p: float) -> float:
    """Linear-interpolation percentile on a pre-sorted list."""
    if not sorted_data:
        return 0.0
    k = (p / 100.0) * (len(sorted_data) - 1)
    lo, hi = int(k), min(int(k) + 1, len(sorted_data) - 1)
    return sorted_data[lo] + (sorted_data[hi] - sorted_data[lo]) * (k - lo)


# ---------------------------------------------------------------------------
# File preallocator
# ---------------------------------------------------------------------------

# 1 MiB random buffer reused for preallocating — urandom() is expensive to
# call for every block; repeating one random buffer is sufficient to prevent
# sparse-file / copy-on-write shortcuts without being cryptographically strong.
_PREALLOC_BUF = os.urandom(1024 * 1024)


def preallocate(path: Path, size: int, block: int = 1024 * 1024) -> None:
    """Write `size` bytes of non-zero data so the OS allocates real extents."""
    remaining = size
    with open(path, "wb") as f:
        while remaining > 0:
            chunk = min(block, remaining)
            # Slice from our pre-generated random buffer (wraps if block > 1 MiB)
            f.write(_PREALLOC_BUF[:chunk])
            remaining -= chunk
        f.flush()
        os.fsync(f.fileno())


# ---------------------------------------------------------------------------
# I/O worker functions (one per thread)
# ---------------------------------------------------------------------------

def _buf(size: int) -> bytes:
    """Generate a write buffer of `size` bytes."""
    return os.urandom(min(size, 1024 * 1024)) * (max(1, size // (1024 * 1024)) + 1)


def worker_seq_write(
    path: Path,
    block_size: int,
    num_ops: int,
    metrics: Metrics,
    use_fsync: bool,
    unbuffered: bool,
) -> None:
    """Sequential write: advance through the file one block at a time, wrap at EOF."""
    buf = _buf(block_size)[:block_size]
    buf_arg = 0 if unbuffered else -1
    try:
        with open(path, "r+b", buffering=buf_arg) as f:
            for _ in range(num_ops):
                pos = f.tell()
                # Wrap-around so we stay within the preallocated file
                if pos + block_size > os.path.getsize(path):
                    f.seek(0)
                t0 = time.perf_counter()
                f.write(buf)
                if use_fsync:
                    os.fsync(f.fileno())
                metrics.record(time.perf_counter() - t0, block_size)
    except OSError as exc:
        print(f"[ERROR] seq-write {path.name}: {exc}", file=sys.stderr)
        metrics.record_error()


def worker_seq_read(
    path: Path,
    block_size: int,
    num_ops: int,
    metrics: Metrics,
    unbuffered: bool,
) -> None:
    """Sequential read: advance through the file, wrap at EOF."""
    buf_arg = 0 if unbuffered else -1
    try:
        with open(path, "rb", buffering=buf_arg) as f:
            for _ in range(num_ops):
                pos = f.tell()
                if pos + block_size > os.path.getsize(path):
                    f.seek(0)
                t0 = time.perf_counter()
                data = f.read(block_size)
                elapsed = time.perf_counter() - t0
                if data:
                    metrics.record(elapsed, len(data))
    except OSError as exc:
        print(f"[ERROR] seq-read {path.name}: {exc}", file=sys.stderr)
        metrics.record_error()


def worker_rnd_write(
    path: Path,
    file_size: int,
    block_size: int,
    num_ops: int,
    metrics: Metrics,
    use_fsync: bool,
    unbuffered: bool,
) -> None:
    """Random write: seek to a uniformly random block-aligned offset per op."""
    buf = _buf(block_size)[:block_size]
    num_blocks = max(1, file_size // block_size)
    buf_arg = 0 if unbuffered else -1
    try:
        with open(path, "r+b", buffering=buf_arg) as f:
            for _ in range(num_ops):
                offset = random.randrange(num_blocks) * block_size
                t0 = time.perf_counter()
                f.seek(offset)
                f.write(buf)
                if use_fsync:
                    os.fsync(f.fileno())
                metrics.record(time.perf_counter() - t0, block_size)
    except OSError as exc:
        print(f"[ERROR] rnd-write {path.name}: {exc}", file=sys.stderr)
        metrics.record_error()


def worker_rnd_read(
    path: Path,
    file_size: int,
    block_size: int,
    num_ops: int,
    metrics: Metrics,
    unbuffered: bool,
) -> None:
    """Random read: seek to a uniformly random block-aligned offset per op."""
    num_blocks = max(1, file_size // block_size)
    buf_arg = 0 if unbuffered else -1
    try:
        with open(path, "rb", buffering=buf_arg) as f:
            for _ in range(num_ops):
                offset = random.randrange(num_blocks) * block_size
                t0 = time.perf_counter()
                f.seek(offset)
                data = f.read(block_size)
                elapsed = time.perf_counter() - t0
                if data:
                    metrics.record(elapsed, len(data))
    except OSError as exc:
        print(f"[ERROR] rnd-read {path.name}: {exc}", file=sys.stderr)
        metrics.record_error()


# ---------------------------------------------------------------------------
# Benchmark orchestration
# ---------------------------------------------------------------------------

def _build_task(
    *,
    mode: str,
    operation: str,
    path: Path,
    file_size: int,
    block_size: int,
    num_ops: int,
    metrics: Metrics,
    use_fsync: bool,
    unbuffered: bool,
    is_warmup: bool = False,
):
    """Return a callable that executes one worker task."""
    # Warmup uses a throwaway metrics object so it doesn't pollute results.
    m = Metrics() if is_warmup else metrics

    if mode == "sequential":
        if operation == "write":
            return lambda: worker_seq_write(path, block_size, num_ops, m, use_fsync, unbuffered)
        if operation == "read":
            return lambda: worker_seq_read(path, block_size, num_ops, m, unbuffered)
        # mixed: interleave write / read ops
        def _mixed_seq():
            for i in range(num_ops):
                if i % 2 == 0:
                    worker_seq_write(path, block_size, 1, m, use_fsync, unbuffered)
                else:
                    worker_seq_read(path, block_size, 1, m, unbuffered)
        return _mixed_seq
    else:  # random
        if operation == "write":
            return lambda: worker_rnd_write(path, file_size, block_size, num_ops, m, use_fsync, unbuffered)
        if operation == "read":
            return lambda: worker_rnd_read(path, file_size, block_size, num_ops, m, unbuffered)
        def _mixed_rnd():
            for i in range(num_ops):
                if i % 2 == 0:
                    worker_rnd_write(path, file_size, block_size, 1, m, use_fsync, unbuffered)
                else:
                    worker_rnd_read(path, file_size, block_size, 1, m, unbuffered)
        return _mixed_rnd


def run_phase(
    *,
    mode: str,
    operation: str,
    files: List[Path],
    file_size: int,
    block_size: int,
    num_ops: int,
    num_threads: int,
    use_fsync: bool,
    unbuffered: bool,
    warmup_ops: int,
) -> dict:
    """Run one benchmark phase across all threads; return summary dict."""
    metrics = Metrics()
    ops_per_thread = max(1, num_ops // num_threads)

    tasks = []
    for i in range(num_threads):
        path = files[i % len(files)]
        if warmup_ops > 0:
            tasks.append((_build_task(
                mode=mode, operation=operation, path=path,
                file_size=file_size, block_size=block_size, num_ops=warmup_ops,
                metrics=metrics, use_fsync=False, unbuffered=unbuffered, is_warmup=True,
            ), _build_task(
                mode=mode, operation=operation, path=path,
                file_size=file_size, block_size=block_size, num_ops=ops_per_thread,
                metrics=metrics, use_fsync=use_fsync, unbuffered=unbuffered,
            )))
        else:
            tasks.append((None, _build_task(
                mode=mode, operation=operation, path=path,
                file_size=file_size, block_size=block_size, num_ops=ops_per_thread,
                metrics=metrics, use_fsync=use_fsync, unbuffered=unbuffered,
            )))

    def run_thread(warmup_fn, main_fn):
        if warmup_fn is not None:
            warmup_fn()
        main_fn()

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futs = [pool.submit(run_thread, w, m) for w, m in tasks]
        for fut in as_completed(futs):
            fut.result()  # re-raise any exceptions from workers
    wall = time.perf_counter() - t_start

    return metrics.summarise(wall)


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

BAR = "─" * 64


def _banner(text: str) -> None:
    print(f"\n{BAR}")
    print(f"  {text}")
    print(BAR)


def _print_result(label: str, r: dict) -> None:
    lat = r.get("latency_ms", {})
    print(f"\n  ▶  {label}")
    print(f"     Operations  : {r['operations']:>12,}")
    print(f"     Transferred : {r['total_mb']:>10.2f} MB  ({human_size(r['total_bytes'])})")
    print(f"     Wall time   : {r['wall_time_s']:>10.3f} s")
    print(f"     Throughput  : {r['throughput_mb_s']:>10.2f} MB/s")
    print(f"     IOPS        : {r['iops']:>12.1f}")
    print(f"     Latency avg : {lat.get('avg', 0):>10.3f} ms")
    print(f"     Latency min : {lat.get('min', 0):>10.3f} ms")
    print(f"     Latency max : {lat.get('max', 0):>10.3f} ms")
    print(f"     p50 / p95 / p99 : "
          f"{lat.get('p50', 0):.3f} ms / "
          f"{lat.get('p95', 0):.3f} ms / "
          f"{lat.get('p99', 0):.3f} ms")
    if r.get("errors"):
        print(f"     !! Errors   : {r['errors']}")


def _print_histogram(lats_ms: List[float], bins: int = 16) -> None:
    """Print a simple ASCII-art latency histogram."""
    if not lats_ms:
        return
    lo, hi = min(lats_ms), max(lats_ms)
    if lo == hi:
        print(f"     [all latencies identical: {lo:.3f} ms]")
        return
    width = (hi - lo) / bins
    counts = [0] * bins
    for v in lats_ms:
        idx = min(int((v - lo) / width), bins - 1)
        counts[idx] += 1
    max_count = max(counts)
    bar_max = 36
    print()
    print("     Latency histogram (ms → count)")
    for i, cnt in enumerate(counts):
        lo_b = lo + i * width
        hi_b = lo_b + width
        bar = "█" * int(bar_max * cnt / max_count) if max_count else ""
        print(f"     {lo_b:8.3f}–{hi_b:8.3f} | {bar:<{bar_max}} {cnt}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="bench-o-matic",
        description="Disk I/O benchmarking tool — measures sequential/random read/write performance.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--dir", default="/tmp/bench_io",
        help="Directory where test files are created",
    )
    p.add_argument(
        "--file-size", default="256M",
        help="Size of each per-thread test file (e.g. 256M, 1G)",
    )
    p.add_argument(
        "--block-size", default="4K",
        help="I/O block size per operation (e.g. 4K, 64K, 1M)",
    )
    p.add_argument(
        "--num-ops", type=int, default=1000,
        help="Number of I/O operations per thread",
    )
    p.add_argument(
        "--threads", type=int, default=1,
        help="Number of concurrent threads (one file per thread)",
    )
    p.add_argument(
        "--workload", choices=["write", "read", "mixed"], default="mixed",
        help="Workload type",
    )
    p.add_argument(
        "--mode", choices=["sequential", "random", "both"], default="both",
        help="Access pattern to benchmark",
    )
    p.add_argument(
        "--fsync", action="store_true",
        help="Call fsync() after every write — slower but bypasses write-back cache",
    )
    p.add_argument(
        "--unbuffered", action="store_true",
        help="Use unbuffered I/O (O_DIRECT on Linux; ignored on macOS)",
    )
    p.add_argument(
        "--warmup", type=int, default=50,
        help="Warm-up operations run before measurement begins (results discarded)",
    )
    p.add_argument(
        "--no-cleanup", action="store_true",
        help="Keep test files on disk after the run",
    )
    p.add_argument(
        "--json", action="store_true",
        help="Also emit results as JSON to stdout",
    )
    p.add_argument(
        "--histogram", action="store_true",
        help="Print an ASCII latency histogram for each phase",
    )
    return p


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    file_size = parse_size(args.file_size)
    block_size = parse_size(args.block_size)
    test_dir = Path(args.dir)
    num_threads = max(1, args.threads)

    # Validate
    if block_size <= 0:
        parser.error("--block-size must be positive")
    if block_size > file_size:
        parser.error(
            f"--block-size ({human_size(block_size)}) must be ≤ "
            f"--file-size ({human_size(file_size)})"
        )
    if args.num_ops < 1:
        parser.error("--num-ops must be ≥ 1")

    # Safety guard: refuse to fill more than 80 % of available disk space.
    total_test_bytes = file_size * num_threads
    try:
        stat = os.statvfs(test_dir.parent if not test_dir.exists() else test_dir)
        free_bytes = stat.f_bavail * stat.f_frsize
        if total_test_bytes > 0.80 * free_bytes:
            print(
                f"[ERROR] Test files would consume {human_size(total_test_bytes)} "
                f"but only {human_size(free_bytes)} is free (limit: 80 %).",
                file=sys.stderr,
            )
            sys.exit(1)
    except AttributeError:
        pass  # statvfs not available on Windows; skip the guard

    test_dir.mkdir(parents=True, exist_ok=True)
    test_files = [test_dir / f"bench_t{i:02d}.dat" for i in range(num_threads)]

    _banner("bench-o-matic  ·  Disk I/O Benchmark")
    print(f"  Directory    : {test_dir}")
    print(f"  File size    : {human_size(file_size)} × {num_threads} thread(s) "
          f"= {human_size(total_test_bytes)} total")
    print(f"  Block size   : {human_size(block_size)}")
    print(f"  Operations   : {args.num_ops:,} per thread  (warm-up: {args.warmup})")
    print(f"  Workload     : {args.workload}  |  Mode: {args.mode}")
    print(f"  fsync        : {args.fsync}  |  Unbuffered: {args.unbuffered}")

    # --- Preallocate test files ---
    _banner("Preallocating test files")
    for path in test_files:
        try:
            sys.stdout.write(f"  {path.name}  ({human_size(file_size)}) ... ")
            sys.stdout.flush()
            preallocate(path, file_size)
            print("OK")
        except OSError as exc:
            print(f"FAILED\n[ERROR] {exc}", file=sys.stderr)
            sys.exit(1)

    # --- Run benchmark phases ---
    modes_to_run = ["sequential", "random"] if args.mode == "both" else [args.mode]
    # For "mixed" workload we run write + read separately (gives cleaner numbers
    # than alternating ops in a single phase) unless the user wants to see the
    # interleaved behaviour — the mixed worker inside run_phase handles that.
    if args.workload == "mixed":
        ops_list = [("write", "Write"), ("read", "Read")]
    else:
        ops_list = [(args.workload, args.workload.capitalize())]

    all_results: Dict[str, dict] = {}

    for mode in modes_to_run:
        for op, op_label in ops_list:
            phase_key = f"{mode}_{op}"
            label = f"{mode.capitalize()} {op_label}"
            _banner(f"Running: {label}")
            sys.stdout.write("  Working ... ")
            sys.stdout.flush()
            result = run_phase(
                mode=mode,
                operation=op,
                files=test_files,
                file_size=file_size,
                block_size=block_size,
                num_ops=args.num_ops,
                num_threads=num_threads,
                use_fsync=args.fsync,
                unbuffered=args.unbuffered,
                warmup_ops=args.warmup,
            )
            print("done.")
            all_results[phase_key] = result

    # --- Print results ---
    _banner("Results")
    for key, result in all_results.items():
        label = key.replace("_", " ").title()
        _print_result(label, result)
        if args.histogram:
            _print_histogram(result.get("_latencies", []))

    # --- JSON output ---
    if args.json:
        _banner("JSON")
        # Strip internal _latencies list; keep everything else.
        clean = {k: {ik: iv for ik, iv in v.items() if ik != "_latencies"}
                 for k, v in all_results.items()}
        payload = {
            "config": {
                "dir": str(test_dir),
                "file_size_bytes": file_size,
                "block_size_bytes": block_size,
                "num_ops": args.num_ops,
                "threads": num_threads,
                "workload": args.workload,
                "mode": args.mode,
                "fsync": args.fsync,
                "unbuffered": args.unbuffered,
                "warmup_ops": args.warmup,
            },
            "results": clean,
        }
        print(json.dumps(payload, indent=2))

    # --- Cleanup ---
    if args.no_cleanup:
        print(f"\n  Test files retained in {test_dir}")
    else:
        _banner("Cleanup")
        for path in test_files:
            try:
                path.unlink(missing_ok=True)
                print(f"  Removed {path.name}")
            except OSError as exc:
                print(f"  [WARN] Could not remove {path.name}: {exc}", file=sys.stderr)

    print()


if __name__ == "__main__":
    main()
