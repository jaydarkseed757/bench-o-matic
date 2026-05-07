//! bench-o-matic: Disk I/O Performance Benchmarking Tool (Rust port)
//!
//! Measures sequential and random read/write performance with configurable
//! parameters, multi-threaded workloads, and full latency statistics.

mod cli;
mod metrics;
mod output;
mod pattern;
mod report;
mod worker;

#[cfg(target_os = "linux")]
mod uring;

use std::{
    io::Write as _,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Instant,
};

use cli::{Access, Config, Mode, Op, Workload};
use metrics::{IntervalSnapshot, Metrics, PhaseResult};
use output::{human_size, print_banner, print_histogram, print_result, print_time_series};
use worker::{preallocate, run_worker, run_worker_ops, Limit, WorkerParams};

fn main() {
    let cfg = cli::parse();
    validate(&cfg);

    std::fs::create_dir_all(&cfg.dir).unwrap_or_else(|e| {
        eprintln!("[ERROR] Cannot create {:?}: {}", cfg.dir, e);
        std::process::exit(1);
    });

    // Feature 5: generate num_files test files.
    let files: Vec<PathBuf> = (0..cfg.num_files)
        .map(|i| cfg.dir.join(format!("bench_t{i:02}.dat")))
        .collect();

    // ── Config summary ────────────────────────────────────────────────────────

    print_banner("bench-o-matic  ·  Disk I/O Benchmark");
    let total = cfg.file_size * cfg.num_files as u64;
    println!("  Directory    : {}", cfg.dir.display());
    println!(
        "  File size    : {} × {} file(s) = {} total",
        human_size(cfg.file_size),
        cfg.num_files,
        human_size(total)
    );
    println!("  Block size   : {}", human_size(cfg.block_size as u64));

    if let Some(dur) = cfg.duration {
        println!(
            "  Duration     : {:.0}s  (warm-up: {} ops)",
            dur.as_secs_f64(),
            cfg.warmup
        );
    } else {
        println!(
            "  Operations   : {} per thread  (warm-up: {})",
            cfg.num_ops, cfg.warmup
        );
    }

    println!(
        "  Workload     : {}  |  Mode: {}",
        cfg.workload.as_str(),
        cfg.mode.as_str()
    );
    println!(
        "  fsync        : {}  |  Unbuffered: {}",
        cfg.fsync, cfg.unbuffered
    );
    println!(
        "  Threads      : {}  |  Queue depth: {}  |  Engine: {:?}",
        cfg.threads, cfg.queue_depth, cfg.engine
    );
    if let Some(pct) = cfg.read_pct {
        println!("  Read pct     : {pct}% (mixed mode)");
    }
    if let Some(iv) = cfg.interval {
        println!("  Interval     : {:.0}s", iv.as_secs_f64());
    }
    if let Some(pat) = cfg.pattern {
        println!("  Pattern      : {pat:?}  (overrides workload/mode)");
    }

    // ── Preallocate ───────────────────────────────────────────────────────────

    print_banner("Preallocating test files");
    for path in &files {
        let name = path.file_name().unwrap().to_string_lossy();
        print!("  {name}  ({}) ... ", human_size(cfg.file_size));
        std::io::stdout().flush().ok();
        if let Err(e) = preallocate(path, cfg.file_size) {
            eprintln!("FAILED\n[ERROR] {e}");
            std::process::exit(1);
        }
        println!("OK");
    }

    // ── Pattern dispatch ──────────────────────────────────────────────────────

    if let Some(pat) = cfg.pattern {
        let results = pattern::run_pattern(&cfg, pat);
        print_banner("Results");
        for (name, result) in &results {
            print_result(name, result);
            if cfg.histogram {
                print_histogram(&result.raw_latencies);
            }
        }
        if cfg.json {
            print_banner("JSON");
            let json_results: serde_json::Map<String, serde_json::Value> = results
                .iter()
                .map(|(k, v)| (k.clone(), v.to_json_value()))
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "pattern": format!("{pat:?}"),
                    "results": json_results,
                }))
                .unwrap()
            );
        }
        if cfg.no_cleanup {
            println!("\n  Pattern files retained in {}", cfg.dir.display());
        } else {
            print_banner("Cleanup");
            pattern::cleanup_pattern_files(&cfg, pat);
        }
        if cfg.report {
            if let Err(e) = report::generate(&cfg, &results) {
                eprintln!("[WARN] Could not write report: {e}");
            }
        }
        println!();
        return;
    }

    // ── Determine phases ──────────────────────────────────────────────────────

    let accesses: Vec<Access> = match cfg.mode {
        Mode::Both => vec![Access::Sequential, Access::Random],
        Mode::Sequential => vec![Access::Sequential],
        Mode::Random => vec![Access::Random],
    };

    // Feature 2: if mixed workload with read_pct, emit ONE Mixed phase.
    // Otherwise fall back to old behavior (separate write + read).
    let phases: Vec<(Op, String)> = match cfg.workload {
        Workload::Mixed => {
            if let Some(pct) = cfg.read_pct {
                vec![(Op::Mixed(pct), format!("Mixed ({pct}R/{}W)", 100 - pct))]
            } else {
                vec![
                    (Op::Write, "Write".to_string()),
                    (Op::Read, "Read".to_string()),
                ]
            }
        }
        Workload::Write => vec![(Op::Write, "Write".to_string())],
        Workload::Read => vec![(Op::Read, "Read".to_string())],
    };

    // ── Run phases ────────────────────────────────────────────────────────────

    let mut results: Vec<(String, PhaseResult, Vec<IntervalSnapshot>)> = Vec::new();

    for &access in &accesses {
        for (op, op_label) in &phases {
            let key = match op {
                Op::Mixed(_) => format!("{}_{}", access.as_str(), "mixed"),
                _ => format!("{}_{}", access.as_str(), op.as_str()),
            };
            let label = format!("{} {op_label}", access.display());

            print_banner(&format!("Running: {label}"));
            print!("  Working ... ");
            std::io::stdout().flush().ok();

            let (result, snapshots) = run_phase(&cfg, access, *op, &files);
            println!("done.");
            results.push((key, result, snapshots));
        }
    }

    // ── Print results ─────────────────────────────────────────────────────────

    print_banner("Results");
    for (key, result, snapshots) in &results {
        let label = title_case(&key.replace('_', " "));
        print_result(&label, result);
        if !snapshots.is_empty() {
            print_time_series(snapshots);
        }
        if cfg.histogram {
            print_histogram(&result.raw_latencies);
        }
    }

    // ── JSON output ───────────────────────────────────────────────────────────

    if cfg.json {
        print_banner("JSON");
        let json_results: serde_json::Map<String, serde_json::Value> = results
            .iter()
            .map(|(k, v, _)| (k.clone(), v.to_json_value()))
            .collect();

        let payload = serde_json::json!({
            "config": {
                "dir":              cfg.dir.display().to_string(),
                "file_size_bytes":  cfg.file_size,
                "block_size_bytes": cfg.block_size,
                "num_ops":          cfg.num_ops,
                "threads":          cfg.threads,
                "num_files":        cfg.num_files,
                "workload":         cfg.workload.as_str(),
                "mode":             cfg.mode.as_str(),
                "fsync":            cfg.fsync,
                "unbuffered":       cfg.unbuffered,
                "warmup_ops":       cfg.warmup,
                "queue_depth":      cfg.queue_depth,
            },
            "results": json_results,
        });

        println!("{}", serde_json::to_string_pretty(&payload).unwrap());
    }

    // ── HTML report ───────────────────────────────────────────────────────────

    if cfg.report {
        let report_results: Vec<(String, PhaseResult)> = results
            .into_iter()
            .map(|(k, r, _)| (title_case(&k.replace('_', " ")), r))
            .collect();
        if let Err(e) = report::generate(&cfg, &report_results) {
            eprintln!("[WARN] Could not write report: {e}");
        }
        // Re-bind results without the moved values — cleanup uses the paths, not results.
        // (nothing to rebind; results were consumed above, cleanup only needs `files`)
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    if cfg.no_cleanup {
        println!("\n  Test files retained in {}", cfg.dir.display());
    } else {
        print_banner("Cleanup");
        for path in &files {
            let name = path.file_name().unwrap().to_string_lossy();
            match std::fs::remove_file(path) {
                Ok(()) => println!("  Removed {name}"),
                Err(e) => eprintln!("  [WARN] Could not remove {name}: {e}"),
            }
        }
    }

    println!();
}

// ── Phase runner ──────────────────────────────────────────────────────────────

fn run_phase(
    cfg: &Config,
    access: Access,
    op: Op,
    files: &[PathBuf],
) -> (PhaseResult, Vec<IntervalSnapshot>) {
    let metrics = Arc::new(Metrics::new());

    // Feature 1: build Limit based on --duration and/or --num-ops.
    let measured_limit = match cfg.duration {
        Some(dur) => {
            let deadline = Instant::now() + dur;
            if cfg.num_ops > 0 {
                // Both set: will be checked inside workers (whichever fires first).
                // We use Until here; workers also check num_ops through the shared counter.
                Limit::Until(deadline)
            } else {
                Limit::Until(deadline)
            }
        }
        None => Limit::Ops((cfg.num_ops / cfg.threads as u64).max(1)),
    };

    // Warmup always uses Ops limit.
    let _warmup_limit = Limit::Ops(cfg.warmup);

    let params = Arc::new(WorkerParams {
        access,
        op,
        limit: measured_limit,
        block_size: cfg.block_size,
        file_size: cfg.file_size,
        use_fsync: cfg.fsync,
        unbuffered: cfg.unbuffered,
        queue_depth: cfg.queue_depth,
        engine: cfg.engine,
    });

    let t_start = Instant::now();

    // Feature 3: optional interval reporter thread.
    let stop_flag = Arc::new(AtomicBool::new(false));
    let snapshots_arc: Arc<std::sync::Mutex<Vec<IntervalSnapshot>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let reporter_handle: Option<thread::JoinHandle<()>> = cfg.interval.map(|interval| {
        let metrics_clone = Arc::clone(&metrics);
        let stop = Arc::clone(&stop_flag);
        let snaps = Arc::clone(&snapshots_arc);
        let phase_start = t_start;

        thread::spawn(move || {
            let interval_s = interval.as_secs_f64();
            loop {
                thread::sleep(interval);
                let elapsed_s = phase_start.elapsed().as_secs_f64();
                let snap = metrics_clone.drain_interval(elapsed_s, interval_s);
                // Live line: "  [ 10s]  278.3 MB/s  71204 iops  p99: 0.312 ms"
                println!(
                    "  [{:4.0}s]  {:7.1} MB/s  {:6.0} iops  p99: {:.3} ms",
                    elapsed_s,
                    snap.throughput_mb_s,
                    snap.iops,
                    snap.latency_ms.p99,
                );
                snaps.lock().unwrap().push(snap);

                if stop.load(Ordering::Relaxed) {
                    // One final drain for the last partial interval.
                    let elapsed_s2 = phase_start.elapsed().as_secs_f64();
                    let actual_interval = elapsed_s2 - elapsed_s;
                    if actual_interval > 0.001 {
                        let snap2 = metrics_clone.drain_interval(elapsed_s2, actual_interval);
                        if snap2.ops > 0 {
                            println!(
                                "  [{:4.0}s]  {:7.1} MB/s  {:6.0} iops  p99: {:.3} ms  (partial)",
                                elapsed_s2,
                                snap2.throughput_mb_s,
                                snap2.iops,
                                snap2.latency_ms.p99,
                            );
                            snaps.lock().unwrap().push(snap2);
                        }
                    }
                    break;
                }
            }
        })
    });

    // Spawn worker threads.
    let handles: Vec<_> = (0..cfg.threads)
        .map(|i| {
            let all_files: Vec<PathBuf> = files.to_vec();
            let m = Arc::clone(&metrics);
            let p = Arc::clone(&params);
            let warmup = cfg.warmup;
            let access = access;
            let op = op;
            let file_size = cfg.file_size;
            let block_size = cfg.block_size;
            let unbuffered = cfg.unbuffered;

            thread::spawn(move || {
                // Warm-up phase: results go to a throwaway collector.
                if warmup > 0 {
                    let discard = Metrics::new();
                    let warmup_files = vec![all_files[i % all_files.len()].clone()];
                    run_worker_ops(
                        &warmup_files,
                        access,
                        op,
                        warmup,
                        &discard,
                        false,
                        unbuffered,
                        file_size,
                        block_size,
                    );
                }

                // Measured phase — choose engine.
                #[cfg(target_os = "linux")]
                if p.engine == Engine::IoUring {
                    match uring::run_uring_worker(
                        &all_files,
                        p.access,
                        p.op,
                        p.queue_depth,
                        p.block_size,
                        p.limit,
                        &m,
                        p.file_size,
                    ) {
                        Ok(()) => return,
                        Err(e) => {
                            eprintln!("[WARN] io_uring failed ({e}), falling back to sync");
                        }
                    }
                }

                // Sync engine (default or fallback).
                run_worker(&all_files, &p, &m);
            })
        })
        .collect();

    for h in handles {
        if let Err(e) = h.join() {
            eprintln!("[ERROR] Worker thread panicked: {e:?}");
        }
    }

    // Signal reporter to stop and join it.
    if let Some(h) = reporter_handle {
        stop_flag.store(true, Ordering::Relaxed);
        if let Err(e) = h.join() {
            eprintln!("[ERROR] Reporter thread panicked: {e:?}");
        }
    }

    let snapshots = Arc::try_unwrap(snapshots_arc)
        .unwrap_or_else(|a| std::sync::Mutex::new(std::mem::take(&mut *a.lock().unwrap())))
        .into_inner()
        .unwrap_or_default();

    (metrics.summarise(t_start.elapsed()), snapshots)
}

// ── Validation ────────────────────────────────────────────────────────────────

fn validate(cfg: &Config) {
    // Pattern mode uses hardcoded block sizes (max 64 KB for rocksdb).
    if cfg.pattern.is_some() {
        if cfg.file_size < 65536 {
            eprintln!("[ERROR] --file-size must be at least 64K when using --pattern");
            std::process::exit(1);
        }
        return;
    }

    if cfg.block_size == 0 {
        eprintln!("[ERROR] --block-size must be > 0");
        std::process::exit(1);
    }
    if cfg.block_size as u64 > cfg.file_size {
        eprintln!(
            "[ERROR] --block-size ({}) must be ≤ --file-size ({})",
            human_size(cfg.block_size as u64),
            human_size(cfg.file_size)
        );
        std::process::exit(1);
    }
    if cfg.duration.is_none() && cfg.num_ops == 0 {
        eprintln!("[ERROR] --num-ops must be ≥ 1 when --duration is not set");
        std::process::exit(1);
    }

    // Refuse to consume more than 80% of available disk space.
    #[cfg(unix)]
    if let Some(free) = free_bytes(&cfg.dir) {
        let needed = cfg.file_size * cfg.num_files as u64;
        if needed > (free as f64 * 0.80) as u64 {
            eprintln!(
                "[ERROR] Test requires {} but only {} free (80% safety limit).",
                human_size(needed),
                human_size(free)
            );
            std::process::exit(1);
        }
    }
}

#[cfg(unix)]
fn free_bytes(path: &std::path::Path) -> Option<u64> {
    let check = if path.exists() {
        path
    } else {
        path.parent()?
    };

    let c_path = std::ffi::CString::new(check.to_str()?).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut st) };
    if ret == 0 {
        Some(st.f_bavail as u64 * st.f_frsize as u64)
    } else {
        None
    }
}

// ── Utilities ─────────────────────────────────────────────────────────────────

fn title_case(s: &str) -> String {
    s.split_whitespace()
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
