//! Human-readable output formatting.

use crate::metrics::{IntervalSnapshot, PhaseResult};

const BAR: &str = "────────────────────────────────────────────────────────────────";
const HISTOGRAM_BINS: usize = 16;
const BAR_WIDTH: usize = 36;

pub fn print_banner(text: &str) {
    println!("\n{BAR}");
    println!("  {text}");
    println!("{BAR}");
}

pub fn print_result(label: &str, r: &PhaseResult) {
    let lat = &r.latency_ms;
    println!("\n  ▶  {label}");
    println!("     Operations  : {:>12}", r.operations);
    println!(
        "     Transferred : {:>10.2} MB  ({})",
        r.total_mb,
        human_size(r.total_bytes)
    );
    println!("     Wall time   : {:>10.3} s", r.wall_time_s);
    println!("     Throughput  : {:>10.2} MB/s", r.throughput_mb_s);
    println!("     IOPS        : {:>12.1}", r.iops);
    println!("     Latency avg : {:>10.3} ms", lat.avg);
    println!("     Latency min : {:>10.3} ms", lat.min);
    println!("     Latency max : {:>10.3} ms", lat.max);
    println!(
        "     p50/p95/p99 : {:.3} ms / {:.3} ms / {:.3} ms",
        lat.p50, lat.p95, lat.p99
    );
    if r.errors > 0 {
        println!("     !! Errors   : {}", r.errors);
    }
}

/// Print a plain aligned table of interval snapshots.
pub fn print_time_series(snapshots: &[IntervalSnapshot]) {
    if snapshots.is_empty() {
        return;
    }
    println!();
    println!("     Time-series intervals:");
    println!("     {:>6}  {:>12}  {:>10}  {:>14}", "Elapsed", "Throughput", "IOPS", "p99 lat");
    for snap in snapshots {
        println!(
            "     {:>5.0}s  {:>9.1} MB/s  {:>10.0}  {:>11.3} ms",
            snap.elapsed_s,
            snap.throughput_mb_s,
            snap.iops,
            snap.latency_ms.p99,
        );
    }
}

/// Print an ASCII-art latency histogram using `HISTOGRAM_BINS` buckets.
pub fn print_histogram(lats_ms: &[f64]) {
    if lats_ms.is_empty() {
        return;
    }

    let lo = lats_ms.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = lats_ms.iter().copied().fold(f64::NEG_INFINITY, f64::max);

    if (hi - lo).abs() < f64::EPSILON {
        println!("     [all latencies identical: {lo:.3} ms]");
        return;
    }

    let width = (hi - lo) / HISTOGRAM_BINS as f64;
    let mut counts = vec![0usize; HISTOGRAM_BINS];
    for &v in lats_ms {
        let idx = ((v - lo) / width) as usize;
        counts[idx.min(HISTOGRAM_BINS - 1)] += 1;
    }

    let max_count = *counts.iter().max().unwrap_or(&1);

    println!();
    println!("     Latency histogram (ms → count)");
    for (i, &cnt) in counts.iter().enumerate() {
        let lo_b = lo + i as f64 * width;
        let hi_b = lo_b + width;
        let bar_len = if max_count > 0 {
            BAR_WIDTH * cnt / max_count
        } else {
            0
        };
        let bar = "█".repeat(bar_len);
        println!("     {lo_b:8.3}–{hi_b:8.3} | {bar:<BAR_WIDTH$} {cnt}");
    }
}

/// Format bytes as a human-readable string (1024-based units).
pub fn human_size(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    for &unit in UNITS {
        if v < 1024.0 {
            return format!("{v:.1} {unit}");
        }
        v /= 1024.0;
    }
    format!("{v:.1} PB")
}
