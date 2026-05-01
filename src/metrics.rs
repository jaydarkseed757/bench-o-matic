//! Thread-safe metrics collector and result computation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

// ── Collector ─────────────────────────────────────────────────────────────────

/// Accumulates per-operation timings and byte counts across threads.
///
/// `Mutex` guards the latency vec; atomics handle the counters lock-free.
pub struct Metrics {
    latencies_ms: Mutex<Vec<f64>>,
    bytes: AtomicU64,
    errors: AtomicU64,
    // Per-interval counters (Feature 3).
    pub interval_ops: AtomicU64,
    pub interval_bytes: AtomicU64,
    pub interval_lats: Mutex<Vec<f64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            latencies_ms: Mutex::new(Vec::new()),
            bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            interval_ops: AtomicU64::new(0),
            interval_bytes: AtomicU64::new(0),
            interval_lats: Mutex::new(Vec::new()),
        }
    }

    #[inline]
    pub fn record(&self, elapsed: Duration, nbytes: usize) {
        let ms = elapsed.as_secs_f64() * 1_000.0;
        self.latencies_ms.lock().unwrap().push(ms);
        self.bytes.fetch_add(nbytes as u64, Ordering::Relaxed);
        // Interval counters.
        self.interval_ops.fetch_add(1, Ordering::Relaxed);
        self.interval_bytes.fetch_add(nbytes as u64, Ordering::Relaxed);
        self.interval_lats.lock().unwrap().push(ms);
    }

    #[inline]
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically drain the interval counters and return a snapshot.
    ///
    /// `elapsed_s` is the total elapsed time since the phase started.
    /// `interval_s` is the length of this interval window.
    pub fn drain_interval(&self, elapsed_s: f64, interval_s: f64) -> IntervalSnapshot {
        let ops = self.interval_ops.swap(0, Ordering::Relaxed);
        let bytes = self.interval_bytes.swap(0, Ordering::Relaxed);
        let mut lats = {
            let mut guard = self.interval_lats.lock().unwrap();
            std::mem::take(&mut *guard)
        };

        lats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let latency_ms = if lats.is_empty() {
            LatencyStats::default()
        } else {
            let n = lats.len();
            let avg = lats.iter().sum::<f64>() / n as f64;
            let stddev = if n > 1 {
                let var = lats.iter().map(|x| (x - avg).powi(2)).sum::<f64>() / (n - 1) as f64;
                var.sqrt()
            } else {
                0.0
            };
            LatencyStats {
                avg: r4(avg),
                min: r4(lats[0]),
                max: r4(lats[n - 1]),
                p50: r4(percentile(&lats, 50.0)),
                p95: r4(percentile(&lats, 95.0)),
                p99: r4(percentile(&lats, 99.0)),
                stddev: r4(stddev),
            }
        };

        let mb = bytes as f64 / (1024.0 * 1024.0);
        let throughput_mb_s = if interval_s > 0.0 { mb / interval_s } else { 0.0 };
        let iops = if interval_s > 0.0 { ops as f64 / interval_s } else { 0.0 };

        IntervalSnapshot {
            elapsed_s,
            ops,
            bytes,
            duration_s: interval_s,
            throughput_mb_s: r3(throughput_mb_s),
            iops: r1(iops),
            latency_ms,
        }
    }

    /// Compute the final summary given the measured wall-clock time.
    pub fn summarise(&self, wall: Duration) -> PhaseResult {
        let mut lats = self.latencies_ms.lock().unwrap().clone();
        let n = lats.len();
        let bytes = self.bytes.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);

        if n == 0 {
            return PhaseResult {
                operations: 0,
                errors,
                total_bytes: 0,
                total_mb: 0.0,
                wall_time_s: wall.as_secs_f64(),
                throughput_mb_s: 0.0,
                iops: 0.0,
                latency_ms: LatencyStats::default(),
                raw_latencies: Vec::new(),
            };
        }

        lats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let wall_s = wall.as_secs_f64();
        let mb = bytes as f64 / (1024.0 * 1024.0);

        let avg = lats.iter().sum::<f64>() / n as f64;
        let stddev = if n > 1 {
            let var = lats.iter().map(|x| (x - avg).powi(2)).sum::<f64>() / (n - 1) as f64;
            var.sqrt()
        } else {
            0.0
        };

        PhaseResult {
            operations: n as u64,
            errors,
            total_bytes: bytes,
            total_mb: r3(mb),
            wall_time_s: r4(wall_s),
            throughput_mb_s: r3(if wall_s > 0.0 { mb / wall_s } else { 0.0 }),
            iops: r1(if wall_s > 0.0 { n as f64 / wall_s } else { 0.0 }),
            latency_ms: LatencyStats {
                avg: r4(avg),
                min: r4(lats[0]),
                max: r4(lats[n - 1]),
                p50: r4(percentile(&lats, 50.0)),
                p95: r4(percentile(&lats, 95.0)),
                p99: r4(percentile(&lats, 99.0)),
                stddev: r4(stddev),
            },
            raw_latencies: lats,
        }
    }
}

// ── Result types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct LatencyStats {
    pub avg: f64,
    pub min: f64,
    pub max: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub stddev: f64,
}

pub struct PhaseResult {
    pub operations: u64,
    pub errors: u64,
    pub total_bytes: u64,
    pub total_mb: f64,
    pub wall_time_s: f64,
    pub throughput_mb_s: f64,
    pub iops: f64,
    pub latency_ms: LatencyStats,
    /// Raw sorted latencies retained for histogram rendering; excluded from JSON.
    pub raw_latencies: Vec<f64>,
}

impl PhaseResult {
    /// Return a `serde_json::Value` without the raw latency list.
    pub fn to_json_value(&self) -> serde_json::Value {
        let lat = &self.latency_ms;
        serde_json::json!({
            "operations":      self.operations,
            "errors":          self.errors,
            "total_bytes":     self.total_bytes,
            "total_mb":        self.total_mb,
            "wall_time_s":     self.wall_time_s,
            "throughput_mb_s": self.throughput_mb_s,
            "iops":            self.iops,
            "latency_ms": {
                "avg":    lat.avg,
                "min":    lat.min,
                "max":    lat.max,
                "p50":    lat.p50,
                "p95":    lat.p95,
                "p99":    lat.p99,
                "stddev": lat.stddev,
            }
        })
    }
}

/// A single interval snapshot for time-series reporting (Feature 3).
#[allow(dead_code)]
pub struct IntervalSnapshot {
    pub elapsed_s: f64,
    pub ops: u64,
    pub bytes: u64,
    pub duration_s: f64,
    pub throughput_mb_s: f64,
    pub iops: f64,
    pub latency_ms: LatencyStats,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let k = (p / 100.0) * (sorted.len() - 1) as f64;
    let lo = k as usize;
    let hi = (lo + 1).min(sorted.len() - 1);
    sorted[lo] + (sorted[hi] - sorted[lo]) * (k - lo as f64)
}

#[inline]
fn r1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
}
#[inline]
fn r3(x: f64) -> f64 {
    (x * 1_000.0).round() / 1_000.0
}
#[inline]
fn r4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}
