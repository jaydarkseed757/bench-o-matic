//! CLI argument parsing and configuration.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};

// ── Enums exposed to the rest of the crate ───────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Sequential,
    Random,
}

impl Access {
    pub fn as_str(self) -> &'static str {
        match self {
            Access::Sequential => "sequential",
            Access::Random => "random",
        }
    }

    pub fn display(self) -> &'static str {
        match self {
            Access::Sequential => "Sequential",
            Access::Random => "Random",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Write,
    Read,
    /// Mixed I/O: the u8 is the read percentage (0–100).
    Mixed(u8),
}

impl Op {
    pub fn as_str(self) -> &'static str {
        match self {
            Op::Write => "write",
            Op::Read => "read",
            Op::Mixed(_) => "mixed",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum Workload {
    Write,
    Read,
    Mixed,
}

impl Workload {
    pub fn as_str(self) -> &'static str {
        match self {
            Workload::Write => "write",
            Workload::Read => "read",
            Workload::Mixed => "mixed",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum Mode {
    Sequential,
    Random,
    Both,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Sequential => "sequential",
            Mode::Random => "random",
            Mode::Both => "both",
        }
    }
}

/// I/O engine selection.
#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum Engine {
    Sync,
    IoUring,
}

// ── Parsed, validated config ──────────────────────────────────────────────────

pub struct Config {
    pub dir: PathBuf,
    pub file_size: u64,
    pub block_size: usize,
    pub num_ops: u64,
    pub threads: usize,
    pub workload: Workload,
    pub mode: Mode,
    pub fsync: bool,
    pub unbuffered: bool,
    pub warmup: u64,
    pub no_cleanup: bool,
    pub json: bool,
    pub histogram: bool,
    /// Optional time-bounded execution limit (Feature 1).
    pub duration: Option<Duration>,
    /// Optional read percentage for mixed workload (Feature 2).
    pub read_pct: Option<u8>,
    /// Optional per-interval reporting duration (Feature 3).
    pub interval: Option<Duration>,
    /// Queue depth for async I/O (Feature 4).
    pub queue_depth: usize,
    /// I/O engine (Feature 4).
    pub engine: Engine,
    /// Number of test files (Feature 5).
    pub num_files: usize,
}

// ── Raw clap args ─────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "bench-o-matic",
    about = "Disk I/O benchmarking tool — sequential/random read/write performance"
)]
struct Args {
    /// Directory where test files are created
    #[arg(long, default_value = "/tmp/bench_io")]
    dir: PathBuf,

    /// Size of each per-thread test file (e.g. 256M, 1G)
    #[arg(long, default_value = "256M")]
    file_size: String,

    /// I/O block size per operation (e.g. 4K, 64K, 1M)
    #[arg(long, default_value = "4K")]
    block_size: String,

    /// Number of I/O operations per thread
    #[arg(long, default_value_t = 1000)]
    num_ops: u64,

    /// Number of concurrent threads (one file per thread)
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Workload type
    #[arg(long, value_enum, default_value_t = Workload::Mixed)]
    workload: Workload,

    /// Access pattern
    #[arg(long, value_enum, default_value_t = Mode::Both)]
    mode: Mode,

    /// fsync after every write — measures durable write throughput
    #[arg(long)]
    fsync: bool,

    /// Bypass OS page cache (O_DIRECT on Linux, F_NOCACHE on macOS)
    #[arg(long)]
    unbuffered: bool,

    /// Warm-up operations before measurement (results discarded)
    #[arg(long, default_value_t = 50)]
    warmup: u64,

    /// Keep test files after the run
    #[arg(long)]
    no_cleanup: bool,

    /// Emit results as JSON in addition to human-readable output
    #[arg(long)]
    json: bool,

    /// Print ASCII latency histogram for each phase
    #[arg(long)]
    histogram: bool,

    /// Time-bounded execution (e.g. "10s", "1m", "2m30s", "1h")
    #[arg(long)]
    duration: Option<String>,

    /// Read percentage for mixed workload (0–100); requires --workload mixed
    #[arg(long)]
    read_pct: Option<u8>,

    /// Interval for per-interval time-series reporting (e.g. "5s", "1m")
    #[arg(long)]
    interval: Option<String>,

    /// Queue depth for concurrent I/O slots (default 1)
    #[arg(long, default_value_t = 1)]
    queue_depth: usize,

    /// I/O engine
    #[arg(long, value_enum, default_value_t = Engine::Sync)]
    engine: Engine,

    /// Number of test files (default = threads)
    #[arg(long)]
    num_files: Option<usize>,
}

pub fn parse() -> Config {
    let args = Args::parse();

    let file_size = parse_size(&args.file_size).unwrap_or_else(|e| {
        eprintln!("[ERROR] Invalid --file-size '{}': {}", args.file_size, e);
        std::process::exit(1);
    });

    let block_size = parse_size(&args.block_size).unwrap_or_else(|e| {
        eprintln!("[ERROR] Invalid --block-size '{}': {}", args.block_size, e);
        std::process::exit(1);
    }) as usize;

    let duration = args.duration.as_deref().map(|s| {
        parse_duration(s).unwrap_or_else(|e| {
            eprintln!("[ERROR] Invalid --duration '{}': {}", s, e);
            std::process::exit(1);
        })
    });

    let interval = args.interval.as_deref().map(|s| {
        parse_duration(s).unwrap_or_else(|e| {
            eprintln!("[ERROR] Invalid --interval '{}': {}", s, e);
            std::process::exit(1);
        })
    });

    let read_pct = args.read_pct.map(|p| {
        if p > 100 {
            eprintln!("[ERROR] --read-pct must be 0–100, got {p}");
            std::process::exit(1);
        }
        p
    });

    // Validate io_uring is only used on Linux.
    if args.engine == Engine::IoUring {
        #[cfg(not(target_os = "linux"))]
        {
            eprintln!("[ERROR] --engine io-uring is only supported on Linux");
            std::process::exit(1);
        }
    }

    let threads = args.threads.max(1);
    let num_files = args.num_files.unwrap_or(threads).max(1);

    Config {
        dir: args.dir,
        file_size,
        block_size,
        num_ops: args.num_ops,
        threads,
        workload: args.workload,
        mode: args.mode,
        fsync: args.fsync,
        unbuffered: args.unbuffered,
        warmup: args.warmup,
        no_cleanup: args.no_cleanup,
        json: args.json,
        histogram: args.histogram,
        duration,
        read_pct,
        interval,
        queue_depth: args.queue_depth.max(1),
        engine: args.engine,
        num_files,
    }
}

// ── Duration parser ───────────────────────────────────────────────────────────

/// Parse human-readable durations like "10s", "1m", "2m30s", "1h".
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration string".to_string());
    }

    let mut total_secs: u64 = 0;
    let mut remaining = s;

    // Parse optional hours.
    if let Some(idx) = remaining.find('h') {
        let n: u64 = remaining[..idx]
            .parse()
            .map_err(|_| format!("invalid hours in '{s}'"))?;
        total_secs += n * 3600;
        remaining = &remaining[idx + 1..];
    }

    // Parse optional minutes.
    if let Some(idx) = remaining.find('m') {
        let n: u64 = remaining[..idx]
            .parse()
            .map_err(|_| format!("invalid minutes in '{s}'"))?;
        total_secs += n * 60;
        remaining = &remaining[idx + 1..];
    }

    // Parse optional seconds.
    if let Some(idx) = remaining.find('s') {
        let n: u64 = remaining[..idx]
            .parse()
            .map_err(|_| format!("invalid seconds in '{s}'"))?;
        total_secs += n;
        remaining = &remaining[idx + 1..];
    } else if !remaining.is_empty() {
        // Plain number with no unit — treat as seconds.
        let n: u64 = remaining
            .parse()
            .map_err(|_| format!("unrecognised duration component '{remaining}' in '{s}'"))?;
        total_secs += n;
        remaining = "";
    }

    if !remaining.is_empty() {
        return Err(format!("unrecognised trailing '{remaining}' in duration '{s}'"));
    }

    Ok(Duration::from_secs(total_secs))
}

// ── Size parser ───────────────────────────────────────────────────────────────

/// Parse human-readable byte counts. All units are 1024-based (matching Python version).
pub fn parse_size(s: &str) -> Result<u64, String> {
    // Checked longest-first so "MB" doesn't accidentally match before "MIB".
    const UNITS: &[(&str, u64)] = &[
        ("GIB", 1 << 30),
        ("MIB", 1 << 20),
        ("KIB", 1 << 10),
        ("GB", 1 << 30),
        ("MB", 1 << 20),
        ("KB", 1 << 10),
        ("G", 1 << 30),
        ("M", 1 << 20),
        ("K", 1 << 10),
        ("B", 1),
    ];

    let upper = s.trim().to_uppercase();
    for &(suffix, mult) in UNITS {
        if upper.ends_with(suffix) {
            let num_str = upper[..upper.len() - suffix.len()].trim();
            let n: f64 = num_str
                .parse()
                .map_err(|e: std::num::ParseFloatError| e.to_string())?;
            return Ok((n * mult as f64) as u64);
        }
    }
    upper.parse::<u64>().map_err(|e| e.to_string())
}
