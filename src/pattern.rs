//! Database I/O pattern simulations.
//!
//! Each pattern runs a series of sub-phases that mimic how a real database
//! engine issues I/O — block sizes, access patterns, and fsync cadence are
//! chosen to match the actual engine's behavior.

use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

use rand::Rng;

use crate::cli::{Config, DbPattern};
use crate::metrics::{Metrics, PhaseResult};
use crate::output::{human_size, print_banner};
use crate::worker::{open_ro, open_rw, preallocate, AlignedBuf, Limit};

// ── Public entry points ───────────────────────────────────────────────────────

/// Run the requested database pattern and return one PhaseResult per sub-phase.
pub fn run_pattern(cfg: &Config, pattern: DbPattern) -> Vec<(String, PhaseResult)> {
    let paths = pattern_paths(cfg, pattern);

    print_banner("Preallocating pattern files");
    for path in &paths {
        let name = path.file_name().unwrap().to_string_lossy();
        print!("  {name}  ({}) ... ", human_size(cfg.file_size));
        std::io::Write::flush(&mut std::io::stdout()).ok();
        if let Err(e) = preallocate(path, cfg.file_size) {
            eprintln!("FAILED\n[ERROR] {e}");
            std::process::exit(1);
        }
        println!("OK");
    }

    match pattern {
        DbPattern::SqliteWal => run_sqlite_wal(cfg),
        DbPattern::Postgres  => run_postgres(cfg),
        DbPattern::Rocksdb   => run_rocksdb(cfg),
        DbPattern::Mysql     => run_mysql(cfg),
    }
}

/// Remove all files created by the pattern run.
pub fn cleanup_pattern_files(cfg: &Config, pattern: DbPattern) {
    for path in pattern_paths(cfg, pattern) {
        let name = path.file_name().unwrap().to_string_lossy();
        match std::fs::remove_file(&path) {
            Ok(()) => println!("  Removed {name}"),
            Err(e) => eprintln!("  [WARN] Could not remove {name}: {e}"),
        }
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn pattern_paths(cfg: &Config, pattern: DbPattern) -> Vec<PathBuf> {
    match pattern {
        DbPattern::SqliteWal => vec![
            cfg.dir.join("pat_wal.dat"),
            cfg.dir.join("pat_db.dat"),
        ],
        DbPattern::Postgres => vec![
            cfg.dir.join("pat_rel.dat"),
            cfg.dir.join("pat_pgwal.dat"),
        ],
        DbPattern::Rocksdb => {
            let mut v: Vec<PathBuf> = (0..4)
                .map(|i| cfg.dir.join(format!("pat_sst{i}.dat")))
                .collect();
            v.push(cfg.dir.join("pat_sst_out.dat"));
            v
        }
        DbPattern::Mysql => vec![
            cfg.dir.join("pat_ibdata.dat"),     // InnoDB tablespace / buffer pool
            cfg.dir.join("pat_iblog.dat"),      // InnoDB redo log
            cfg.dir.join("pat_dblwr.dat"),      // Doublewrite buffer
        ],
    }
}

/// Build a `Limit` for one sub-phase.
///
/// When duration-based, the deadline is computed from *now* so sub-phases
/// running sequentially each get their full share of time.
fn subphase_limit(per_dur: Option<Duration>, ops: u64) -> Limit {
    match per_dur {
        Some(d) => Limit::Until(Instant::now() + d),
        None    => Limit::Ops(ops.max(1)),
    }
}

/// Split the total budget 75 / 25.
fn split_75_25(cfg: &Config) -> (Option<Duration>, u64, Option<Duration>, u64) {
    match cfg.duration {
        Some(d) => {
            let a = d.mul_f64(0.75);
            (Some(a), 0, Some(d - a), 0)
        }
        None => {
            let a = (cfg.num_ops * 3 / 4).max(1);
            let b = (cfg.num_ops - a).max(1);
            (None, a, None, b)
        }
    }
}

/// Split the total budget 60 / 30 / 10 (A / B / C).
fn split_60_30_10(cfg: &Config) -> (Option<Duration>, u64, Option<Duration>, u64, Option<Duration>, u64) {
    match cfg.duration {
        Some(d) => {
            let a = d.mul_f64(0.60);
            let b = d.mul_f64(0.30);
            let c = d - a - b;
            (Some(a), 0, Some(b), 0, Some(c), 0)
        }
        None => {
            let a = (cfg.num_ops * 6 / 10).max(1);
            let b = (cfg.num_ops * 3 / 10).max(1);
            let c = (cfg.num_ops - a - b).max(1);
            (None, a, None, b, None, c)
        }
    }
}

/// Split the total budget 60 / 40.
fn split_60_40(cfg: &Config) -> (Option<Duration>, u64, Option<Duration>, u64) {
    match cfg.duration {
        Some(d) => {
            let a = d.mul_f64(0.60);
            (Some(a), 0, Some(d - a), 0)
        }
        None => {
            let a = (cfg.num_ops * 3 / 5).max(1);
            let b = (cfg.num_ops - a).max(1);
            (None, a, None, b)
        }
    }
}

// ── SQLite WAL pattern ────────────────────────────────────────────────────────

/// Simulate SQLite WAL-mode I/O:
///   Sub-phase 1 (75%): sequential 4 KB writes to the WAL file, fsync every 64
///                      ops (one fsync per simulated transaction commit).
///   Sub-phase 2 (25%): checkpoint — sequential reads from WAL + random writes
///                      back to the main DB file at block-aligned offsets.
fn run_sqlite_wal(cfg: &Config) -> Vec<(String, PhaseResult)> {
    const BLOCK: usize = 4096;
    let wal_path = cfg.dir.join("pat_wal.dat");
    let db_path  = cfg.dir.join("pat_db.dat");
    let file_size = cfg.file_size;
    let num_blocks = (file_size / BLOCK as u64).max(1);

    let (dur_a, ops_a, dur_b, ops_b) = split_75_25(cfg);

    // ── Sub-phase 1: WAL writes ───────────────────────────────────────────────
    let m1 = Metrics::new();
    let t1 = Instant::now();
    {
        let limit = subphase_limit(dur_a, ops_a);
        let buf = AlignedBuf::new(BLOCK);
        let mut f = match open_rw(&wal_path, cfg.unbuffered) {
            Ok(f) => f,
            Err(e) => { eprintln!("[ERROR] sqlite-wal wal open: {e}"); return vec![]; }
        };
        let mut ops_done = 0u64;
        loop {
            if limit.is_done(ops_done) { break; }
            if f.stream_position().unwrap_or(0) + BLOCK as u64 > file_size {
                let _ = f.seek(SeekFrom::Start(0));
            }
            let t0 = Instant::now();
            if let Err(e) = f.write_all(buf.as_slice()) {
                eprintln!("[ERROR] sqlite-wal wal write: {e}");
                m1.record_error();
                break;
            }
            // fsync every 64 ops — simulates a transaction commit boundary.
            if ops_done % 64 == 63 {
                let _ = f.sync_all();
            }
            m1.record(t0.elapsed(), BLOCK);
            ops_done += 1;
        }
    }
    let r1 = m1.summarise(t1.elapsed());

    // ── Sub-phase 2: Checkpoint ───────────────────────────────────────────────
    let m2 = Metrics::new();
    let t2 = Instant::now();
    {
        let limit = subphase_limit(dur_b, ops_b);
        let mut read_buf  = AlignedBuf::new(BLOCK);
        let write_buf = AlignedBuf::new(BLOCK);
        let mut rng = rand::thread_rng();

        let mut wal_f = match open_ro(&wal_path, cfg.unbuffered) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[ERROR] sqlite-wal checkpoint open wal: {e}");
                return vec![("SQLite-WAL / WAL Writes".to_string(), r1)];
            }
        };
        let mut db_f = match open_rw(&db_path, cfg.unbuffered) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[ERROR] sqlite-wal checkpoint open db: {e}");
                return vec![("SQLite-WAL / WAL Writes".to_string(), r1)];
            }
        };

        let mut ops_done = 0u64;
        loop {
            if limit.is_done(ops_done) { break; }

            // Sequential read from WAL (wrap at EOF).
            if wal_f.stream_position().unwrap_or(0) + BLOCK as u64 > file_size {
                let _ = wal_f.seek(SeekFrom::Start(0));
            }
            let t0 = Instant::now();
            match wal_f.read(read_buf.as_mut_slice()) {
                Ok(0) => { let _ = wal_f.seek(SeekFrom::Start(0)); continue; }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[ERROR] sqlite-wal checkpoint read: {e}");
                    m2.record_error();
                    break;
                }
            }

            // Random write back to DB at a block-aligned offset.
            let offset = rng.gen_range(0..num_blocks) * BLOCK as u64;
            if db_f.seek(SeekFrom::Start(offset)).is_err() {
                m2.record_error();
                break;
            }
            if let Err(e) = db_f.write_all(write_buf.as_slice()) {
                eprintln!("[ERROR] sqlite-wal checkpoint write: {e}");
                m2.record_error();
                break;
            }
            // Count both the read and the write in the byte total.
            m2.record(t0.elapsed(), BLOCK * 2);
            ops_done += 1;
        }
    }
    let r2 = m2.summarise(t2.elapsed());

    vec![
        ("SQLite-WAL / WAL Writes".to_string(),  r1),
        ("SQLite-WAL / Checkpoint".to_string(), r2),
    ]
}

// ── PostgreSQL pattern ────────────────────────────────────────────────────────

/// Simulate PostgreSQL I/O:
///   Sub-phase 1 (60%): bgwriter — random 8 KB writes to the relation/heap file
///                      (dirty-page flush, no per-op fsync).
///   Sub-phase 2 (40%): WAL writer — sequential 8 KB writes to the WAL segment,
///                      fsync every 32 ops (WAL commit).
fn run_postgres(cfg: &Config) -> Vec<(String, PhaseResult)> {
    const BLOCK: usize = 8192;
    let rel_path = cfg.dir.join("pat_rel.dat");
    let wal_path = cfg.dir.join("pat_pgwal.dat");
    let file_size = cfg.file_size;
    let num_blocks = (file_size / BLOCK as u64).max(1);

    let (dur_a, ops_a, dur_b, ops_b) = split_60_40(cfg);

    // ── Sub-phase 1: bgwriter ─────────────────────────────────────────────────
    let m1 = Metrics::new();
    let t1 = Instant::now();
    {
        let limit = subphase_limit(dur_a, ops_a);
        let buf = AlignedBuf::new(BLOCK);
        let mut rng = rand::thread_rng();
        let mut f = match open_rw(&rel_path, cfg.unbuffered) {
            Ok(f) => f,
            Err(e) => { eprintln!("[ERROR] postgres bgwriter open: {e}"); return vec![]; }
        };
        let mut ops_done = 0u64;
        loop {
            if limit.is_done(ops_done) { break; }
            let offset = rng.gen_range(0..num_blocks) * BLOCK as u64;
            if f.seek(SeekFrom::Start(offset)).is_err() {
                m1.record_error();
                break;
            }
            let t0 = Instant::now();
            if let Err(e) = f.write_all(buf.as_slice()) {
                eprintln!("[ERROR] postgres bgwriter write: {e}");
                m1.record_error();
                break;
            }
            m1.record(t0.elapsed(), BLOCK);
            ops_done += 1;
        }
    }
    let r1 = m1.summarise(t1.elapsed());

    // ── Sub-phase 2: WAL writer ───────────────────────────────────────────────
    let m2 = Metrics::new();
    let t2 = Instant::now();
    {
        let limit = subphase_limit(dur_b, ops_b);
        let buf = AlignedBuf::new(BLOCK);
        let mut f = match open_rw(&wal_path, cfg.unbuffered) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[ERROR] postgres wal open: {e}");
                return vec![("Postgres / bgwriter".to_string(), r1)];
            }
        };
        let mut ops_done = 0u64;
        loop {
            if limit.is_done(ops_done) { break; }
            if f.stream_position().unwrap_or(0) + BLOCK as u64 > file_size {
                let _ = f.seek(SeekFrom::Start(0));
            }
            let t0 = Instant::now();
            if let Err(e) = f.write_all(buf.as_slice()) {
                eprintln!("[ERROR] postgres wal write: {e}");
                m2.record_error();
                break;
            }
            // fsync every 32 ops — simulates WAL segment commit.
            if ops_done % 32 == 31 {
                let _ = f.sync_all();
            }
            m2.record(t0.elapsed(), BLOCK);
            ops_done += 1;
        }
    }
    let r2 = m2.summarise(t2.elapsed());

    vec![
        ("Postgres / bgwriter".to_string(),   r1),
        ("Postgres / WAL writer".to_string(), r2),
    ]
}

// ── RocksDB compaction pattern ────────────────────────────────────────────────

/// Simulate RocksDB SST compaction I/O:
///   Single sub-phase: sequential 64 KB reads from 4 input SST files
///   (round-robin) and sequential 64 KB writes to an output SST file.
///   A final fsync on the output simulates SST file completion.
fn run_rocksdb(cfg: &Config) -> Vec<(String, PhaseResult)> {
    const BLOCK: usize = 65536; // 64 KB — typical compaction read unit
    let file_size = cfg.file_size;

    let input_paths: Vec<PathBuf> = (0..4)
        .map(|i| cfg.dir.join(format!("pat_sst{i}.dat")))
        .collect();
    let output_path = cfg.dir.join("pat_sst_out.dat");

    let limit = subphase_limit(cfg.duration, cfg.num_ops);

    let m = Metrics::new();
    let t_start = Instant::now();
    {
        // Open all input files for sequential reading.
        let mut inputs: Vec<_> = input_paths.iter().map(|p| {
            open_ro(p, cfg.unbuffered).unwrap_or_else(|e| {
                eprintln!("[ERROR] rocksdb input open {:?}: {e}", p);
                std::process::exit(1);
            })
        }).collect();

        let mut out_f = match open_rw(&output_path, cfg.unbuffered) {
            Ok(f) => f,
            Err(e) => { eprintln!("[ERROR] rocksdb output open: {e}"); return vec![]; }
        };

        let mut read_buf  = AlignedBuf::new(BLOCK);
        let write_buf = AlignedBuf::new(BLOCK);
        let mut idx = 0usize;
        let mut ops_done = 0u64;

        loop {
            if limit.is_done(ops_done) { break; }

            // Sequential read from input SSTs, cycling across all 4 files.
            let src = &mut inputs[idx % 4];
            idx += 1;

            let t0 = Instant::now();

            // Wrap input file at EOF.
            if src.stream_position().unwrap_or(0) + BLOCK as u64 > file_size {
                let _ = src.seek(SeekFrom::Start(0));
            }
            match src.read(read_buf.as_mut_slice()) {
                Ok(0) => { let _ = src.seek(SeekFrom::Start(0)); continue; }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[ERROR] rocksdb read: {e}");
                    m.record_error();
                    break;
                }
            }

            // Sequential write to output SST (wrap at EOF).
            if out_f.stream_position().unwrap_or(0) + BLOCK as u64 > file_size {
                let _ = out_f.seek(SeekFrom::Start(0));
            }
            if let Err(e) = out_f.write_all(write_buf.as_slice()) {
                eprintln!("[ERROR] rocksdb write: {e}");
                m.record_error();
                break;
            }

            // Count both read + write in byte total.
            m.record(t0.elapsed(), BLOCK * 2);
            ops_done += 1;
        }

        // Final fsync — simulates SST file completion.
        let _ = out_f.sync_all();
    }

    vec![("RocksDB / Compaction".to_string(), m.summarise(t_start.elapsed()))]
}

// ── MySQL / MariaDB InnoDB pattern ────────────────────────────────────────────

/// Simulate MySQL/MariaDB InnoDB I/O:
///
///   Sub-phase 1 (60%): Buffer pool flush — random 16 KB page writes to the
///                      tablespace file (ibdata / .ibd), matching InnoDB's
///                      default page size and page-cleaner behavior.
///
///   Sub-phase 2 (30%): Redo log — sequential 4 KB writes to the redo log
///                      (ib_logfile0) with an fsync after every write,
///                      simulating innodb_flush_log_at_trx_commit=1 (full
///                      durability mode).
///
///   Sub-phase 3 (10%): Doublewrite buffer — InnoDB's crash-safety mechanism
///                      that writes each dirty page twice: first sequentially
///                      to the doublewrite buffer file (fsync), then at its
///                      real random offset in the tablespace (fsync).  This
///                      sub-phase alternates between those two writes per op.
fn run_mysql(cfg: &Config) -> Vec<(String, PhaseResult)> {
    const PAGE:  usize = 16384; // 16 KB — InnoDB default page size
    const RLOG:  usize = 4096;  //  4 KB — typical redo log write unit

    let ibdata_path = cfg.dir.join("pat_ibdata.dat");
    let iblog_path  = cfg.dir.join("pat_iblog.dat");
    let dblwr_path  = cfg.dir.join("pat_dblwr.dat");
    let file_size   = cfg.file_size;
    let num_pages   = (file_size / PAGE as u64).max(1);

    let (dur_a, ops_a, dur_b, ops_b, dur_c, ops_c) = split_60_30_10(cfg);

    // ── Sub-phase 1: Buffer pool flush ────────────────────────────────────────
    let m1 = Metrics::new();
    let t1 = Instant::now();
    {
        let limit = subphase_limit(dur_a, ops_a);
        let buf   = AlignedBuf::new(PAGE);
        let mut rng = rand::thread_rng();
        let mut f = match open_rw(&ibdata_path, cfg.unbuffered) {
            Ok(f) => f,
            Err(e) => { eprintln!("[ERROR] mysql ibdata open: {e}"); return vec![]; }
        };
        let mut ops_done = 0u64;
        loop {
            if limit.is_done(ops_done) { break; }
            let offset = rng.gen_range(0..num_pages) * PAGE as u64;
            if f.seek(SeekFrom::Start(offset)).is_err() {
                m1.record_error(); break;
            }
            let t0 = Instant::now();
            if let Err(e) = f.write_all(buf.as_slice()) {
                eprintln!("[ERROR] mysql buffer pool flush write: {e}");
                m1.record_error(); break;
            }
            m1.record(t0.elapsed(), PAGE);
            ops_done += 1;
        }
    }
    let r1 = m1.summarise(t1.elapsed());

    // ── Sub-phase 2: Redo log (innodb_flush_log_at_trx_commit = 1) ───────────
    let m2 = Metrics::new();
    let t2 = Instant::now();
    {
        let limit = subphase_limit(dur_b, ops_b);
        let buf   = AlignedBuf::new(RLOG);
        let mut f = match open_rw(&iblog_path, cfg.unbuffered) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[ERROR] mysql redo log open: {e}");
                return vec![("InnoDB / Buffer Pool Flush".to_string(), r1)];
            }
        };
        let mut ops_done = 0u64;
        loop {
            if limit.is_done(ops_done) { break; }
            if f.stream_position().unwrap_or(0) + RLOG as u64 > file_size {
                let _ = f.seek(SeekFrom::Start(0));
            }
            let t0 = Instant::now();
            if let Err(e) = f.write_all(buf.as_slice()) {
                eprintln!("[ERROR] mysql redo log write: {e}");
                m2.record_error(); break;
            }
            // Full fsync on every write — innodb_flush_log_at_trx_commit=1.
            let _ = f.sync_all();
            m2.record(t0.elapsed(), RLOG);
            ops_done += 1;
        }
    }
    let r2 = m2.summarise(t2.elapsed());

    // ── Sub-phase 3: Doublewrite buffer ───────────────────────────────────────
    // Each op: write the page sequentially to the doublewrite buffer + fsync,
    // then write it to a random offset in the tablespace + fsync.
    // This is the exact two-write sequence InnoDB uses for crash safety.
    let m3 = Metrics::new();
    let t3 = Instant::now();
    {
        let limit   = subphase_limit(dur_c, ops_c);
        let buf     = AlignedBuf::new(PAGE);
        let mut rng = rand::thread_rng();
        let mut dblwr_f = match open_rw(&dblwr_path, cfg.unbuffered) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[ERROR] mysql doublewrite open: {e}");
                return vec![
                    ("InnoDB / Buffer Pool Flush".to_string(), r1),
                    ("InnoDB / Redo Log".to_string(),          r2),
                ];
            }
        };
        let mut ibd_f = match open_rw(&ibdata_path, cfg.unbuffered) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[ERROR] mysql ibdata reopen: {e}");
                return vec![
                    ("InnoDB / Buffer Pool Flush".to_string(), r1),
                    ("InnoDB / Redo Log".to_string(),          r2),
                ];
            }
        };

        let mut ops_done = 0u64;
        loop {
            if limit.is_done(ops_done) { break; }

            let t0 = Instant::now();

            // Step 1: sequential write to doublewrite buffer + fsync.
            if dblwr_f.stream_position().unwrap_or(0) + PAGE as u64 > file_size {
                let _ = dblwr_f.seek(SeekFrom::Start(0));
            }
            if let Err(e) = dblwr_f.write_all(buf.as_slice()) {
                eprintln!("[ERROR] mysql doublewrite write: {e}");
                m3.record_error(); break;
            }
            let _ = dblwr_f.sync_all();

            // Step 2: write the page to its real location in the tablespace + fsync.
            let offset = rng.gen_range(0..num_pages) * PAGE as u64;
            if ibd_f.seek(SeekFrom::Start(offset)).is_err() {
                m3.record_error(); break;
            }
            if let Err(e) = ibd_f.write_all(buf.as_slice()) {
                eprintln!("[ERROR] mysql doublewrite ibd write: {e}");
                m3.record_error(); break;
            }
            let _ = ibd_f.sync_all();

            // Two PAGE-sized writes per op.
            m3.record(t0.elapsed(), PAGE * 2);
            ops_done += 1;
        }
    }
    let r3 = m3.summarise(t3.elapsed());

    vec![
        ("InnoDB / Buffer Pool Flush".to_string(), r1),
        ("InnoDB / Redo Log".to_string(),          r2),
        ("InnoDB / Doublewrite Buffer".to_string(), r3),
    ]
}
