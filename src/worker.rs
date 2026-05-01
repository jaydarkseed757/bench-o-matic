//! I/O worker functions and file management.
//!
//! Design notes:
//! - One worker per thread; each owns its file handle for the whole run.
//! - Write buffers are pre-filled with random bytes to avoid zero-page folding.
//! - AlignedBuf ensures O_DIRECT requirements (4 KiB alignment) on Linux.
//! - Random offsets use `thread_rng()` which is seeded per-thread and cheap.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{atomic::{AtomicU64, Ordering}, Arc},
    time::Instant,
};

use rand::{Rng, RngCore};

use crate::cli::{Access, Engine, Op};
use crate::metrics::Metrics;

// ── Limit ─────────────────────────────────────────────────────────────────────

/// Determines when a worker stops.
#[derive(Clone, Copy, Debug)]
pub enum Limit {
    /// Stop after this many operations.
    Ops(u64),
    /// Stop when this instant is reached.
    Until(Instant),
}

impl Limit {
    /// Returns true if the limit has been reached.
    #[inline]
    pub fn is_done(self, ops_done: u64) -> bool {
        match self {
            Limit::Ops(n) => ops_done >= n,
            Limit::Until(deadline) => Instant::now() >= deadline,
        }
    }
}

// ── WorkerParams ──────────────────────────────────────────────────────────────

/// All per-worker configuration, passed via Arc into spawned threads.
#[allow(dead_code)]
pub struct WorkerParams {
    pub access: Access,
    pub op: Op,
    pub limit: Limit,
    pub block_size: usize,
    pub file_size: u64,
    pub use_fsync: bool,
    pub unbuffered: bool,
    pub queue_depth: usize,
    pub engine: Engine,
}

// ── Public dispatch ───────────────────────────────────────────────────────────

/// Run a single worker using the given params.
///
/// When `queue_depth > 1` and `Engine::Sync`, spawns slot-threads internally.
/// When `queue_depth == 1` or `Engine::Sync` with depth 1, runs inline.
pub fn run_worker(
    files: &[PathBuf],
    params: &WorkerParams,
    metrics: &Metrics,
) {
    let qd = params.queue_depth;

    if qd <= 1 {
        // Single-threaded path (existing behavior).
        run_single(files, params, metrics);
    } else {
        // Thread-sim: spawn qd slot-threads sharing the same metrics.
        run_slot_threads(files, params, metrics, qd);
    }
}

/// Warmup-compatible single-op runner; always uses Ops limit.
pub fn run_worker_ops(
    files: &[PathBuf],
    access: Access,
    op: Op,
    num_ops: u64,
    metrics: &Metrics,
    use_fsync: bool,
    unbuffered: bool,
    file_size: u64,
    block_size: usize,
) {
    let params = WorkerParams {
        access,
        op,
        limit: Limit::Ops(num_ops),
        block_size,
        file_size,
        use_fsync,
        unbuffered,
        queue_depth: 1,
        engine: Engine::Sync,
    };
    run_single(files, &params, metrics);
}

fn run_single(files: &[PathBuf], params: &WorkerParams, metrics: &Metrics) {
    let path = &files[0]; // Single slot uses first file.
    match (params.access, params.op) {
        (Access::Sequential, Op::Write) => {
            seq_write(path, params.block_size, params.limit, metrics, params.use_fsync, params.unbuffered);
        }
        (Access::Sequential, Op::Read) => {
            seq_read(path, params.block_size, params.limit, metrics, params.unbuffered);
        }
        (Access::Sequential, Op::Mixed(pct)) => {
            seq_mixed(path, params.file_size, params.block_size, params.limit, metrics, params.use_fsync, params.unbuffered, pct);
        }
        (Access::Random, Op::Write) => {
            rnd_write(path, params.file_size, params.block_size, params.limit, metrics, params.use_fsync, params.unbuffered);
        }
        (Access::Random, Op::Read) => {
            rnd_read(path, params.file_size, params.block_size, params.limit, metrics, params.unbuffered);
        }
        (Access::Random, Op::Mixed(pct)) => {
            rnd_mixed(files, params.file_size, params.block_size, params.limit, metrics, params.use_fsync, params.unbuffered, pct);
        }
    }
}

fn run_slot_threads(files: &[PathBuf], params: &WorkerParams, metrics: &Metrics, qd: usize) {
    // Shared op counter for Ops limit.
    let shared_ops = Arc::new(AtomicU64::new(0));
    let ops_limit = match params.limit {
        Limit::Ops(n) => n,
        Limit::Until(_) => u64::MAX,
    };

    let mut handles = Vec::with_capacity(qd);

    for slot in 0..qd {
        let file_path = files[slot % files.len()].clone();
        let all_files: Vec<PathBuf> = files.to_vec();
        let shared_ops = Arc::clone(&shared_ops);
        let access = params.access;
        let op = params.op;
        let limit = params.limit;
        let block_size = params.block_size;
        let file_size = params.file_size;
        let use_fsync = params.use_fsync;
        let unbuffered = params.unbuffered;

        // We need to share metrics. Use a raw pointer trick with explicit lifetime.
        // SAFETY: metrics lives for the duration of this function; all threads are
        // joined before the function returns.
        let metrics_ptr = metrics as *const Metrics as usize;

        let handle = std::thread::spawn(move || {
            // SAFETY: metrics_ptr was derived from a valid reference that outlives
            // all slot threads (joined below). No thread can outlive the referent.
            let metrics = unsafe { &*(metrics_ptr as *const Metrics) };

            match limit {
                Limit::Until(_) => {
                    // Each slot checks time independently.
                    let slot_params = WorkerParams {
                        access,
                        op,
                        limit,
                        block_size,
                        file_size,
                        use_fsync,
                        unbuffered,
                        queue_depth: 1,
                        engine: Engine::Sync,
                    };
                    let files_for_slot = vec![file_path];
                    run_single(&files_for_slot, &slot_params, metrics);
                }
                Limit::Ops(_) => {
                    // Slots share the op counter; each claims one op at a time.
                    run_slot_ops(
                        &file_path,
                        &all_files,
                        access,
                        op,
                        block_size,
                        file_size,
                        use_fsync,
                        unbuffered,
                        &shared_ops,
                        ops_limit,
                        metrics,
                    );
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        if let Err(e) = h.join() {
            eprintln!("[ERROR] Slot thread panicked: {e:?}");
        }
    }
}

fn run_slot_ops(
    file_path: &Path,
    all_files: &[PathBuf],
    access: Access,
    op: Op,
    block_size: usize,
    file_size: u64,
    use_fsync: bool,
    unbuffered: bool,
    shared_ops: &AtomicU64,
    ops_limit: u64,
    metrics: &Metrics,
) {
    let mut rng = rand::thread_rng();
    let num_blocks = (file_size / block_size as u64).max(1);
    let write_buf = AlignedBuf::new(block_size);
    let mut read_buf = AlignedBuf::new(block_size);

    // Open primary file.
    let f_result = match op {
        Op::Write | Op::Mixed(_) => open_rw(file_path, unbuffered),
        Op::Read => open_ro(file_path, unbuffered),
    };
    let mut f = match f_result {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[ERROR] slot open {:?}: {}", file_path, e);
            metrics.record_error();
            return;
        }
    };

    loop {
        // Atomically claim one op.
        let done = shared_ops.fetch_add(1, Ordering::Relaxed);
        if done >= ops_limit {
            break;
        }

        let actual_op = match op {
            Op::Mixed(pct) => {
                if rng.gen_range(0..100u8) < pct { Op::Read } else { Op::Write }
            }
            other => other,
        };

        // For Mixed, pick a random file.
        let use_path: &Path = if matches!(op, Op::Mixed(_)) {
            &all_files[rng.gen_range(0..all_files.len())]
        } else {
            file_path
        };

        match (access, actual_op) {
            (Access::Sequential, Op::Write) => {
                let pos = f.stream_position().unwrap_or(0);
                if pos + block_size as u64 > file_size {
                    let _ = f.seek(SeekFrom::Start(0));
                }
                let t0 = Instant::now();
                if let Err(e) = f.write_all(write_buf.as_slice()) {
                    eprintln!("[ERROR] slot seq-write {:?}: {}", use_path, e);
                    metrics.record_error();
                    return;
                }
                if use_fsync { let _ = f.sync_all(); }
                metrics.record(t0.elapsed(), block_size);
            }
            (Access::Sequential, Op::Read) => {
                let pos = f.stream_position().unwrap_or(0);
                if pos + block_size as u64 > file_size {
                    let _ = f.seek(SeekFrom::Start(0));
                }
                let t0 = Instant::now();
                match f.read(read_buf.as_mut_slice()) {
                    Ok(n) if n > 0 => metrics.record(t0.elapsed(), n),
                    Ok(_) => { let _ = f.seek(SeekFrom::Start(0)); }
                    Err(e) => {
                        eprintln!("[ERROR] slot seq-read {:?}: {}", use_path, e);
                        metrics.record_error();
                        return;
                    }
                }
            }
            (Access::Random, Op::Write) => {
                let offset = rng.gen_range(0..num_blocks) * block_size as u64;
                if f.seek(SeekFrom::Start(offset)).is_err() {
                    metrics.record_error();
                    return;
                }
                let t0 = Instant::now();
                if let Err(e) = f.write_all(write_buf.as_slice()) {
                    eprintln!("[ERROR] slot rnd-write {:?}: {}", use_path, e);
                    metrics.record_error();
                    return;
                }
                if use_fsync { let _ = f.sync_all(); }
                metrics.record(t0.elapsed(), block_size);
            }
            (Access::Random, Op::Read) => {
                let offset = rng.gen_range(0..num_blocks) * block_size as u64;
                if f.seek(SeekFrom::Start(offset)).is_err() {
                    metrics.record_error();
                    return;
                }
                let t0 = Instant::now();
                match f.read(read_buf.as_mut_slice()) {
                    Ok(n) if n > 0 => metrics.record(t0.elapsed(), n),
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("[ERROR] slot rnd-read {:?}: {}", use_path, e);
                        metrics.record_error();
                        return;
                    }
                }
            }
            _ => {} // Mixed resolved above.
        }
    }
}

// ── Preallocation ─────────────────────────────────────────────────────────────

/// Write `size` bytes of non-zero data so the OS allocates real extents.
///
/// We generate a 1 MiB random buffer once and repeat it.  This defeats
/// sparse-file / copy-on-write shortcuts without being slow.
pub fn preallocate(path: &Path, size: u64) -> std::io::Result<()> {
    const BUF: usize = 1024 * 1024;
    let mut buf = vec![0u8; BUF];
    rand::thread_rng().fill_bytes(&mut buf);

    let mut f = File::create(path)?;
    let mut remaining = size;
    while remaining > 0 {
        let chunk = remaining.min(BUF as u64) as usize;
        f.write_all(&buf[..chunk])?;
        remaining -= chunk as u64;
    }
    f.sync_all()?;
    Ok(())
}

// ── Sequential workers ────────────────────────────────────────────────────────

fn seq_write(
    path: &Path,
    block_size: usize,
    limit: Limit,
    metrics: &Metrics,
    use_fsync: bool,
    unbuffered: bool,
) {
    let buf = AlignedBuf::new(block_size);

    let mut f = match open_rw(path, unbuffered) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[ERROR] seq-write open {:?}: {}", path, e);
            metrics.record_error();
            return;
        }
    };
    let file_size = match f.metadata() {
        Ok(m) => m.len(),
        Err(e) => {
            eprintln!("[ERROR] seq-write stat {:?}: {}", path, e);
            metrics.record_error();
            return;
        }
    };

    let mut ops_done = 0u64;
    loop {
        if limit.is_done(ops_done) { break; }
        // Wrap around so we stay within the preallocated extent.
        if f.stream_position().unwrap_or(0) + block_size as u64 > file_size {
            let _ = f.seek(SeekFrom::Start(0));
        }

        let t0 = Instant::now();
        if let Err(e) = f.write_all(buf.as_slice()) {
            eprintln!("[ERROR] seq-write {:?}: {}", path, e);
            metrics.record_error();
            return;
        }
        if use_fsync {
            let _ = f.sync_all();
        }
        metrics.record(t0.elapsed(), block_size);
        ops_done += 1;
    }
}

fn seq_read(
    path: &Path,
    block_size: usize,
    limit: Limit,
    metrics: &Metrics,
    unbuffered: bool,
) {
    let mut buf = AlignedBuf::new(block_size);

    let mut f = match open_ro(path, unbuffered) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[ERROR] seq-read open {:?}: {}", path, e);
            metrics.record_error();
            return;
        }
    };
    let file_size = match f.metadata() {
        Ok(m) => m.len(),
        Err(e) => {
            eprintln!("[ERROR] seq-read stat {:?}: {}", path, e);
            metrics.record_error();
            return;
        }
    };

    let mut ops_done = 0u64;
    loop {
        if limit.is_done(ops_done) { break; }
        if f.stream_position().unwrap_or(0) + block_size as u64 > file_size {
            let _ = f.seek(SeekFrom::Start(0));
        }

        let t0 = Instant::now();
        match f.read(buf.as_mut_slice()) {
            Ok(0) => {
                let _ = f.seek(SeekFrom::Start(0));
                continue;
            }
            Ok(n) => {
                metrics.record(t0.elapsed(), n);
                ops_done += 1;
            }
            Err(e) => {
                eprintln!("[ERROR] seq-read {:?}: {}", path, e);
                metrics.record_error();
                return;
            }
        }
    }
}

fn seq_mixed(
    path: &Path,
    file_size: u64,
    block_size: usize,
    limit: Limit,
    metrics: &Metrics,
    use_fsync: bool,
    unbuffered: bool,
    read_pct: u8,
) {
    let write_buf = AlignedBuf::new(block_size);
    let mut read_buf = AlignedBuf::new(block_size);
    let mut rng = rand::thread_rng();

    let mut f = match open_rw(path, unbuffered) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[ERROR] seq-mixed open {:?}: {}", path, e);
            metrics.record_error();
            return;
        }
    };

    let mut ops_done = 0u64;
    loop {
        if limit.is_done(ops_done) { break; }
        let pos = f.stream_position().unwrap_or(0);
        if pos + block_size as u64 > file_size {
            let _ = f.seek(SeekFrom::Start(0));
        }

        let t0 = Instant::now();
        if rng.gen_range(0..100u8) < read_pct {
            // Read.
            match f.read(read_buf.as_mut_slice()) {
                Ok(n) if n > 0 => metrics.record(t0.elapsed(), n),
                Ok(_) => { let _ = f.seek(SeekFrom::Start(0)); continue; }
                Err(e) => {
                    eprintln!("[ERROR] seq-mixed read {:?}: {}", path, e);
                    metrics.record_error();
                    return;
                }
            }
        } else {
            // Write.
            if let Err(e) = f.write_all(write_buf.as_slice()) {
                eprintln!("[ERROR] seq-mixed write {:?}: {}", path, e);
                metrics.record_error();
                return;
            }
            if use_fsync { let _ = f.sync_all(); }
            metrics.record(t0.elapsed(), block_size);
        }
        ops_done += 1;
    }
}

// ── Random workers ────────────────────────────────────────────────────────────

fn rnd_write(
    path: &Path,
    file_size: u64,
    block_size: usize,
    limit: Limit,
    metrics: &Metrics,
    use_fsync: bool,
    unbuffered: bool,
) {
    let buf = AlignedBuf::new(block_size);
    let num_blocks = (file_size / block_size as u64).max(1);
    let mut rng = rand::thread_rng();

    let mut f = match open_rw(path, unbuffered) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[ERROR] rnd-write open {:?}: {}", path, e);
            metrics.record_error();
            return;
        }
    };

    let mut ops_done = 0u64;
    loop {
        if limit.is_done(ops_done) { break; }
        let offset = rng.gen_range(0..num_blocks) * block_size as u64;

        let t0 = Instant::now();
        if f.seek(SeekFrom::Start(offset)).is_err() {
            metrics.record_error();
            return;
        }
        if let Err(e) = f.write_all(buf.as_slice()) {
            eprintln!("[ERROR] rnd-write {:?}: {}", path, e);
            metrics.record_error();
            return;
        }
        if use_fsync {
            let _ = f.sync_all();
        }
        metrics.record(t0.elapsed(), block_size);
        ops_done += 1;
    }
}

fn rnd_read(
    path: &Path,
    file_size: u64,
    block_size: usize,
    limit: Limit,
    metrics: &Metrics,
    unbuffered: bool,
) {
    let mut buf = AlignedBuf::new(block_size);
    let num_blocks = (file_size / block_size as u64).max(1);
    let mut rng = rand::thread_rng();

    let mut f = match open_ro(path, unbuffered) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[ERROR] rnd-read open {:?}: {}", path, e);
            metrics.record_error();
            return;
        }
    };

    let mut ops_done = 0u64;
    loop {
        if limit.is_done(ops_done) { break; }
        let offset = rng.gen_range(0..num_blocks) * block_size as u64;

        let t0 = Instant::now();
        if f.seek(SeekFrom::Start(offset)).is_err() {
            metrics.record_error();
            return;
        }
        match f.read(buf.as_mut_slice()) {
            Ok(n) if n > 0 => {
                metrics.record(t0.elapsed(), n);
                ops_done += 1;
            }
            Ok(_) => continue,
            Err(e) => {
                eprintln!("[ERROR] rnd-read {:?}: {}", path, e);
                metrics.record_error();
                return;
            }
        }
    }
}

fn rnd_mixed(
    files: &[PathBuf],
    file_size: u64,
    block_size: usize,
    limit: Limit,
    metrics: &Metrics,
    use_fsync: bool,
    unbuffered: bool,
    read_pct: u8,
) {
    let num_blocks = (file_size / block_size as u64).max(1);
    let write_buf = AlignedBuf::new(block_size);
    let mut read_buf = AlignedBuf::new(block_size);
    let mut rng = rand::thread_rng();

    // Open all files for read-write.
    let mut handles: Vec<File> = files.iter().map(|p| {
        open_rw(p, unbuffered).unwrap_or_else(|e| {
            panic!("[ERROR] rnd-mixed open {:?}: {}", p, e);
        })
    }).collect();

    let mut ops_done = 0u64;
    loop {
        if limit.is_done(ops_done) { break; }
        // Pick a random file for this operation.
        let fi = rng.gen_range(0..handles.len());
        let f = &mut handles[fi];
        let offset = rng.gen_range(0..num_blocks) * block_size as u64;

        if f.seek(SeekFrom::Start(offset)).is_err() {
            metrics.record_error();
            return;
        }

        let t0 = Instant::now();
        if rng.gen_range(0..100u8) < read_pct {
            // Read.
            match f.read(read_buf.as_mut_slice()) {
                Ok(n) if n > 0 => metrics.record(t0.elapsed(), n),
                Ok(_) => continue,
                Err(e) => {
                    eprintln!("[ERROR] rnd-mixed read: {}", e);
                    metrics.record_error();
                    return;
                }
            }
        } else {
            // Write.
            if let Err(e) = f.write_all(write_buf.as_slice()) {
                eprintln!("[ERROR] rnd-mixed write: {}", e);
                metrics.record_error();
                return;
            }
            if use_fsync { let _ = f.sync_all(); }
            metrics.record(t0.elapsed(), block_size);
        }
        ops_done += 1;
    }
}

// ── File openers (platform-specific) ─────────────────────────────────────────

/// Open an existing file for read-write without truncation.
pub fn open_rw(path: &Path, unbuffered: bool) -> std::io::Result<File> {
    platform::open(path, true, unbuffered)
}

/// Open an existing file for reading.
pub fn open_ro(path: &Path, unbuffered: bool) -> std::io::Result<File> {
    platform::open(path, false, unbuffered)
}

#[cfg(target_os = "linux")]
mod platform {
    use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt, path::Path};
    use std::fs::File;

    pub fn open(path: &Path, write: bool, unbuffered: bool) -> std::io::Result<File> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(write);
        if unbuffered {
            // O_DIRECT requires the buffer address, offset, and length to be
            // aligned to the logical block size (typically 512 B or 4 KiB).
            // AlignedBuf guarantees 4 KiB alignment; the caller must use a
            // block size that is a multiple of 512 B.
            opts.custom_flags(libc::O_DIRECT);
        }
        opts.open(path)
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::{fs::OpenOptions, os::unix::io::AsRawFd, path::Path};
    use std::fs::File;

    pub fn open(path: &Path, write: bool, unbuffered: bool) -> std::io::Result<File> {
        let file = OpenOptions::new().read(true).write(write).open(path)?;
        if unbuffered {
            // F_NOCACHE disables the unified buffer cache for this fd without
            // alignment requirements, making it simpler than O_DIRECT.
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1i32) };
        }
        Ok(file)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use std::{fs::OpenOptions, path::Path};
    use std::fs::File;

    pub fn open(path: &Path, write: bool, _unbuffered: bool) -> std::io::Result<File> {
        OpenOptions::new().read(true).write(write).open(path)
    }
}

// ── Aligned buffer ────────────────────────────────────────────────────────────

/// A buffer aligned to 4 KiB, required for O_DIRECT I/O.
///
/// Pre-filled with random bytes to avoid zero-page folding optimisations.
pub struct AlignedBuf {
    raw: Vec<u8>,
    start: usize,
    len: usize,
}

#[allow(dead_code)]
impl AlignedBuf {
    pub const ALIGN: usize = 4096;

    pub fn new(size: usize) -> Self {
        let mut raw = vec![0u8; size + Self::ALIGN];
        let ptr = raw.as_ptr() as usize;
        let start = if ptr % Self::ALIGN == 0 {
            0
        } else {
            Self::ALIGN - (ptr % Self::ALIGN)
        };
        // Fill the aligned region with random data to avoid zero-page folding.
        rand::thread_rng().fill_bytes(&mut raw[start..start + size]);
        Self { raw, start, len: size }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.raw[self.start..self.start + self.len]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.raw[self.start..self.start + self.len]
    }

    /// Return a const pointer to the start of the aligned buffer region.
    pub fn as_ptr(&self) -> *const u8 {
        self.raw[self.start..].as_ptr()
    }

    /// Return a mutable pointer to the start of the aligned buffer region.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.raw[self.start..].as_mut_ptr()
    }

    pub fn len(&self) -> usize {
        self.len
    }
}
