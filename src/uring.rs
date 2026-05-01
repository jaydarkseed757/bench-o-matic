//! io_uring-based async I/O worker (Linux only).
//!
//! Uses the completion-refill pattern: a pool of buffer slots is maintained,
//! free slots are submitted as SQEs, and completions are harvested from the CQ.

use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::time::Instant;

use io_uring::{opcode, types::Fd, IoUring};
use rand::Rng;

use crate::cli::{Access, Op};
use crate::metrics::Metrics;
use crate::worker::{AlignedBuf, Limit, open_ro, open_rw};

/// Run an io_uring-backed worker.
///
/// On failure to create the ring, returns the error so the caller can fall
/// back to the sync engine.
pub fn run_uring_worker(
    files: &[PathBuf],
    access: Access,
    op: Op,
    queue_depth: usize,
    block_size: usize,
    limit: Limit,
    metrics: &Metrics,
    file_size: u64,
) -> std::io::Result<()> {
    let mut ring = IoUring::new(queue_depth as u32)?;
    let num_blocks = (file_size / block_size as u64).max(1);
    let mut rng = rand::thread_rng();

    // Pre-allocate one AlignedBuf and one File per slot.
    let mut bufs: Vec<AlignedBuf> = (0..queue_depth).map(|_| AlignedBuf::new(block_size)).collect();

    // Open one file per slot (round-robin from the file list).
    let file_handles: Vec<std::fs::File> = (0..queue_depth)
        .map(|slot| {
            let path = &files[slot % files.len()];
            match op {
                Op::Read => open_ro(path, false),
                Op::Write | Op::Mixed(_) => open_rw(path, false),
            }
            .unwrap_or_else(|e| panic!("[ERROR] uring open {:?}: {}", path, e))
        })
        .collect();

    let fds: Vec<i32> = file_handles.iter().map(|f| f.as_raw_fd()).collect();

    // Track which slots are free and per-slot start times.
    let mut free_slots: Vec<usize> = (0..queue_depth).collect();
    let mut slot_start_times: Vec<Option<Instant>> = vec![None; queue_depth];
    // Per-slot: is this a read or a write? (needed to interpret completion).
    let mut slot_is_read: Vec<bool> = vec![false; queue_depth];

    let mut ops_done: u64 = 0;
    let mut in_flight: usize = 0;

    loop {
        // Submit free slots to the SQ.
        while !free_slots.is_empty() && !limit.is_done(ops_done + in_flight as u64) {
            let slot = free_slots.pop().unwrap();

            let is_read = match op {
                Op::Read => true,
                Op::Write => false,
                Op::Mixed(pct) => rng.gen_range(0..100u8) < pct,
            };
            slot_is_read[slot] = is_read;

            let offset = if access == Access::Sequential {
                // Sequential: advance per-slot.  Simple wraparound.
                (ops_done + in_flight as u64) * block_size as u64 % (num_blocks * block_size as u64)
            } else {
                rng.gen_range(0..num_blocks) * block_size as u64
            };

            let fd = Fd(fds[slot]);
            let len = block_size as u32;

            let sqe = if is_read {
                let buf_ptr = bufs[slot].as_mut_ptr();
                // SAFETY: slot is not in free_slots while this op is in-flight;
                // buf lives for the function duration and is not aliased.
                unsafe {
                    opcode::Read::new(fd, buf_ptr, len)
                        .offset(offset)
                        .build()
                        .user_data(slot as u64)
                }
            } else {
                let buf_ptr = bufs[slot].as_ptr();
                // SAFETY: slot is not in free_slots while this op is in-flight;
                // buf lives for the function duration and is not aliased.
                unsafe {
                    opcode::Write::new(fd, buf_ptr, len)
                        .offset(offset)
                        .build()
                        .user_data(slot as u64)
                }
            };

            slot_start_times[slot] = Some(Instant::now());

            // SAFETY: SQE is valid and points to our bufs which outlive the ring.
            unsafe {
                ring.submission().push(&sqe).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "SQ full")
                })?;
            }
            in_flight += 1;
        }

        if in_flight == 0 {
            break;
        }

        // Wait for at least one completion.
        ring.submit_and_wait(1)?;

        // Drain all available completions.
        let mut cq = ring.completion();
        for cqe in cq.by_ref() {
            let slot = cqe.user_data() as usize;
            let res = cqe.result();

            if let Some(t0) = slot_start_times[slot].take() {
                if res > 0 {
                    metrics.record(t0.elapsed(), res as usize);
                } else if res < 0 {
                    metrics.record_error();
                }
            }

            free_slots.push(slot);
            in_flight -= 1;
            ops_done += 1;

            if limit.is_done(ops_done) {
                break;
            }
        }
    }

    Ok(())
}
