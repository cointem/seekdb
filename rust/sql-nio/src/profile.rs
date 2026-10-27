// Copyright (c) 2025 OceanBase.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Optional runtime diagnostics. No timers, logging or timestamp reads when disabled.
use std::sync::{
    atomic::{AtomicU64, Ordering},
    LazyLock,
};
use std::time::{Duration, Instant};

struct Profile {
    start: Instant,
    commits: AtomicU64,
    full: AtomicU64,
    partial: AtomicU64,
    fallback: AtomicU64,
    errors: AtomicU64,
    writes: AtomicU64,
    write_ns: AtomicU64,
    write_calls: AtomicU64,
    wait_ns: AtomicU64,
    wait_max_ns: AtomicU64,
    wait_hist: [AtomicU64; 32],
    force_fallback: bool,
}
static PROFILE: LazyLock<Option<Profile>> = LazyLock::new(|| {
    if std::env::var("SEEKDB_NIO_PROFILE").as_deref() != Ok("1") {
        return None;
    }
    // Never rely on exit hooks: seekdb may terminate without Rust destructors.
    let _ = std::thread::Builder::new().name("nio-profile".into()).spawn(|| loop {
        std::thread::sleep(Duration::from_secs(1));
        if let Some(p) = PROFILE.as_ref() {
            let histogram: Vec<u64> = p.wait_hist.iter().map(|v| v.load(Ordering::Relaxed)).collect();
            eprintln!("SEEKDB_NIO_PROFILE {{\"commits\":{},\"fast_full\":{},\"fast_partial\":{},\"fallback\":{},\"errors\":{},\"first_writes\":{},\"commit_to_write_ns\":{},\"commit_to_write_max_ns\":{},\"wait_log2_ns\":{:?},\"socket_flush_calls\":{},\"socket_flush_ns\":{}}}",
                p.commits.load(Ordering::Relaxed), p.full.load(Ordering::Relaxed),
                p.partial.load(Ordering::Relaxed), p.fallback.load(Ordering::Relaxed),
                p.errors.load(Ordering::Relaxed), p.writes.load(Ordering::Relaxed),
                p.wait_ns.load(Ordering::Relaxed), p.wait_max_ns.load(Ordering::Relaxed), histogram, p.write_calls.load(Ordering::Relaxed), p.write_ns.load(Ordering::Relaxed));
        }
    });
    Some(Profile {
        start: Instant::now(),
        commits: AtomicU64::new(0),
        full: AtomicU64::new(0),
        partial: AtomicU64::new(0),
        fallback: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        writes: AtomicU64::new(0),
        write_ns: AtomicU64::new(0),
        write_calls: AtomicU64::new(0),
        wait_ns: AtomicU64::new(0),
        wait_max_ns: AtomicU64::new(0),
        wait_hist: std::array::from_fn(|_| AtomicU64::new(0)),
        force_fallback: std::env::var("SEEKDB_NIO_PROFILE_FORCE_FALLBACK").as_deref() == Ok("1"),
    })
});
pub(crate) fn commit_started() -> u64 {
    PROFILE.as_ref().map_or(0, |p| {
        p.commits.fetch_add(1, Ordering::Relaxed);
        p.start.elapsed().as_nanos() as u64 + 1
    })
}
pub(crate) fn write_started(stamp: &mut u64) -> u64 {
    let Some(p) = PROFILE.as_ref() else {
        return 0;
    };
    let now = p.start.elapsed().as_nanos() as u64 + 1;
    if *stamp != 0 {
        let ns = now.saturating_sub(*stamp);
        p.writes.fetch_add(1, Ordering::Relaxed);
        p.wait_ns.fetch_add(ns, Ordering::Relaxed);
        p.wait_max_ns.fetch_max(ns, Ordering::Relaxed);
        p.wait_hist[(63 - ns.max(1).leading_zeros() as usize).min(31)]
            .fetch_add(1, Ordering::Relaxed);
        *stamp = 0;
    }
    now
}
pub(crate) fn write_finished(start: u64) {
    if start != 0 {
        if let Some(p) = PROFILE.as_ref() {
            p.write_calls.fetch_add(1, Ordering::Relaxed);
            p.write_ns.fetch_add(
                (p.start.elapsed().as_nanos() as u64 + 1).saturating_sub(start),
                Ordering::Relaxed,
            );
        }
    }
}
pub(crate) fn force_fallback() -> bool {
    PROFILE.as_ref().is_some_and(|p| p.force_fallback)
}
pub(crate) fn full() {
    if let Some(p) = PROFILE.as_ref() {
        p.full.fetch_add(1, Ordering::Relaxed);
    }
}
pub(crate) fn partial() {
    if let Some(p) = PROFILE.as_ref() {
        p.partial.fetch_add(1, Ordering::Relaxed);
    }
}
pub(crate) fn fallback() {
    if let Some(p) = PROFILE.as_ref() {
        p.fallback.fetch_add(1, Ordering::Relaxed);
    }
}
pub(crate) fn error() {
    if let Some(p) = PROFILE.as_ref() {
        p.errors.fetch_add(1, Ordering::Relaxed);
    }
}
