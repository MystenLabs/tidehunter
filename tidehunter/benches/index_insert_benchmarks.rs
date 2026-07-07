// Benchmarks `IndexTable::insert` for unique keys — the write hot path that
// lands new entries in the in-memory `data` overlay before `promote_to_flat`.
//
// Why this exists: a regression was introduced when the `data` overlay was a
// `BTreeSet<(key, pos)>`, where `checked_insert` did a `data_get_latest` range
// probe (a tree descent plus two key clones for the range bounds) followed by a
// separate set insert — roughly double the tree work of a single `entry()`
// descent. The overlay is now a `BTreeMap<key, positions>` whose insert is one
// `entry()` descent again. This bench isolates the per-insert cost so the
// regression stays fixed (a guard against reintroducing the double descent).
//
// The same source file compiles on both `main` and this branch (it only uses
// the stable `test_utils::IndexTable` / `WalPosition` API), so it can be run in
// each tree against a shared criterion baseline for a direct comparison.
//
// Run with:
//   cargo bench --features test-utils --bench index_insert_benchmarks

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use tidehunter::WalPosition;
use tidehunter::minibytes::Bytes;
use tidehunter::test_utils::IndexTable;

// Fixed seed so `main` and this branch insert the identical random key set.
const SEED: u64 = 0x7106_8077_E847_0001;

// Entry counts inserted per measured iteration. 1k stays in the
// constant-overhead regime (the extra clones/probe dominate); 100k makes the
// doubled O(log n) tree descent visible.
const SIZES: &[usize] = &[1_000, 10_000, 100_000];

/// 32-byte sequential big-endian keys. Cache-friendly insert order (low
/// variance) that isolates the per-op overhead of the second tree traversal.
fn sequential_keys(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let mut k = vec![0u8; 32];
            k[24..].copy_from_slice(&(i as u64).to_be_bytes());
            k
        })
        .collect()
}

/// 32-byte random keys (representative of hashed keys); deterministic via `SEED`
/// so both trees measure the identical key set. With 256-bit keys, collisions
/// are not a practical concern, so the set is unique without an explicit dedup.
fn random_keys(n: usize) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(SEED);
    (0..n)
        .map(|_| {
            let mut k = vec![0u8; 32];
            rng.fill_bytes(&mut k);
            k
        })
        .collect()
}

/// Time inserting all `n` unique keys into a freshly-emptied `IndexTable`,
/// reporting ns/insert via criterion's element throughput.
fn bench_insert(c: &mut Criterion, label: &str, gen_keys: fn(usize) -> Vec<Vec<u8>>) {
    let mut group = c.benchmark_group(format!("index_insert_{label}"));
    for &n in SIZES {
        let master = gen_keys(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &master, |b, master| {
            b.iter_batched(
                || {
                    // Untimed setup: a fresh empty table plus fresh,
                    // uniquely-owned keys so `checked_insert`'s `into_owned()`
                    // is a no-op — no key memcpy in the timed region (the same
                    // on both branches, but it keeps the signal on the tree ops).
                    let table = IndexTable::default();
                    let keys: Vec<Bytes> = master.iter().map(|k| Bytes::from(k.clone())).collect();
                    (table, keys)
                },
                |(mut table, keys)| {
                    // Strictly increasing WAL offsets; with unique keys each
                    // insert takes the vacant path (no prior position to update).
                    let mut off = 1u64;
                    for k in keys {
                        table.insert(k, WalPosition::test_value(off));
                        off += 1;
                    }
                    black_box(table)
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_index_insert(c: &mut Criterion) {
    bench_insert(c, "sequential", sequential_keys);
    bench_insert(c, "random", random_keys);
}

criterion_group!(benches, bench_index_insert);
criterion_main!(benches);
