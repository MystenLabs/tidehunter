// Benchmarks the IndexTable full-scan paths (`keys`, `iter`) that merge the
// flat array with the data overlay. These exercise `iter_combined` and are the
// paths affected by the zero-copy flat-key change (slice_to_bytes vs
// Bytes::from(k.to_vec())) and the removal of the per-flat-element
// `data_contains_key` membership probe.
//
// Run with:
//   cargo bench --features test-utils --bench index_iter_benchmarks

use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use tidehunter::key_shape::{KeyIndexing, KeyShape, KeySpaceDesc, KeyType};
use tidehunter::minibytes::Bytes;
use tidehunter::test_utils::IndexTable;
use tidehunter::{LastProcessed, WalPosition};

/// 32-byte big-endian key so the natural byte order matches the numeric order
/// (serialize/deserialize require sorted, unique keys).
fn make_key(i: usize) -> Bytes {
    let mut k = vec![0u8; 32];
    k[24..].copy_from_slice(&(i as u64).to_be_bytes());
    Bytes::from(k)
}

/// Build a flat-backed IndexTable with `n_flat` entries in `flat` and
/// `n_overlay` entries in the data overlay (colliding with a spread-out subset
/// of the flat keys, so the merge hits the data-shadows-flat tie path).
fn build_table(ks: &KeySpaceDesc, n_flat: usize, n_overlay: usize) -> IndexTable {
    // Stage the entries in `data`, serialize, then deserialize so they land in
    // `flat` (the on-disk blob path), matching a freshly loaded index.
    let mut seed = IndexTable::default();
    for i in 0..n_flat {
        seed.insert(make_key(i), WalPosition::test_value((i as u64) + 1));
    }
    let mut out = BytesMut::new();
    seed.serialize_index_entries(ks, &mut out);
    let mut table = IndexTable::deserialize_index_entries(ks, Bytes::from(out.to_vec()));

    if n_overlay > 0 {
        let step = (n_flat / n_overlay).max(1);
        for j in 0..n_overlay {
            let i = j * step;
            if i >= n_flat {
                break;
            }
            // Higher offset than the flat copy so it is the live position.
            table.insert(
                make_key(i),
                WalPosition::test_value(1_000_000_000 + j as u64),
            );
        }
    }
    table
}

/// Benchmark the `keys`/`iter` scans for one keyspace's index layout. `label`
/// distinguishes the criterion group (`index_iter_fixed32` vs
/// `index_iter_varlen`) so baselines stay comparable per layout.
fn bench_keyspace(c: &mut Criterion, label: &str, ks: &KeySpaceDesc) {
    let mut group = c.benchmark_group(format!("index_iter_{label}"));
    // (flat_entries, overlay_entries)
    for &(n_flat, n_overlay) in &[(10_000usize, 100usize), (100_000usize, 1_000usize)] {
        let table = build_table(ks, n_flat, n_overlay);

        group.bench_with_input(
            BenchmarkId::new("keys", format!("{n_flat}+{n_overlay}")),
            &table,
            |b, table| {
                b.iter(|| {
                    let mut acc = 0usize;
                    for k in table.keys() {
                        acc += k.len();
                    }
                    black_box(acc)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("iter", format!("{n_flat}+{n_overlay}")),
            &table,
            |b, table| {
                b.iter(|| {
                    let mut acc = 0u64;
                    for (k, p) in table.iter() {
                        acc += k.len() as u64;
                        black_box(p);
                    }
                    black_box(acc)
                });
            },
        );
    }
    group.finish();
}

fn bench_index_iter(c: &mut Criterion) {
    // Fixed-length keys are the common case; varlen exercises the offset-table
    // flat format. Both keys are 32 bytes (see `make_key`).
    let layouts = [
        ("fixed32", KeyIndexing::fixed(32)),
        ("varlen", KeyIndexing::variable_length()),
    ];
    for (label, key_indexing) in layouts {
        let shape = KeyShape::new_single_config_indexing(
            key_indexing,
            16,
            KeyType::uniform(16),
            Default::default(),
        );
        bench_keyspace(c, label, shape.iter_ks().next().unwrap());
    }
}

/// Build a table with `n_flat` flat entries (even key indices) interleaved with
/// `n_data` data-overlay entries (odd key indices), all at offsets below the
/// frontier used by the `next_entry_at` bench so every entry is "processed".
/// The disjoint even/odd split makes a full scan visit every entry, so
/// `data_next_forward_at`/`data_next_reverse_at` are exercised once per step.
fn build_interleaved(ks: &KeySpaceDesc, n_flat: usize, n_data: usize) -> IndexTable {
    let mut seed = IndexTable::default();
    for i in 0..n_flat {
        seed.insert(make_key(2 * i), WalPosition::test_value((i as u64) + 1));
    }
    let mut out = BytesMut::new();
    seed.serialize_index_entries(ks, &mut out);
    let mut table = IndexTable::deserialize_index_entries(ks, Bytes::from(out.to_vec()));
    for j in 0..n_data {
        table.insert(
            make_key(2 * j + 1),
            WalPosition::test_value(1_000_000_000 + j as u64),
        );
    }
    table
}

/// Drive a full `next_entry_at` scan in one direction, returning the count.
fn scan_at(table: &IndexTable, reverse: bool, lp: LastProcessed) -> usize {
    let mut count = 0usize;
    let mut prev: Option<Bytes> = None;
    while let Some((k, p)) = table.next_entry_at(prev, reverse, lp) {
        black_box(p);
        count += 1;
        prev = Some(k);
    }
    count
}

fn bench_next_entry_at(c: &mut Criterion) {
    // Fixed-length keys (the common case) for the as-of iteration path.
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::fixed(32),
        16,
        KeyType::uniform(16),
        Default::default(),
    );
    let ks = shape.iter_ks().next().unwrap();
    // Frontier above every offset used in `build_interleaved`, so all entries
    // are processed (no skipping) — isolates the per-step overlay cost.
    let lp = LastProcessed::from_u64_for_test(u64::MAX);

    let mut group = c.benchmark_group("next_entry_at");
    // (flat_entries, data_entries)
    for &(n_flat, n_data) in &[(10_000usize, 200usize), (200usize, 10_000usize)] {
        let table = build_interleaved(ks, n_flat, n_data);
        for (dir_label, reverse) in [("forward", false), ("backward", true)] {
            group.bench_with_input(
                BenchmarkId::new(dir_label, format!("{n_flat}flat+{n_data}data")),
                &table,
                |b, table| b.iter(|| black_box(scan_at(table, reverse, lp))),
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_index_iter, bench_next_entry_at);
criterion_main!(benches);
