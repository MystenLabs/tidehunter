# Tidehunter Antithesis Semantics Workload

This workload exercises Tidehunter behavior that can run inside Antithesis
without LazyFS:

- write batch atomicity and last-write-wins ordering
- forward, reverse, bounded, and checkpoint iterators
- relocation-filter pruning for ordered checkpoint-style keys

It intentionally deletes its root directory on startup. This is not a crash
recovery harness; it keeps an in-memory oracle and verifies the database inside
one process run.

## Local run

```sh
SEMANTICS_TEST_OPS=1000 cargo run --release
```

Build with Antithesis SDK instrumentation:

```sh
cargo build --release --features sdk
```

## Environment

- `SEMANTICS_TEST_ROOT`: database root. Defaults to a temp directory.
- `SEMANTICS_TEST_OPS`: randomized batch and iterator operations. Defaults to
  `20000`.
- `SEMANTICS_TEST_SEED`: local deterministic seed. Ignored by Antithesis RNG.
- `SEMANTICS_TEST_KEYS`: key domain for random batch and iterator operations.
- `SEMANTICS_TEST_VERIFY_EVERY`: full iterator verification cadence.
- `SEMANTICS_TEST_RELOCATION_EPOCHS`: ordered epochs used by the pruning test.
- `SEMANTICS_TEST_RELOCATION_KEYS_PER_EPOCH`: keys written per epoch.
- `SEMANTICS_TEST_RELOCATION_VALUE_BYTES`: value payload size for relocation.
  The strict pruning assertion needs the old epoch prefix to span reclaimable WAL
  files; if these relocation knobs are set too small, the workload skips that
  assertion instead of reporting a false failure.

When `ANTITHESIS_OUTPUT_DIR` is present the binary requires `--features sdk`.

## Antithesis assertions

Always assertions:

- `semantics_batch_key_matches_model`
- `semantics_iterator_matches_model`
- `semantics_checkpoint_iterator_stable`
- `semantics_relocation_prunes_old_keys`
- `semantics_relocation_keeps_live_keys`

Relocation pruning is verified with point reads across the finite key domain.
Iterator correctness is covered by the dedicated iterator keyspace before
relocation.

Known checkpoint issue: `semantics_checkpoint_iterator_stable` currently catches
a Tidehunter `DbCheckpoint` stale-read bug under background flushing. The
checkpoint can return an older flushed value instead of the latest
pre-checkpoint overwrite. Keep this assertion strict; hold or expect-fail the
semantics Antithesis campaign until Tidehunter fixes the checkpoint read path.

Sometimes assertions:

- `semantics_batch_committed`
- `semantics_batch_repeated_key`
- `semantics_iterator_forward`
- `semantics_iterator_reverse`
- `semantics_iterator_bounded`
- `semantics_checkpoint_iterator`
- `semantics_relocation_ran`
- `semantics_pruning_verified`
