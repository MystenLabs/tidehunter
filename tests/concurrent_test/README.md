# Tidehunter concurrent test

`concurrent_test` drives Tidehunter's public API from multiple client threads against
overlapping keys. It uses per-key locks plus an in-memory shadow model to check that
concurrent inserts, deletes, reads, restarts, relocation, flat promotion, and secondary
key-space operations stay consistent.

## Local run

```bash
cargo run -p concurrent_test --release
```

For a shorter non-interactive run:

```bash
CONCURRENT_TEST_OPS_PER_THREAD=2000 NO_PROGRESS=1 cargo run -p concurrent_test --release
```

## Antithesis SDK build

The `antithesis_sdk` feature (the implicit feature of the optional `antithesis_sdk`
dependency) wires the workload to Antithesis assertions, lifecycle events, and
randomness:

```bash
cargo build -p concurrent_test --release --features concurrent_test/antithesis_sdk
```

In an Antithesis runtime, set `ANTITHESIS_OUTPUT_DIR`; the workload then uses
`AntithesisRng`, emits `setup_complete()`, records named Always assertions for
correctness checks, and skips the local `lsof` file-descriptor check.

## Environment knobs

- `CONCURRENT_TEST_THREADS`: worker thread count, default `8`.
- `CONCURRENT_TEST_OPS_PER_THREAD`: operations per worker, default `320000`.
- `CONCURRENT_TEST_ROOT`: optional database root. The workload wipes this directory
  on startup; in Docker it should point to a subdirectory such as `/data/run`, not
  the volume mountpoint. If unset, the workload uses a fresh tempdir.
- `NO_PROGRESS`: hide progress bars when set.

## Scope

This workload tests concurrency under fault injection. Its oracle is an in-memory shadow
model, so a process kill loses the oracle and the next process boot starts from a
clean database root. It does not assert crash recovery or cross-process
durability; that remains the responsibility of the persisted `antithesis_harness`
workload.
