# Antithesis Harness

This package is a standalone process-level Tidehunter harness intended for
containerized Antithesis runs. It does not link through Sui. The binary opens a
small Tidehunter database, drives randomized public API operations, and checks
each result against an in-memory model.

For external process-kill durability, values are self-validating: each value
embeds the key, version, and checksum needed to verify it without an in-memory
oracle. The harness also writes an atomic high-water-mark file after checkpoints.
On restart it reopens the same database path, scans recovered data, and verifies
all checkpointed durable records are still present and valid.

Real process kill, disk, and thread-pausing fault injection should run only under
Antithesis, scheduled from `sui-operations`. Local CI should only do a short
non-fault smoke run to make sure the harness builds and the in-process checks
still pass.

Covered API surface:

- `Db::open`
- `insert`, `remove`, `get`, `exists`
- ordered iterators, including bounded and reverse scans
- `write_batch`
- `rebuild_control_region`, `force_rebuild_control_region`
- `create_state_snapshot`, `restore_state_snapshot`
- WAL and index relocation entrypoints

Run locally:

```bash
cargo run --manifest-path tests/antithesis_harness/Cargo.toml --release
```

Short smoke run:

```bash
TIDEHUNTER_ANTITHESIS_OPS=1000 \
  cargo run --manifest-path tests/antithesis_harness/Cargo.toml
```

Build the container from the repository root:

```bash
docker build -f tests/antithesis_harness/Dockerfile -t tidehunter-antithesis .
```

Useful environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `TIDEHUNTER_ANTITHESIS_ROOT` | temp dir | Root directory for DB and saved snapshots |
| `TIDEHUNTER_ANTITHESIS_KEEP_DB` | unset | Keep harness DB files after exit |
| `TIDEHUNTER_ANTITHESIS_SEED` | fixed seed | Random operation stream seed |
| `TIDEHUNTER_ANTITHESIS_OPS` | `20000` locally, `100000` in Docker | Operation count |
| `TIDEHUNTER_ANTITHESIS_KEYS` | `192` | Logical key domain per keyspace |
| `TIDEHUNTER_ANTITHESIS_VERIFY_EVERY` | `250` | Full model verification cadence |
| `TIDEHUNTER_ANTITHESIS_MAX_SNAPSHOTS` | `8` | Retained state snapshots |

The state snapshot restore path currently only restores snapshots whose saved WAL
position is still inside WAL file 0, matching the current implementation in
`state_snapshot.rs`.

When `ANTITHESIS_OUTPUT_DIR` is set, the binary expects to be built with
`--features sdk`. In that mode it initializes the Antithesis SDK, emits
`setup_complete()` after recovery scanning, and uses Antithesis-controlled
randomness instead of the fixed local seed.
