pub(crate) mod control_region;
pub(crate) mod db_fns;
pub(crate) mod util;
pub(crate) mod verify;
pub(crate) mod wal;

use parking_lot::Mutex;
use rhai::{Dynamic, Engine, EvalAltResult, Scope};
use std::path::PathBuf;
use std::sync::Arc;
use tidehunter::config::Config;
use tidehunter::db::{Db, DbResult};
use tidehunter::key_shape::{KeyShape, KeyShapeBuilder, KeySpace, KeyType};
use tidehunter::test_utils::{Metrics, WalEntry};

// ---------------------------------------------------------------------------
// Shared console state (engine-level)
// ---------------------------------------------------------------------------

pub struct ConsoleContext {
    /// Output sink used by the engine's print/debug hooks.
    /// Defaults to `println!`; override in tests to capture output.
    pub print_fn: Arc<dyn Fn(&str) + Send + Sync>,
}

impl Default for ConsoleContext {
    fn default() -> Self {
        Self {
            print_fn: Arc::new(|s| println!("{s}")),
        }
    }
}

// ---------------------------------------------------------------------------
// DbState — per-database state held inside a DbHandle
// ---------------------------------------------------------------------------

pub struct DbState {
    pub db_path: PathBuf,
    pub config: Config,
    pub key_shape: KeyShape,
    /// Live database handle; `None` until the first CRUD operation triggers WAL replay.
    pub db: Option<Arc<Db>>,
    /// Output sink captured from `ConsoleContext` at `open()` time.
    pub print_fn: Arc<dyn Fn(&str) + Send + Sync>,
}

// ---------------------------------------------------------------------------
// DbHandle — Rhai-visible type returned by open()
// ---------------------------------------------------------------------------

/// A handle to an open TideHunter database returned by `open()`.
/// All query and inspection methods are called on this handle.
/// Multiple handles can coexist in the same session.
///
/// Example:
/// ```rhai
/// let db  = open("/data/db1");
/// let db2 = open("/data/db2");
/// db.scan("objects", |k, v| { print(k); });
/// ```
#[derive(Clone)]
pub struct DbHandle(pub Arc<Mutex<DbState>>);

impl std::fmt::Display for DbHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.0.lock();
        write!(f, "DbHandle({})", state.db_path.display())
    }
}

impl DbHandle {
    /// Ensure the database is open (triggering WAL replay if needed) and return the `Arc<Db>`.
    pub(crate) fn require_db(&self) -> Result<Arc<Db>, Box<EvalAltResult>> {
        let mut state = self.0.lock();
        if state.db.is_none() {
            let db_path = state.db_path.clone();
            (state.print_fn)("Opening database (replaying WAL, this may take a while)...");
            let key_shape = state.key_shape.clone();
            let config = state.config.clone();
            let db = Db::open(&db_path, key_shape, Arc::new(config), Metrics::new()).map_err(
                |e| -> Box<EvalAltResult> { format!("Failed to open database: {e:?}").into() },
            )?;
            state.db = Some(db);
        }
        Ok(state.db.clone().unwrap())
    }

    /// Ensure the database is open (triggering WAL replay if needed) and
    /// resolve `ks` to a `KeySpace`.  Returns `(db_arc, key_space)`.
    pub(crate) fn require_db_and_ks(
        &self,
        ks: Dynamic,
    ) -> Result<(Arc<Db>, KeySpace), Box<EvalAltResult>> {
        let db = self.require_db()?;
        let state = self.0.lock();
        let ks = resolve_ks(&state, ks)?;
        Ok((db, ks))
    }
}

// ---------------------------------------------------------------------------
// Entry — the Rhai-visible WAL entry type
// ---------------------------------------------------------------------------

/// Represents a single WAL entry exposed to Rhai scripts.
#[derive(Clone, Debug)]
pub struct Entry {
    /// "record" | "remove" | "index" | "batch_start" | "drop_cells"
    pub entry_type: String,
    /// Keyspace ID (u8 widened to i64 for Rhai)
    pub keyspace: i64,
    /// Key bytes (hex-encoded lazily when accessed from Rhai)
    pub key: Vec<u8>,
    /// Value bytes (hex-encoded lazily when accessed from Rhai; empty for non-records)
    pub value: Vec<u8>,
    /// Value length in bytes
    pub value_len: i64,
    /// Byte offset of this frame in the WAL
    pub position: i64,
    /// Total frame size (including CRC header) in bytes
    pub raw_size: i64,
}

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Entry {{ type: {}, ks: {}, key: {}, value_len: {}, pos: {:#x} }}",
            self.entry_type,
            self.keyspace,
            hex::encode(&self.key),
            self.value_len,
            self.position
        )
    }
}

pub(crate) fn from_wal_entry(entry: WalEntry, offset: u64, raw_size: usize) -> Entry {
    let position = offset as i64;
    let raw_size = raw_size as i64;
    match entry {
        WalEntry::Record(ks, key, value, _relocated) => Entry {
            entry_type: "record".into(),
            keyspace: ks.as_u8() as i64,
            value_len: value.len() as i64,
            key: key.to_vec(),
            value: value.to_vec(),
            position,
            raw_size,
        },
        WalEntry::Remove(ks, key) => Entry {
            entry_type: "remove".into(),
            keyspace: ks.as_u8() as i64,
            key: key.to_vec(),
            value: vec![],
            value_len: 0,
            position,
            raw_size,
        },
        WalEntry::Index(ks, _data) => Entry {
            entry_type: "index".into(),
            keyspace: ks.as_u8() as i64,
            key: vec![],
            value: vec![],
            value_len: 0,
            position,
            raw_size,
        },
        WalEntry::BatchStart(size) => Entry {
            entry_type: "batch_start".into(),
            keyspace: 0,
            key: vec![],
            value: vec![],
            value_len: size as i64,
            position,
            raw_size,
        },
        WalEntry::DropCells(ks, _from, _to) => Entry {
            entry_type: "drop_cells".into(),
            keyspace: ks.as_u8() as i64,
            key: vec![],
            value: vec![],
            value_len: 0,
            position,
            raw_size,
        },
        WalEntry::CompressedBatch(_codec, uncompressed_len, _body) => Entry {
            entry_type: "compressed_batch".into(),
            keyspace: 0,
            key: vec![],
            value: vec![],
            value_len: uncompressed_len as i64,
            position,
            raw_size,
        },
    }
}

// ---------------------------------------------------------------------------
// Helpers used by db_fns / wal / control_region
// ---------------------------------------------------------------------------

/// Decode a hex string to bytes, returning an EvalAltResult on failure.
pub(crate) fn decode_hex(s: &str) -> Result<Vec<u8>, Box<rhai::EvalAltResult>> {
    hex::decode(s).map_err(|e| -> Box<rhai::EvalAltResult> {
        format!("Invalid hex string \"{s}\": {e}").into()
    })
}

/// Map a DbResult to an EvalAltResult.
pub(crate) fn db_err<T>(r: DbResult<T>) -> Result<T, Box<rhai::EvalAltResult>> {
    r.map_err(|e| -> Box<rhai::EvalAltResult> { format!("Database error: {e:?}").into() })
}

/// Resolve a Rhai `Dynamic` value to a `KeySpace` using the given `DbState`.
/// Accepts an integer keyspace ID (`i64`) or a keyspace name (`String`).
pub(crate) fn resolve_ks(state: &DbState, ks: Dynamic) -> Result<KeySpace, Box<EvalAltResult>> {
    if ks.is::<i64>() {
        return Ok(KeySpace::new(ks.cast::<i64>() as u8));
    }
    if ks.is::<String>() {
        let name = ks.cast::<String>();
        return state
            .key_shape
            .iter_ks()
            .find(|ksd| ksd.name() == name)
            .map(|ksd| ksd.id())
            .ok_or_else(|| -> Box<EvalAltResult> {
                format!("Unknown keyspace \"{name}\"").into()
            });
    }
    Err("ks must be an integer ID or a name string".into())
}

// ---------------------------------------------------------------------------
// Engine factory
// ---------------------------------------------------------------------------

pub fn create_engine(ctx: Arc<Mutex<ConsoleContext>>) -> Engine {
    let mut engine = Engine::new();

    // --- Register Entry type ---
    engine.register_type_with_name::<Entry>("Entry");
    engine.register_get("entry_type", |e: &mut Entry| e.entry_type.clone());
    engine.register_get("keyspace", |e: &mut Entry| e.keyspace);
    engine.register_get("key", |e: &mut Entry| hex::encode(&e.key));
    engine.register_get("value", |e: &mut Entry| hex::encode(&e.value));
    engine.register_get("value_len", |e: &mut Entry| e.value_len);
    engine.register_get("position", |e: &mut Entry| e.position);
    engine.register_get("raw_size", |e: &mut Entry| e.raw_size);
    engine.register_fn("to_string", |e: &mut Entry| e.to_string());
    engine.register_fn("to_debug", |e: &mut Entry| format!("{e:?}"));

    // --- Register DbHandle type ---
    engine.register_type_with_name::<DbHandle>("DbHandle");
    engine.register_fn("to_string", |h: &mut DbHandle| h.to_string());

    // --- open(path) → DbHandle ---
    // Loads config and key shape only; WAL replay is deferred until the first CRUD operation.
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "open",
            move |path: &str| -> Result<DbHandle, Box<EvalAltResult>> {
                let db_path = PathBuf::from(path);
                let config = Db::load_config(&db_path).unwrap_or_default();
                let key_shape =
                    Db::load_key_shape(&db_path).map_err(|e| -> Box<EvalAltResult> {
                        format!("Failed to load key shape: {e:?}").into()
                    })?;
                let print_fn = ctx.lock().print_fn.clone();
                (print_fn)(&format!(
                    "Loaded {path} (WAL replay deferred until first query)"
                ));
                Ok(DbHandle(Arc::new(Mutex::new(DbState {
                    db_path,
                    config,
                    key_shape,
                    db: None,
                    print_fn,
                }))))
            },
        );
    }

    // --- create_db(path, [{name, key_size}, ...]) → DbHandle ---
    // Creates a new database with the given keyspaces and returns an open handle.
    // Unlike open(), WAL replay is not needed because the database is freshly created.
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "create_db",
            move |path: &str, ks_specs: rhai::Array| -> Result<DbHandle, Box<EvalAltResult>> {
                let db_path = PathBuf::from(path);
                std::fs::create_dir_all(&db_path).map_err(|e| -> Box<EvalAltResult> {
                    format!("Failed to create directory \"{path}\": {e}").into()
                })?;

                let mut builder = KeyShapeBuilder::new();
                for spec in &ks_specs {
                    let map = spec.clone().try_cast::<rhai::Map>().ok_or_else(
                        || -> Box<EvalAltResult> {
                            "each keyspace spec must be a map #{name, key_size}".into()
                        },
                    )?;
                    let name = map
                        .get("name")
                        .ok_or_else(|| -> Box<EvalAltResult> {
                            "keyspace spec missing 'name'".into()
                        })?
                        .clone()
                        .cast::<String>();
                    let key_size = map
                        .get("key_size")
                        .ok_or_else(|| -> Box<EvalAltResult> {
                            "keyspace spec missing 'key_size'".into()
                        })?
                        .clone()
                        .cast::<i64>() as usize;
                    builder.add_key_space(&name, key_size, 16, KeyType::uniform(key_size));
                }
                let key_shape = builder.build();
                let config = Config::default();
                let db = Db::open(
                    &db_path,
                    key_shape.clone(),
                    Arc::new(config.clone()),
                    Metrics::new(),
                )
                .map_err(|e| -> Box<EvalAltResult> {
                    format!("Failed to create database: {e:?}").into()
                })?;
                let print_fn = ctx.lock().print_fn.clone();
                Ok(DbHandle(Arc::new(Mutex::new(DbState {
                    db_path,
                    config,
                    key_shape,
                    db: Some(db),
                    print_fn,
                }))))
            },
        );
    }

    db_fns::register(&mut engine);
    wal::register(&mut engine);
    control_region::register(&mut engine);
    verify::register(&mut engine);

    // --- Scripting helpers (useful in test scripts and interactive sessions) ---
    engine.register_fn(
        "assert",
        |condition: bool, msg: &str| -> Result<(), Box<EvalAltResult>> {
            if !condition {
                Err(format!("Assertion failed: {msg}").into())
            } else {
                Ok(())
            }
        },
    );
    engine.register_fn(
        "assert_eq",
        |lhs: Dynamic, rhs: Dynamic, msg: &str| -> Result<(), Box<EvalAltResult>> {
            let l = lhs.to_string();
            let r = rhs.to_string();
            if l != r {
                Err(format!("Assertion failed ({msg}): {l:?} != {r:?}").into())
            } else {
                Ok(())
            }
        },
    );
    // to_hex("key02___") → hex string of the UTF-8 bytes
    engine.register_fn("to_hex", |s: &str| hex::encode(s.as_bytes()));
    // bytes_hex(16, 2) → hex string of 16 bytes all equal to 2
    engine.register_fn("bytes_hex", |count: i64, byte_val: i64| {
        hex::encode(vec![byte_val as u8; count as usize])
    });

    // Redirect Rhai's built-in print/debug through ctx.print_fn so tests can capture it.
    let ctx_print = ctx.clone();
    engine.on_print(move |s| (ctx_print.lock().print_fn)(s));
    let ctx_debug = ctx.clone();
    engine.on_debug(move |s, src, pos| {
        let msg = match src {
            Some(src) => format!("[dbg {src}:{pos}] {s}"),
            None => format!("[dbg] {s}"),
        };
        (ctx_debug.lock().print_fn)(&msg);
    });

    engine
}

/// Returns true when `input` has balanced brackets/parens/braces and is safe to evaluate.
pub fn is_complete(input: &str) -> bool {
    let mut depth: i32 = 0;
    let mut in_str = false;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if in_str {
            if ch == '\\' {
                chars.next(); // skip escaped char
            } else if ch == '"' {
                in_str = false;
            }
        } else {
            match ch {
                '"' => in_str = true,
                '{' | '(' | '[' => depth += 1,
                '}' | ')' | ']' => depth -= 1,
                _ => {}
            }
        }
    }
    depth <= 0
}

/// Open a database and bind it to the variable `db` in the given scope.
/// This is used when `--db` is passed on the command line.
pub fn init_scope_with_db(
    engine: &Engine,
    scope: &mut Scope,
    _ctx: &Arc<Mutex<ConsoleContext>>,
    db_path: &str,
) -> Result<(), anyhow::Error> {
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(scope, &format!("let db = open(\"{db_path}\")"))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
