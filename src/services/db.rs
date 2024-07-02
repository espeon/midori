// Blatantly stolen from https://seaforge.org/xacrimon/no-no/src/branch/main/backend/src/services/database.rs

use axum::async_trait;
use rusqlite::{types::FromSql, Connection, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::task;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::{fs, thread};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use thread_local::ThreadLocal;

use anyhow::{anyhow, bail, Result};
use tracing::{debug, info, instrument, warn};

use super::job::{Job, JobCtx, Schedulable, State};

static DB_SETTINGS: &[(&str, &str)] = &[
    ("journal_mode", "delete"),
    ("synchronous", "full"),
    ("cache_size", "-8192"),
    ("busy_timeout", "100"),
    ("temp_store", "memory"),
    ("foreign_key", "on"),
];

const DB_TIMEOUT: Duration = Duration::from_secs(10);
const DB_FILE_NAME: &str = "sqlite.db";
const CACHED_QUERIES: usize = 256;

pub struct Database {
    connections: ThreadLocal<UnsafeCell<Connection>>,
    path: PathBuf,
}

impl Database {
    pub fn new() -> Result<Arc<Self>> {
        debug!("detected sqlite library version: {}", rusqlite::version());
        info!("connecting to database...");
        let path = PathBuf::from(DB_FILE_NAME);

        Ok(Arc::new(Self {
            connections: ThreadLocal::new(),
            path,
        }))
    }

    pub fn acquire_conn(&self) -> Result<&UnsafeCell<Connection>> {
        self.connections.get_or_try(|| {
            let mut conn = Connection::open(&self.path)?;
            conn.set_prepared_statement_cache_capacity(CACHED_QUERIES);
            apply_pragmas(&conn)?;
            apply_migrations(&mut conn)?;
            Ok(UnsafeCell::new(conn))
        })
    }
}

fn apply_pragmas(conn: &Connection) -> Result<()> {
    for (pragma, value) in DB_SETTINGS {
        conn.pragma_update(None, pragma, value)?;
    }

    Ok(())
}

fn apply_migrations(conn: &mut Connection) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Exclusive)?;
    let current_version: i32 =
        tx.query_row("SELECT user_version FROM pragma_user_version", [], |row| {
            row.get(0)
        })?;

    let migrations = load_migrations(current_version)?;
    for (name, version, migration) in migrations {
        if version > current_version {
            info!("applying migration: {}...", name);
            tx.execute_batch(&migration)?;
            tx.pragma_update(None, "user_version", version)?;
        }
    }

    tx.commit()?;
    Ok(())
}

fn load_migrations(above: i32) -> Result<Vec<(String, i32, String)>> {
    let mut migrations = Vec::new();
    let path = if Path::exists(Path::new("./backend")) {
        "./backend/migrations"
    } else {
        "./migrations"
    };

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let (name, version) = extract_key_from_entry(&entry)?;
        let migration = fs::read_to_string(entry.path())?;
        if version > above {
            migrations.push((name, version, migration));
        }
    }

    migrations.sort_by_key(|(_, num, _)| *num);
    Ok(migrations)
}

fn extract_key_from_entry(entry: &fs::DirEntry) -> Result<(String, i32)> {
    let raw_name = entry.file_name();
    let name = raw_name
        .to_str()
        .ok_or_else(|| anyhow!("invalid file name"))?;

    let num = name
        .split('-')
        .next()
        .ok_or_else(|| anyhow!("missing number in migration name"))?
        .parse()?;

    Ok((name.to_owned(), num))
}

trait QueryMode {
    type Handle<'conn>;

    fn handle(connection: &mut rusqlite::Connection) -> rusqlite::Result<Self::Handle<'_>>;
}

struct NoTx;

impl QueryMode for NoTx {
    type Handle<'conn> = &'conn rusqlite::Connection;

    fn handle(connection: &mut rusqlite::Connection) -> rusqlite::Result<Self::Handle<'_>> {
        Ok(connection)
    }
}

struct Tx;

impl QueryMode for Tx {
    type Handle<'conn> = rusqlite::Transaction<'conn>;

    fn handle(connection: &mut rusqlite::Connection) -> rusqlite::Result<Self::Handle<'_>> {
        connection.transaction()
    }
}


#[instrument(skip(db, query), err)]
fn query_inner<M, F, T>(db: &Database, mut query: F) -> Result<T>
where
    T: Send,
    M: QueryMode,
    F: FnMut(M::Handle<'_>) -> Result<T> + Send,
{
    task::block_in_place(move || {
        let start = Instant::now();
        let end = start + DB_TIMEOUT;
        let conn = unsafe { &mut *db.acquire_conn()?.get() };

        let ret = loop {
            let handle = M::handle(conn)?;

            match query(handle) {
                Ok(item) => break Ok(item),
                Err(err) => {
                    if let Some(sq_err) = err.downcast_ref::<rusqlite::Error>() {
                        if let Some(code) = sq_err.sqlite_error_code() {
                            if code == rusqlite::ErrorCode::DatabaseBusy {
                                warn!("database is busy, retrying");

                                if Instant::now() > end {
                                    bail!("database busy, timed out");
                                } else {
                                    thread::yield_now();
                                    continue;
                                }
                            }
                        }
                    }

                    break Err(err);
                },
            }
        };

        debug!("transaction took {:?}", start.elapsed());
        ret
    })
}

pub fn query<F, T>(db: &Database, query: F) -> Result<T>
where
    T: Send,
    F: FnMut(&rusqlite::Connection) -> Result<T> + Send,
{
    query_inner::<NoTx, _, _>(db, query)
}

pub fn query_tx<F, T>(db: &Database, query: F) -> Result<T>
where
    T: Send,
    F: FnMut(rusqlite::Transaction) -> Result<T> + Send,
{
    query_inner::<Tx, _, _>(db, query)
}

#[derive(Default, Serialize, Deserialize)]
pub struct DatabaseAnalyzeJob;

#[async_trait]
#[typetag::serde]
impl Job for DatabaseAnalyzeJob {
    async fn run(&self, state: &State, jctx:JobCtx) -> Result<serde_json::Value> {
        jctx.update(state, &json!({
            "state": "analyzing",
            "progress": 0
        }))?;
        query(&state.db, |conn| {
            conn.execute_batch(
                r#"
                PRAGMA analysis_limit=400;
                ANALYZE;
                "#,
            )?;

            Ok(())
        })?;

        jctx.update(state, &json!({
            "state": "analyzed",
            "progress": 100
        }))?;
        debug!("database analyzed - job id {}", jctx.id);

        Ok(json!({}))
    }
}

impl Schedulable for DatabaseAnalyzeJob {
    const INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);
}

pub fn row_extract<T: FromRow>(row: &rusqlite::Row) -> rusqlite::Result<T> {
    T::from_row(row)
}

pub trait FromRow {
    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self>
    where
        Self: Sized;
}

impl<A> FromRow for (A,)
where
    A: FromSql,
{
    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok((row.get(0)?,))
    }
}

impl<A, B> FromRow for (A, B)
where
    A: FromSql,
    B: FromSql,
{
    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok((row.get(0)?, row.get(1)?))
    }
}

impl<A, B, C> FromRow for (A, B, C)
where
    A: FromSql,
    B: FromSql,
    C: FromSql,
{
    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    }
}

impl<A, B, C, D> FromRow for (A, B, C, D)
where
    A: FromSql,
    B: FromSql,
    C: FromSql,
    D: FromSql,
{
    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    }
}

impl<A, B, C, D, E> FromRow for (A, B, C, D, E)
where
    A: FromSql,
    B: FromSql,
    C: FromSql,
    D: FromSql,
    E: FromSql,
{
    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
        ))
    }
}

impl<A, B, C, D, E, F> FromRow for (A, B, C, D, E, F)
where
    A: FromSql,
    B: FromSql,
    C: FromSql,
    D: FromSql,
    E: FromSql,
    F: FromSql,
{
    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
        ))
    }
}