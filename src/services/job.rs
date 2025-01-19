use std::{sync::Arc, time::Duration};

use anyhow::Result;
use axum::async_trait;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    select,
    sync::{oneshot, Mutex},
    task,
    time::{self, Instant},
};
use tracing::{debug, error, info, instrument, trace};

use super::db::{query, row_extract, Database};
use crate::helpers::Closer;

const JOB_POLL_INTERVAL: Duration = Duration::from_secs(10);
const JOB_SCHEDULER_POLL_INTERVAL: Duration = Duration::from_secs(60);
const JOB_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const JOB_REFRESH_EXPIRY: Duration = Duration::from_secs(5 * 60);

pub struct JobsService {
    #[allow(clippy::type_complexity)]
    scheduler: Arc<
        Mutex<
            Vec<(
                Instant,
                Duration,
                fn(Arc<Mutex<Database>>) -> Result<(String, i32)>,
                &'static str,
            )>,
        >,
    >,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobRow {
    pub id: i32,
    pub args: String,
    pub job_type: String,
    pub state: String,
    pub scheduled: i64,
    pub completed: Option<i64>,
    pub active: Option<i64>,
    pub output: Option<String>,
}

impl JobsService {
    pub fn new(db: Arc<Mutex<Database>>, closer: &mut Closer, concurrent_jobs: usize) -> Arc<Self> {
        #[allow(clippy::type_complexity)]
        let scheduler: Arc<
            Mutex<
                Vec<(
                    Instant,
                    Duration,
                    fn(Arc<Mutex<Database>>) -> Result<(String, i32)>,
                    &'static str,
                )>,
            >,
        > = Arc::new(Mutex::new(vec![]));

        for _ in 0..concurrent_jobs {
            let state = State {
                db: Arc::clone(&db),
            };

            let (close_tx, close_rx) = oneshot::channel();
            let worker = task::spawn(async move {
                info!("starting background job worker");
                worker_jobs_loop(&state, close_rx).await;
            });

            closer.add(async {
                close_tx.send(()).unwrap();
                worker.await.unwrap();
            });
        }

        {
            let db = Arc::clone(&db);
            let scheduler_clone = Arc::clone(&scheduler);
            let (close_tx, close_rx) = oneshot::channel();
            let worker = task::spawn(async move {
                info!("starting background job overwatch");
                overwatch(db, &scheduler_clone, close_rx).await;
            });

            closer.add(async {
                close_tx.send(()).unwrap();
                worker.await.unwrap();
            });
        }

        Arc::new(Self { scheduler })
    }

    /// Enqueue a new job, returning its id.
    #[instrument(skip(self, conn, job))]
    pub async fn enqueue<J>(&self, conn: Arc<Mutex<Database>>, job: J) -> Result<(String, i32)>
    where
        J: Job + Serialize,
    {
        // get db connection from mutex
        Self::enqueue_inner(conn, &job).await
    }

    async fn enqueue_inner(conn: Arc<Mutex<Database>>, job: &dyn Job) -> Result<(String, i32)> {
        let kind = job.typetag_name();
        let args = serde_json::to_string(job)?;

        let db = conn.lock().await;
        let id: i32 = query(&db, |conn| {
            let mut stmt = conn.prepare_cached(
                    "INSERT INTO job_queue (scheduled, state, args, job_type) VALUES (unixepoch(), 'queued', ?, ?) \
                     RETURNING id",
                ).map_err(|e| anyhow::anyhow!("failed to enqueue job: {}", e))?;

            let row = stmt.query_row((&args, job.typetag_name()), |row| row.get::<_, i32>(0))?;
            Ok(row)
        })?;

        info!("enqueued background job of kind: {} with id: {}", kind, id);
        Ok((kind.to_string(), id))
    }

    /// List queued jobs
    pub async fn list_queued(&self, conn: Arc<Mutex<Database>>) -> Result<Vec<JobRow>> {
        trace!("Attempting to acquire lock in list_queued");
        let dbm = conn.lock().await;
        trace!("Lock acquired in list_queued");
        query(&dbm, |conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT id, args, job_type, state, scheduled, completed, active, output \
                     FROM job_queue \
                     ORDER BY scheduled",
            )?;

            let job = stmt.query_map([], |row| {
                Ok(JobRow {
                    id: row.get(0)?,
                    args: row.get(1)?,
                    job_type: row.get(2)?,
                    state: row.get(3)?,
                    scheduled: row.get(4)?,
                    completed: row.get(5)?,
                    active: row.get(6)?,
                    output: row.get(7)?,
                })
            })?;
            let rows = job
                .into_iter()
                .filter_map(|r| r.ok())
                .collect::<Vec<JobRow>>();
            Ok(rows)
        })
    }

    /// Schedule a job that impls `Schedulable`
    pub async fn schedule<J>(&self)
    where
        J: Schedulable + Default,
    {
        #[allow(clippy::type_complexity)]
        let schedule = |conn: Arc<Mutex<Database>>| -> Result<(String, i32)> {
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async move { Self::enqueue_inner(conn, &J::default()).await })
            })
            .join()
            .map_err(|e| anyhow::anyhow!("Thread panicked: {:?}", e))?
        };

        let modulate = rand::thread_rng().gen_range(0.0..1.0);
        let stamp = Instant::now() + J::INTERVAL.mul_f32(modulate);

        self.scheduler.lock().await.push((
            stamp,
            J::INTERVAL,
            schedule,
            J::typetag_name(&J::default()),
        ));
    }

    /// List upcoming scheduled jobs
    pub async fn scheduled(&self) -> Vec<(Duration, &'static str)> {
        let round = |dur: Duration| Duration::from_secs(dur.as_secs());
        self.scheduler
            .lock()
            .await
            .iter()
            .map(|(next, _, _, kind)| (round(*next - Instant::now()), *kind))
            .collect()
    }
}

#[instrument(skip(db, scheduler, close_rx))]
#[allow(clippy::type_complexity)]
async fn overwatch(
    db: Arc<Mutex<Database>>,
    scheduler: &Mutex<
        Vec<(
            Instant,
            Duration,
            fn(conn: Arc<Mutex<Database>>) -> Result<(String, i32)>,
            &'static str,
        )>,
    >,
    mut close_rx: oneshot::Receiver<()>,
) {
    let schedule_pending = || async {
        let scheduler = &mut *scheduler.lock().await;
        for (next, interval, job, _) in scheduler {
            if *next > Instant::now() {
                continue;
            }

            *next = Instant::now() + *interval;
            {
                job(Arc::clone(&db))?;
            }
        }

        Result::<()>::Ok(())
    };

    let mut schedule_pending_interval = time::interval(JOB_SCHEDULER_POLL_INTERVAL);
    loop {
        select! {
            _ = schedule_pending_interval.tick() => {
                if let Err(err) = schedule_pending().await {
                    error!("failed to schedule pending jobs: {}", err);
                }
            }
            _ = &mut close_rx => {
                info!("background job overwatch shutting down...");
                return;
            }
        }
    }
}

async fn worker_jobs_loop(state: &State, mut close_rx: oneshot::Receiver<()>) {
    loop {
        select! {
            result = execute_next_job(state) => {
                if let Err(err) = result {
                    error!("error performing background job work: {}", err);
                }
            }
            _ = &mut close_rx => {
                info!("background job worker shutting down...");
                return;
            }
        }
    }
}

async fn load_job(db: &Arc<Mutex<Database>>) -> Result<(i32, Box<dyn Job>)> {
    let (id, args) = loop {
        trace!("attempting to load job from queue");
        let maybe_data = {
            let db = db.lock().await;
            query(&db, |conn| {
                let maybe_row: rusqlite::Result<(i32, String)> = conn
                    .prepare_cached(
                        r#"
                    UPDATE job_queue
                    SET state = 'running',
                        active = unixepoch()
                    WHERE id =
                        (SELECT id
                         FROM job_queue
                         WHERE state = 'queued'
                           OR (state = 'running'
                               AND active < (unixepoch() - ?))
                         ORDER BY scheduled
                         LIMIT 1) RETURNING id,
                                            args
                    "#,
                    )?
                    .query_row((JOB_REFRESH_EXPIRY.as_secs(),), row_extract);

                match maybe_row {
                    Ok(data) => Ok(Some(data)),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                    Err(err) => Err(err.into()),
                }
            })?
        };
        if let Some(data) = maybe_data {
            break data;
        } else {
            trace!(
                "background job queue is empty. trying again in {:?}",
                JOB_POLL_INTERVAL
            );

            time::sleep(JOB_POLL_INTERVAL).await;
        }
    };

    Ok((id, serde_json::from_str(&args)?))
}

async fn run_job(state: &State) -> Result<(i32, Result<serde_json::Value>)> {
    let (id, job) = load_job(&state.db).await?;
    debug!("pulled background job with id: {} from queue", id);
    let start = Instant::now();
    let mut lock_update = time::interval_at(start + JOB_REFRESH_INTERVAL, JOB_REFRESH_INTERVAL);
    let res = loop {
        select! {
            res = job.run(state, JobCtx { id }) => {
                break res;
            }
            _ = lock_update.tick() => {
                debug!("updating lock for background job with id: {}", id);
                let db = state.db.lock().await;
                query(&db, |conn| {
                    conn.prepare_cached("UPDATE job_queue SET active = unixepoch() WHERE id = ?")?.execute((id,))?;
                    Ok(())
                })?;
            }
        }
    };

    debug!(
        "finished running background job with id: {}, took {:?}",
        id,
        start.elapsed()
    );

    Ok((id, res))
}

// Update job information without setting state to completed
fn update_job(state: &State, id: i32, new_data: &serde_json::Value) -> Result<()> {
    debug!("Attempting to acquire lock in update_job");

    // Clone/copy the values we need to move into the new thread
    let state = state.clone();
    let new_data = new_data.clone();

    // Spawn a new thread with its own runtime
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let db = state.db.lock().await;
            trace!("Lock acquired in update_job");
            query(&db, |conn| {
                conn.prepare_cached("UPDATE job_queue SET output = ? WHERE id = ?")?
                    .execute((new_data.to_string(), id))?;
                Ok(())
            })
        })
    });

    Ok(())
}

async fn save_job_result(state: &State, id: i32, res: &Result<serde_json::Value>) -> Result<()> {
    async fn update(
        job_state: &str,
        output: &str,
        id: &i32,
        state_db: &Arc<Mutex<Database>>,
    ) -> std::result::Result<(), anyhow::Error> {
        trace!("Attempting to acquire lock in save_job_result");
        let db = state_db.lock().await;
        trace!("Lock acquired in save_job_result");
        query(&db, |conn| {
            conn.prepare_cached(
                "UPDATE job_queue SET state = ?, completed = unixepoch(), output = ?, active = \
                 NULL WHERE id = ?",
            )?
            .execute((job_state, &output, *id))?;

            Ok(())
        })
    }

    match res {
        Ok(output) => {
            let output = serde_json::to_string(&output)?;
            update("completed", &output, &id, &state.db).await?;
            debug!("successfully completed background job with id: {}", id);
            Ok(())
        }
        Err(err) => {
            let output = json!({ "error": err.to_string() });
            let output = serde_json::to_string(&output)?;
            update("failed", &output, &id, &state.db).await?;
            error!("error executing background job with id {}: {}", id, err);
            Ok(())
        }
    }
}

#[instrument(skip(state), err)]
async fn execute_next_job(state: &State) -> Result<()> {
    let (id, res) = run_job(state).await?;
    save_job_result(state, id, &res).await?;
    Ok(())
}

pub struct JobCtx {
    pub id: i32,
}

impl JobCtx {
    /// Updates job information (in 'result' column). Will not change the job state.
    pub fn update(&self, state: &State, update_to: &serde_json::Value) -> Result<()> {
        update_job(state, self.id, update_to)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct State {
    pub db: Arc<Mutex<Database>>,
}

#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Job: Send + Sync {
    async fn run(&self, _state: &State, info: JobCtx) -> Result<serde_json::Value>;
}

pub trait Schedulable: Job {
    const INTERVAL: Duration;
}
