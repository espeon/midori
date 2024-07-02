use std::{sync::Arc, time::Duration};

use anyhow::Result;
use axum::async_trait;
use rand::Rng;
use serde::Serialize;
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
                fn(conn: &rusqlite::Connection) -> Result<(String, i32)>,
                &'static str,
            )>,
        >,
    >,
}

impl JobsService {
    
    pub fn new(db: Arc<Database>, closer: &mut Closer, concurrent_jobs: usize) -> Arc<Self> {
        let scheduler = Arc::new(Mutex::new(vec![]));

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
            let scheduler = Arc::clone(&scheduler);
            let (close_tx, close_rx) = oneshot::channel();
            let worker = task::spawn(async move {
                info!("starting background job overwatch");
                overwatch(&db, &scheduler, close_rx).await;
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
    pub fn enqueue<J>(&self, conn: &rusqlite::Connection, job: J) -> Result<(String, i32)>
    where
        J: Job + Serialize,
    {
        Self::enqueue_inner(conn, &job)
    }

    fn enqueue_inner(conn: &rusqlite::Connection, job: &dyn Job) -> Result<(String, i32)> {
        let kind = job.typetag_name();
        let args = serde_json::to_string(job)?;
        let (id,): (i32,) = conn
            .prepare_cached(
                "INSERT INTO job_queue (scheduled, state, args, job_type) VALUES (unixepoch(), 'queued', ?, ?) \
                 RETURNING id",
            )?
            .query_row((&args, job.typetag_name()), row_extract)?;

        info!("enqueued background job of kind: {} with id: {}", kind, id);
        Ok((kind.to_string(), id))
    }

    /// Schedule a job that impls `Schedulable`
    pub async fn schedule<J>(&self)
    where
        J: Schedulable + Default,
    {
        let schedule: fn(&rusqlite::Connection) -> Result<(String, i32)> =
            |conn| Self::enqueue_inner(conn, &J::default());

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
    db: &Database,
    scheduler: &Mutex<
        Vec<(
            Instant,
            Duration,
            fn(conn: &rusqlite::Connection) -> Result<(String, i32)>,
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
            query(db, |conn| {
                job(conn)?;
                Ok(())
            })?;
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

async fn load_job(db: &Database) -> Result<(i32, Box<dyn Job>)> {
    let (id, args) = loop {
        let maybe_data = query(db, |conn| {
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
        })?;

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
                query(&state.db, |conn| {
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
    query(&state.db, |conn| {
        conn.prepare_cached("UPDATE job_queue SET output = ? WHERE id = ?")?
            .execute((new_data.to_string(), id))?;

        Ok(())
    })
}

fn save_job_result(state: &State, id: i32, res: &Result<serde_json::Value>) -> Result<()> {
    let update = |job_state: &str, output: &str| {
        query(&state.db, |conn| {
            conn.prepare_cached(
                "UPDATE job_queue SET state = ?, completed = unixepoch(), output = ?, active = \
                 NULL WHERE id = ?",
            )?
            .execute((job_state, &output, id))?;

            Ok(())
        })
    };

    match res {
        Ok(output) => {
            let output = serde_json::to_string(&output)?;
            update("completed", &output)?;
            info!("successfully completed background job with id: {}", id);
            Ok(())
        }
        Err(err) => {
            let output = json!({ "error": err.to_string() });
            let output = serde_json::to_string(&output)?;
            update("failed", &output)?;
            error!("error executing background job with id {}: {}", id, err);
            Ok(())
        }
    }
}

#[instrument(skip(state), err)]
async fn execute_next_job(state: &State) -> Result<()> {
    let (id, res) = run_job(state).await?;
    save_job_result(state, id, &res)?;
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
pub struct State {
    pub db: Arc<Database>,
}

#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Job: Send + Sync {
    async fn run(&self, _state: &State, info: JobCtx) -> Result<serde_json::Value>;
}

pub trait Schedulable: Job {
    const INTERVAL: Duration;
}
