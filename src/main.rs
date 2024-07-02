pub mod error;
pub mod helpers;
pub mod jobs;
pub mod services;

use std::{path::PathBuf, sync::Arc};

use axum::{Extension, Json};
use error::AppError;
use rusqlite::Connection;
use serde_json::json;
use services::db::DatabaseAnalyzeJob;
use tracing::debug;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let mut closer = helpers::Closer::new();
    let db = services::db::Database::new()?;
    let jobs = services::job::JobsService::new(db.clone(), &mut closer, 1);
    jobs.schedule::<DatabaseAnalyzeJob>().await;
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    println!("Hello, world!");

    let app = axum::Router::new()
        .route("/", axum::routing::get(test_queue))
        .route("/transcode", axum::routing::get(test_transcode))
        .layer(Extension(db))
        .layer(Extension(jobs));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

fn unsafe_get_conn(db: &Arc<services::db::Database>) -> anyhow::Result<&mut Connection> {
    unsafe { Ok(&mut *db.acquire_conn()?.get()) }
}

async fn test_queue(
    Extension(jobs): Extension<Arc<services::job::JobsService>>,
    Extension(db): Extension<Arc<services::db::Database>>,
) -> Result<axum::Json<serde_json::Value>, AppError> {
    // TODO: find a safer way to do this lol
    let d = unsafe_get_conn(&db)?;
    let job = jobs::queue_torrent::QueueTorrentJob::new("magnet:?xt=urn:btih:GQ2K3ADFBGX5R3A7A3ZRIS25H7Z6RJOE&dn=%5BSubsPlease%5D%20Seiyuu%20Radio%20no%20Uraomote%20-%2012%20%281080p%29%20%5BF45E1BD5%5D.mkv&xl=1448977027&tr=http%3A%2F%2Fnyaa.tracker.wf%3A7777%2Fannounce&tr=udp%3A%2F%2Ftracker.coppersurfer.tk%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=udp%3A%2F%2F9.rarbg.to%3A2710%2Fannounce&tr=udp%3A%2F%2F9.rarbg.me%3A2710%2Fannounce&tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.internetwarriors.net%3A1337%2Fannounce&tr=udp%3A%2F%2Ftracker.cyberia.is%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker3.itzmx.com%3A6961%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.tiny-vps.com%3A6969%2Fannounce&tr=udp%3A%2F%2Fretracker.lanta-net.ru%3A2710%2Fannounce&tr=http%3A%2F%2Fopen.acgnxtracker.com%3A80%2Fannounce&tr=wss%3A%2F%2Ftracker.openwebtorrent.com".to_string());
    let j = &(jobs.enqueue(d, job)?);

    dbg!(j);

    Ok(Json(json!({
        "job_type": j.0,
        "job_id": j.1
    })))
}

async fn get_jobs(Extension(jobs): Extension<Arc<services::job::JobsService>>) -> Result<axum::Json<serde_json::Value>, AppError> {
    
    Ok(Json(json!({})))
}

async fn test_transcode(
    Extension(jobs): Extension<Arc<services::job::JobsService>>,
    Extension(db): Extension<Arc<services::db::Database>>,
) -> Result<axum::Json<serde_json::Value>, AppError> {
    let d = unsafe_get_conn(&db).unwrap();
    let filename = "[SubsPlease] Seiyuu Radio no Uraomote - 12 (1080p) [F45E1BD5].mkv";
    let path: PathBuf = format!("{}/{}", std::env::var("INPUT_FOLDER")?, filename).into();
    debug!("transcoding {}", path.display());
    let job = jobs::transcode::TranscodeJob::new(path);

    let j = &(jobs.enqueue(d, job)?);

    dbg!(j);

    Ok(Json(json!({
        "job_type": j.0,
        "job_id": j.1
    })))
}
