use anyhow::Result;
use std::{borrow::BorrowMut, time::Duration};
use tracing::debug;

use axum::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::fs;

use crate::services::job::{Job, JobCtx, Schedulable, State};

#[derive(Default, Serialize, Deserialize)]
pub struct FilesystemUpdateJob;

#[async_trait]
#[typetag::serde]
impl Job for FilesystemUpdateJob {
    async fn run(&self, state: &State, jctx: JobCtx) -> Result<serde_json::Value> {
        // Check for updates to the filesystem
        // Assume a file moved to the specified location is ready to encode

        // Get .fs.tmp file (list of files in directory)
        let file = fs::read_to_string("fs.tmp").await?;
        let file_list = file.split("\n").collect::<Vec<&str>>();

        // get list of files in directory
        let mut files = Vec::new();
        let mut r = fs::read_dir("files").await?;

        while let Some(entry) = r.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            }
        }

        // check for new files
        for file in files {
            if let Some(file_str) = file.to_str() {
                if file_list.contains(&file_str) {
                    // file is missing, add to queue
                    //let _ = jctx.borrow_mut()
                    debug!("adding file to queue, but not implemented! {}", file_str);
                }
            }
        }

        Ok(json!({}))
    }
}

impl Schedulable for FilesystemUpdateJob {
    const INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);
}
