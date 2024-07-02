use std::time::Duration;

use anyhow::Error;
use axum::async_trait;
use qbit_api_rs::types::torrents::{InfoQuery, InfoState};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::debug;

use crate::services::job::{Job, JobCtx, State};

#[derive(Default, Serialize, Deserialize)]
pub struct QueueTorrentJob {
    magnet: String,
}

impl QueueTorrentJob {
    pub fn new(magnet: String) -> Self {
        Self { magnet }
    }
}

#[async_trait]
#[typetag::serde]
impl Job for QueueTorrentJob {
    async fn run(&self, state: &State, jctx: JobCtx) -> Result<serde_json::Value, Error> {
        // envs
        let host = std::env::var("QBIT_HOST")?;
        let username = std::env::var("QBIT_USERNAME")?;
        let password = std::env::var("QBIT_PASSWORD")?;
    
        let qb = qbit_api_rs::client::QbitClient::new_with_user_pwd(host, username, password)?;
    
        // TODO: find some way to log in every hour (default expiry is 1 hour)
        // unlikely most torrents will take longer, but just in case
        qb.auth_login().await?;
    
        let magnet = &self.magnet;
        // get dn from magnet
        let dn = magnet.split("&dn=").collect::<Vec<&str>>().get(1)
            .ok_or_else(|| Error::msg("Could not extract dn"))?
            .split('&')
            .next()
            .ok_or_else(|| Error::msg("Could not extract dn"))?;
    
        qb.torrents_add_by_url::<&String>(&[magnet]).await?;
    
        jctx.update(
            state,
            &json!({
                "state": "queued",
                "progress": 100
            }),
        )?;
        debug!("torrent queued - job id {}", jctx.id);
    
        // use the dn to get the hash
        // qbit likes to change the hash, so we can't use xt directly.
        let q: InfoQuery = InfoQuery {
            ..Default::default()
        };
        let tinfo = qb.torrents_info(&q).await?;
    
        // get the hash based on the download name
        let hash = tinfo
            .iter().find(|t| t.magnet_uri.contains(dn))
            .ok_or_else(|| Error::msg("Could not find hash"))?
            .clone();
    
        let mut torrent_info = tinfo[0].clone();
        loop {
            let state_enum = torrent_info.state;
            // if torrent is done, then break
            if matches!(
                state_enum,
                InfoState::Uploading | InfoState::PausedUP | InfoState::StalledUP
            ) {
                break;
            }
            let q = InfoQuery {
                hashes: vec![hash.hash.clone()].into(),
                ..Default::default()
            };
            torrent_info = qb
                .torrents_info(&q)
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| Error::msg("Could not get torrent info"))?;
            debug!("{}", torrent_info.progress);
            jctx.update(
                state,
                &json!({
                    "progress": torrent_info.progress,
                    "state": torrent_info.state,
                    "hash": torrent_info.hash,
                    "name": torrent_info.name,
                    "added": torrent_info.added_on,
                    "finished_time": 0
                }),
            )?;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    
        Ok(json!({}))
    }
}
