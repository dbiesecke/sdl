use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct ApiState {
    pub client: reqwest::Client,
    pub download_semaphore: Arc<Semaphore>,
    pub extraction_timeout: Duration,
    pub permit_timeout: Duration,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlayRequest {
    pub url: String,
    pub language: Option<String>,
    pub video_type: Option<String>,
    pub episodes: Option<String>,
    pub seasons: Option<String>,
    pub extractor_priorities: Option<String>,
    pub extractor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InfoRequest {
    pub url: String,
    pub language: Option<String>,
    pub video_type: Option<String>,
    pub episodes: Option<String>,
    pub seasons: Option<String>,
    pub extractor_priorities: Option<String>,
    pub resolve: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ResolvedVideo {
    pub url: String,
    pub referer: Option<String>,
    pub filename: String,
}

#[derive(Debug, Serialize)]
pub struct InfoResponse {
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub status: Option<String>,
    pub year: Option<u32>,
    pub items: Vec<InfoItem>,
}

#[derive(Debug, Serialize)]
pub struct InfoItem {
    pub name: Option<String>,
    pub season_number: Option<u32>,
    pub episode_number: String,
    pub language: String,
    pub resolved_url: Option<String>,
    pub referer: Option<String>,
}
