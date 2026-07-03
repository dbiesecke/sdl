use std::sync::Arc;

use crate::api::browser::ApiBrowser;
use crate::downloaders::SeriesCatalogInfo;
use serde::{Deserialize, Serialize};

/// Request payload/query for `/api/play`.
#[derive(Debug, Clone, Deserialize)]
pub struct PlayRequest {
    pub url: String,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub video_type: Option<String>,
    #[serde(default)]
    pub episodes: Option<String>,
    #[serde(default)]
    pub seasons: Option<String>,
    #[serde(default)]
    pub extractor_priorities: Option<Vec<String>>,
    #[serde(default)]
    pub extractor: Option<String>,
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub referer: Option<String>,
}

pub type PlayQuery = PlayRequest;
pub type PlayJson = PlayRequest;

/// Request payload/query for `/api/download`.
pub type DownloadRequest = PlayRequest;
pub type DownloadQuery = DownloadRequest;
pub type DownloadJson = DownloadRequest;

/// Shared state for API handlers.
#[derive(Debug, Clone)]
pub struct ApiState {
    pub download_semaphore: Arc<tokio::sync::Semaphore>,
    pub client: reqwest::Client,
    pub browser: Arc<ApiBrowser>,
}

/// Response payload for `/api/play` metadata endpoints or tests.
#[derive(Debug, Clone, Serialize)]
pub struct PlayResponse {
    pub url: String,
    pub referer: Option<String>,
}

/// Request payload for `/api/info`.
#[derive(Debug, Clone, Deserialize)]
pub struct InfoRequest {
    pub url: String,
    #[serde(default)]
    pub resolve_streams: bool,
}

/// Request payload/query for `/api/info_resolve`.
#[derive(Debug, Clone, Deserialize)]
pub struct InfoResolveRequest {
    pub url: String,
    #[serde(default)]
    pub extractor: Option<String>,
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub referer: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InfoResolveResponse {
    pub supported: bool,
    pub input_url: String,
    pub resolved_page_url: Option<String>,
    pub video_url: Option<String>,
    pub referer: Option<String>,
    pub extractor: Option<String>,
    pub error: Option<String>,
}

/// Response payload for `/api/info`.
#[derive(Debug, Clone, Serialize)]
pub struct InfoResponse {
    pub supported: bool,
    pub downloader: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub status: Option<String>,
    pub year: Option<u32>,
    pub catalog: Option<SeriesCatalogInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub name: &'static str,
    pub routes: &'static [&'static str],
}
