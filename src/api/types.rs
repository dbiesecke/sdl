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
