use serde::{Deserialize, Serialize};

/// Request payload for `/api/play`.
#[derive(Debug, Clone, Deserialize)]
pub struct PlayRequest {
    pub url: String,
    #[serde(default)]
    pub extractor: Option<String>,
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub referer: Option<String>,
}

/// Response payload for `/api/play`.
#[derive(Debug, Clone, Serialize)]
pub struct PlayResponse {
    pub url: String,
    pub referer: Option<String>,
}

/// Request payload for `/api/info`.
#[derive(Debug, Clone, Deserialize)]
pub struct InfoRequest {
    pub url: String,
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
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub name: &'static str,
    pub routes: &'static [&'static str],
}
