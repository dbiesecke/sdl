//! Adapter functions between HTTP handlers and SDL internals.
//!
//! This module is the integration point for reusing the existing CLI logic:
//! `downloaders::find_downloader_for_url` selects site downloaders,
//! `InstantiatedDownloader::download` performs episode discovery/download task
//! creation, `AniWorldSerienStream::get_series_info` provides series metadata,
//! and `extractors::extract_video_url*` resolves supported hoster URLs. Keeping
//! the API on these functions gives it the same hoster support as the CLI.

use crate::api::error::ApiError;
use crate::api::types::{InfoResponse, PlayRequest, PlayResponse};
use crate::{downloaders, extractors};

pub async fn play(request: PlayRequest) -> Result<PlayResponse, ApiError> {
    if request.url.trim().is_empty() {
        return Err(ApiError::BadRequest("url must not be empty".to_owned()));
    }

    let result = if let Some(extractor) = request.extractor.as_deref() {
        extractors::extract_video_url_with_extractor_from_url(
            &request.url,
            extractor,
            request.user_agent,
            request.referer,
        )
        .await
    } else {
        extractors::extract_video_url(&request.url, request.user_agent, request.referer).await
    };

    match result {
        Some(Ok(video)) => Ok(PlayResponse {
            url: video.url,
            referer: video.referer,
        }),
        Some(Err(err)) => Err(ApiError::Internal(err)),
        None => Err(ApiError::NotFound("no extractor supports the supplied url".to_owned())),
    }
}

pub async fn info(url: String) -> Result<InfoResponse, ApiError> {
    if url.trim().is_empty() {
        return Err(ApiError::BadRequest("url must not be empty".to_owned()));
    }

    let supported = downloaders::exists_downloader_for_url(&url).await;

    Ok(InfoResponse {
        supported,
        downloader: supported.then_some("auto".to_owned()),
        title: None,
        description: None,
        status: None,
        year: None,
    })
}
