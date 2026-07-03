use std::ops::RangeInclusive;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use url::Url;

use crate::chrome::ChromeDriver;
use crate::dirs;
use crate::downloaders::{
    self, AllOrSpecific, DownloadRequest, DownloadSettings, EpisodeNumber, EpisodesRequest, ExtractorMatch,
    InstantiatedDownloader, Language, VideoType,
};
use crate::extractors;

use super::error::ApiError;
use super::types::{InfoItem, InfoRequest, InfoResponse, PlayRequest, ResolvedVideo};

pub fn validate_url(input: &str) -> Result<Url, ApiError> {
    let url = Url::parse(input).map_err(|_| ApiError::BadRequest("url must be an absolute URL".to_owned()))?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        _ => Err(ApiError::BadRequest("url scheme must be http or https".to_owned())),
    }
}

pub async fn resolve_play_request(request: PlayRequest) -> Result<ResolvedVideo, ApiError> {
    let url = validate_url(&request.url)?;
    let url_str = url.as_str();

    if extractors::exists_extractor_for_url(url_str, request.extractor.as_deref()).await {
        let extracted = if let Some(extractor) = request.extractor.as_deref() {
            extractors::extract_video_url_with_extractor_from_url(url_str, extractor, None, None).await
        } else {
            extractors::extract_video_url(url_str, None, None).await
        }
        .ok_or_else(|| ApiError::UnsupportedUrl("no extractor supports this URL".to_owned()))?
        .map_err(|err| ApiError::ExtractorFailed(format!("failed to extract video URL: {err:#}")))?;

        return Ok(ResolvedVideo {
            url: extracted.url,
            referer: extracted.referer,
            filename: "sdl-stream".to_owned(),
        });
    }

    let task = collect_download_tasks(
        url_str,
        request.language,
        request.video_type,
        request.episodes,
        request.seasons,
        request.extractor_priorities,
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| ApiError::UnsupportedUrl("no downloadable stream was found for this URL".to_owned()))?;

    let filename = task.episode_info.name.unwrap_or_else(|| "sdl-stream".to_owned());
    Ok(ResolvedVideo {
        url: task.download_url,
        referer: task.referer,
        filename,
    })
}

pub async fn build_info_response(request: InfoRequest) -> Result<InfoResponse, ApiError> {
    let url = validate_url(&request.url)?;
    if !downloaders::exists_downloader_for_url(url.as_str()).await {
        return Err(ApiError::UnsupportedUrl("no downloader supports this URL".to_owned()));
    }

    let (driver, mut child) = start_driver().await?;
    let downloader = downloaders::find_downloader_for_url(&driver, false, url.as_str())
        .await
        .ok_or_else(|| ApiError::UnsupportedUrl("no downloader supports this URL".to_owned()))?;

    let series = downloader
        .get_series_info()
        .await
        .map_err(|err| ApiError::UpstreamFailed(format!("failed to read series metadata: {err:#}")))?;
    let tasks = collect_download_tasks_with_driver(
        &driver,
        url.as_str(),
        request.language,
        request.video_type,
        request.episodes,
        request.seasons,
        request.extractor_priorities,
    )
    .await?;

    let _ = driver.quit().await;
    let _ = child.kill();

    Ok(InfoResponse {
        url: url.to_string(),
        title: Some(series.title),
        description: series.description,
        status: series.status.map(|status| format!("{status:?}")),
        year: series.year,
        items: tasks
            .into_iter()
            .map(|task| InfoItem {
                name: task.episode_info.name,
                season_number: task.episode_info.season_number,
                episode_number: match task.episode_info.episode_number {
                    EpisodeNumber::Number(n) => n.to_string(),
                    EpisodeNumber::String(s) => s,
                },
                language: task.language.to_string(),
                resolved_url: if request.resolve.unwrap_or(true) {
                    Some(task.download_url)
                } else {
                    None
                },
                referer: task.referer,
            })
            .collect(),
    })
}

async fn collect_download_tasks(
    url: &str,
    language: Option<String>,
    video_type: Option<String>,
    episodes: Option<String>,
    seasons: Option<String>,
    extractor_priorities: Option<String>,
) -> Result<Vec<downloaders::DownloadTask>, ApiError> {
    let (driver, mut child) = start_driver().await?;
    let result = collect_download_tasks_with_driver(
        &driver,
        url,
        language,
        video_type,
        episodes,
        seasons,
        extractor_priorities,
    )
    .await;
    let _ = driver.quit().await;
    let _ = child.kill();
    result
}

async fn collect_download_tasks_with_driver(
    driver: &thirtyfour::WebDriver,
    url: &str,
    language: Option<String>,
    video_type: Option<String>,
    episodes: Option<String>,
    seasons: Option<String>,
    extractor_priorities: Option<String>,
) -> Result<Vec<downloaders::DownloadTask>, ApiError> {
    let downloader = downloaders::find_downloader_for_url(driver, false, url)
        .await
        .ok_or_else(|| ApiError::UnsupportedUrl("no downloader supports this URL".to_owned()))?;
    let request = DownloadRequest {
        language: parse_video_type(language.as_deref(), video_type.as_deref())?,
        episodes: parse_episodes_request(episodes.as_deref(), seasons.as_deref())?,
        extractor_priorities: parse_extractors(extractor_priorities.as_deref()),
    };
    let settings = DownloadSettings::new(None, || Duration::from_secs(0));
    let (tx, rx) = mpsc::unbounded_channel();

    downloader
        .download(request, settings, tx)
        .await
        .map_err(|err| ApiError::UpstreamFailed(format!("failed to resolve stream URL: {err:#}")))?;
    Ok(UnboundedReceiverStream::new(rx).collect::<Vec<_>>().await)
}

fn parse_video_type(language: Option<&str>, video_type: Option<&str>) -> Result<VideoType, ApiError> {
    let lang = match language {
        Some(value) => Language::try_from(value).map_err(|err| ApiError::BadRequest(err.to_string()))?,
        None => Language::Unspecified,
    };
    Ok(
        match video_type.unwrap_or("unspecified").to_ascii_lowercase().as_str() {
            "raw" => VideoType::Raw,
            "dub" => VideoType::Dub(lang),
            "sub" => VideoType::Sub(lang),
            "unspecified" | "auto" => VideoType::Unspecified(lang),
            other => return Err(ApiError::BadRequest(format!("unknown video_type: {other}"))),
        },
    )
}

fn parse_episodes_request(episodes: Option<&str>, seasons: Option<&str>) -> Result<EpisodesRequest, ApiError> {
    match (episodes, seasons) {
        (Some(_), Some(_)) => Err(ApiError::BadRequest(
            "use either episodes or seasons, not both".to_owned(),
        )),
        (Some(value), None) => Ok(EpisodesRequest::Episodes(parse_all_or_specific(value)?)),
        (None, Some(value)) => Ok(EpisodesRequest::Seasons(parse_all_or_specific(value)?)),
        (None, None) => Ok(EpisodesRequest::Unspecified),
    }
}

fn parse_all_or_specific(value: &str) -> Result<AllOrSpecific, ApiError> {
    if value.eq_ignore_ascii_case("all") {
        return Ok(AllOrSpecific::All);
    }
    let mut ranges = Vec::new();
    for part in value.split(',').map(str::trim).filter(|part| !part.is_empty()) {
        let (start, end) = if let Some((start, end)) = part.split_once('-') {
            (start, end)
        } else {
            (part, part)
        };
        let start = start
            .parse::<u32>()
            .map_err(|_| ApiError::BadRequest(format!("invalid episode/season range: {part}")))?;
        let end = end
            .parse::<u32>()
            .map_err(|_| ApiError::BadRequest(format!("invalid episode/season range: {part}")))?;
        if start > end {
            return Err(ApiError::BadRequest(format!("invalid descending range: {part}")));
        }
        ranges.push(RangeInclusive::new(start, end));
    }
    if ranges.is_empty() {
        Err(ApiError::BadRequest("range list must not be empty".to_owned()))
    } else {
        Ok(AllOrSpecific::Specific(ranges))
    }
}

fn parse_extractors(value: Option<&str>) -> Vec<ExtractorMatch> {
    value
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| {
                    if value.eq_ignore_ascii_case("any") {
                        ExtractorMatch::Any
                    } else {
                        ExtractorMatch::Name(value.to_owned())
                    }
                })
                .collect()
        })
        .unwrap_or_else(|| vec![ExtractorMatch::Any])
}

async fn start_driver() -> Result<(thirtyfour::WebDriver, std::process::Child), ApiError> {
    let data_dir = dirs::get_data_dir()
        .await
        .map_err(|err| ApiError::Internal(format!("failed to prepare data directory: {err:#}")))?;
    ChromeDriver::get_without_ublock(&data_dir, true)
        .await
        .context("failed to start ChromeDriver")
        .map_err(|err| ApiError::Internal(format!("{err:#}")))
}
