//! Adapter functions between HTTP handlers and SDL internals.

use std::ops::RangeInclusive;
use std::time::Duration;

use actix_web::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE, REFERER};
use actix_web::HttpResponse;
use anyhow::Context;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tokio_stream::wrappers::UnboundedReceiverStream;
use url::Url;

use crate::api::error::ApiError;
use crate::api::types::{ApiState, InfoRequest, InfoResponse, PlayRequest};
use crate::downloaders::{
    self, AllOrSpecific, DownloadRequest, DownloadSettings, EpisodesRequest, ExtractorMatch, InstantiatedDownloader,
    Language, VideoType,
};
use crate::logger::log_wrapper::{LogWrapper, SetLogWrapper};
use crate::{chrome, dirs, download, extractors};
use once_cell::sync::Lazy;
use std::sync::Mutex;

const SCRAPE_TIMEOUT: Duration = Duration::from_secs(90);
const STREAM_PERMIT_TIMEOUT: Duration = Duration::from_secs(10);

static API_LOG_WRAPPER: Lazy<Mutex<Option<SetLogWrapper>>> = Lazy::new(|| Mutex::new(None));

pub async fn play(state: ApiState, request: PlayRequest) -> Result<HttpResponse, ApiError> {
    let permit = acquire_stream_permit(&state).await?;
    let url = validate_url(&request.url)?;
    let extracted = if extractors::exists_extractor_for_url(url.as_str(), request.extractor.as_deref()).await {
        extract_direct_video(url.as_str(), &request).await?
    } else if downloaders::exists_downloader_for_url(url.as_str()).await {
        extract_series_video(url.as_str(), request.clone()).await?
    } else {
        return Err(ApiError::UnsupportedUrl(
            "no downloader or extractor supports the supplied url".to_owned(),
        ));
    };

    stream_extracted_video(state.client.clone(), extracted.url, extracted.referer, permit).await
}

async fn acquire_stream_permit(state: &ApiState) -> Result<OwnedSemaphorePermit, ApiError> {
    let timeout = std::env::var("SDL_API_STREAM_PERMIT_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(STREAM_PERMIT_TIMEOUT);

    tokio::time::timeout(timeout, state.download_semaphore.clone().acquire_owned())
        .await
        .map_err(|_| ApiError::ServiceUnavailable("too many concurrent streams".to_owned()))?
        .map_err(|_| ApiError::ServiceUnavailable("stream limiter is unavailable".to_owned()))
}

async fn extract_direct_video(url: &str, request: &PlayRequest) -> Result<extractors::ExtractedVideo, ApiError> {
    let user_agent = request.user_agent.clone();
    let referer = request.referer.clone();
    let extraction = async {
        let extractor = request.extractor.clone().or_else(|| first_named_extractor(request));
        if let Some(extractor) = extractor.as_deref() {
            extractors::extract_video_url_with_extractor_from_url(url, extractor, user_agent, referer).await
        } else {
            extractors::extract_video_url(url, user_agent, referer).await
        }
    };

    match tokio::time::timeout(SCRAPE_TIMEOUT, extraction)
        .await
        .map_err(|_| ApiError::Timeout("video extraction timed out".to_owned()))?
    {
        Some(Ok(video)) => Ok(video),
        Some(Err(err)) => {
            log::warn!("Video extractor failed: {err:#}");
            Err(ApiError::ExtractorFailed(
                "failed to extract video from the supplied url".to_owned(),
            ))
        }
        None => Err(ApiError::UnsupportedUrl(
            "no extractor supports the supplied url".to_owned(),
        )),
    }
}

async fn extract_series_video(url: &str, request: PlayRequest) -> Result<extractors::ExtractedVideo, ApiError> {
    let download_request = build_download_request(&request)?;
    reject_multi_episode_requests(&download_request.episodes)?;

    tokio::time::timeout(SCRAPE_TIMEOUT, async move {
        let data_dir = dirs::get_data_dir().await?;
        let limiter = async_speed_limit::Limiter::new(f64::INFINITY);
        let asset_downloader = {
            let mut log_wrapper = api_log_wrapper()?;
            download::Downloader::new(
                log_wrapper.as_mut().expect("api log wrapper initialized"),
                limiter,
                false,
                None,
                None,
                None,
            )
        };
        let (driver, mut child) = chrome::ChromeDriver::get(&data_dir, &asset_downloader, true).await?;
        let result = async {
            let downloader = downloaders::find_downloader_for_url(&driver, false, url)
                .await
                .context("no downloader supports the supplied url")?;
            let (tx, rx) = mpsc::unbounded_channel();
            let settings = DownloadSettings::new(None, || Duration::ZERO);
            let download_future = downloader.download(download_request, settings, tx);
            tokio::pin!(download_future);
            let mut rx = UnboundedReceiverStream::new(rx);
            tokio::select! {
                task = rx.next() => {
                    let task = task.context("downloader did not produce a streamable episode")?;
                    Ok(extractors::ExtractedVideo { url: task.download_url, referer: task.referer })
                }
                result = &mut download_future => {
                    result?;
                    anyhow::bail!("downloader completed without producing a streamable episode")
                }
            }
        }
        .await;
        let _ = driver.quit().await;
        let _ = child.kill();
        result
    })
    .await
    .map_err(|_| ApiError::Timeout("series scraping timed out".to_owned()))?
    .map_err(|err| {
        log::warn!("Series extractor failed: {err:#}");
        ApiError::ExtractorFailed("failed to scrape a streamable episode from the supplied url".to_owned())
    })
}

fn api_log_wrapper() -> Result<std::sync::MutexGuard<'static, Option<SetLogWrapper>>, anyhow::Error> {
    let mut guard = API_LOG_WRAPPER
        .lock()
        .map_err(|_| anyhow::anyhow!("api logger lock poisoned"))?;
    if guard.is_none() {
        let logger = crate::logger::default_logger(false);
        *guard = Some(LogWrapper::new(None, logger).try_init()?);
    }
    Ok(guard)
}

async fn stream_extracted_video(
    client: reqwest::Client,
    video_url: String,
    referer: Option<String>,
    permit: OwnedSemaphorePermit,
) -> Result<HttpResponse, ApiError> {
    let parsed = Url::parse(&video_url).map_err(|err| {
        log::warn!("Extractor returned an invalid video url: {err:#}");
        ApiError::ExtractorFailed("extractor returned an invalid video url".to_owned())
    })?;
    if parsed.path().to_ascii_lowercase().ends_with(".m3u8") {
        return Ok(stream_m3u8(client, parsed, referer, permit).await?);
    }

    let mut req = client.get(parsed.clone());
    if let Some(referer) = referer.as_deref() {
        req = req.header(REFERER.as_str(), referer);
    }
    let response = req
        .send()
        .await
        .map_err(map_upstream_error)?
        .error_for_status()
        .map_err(map_upstream_error)?;
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    if content_type.contains("mpegurl") || content_type.contains("application/vnd.apple.mpegurl") {
        return Ok(stream_m3u8(client, parsed, referer, permit).await?);
    }
    let header_type = if parsed.path().to_ascii_lowercase().ends_with(".mp4") || content_type.contains("video/mp4") {
        "video/mp4"
    } else {
        "application/octet-stream"
    };
    let stream = response.bytes_stream().map(move |item| {
        let _permit = &permit;
        item.map_err(actix_web::error::ErrorBadGateway)
    });
    Ok(base_stream_response(header_type).streaming(stream))
}

async fn stream_m3u8(
    client: reqwest::Client,
    playlist_url: Url,
    referer: Option<String>,
    permit: OwnedSemaphorePermit,
) -> Result<HttpResponse, ApiError> {
    let mut current_playlist_url = playlist_url;
    let segment_urls = loop {
        let mut req = client.get(current_playlist_url.clone());
        if let Some(referer) = referer.as_deref() {
            req = req.header(REFERER.as_str(), referer);
        }
        let body = req
            .send()
            .await
            .map_err(map_upstream_error)?
            .error_for_status()
            .map_err(map_upstream_error)?
            .bytes()
            .await
            .map_err(map_upstream_error)?;
        let playlist = m3u8_rs::parse_playlist_res(&body).map_err(|err| {
            log::warn!("Failed to parse upstream m3u8 playlist: {err:?}");
            ApiError::UpstreamFailed("failed to parse upstream playlist".to_owned())
        })?;
        match playlist {
            m3u8_rs::Playlist::MediaPlaylist(media) => {
                break media
                    .segments
                    .into_iter()
                    .map(|s| current_playlist_url.join(&s.uri))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|err| {
                        log::warn!("Failed to resolve m3u8 segment url: {err:#}");
                        ApiError::UpstreamFailed("failed to resolve upstream playlist segment".to_owned())
                    })?;
            }
            m3u8_rs::Playlist::MasterPlaylist(master) => {
                let variant = master.variants.first().ok_or_else(|| {
                    log::warn!("Upstream m3u8 master playlist has no variants");
                    ApiError::UpstreamFailed("upstream playlist has no variants".to_owned())
                })?;
                current_playlist_url = current_playlist_url.join(&variant.uri).map_err(|err| {
                    log::warn!("Failed to resolve m3u8 variant url: {err:#}");
                    ApiError::UpstreamFailed("failed to resolve upstream playlist variant".to_owned())
                })?;
            }
        }
    };
    let stream = futures_util::stream::iter(segment_urls).then(move |segment_url| {
        let _permit = &permit;
        let client = client.clone();
        let referer = referer.clone();
        async move {
            let mut req = client.get(segment_url);
            if let Some(referer) = referer.as_deref() {
                req = req.header(REFERER.as_str(), referer);
            }
            let bytes = req
                .send()
                .await
                .map_err(actix_web::error::ErrorBadGateway)?
                .error_for_status()
                .map_err(actix_web::error::ErrorBadGateway)?
                .bytes()
                .await
                .map_err(actix_web::error::ErrorBadGateway)?;
            Ok::<Bytes, actix_web::Error>(bytes)
        }
    });
    Ok(base_stream_response("application/octet-stream").streaming(stream))
}

fn map_upstream_error(err: reqwest::Error) -> ApiError {
    log::warn!("Upstream request failed: {err:#}");
    if err.is_timeout() {
        ApiError::Timeout("upstream request timed out".to_owned())
    } else {
        ApiError::UpstreamFailed("upstream request failed".to_owned())
    }
}

fn base_stream_response(content_type: &'static str) -> actix_web::HttpResponseBuilder {
    let mut builder = HttpResponse::Ok();
    builder.insert_header((CONTENT_TYPE, content_type));
    builder.insert_header((CONTENT_DISPOSITION, "inline; filename=\"sdl-stream\""));
    builder
}

fn validate_url(raw: &str) -> Result<Url, ApiError> {
    if raw.trim().is_empty() {
        return Err(ApiError::BadRequest("url must not be empty".to_owned()));
    }
    let url = Url::parse(raw).map_err(|_| ApiError::BadRequest("invalid url".to_owned()))?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        _ => Err(ApiError::BadRequest("url must use http or https".to_owned())),
    }
}

fn build_download_request(request: &PlayRequest) -> Result<DownloadRequest, ApiError> {
    Ok(DownloadRequest {
        language: parse_video_type(request)?,
        episodes: parse_episodes(&request.episodes, &request.seasons)?,
        extractor_priorities: parse_extractor_priorities(request)?,
    })
}

fn parse_video_type(request: &PlayRequest) -> Result<VideoType, ApiError> {
    let language = match request
        .language
        .as_deref()
        .unwrap_or("unspecified")
        .to_ascii_lowercase()
        .as_str()
    {
        "unspecified" | "any" => Language::Unspecified,
        value => Language::try_from(value).map_err(|err| ApiError::BadRequest(err.to_string()))?,
    };
    match request
        .video_type
        .as_deref()
        .unwrap_or("unspecified")
        .to_ascii_lowercase()
        .as_str()
    {
        "unspecified" | "any" => Ok(VideoType::Unspecified(language)),
        "raw" => Ok(VideoType::Raw),
        "dub" => Ok(VideoType::Dub(language)),
        "sub" => Ok(VideoType::Sub(language)),
        other => Err(ApiError::BadRequest(format!("unknown video_type: {other}"))),
    }
}

fn parse_episodes(episodes: &Option<String>, seasons: &Option<String>) -> Result<EpisodesRequest, ApiError> {
    match (episodes.as_deref(), seasons.as_deref()) {
        (None, None) => Ok(EpisodesRequest::Unspecified),
        (Some(_), Some(_)) => Err(ApiError::BadRequest(
            "episodes and seasons are mutually exclusive".to_owned(),
        )),
        (Some(value), None) => Ok(EpisodesRequest::Episodes(parse_all_or_specific(value)?)),
        (None, Some(value)) => Ok(EpisodesRequest::Seasons(parse_all_or_specific(value)?)),
    }
}

fn parse_all_or_specific(value: &str) -> Result<AllOrSpecific, ApiError> {
    if value.eq_ignore_ascii_case("all") {
        return Ok(AllOrSpecific::All);
    }
    let ranges = value
        .split(',')
        .map(|part| {
            let (start, end) = part.split_once('-').unwrap_or((part, part));
            let start: u32 = start
                .trim()
                .parse()
                .map_err(|_| ApiError::BadRequest(format!("invalid range: {part}")))?;
            let end: u32 = end
                .trim()
                .parse()
                .map_err(|_| ApiError::BadRequest(format!("invalid range: {part}")))?;
            Ok(start..=end)
        })
        .collect::<Result<Vec<RangeInclusive<u32>>, ApiError>>()?;
    Ok(AllOrSpecific::Specific(ranges))
}

fn parse_extractor_priorities(request: &PlayRequest) -> Result<Vec<ExtractorMatch>, ApiError> {
    let names = request.extractor_priorities.clone().unwrap_or_default();
    names
        .into_iter()
        .map(|name| {
            if name == "*" || name.eq_ignore_ascii_case("any") {
                Ok(ExtractorMatch::Any)
            } else if extractors::exists_extractor_with_name(&name) {
                Ok(ExtractorMatch::Name(name))
            } else {
                Err(ApiError::BadRequest(format!("no extractor with name: {name}")))
            }
        })
        .collect()
}

fn first_named_extractor(request: &PlayRequest) -> Option<String> {
    request
        .extractor_priorities
        .as_ref()?
        .iter()
        .find(|name| *name != "*" && !name.eq_ignore_ascii_case("any"))
        .cloned()
}

fn reject_multi_episode_requests(episodes: &EpisodesRequest) -> Result<(), ApiError> {
    match episodes {
        EpisodesRequest::Seasons(AllOrSpecific::All) | EpisodesRequest::Episodes(AllOrSpecific::All) => Err(
            ApiError::BadRequest("/api/play streams a single episode; request one episode instead of all".to_owned()),
        ),
        EpisodesRequest::Seasons(AllOrSpecific::Specific(ranges))
        | EpisodesRequest::Episodes(AllOrSpecific::Specific(ranges))
            if ranges.len() > 1 || ranges.iter().any(|range| range.start() != range.end()) =>
        {
            Err(ApiError::BadRequest(
                "/api/play streams a single episode; multiple episodes are not supported".to_owned(),
            ))
        }
        _ => Ok(()),
    }
}

pub async fn info(request: InfoRequest) -> Result<InfoResponse, ApiError> {
    let url = validate_url(&request.url)?;
    if downloaders::exists_downloader_for_url(url.as_str()).await {
        let catalog = tokio::time::timeout(SCRAPE_TIMEOUT, async {
            let data_dir = dirs::get_data_dir().await?;
            let limiter = async_speed_limit::Limiter::new(f64::INFINITY);
            let asset_downloader = {
                let mut log_wrapper = api_log_wrapper()?;
                download::Downloader::new(
                    log_wrapper.as_mut().expect("api log wrapper initialized"),
                    limiter,
                    false,
                    None,
                    None,
                    None,
                )
            };
            let (driver, mut child) = chrome::ChromeDriver::get(&data_dir, &asset_downloader, true).await?;
            let result = async {
                let downloader = downloaders::find_downloader_for_url(&driver, false, url.as_str())
                    .await
                    .context("no downloader supports the supplied url")?;
                downloader
                    .get_catalog_info(downloaders::InfoRequest {
                        resolve_streams: request.resolve_streams,
                    })
                    .await
            }
            .await;
            let _ = driver.quit().await;
            let _ = child.kill();
            result
        })
        .await
        .map_err(|_| ApiError::Timeout("series info scraping timed out".to_owned()))?
        .map_err(|err| {
            log::warn!("Series info scraping failed: {err:#}");
            ApiError::ExtractorFailed("failed to scrape series info from the supplied url".to_owned())
        })?;

        return Ok(InfoResponse {
            supported: true,
            downloader: Some("auto".to_owned()),
            title: Some(catalog.title.clone()),
            description: catalog.description.clone(),
            status: catalog.status.clone(),
            year: catalog.year,
            catalog: Some(catalog),
        });
    }

    let supported = extractors::exists_extractor_for_url(url.as_str(), None).await;
    Ok(InfoResponse {
        supported,
        downloader: supported.then_some("extractor".to_owned()),
        title: None,
        description: None,
        status: None,
        year: None,
        catalog: None,
    })
}
