use actix_web::{http::header, web, Error, HttpResponse};
use async_stream::try_stream;
use bytes::Bytes;
use futures_util::Stream;
use reqwest::header::{HeaderMap, REFERER};
use tokio::sync::OwnedSemaphorePermit;
use url::Url;

use super::error::ApiError;
use super::sdl_wrapper;
use super::types::{ApiState, InfoRequest, PlayRequest, ResolvedVideo};

pub async fn play_get(state: web::Data<ApiState>, query: web::Query<PlayRequest>) -> Result<HttpResponse, ApiError> {
    play(state, query.into_inner()).await
}

pub async fn play_post(state: web::Data<ApiState>, body: web::Json<PlayRequest>) -> Result<HttpResponse, ApiError> {
    play(state, body.into_inner()).await
}

pub async fn info_get(query: web::Query<InfoRequest>) -> Result<HttpResponse, ApiError> {
    Ok(HttpResponse::Ok().json(sdl_wrapper::build_info_response(query.into_inner()).await?))
}

pub async fn info_post(body: web::Json<InfoRequest>) -> Result<HttpResponse, ApiError> {
    Ok(HttpResponse::Ok().json(sdl_wrapper::build_info_response(body.into_inner()).await?))
}

async fn play(state: web::Data<ApiState>, request: PlayRequest) -> Result<HttpResponse, ApiError> {
    let permit = tokio::time::timeout(state.permit_timeout, state.download_semaphore.clone().acquire_owned())
        .await
        .map_err(|_| ApiError::Overloaded("too many concurrent downloads".to_owned()))?
        .map_err(|_| ApiError::Internal("download semaphore was closed".to_owned()))?;

    let resolved = tokio::time::timeout(state.extraction_timeout, sdl_wrapper::resolve_play_request(request))
        .await
        .map_err(|_| ApiError::Timeout("timed out while resolving stream URL".to_owned()))??;

    let content_type = content_type_for_url(&resolved.url);
    let stream = video_stream(state.client.clone(), resolved.clone(), permit);

    Ok(HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, content_type))
        .insert_header((
            header::CONTENT_DISPOSITION,
            format!("inline; filename=\"{}\"", sanitize_filename(&resolved.filename)),
        ))
        .streaming(stream))
}

fn video_stream(
    client: reqwest::Client,
    video: ResolvedVideo,
    _permit: OwnedSemaphorePermit,
) -> impl Stream<Item = Result<Bytes, Error>> {
    try_stream! {
        if is_m3u8(&video.url) {
            let playlist_url = Url::parse(&video.url).map_err(|err| ApiError::UpstreamFailed(format!("invalid playlist URL: {err}")))?;
            let playlist_bytes = fetch_bytes(&client, playlist_url.as_str(), video.referer.as_deref()).await?;
            let parsed = m3u8_rs::parse_playlist_res(&playlist_bytes)
                .map_err(|err| ApiError::UpstreamFailed(format!("failed to parse m3u8 playlist: {err}")))?;

            match parsed {
                m3u8_rs::Playlist::MasterPlaylist(master) => {
                    let variant = master.variants.into_iter().max_by_key(|variant| variant.bandwidth)
                        .ok_or_else(|| ApiError::UpstreamFailed("m3u8 master playlist has no variants".to_owned()))?;
                    let variant_url = playlist_url.join(&variant.uri)
                        .map_err(|err| ApiError::UpstreamFailed(format!("invalid variant URL: {err}")))?;
                    let media_bytes = fetch_bytes(&client, variant_url.as_str(), video.referer.as_deref()).await?;
                    let (_, media) = m3u8_rs::parse_media_playlist(&media_bytes)
                        .map_err(|_| ApiError::UpstreamFailed("failed to parse media playlist".to_owned()))?;
                    for segment in media.segments {
                        let segment_url = variant_url.join(&segment.uri)
                            .map_err(|err| ApiError::UpstreamFailed(format!("invalid segment URL: {err}")))?;
                        yield fetch_bytes(&client, segment_url.as_str(), video.referer.as_deref()).await?;
                    }
                }
                m3u8_rs::Playlist::MediaPlaylist(media) => {
                    for segment in media.segments {
                        let segment_url = playlist_url.join(&segment.uri)
                            .map_err(|err| ApiError::UpstreamFailed(format!("invalid segment URL: {err}")))?;
                        yield fetch_bytes(&client, segment_url.as_str(), video.referer.as_deref()).await?;
                    }
                }
            }
        } else {
            let mut request = client.get(&video.url);
            if let Some(referer) = &video.referer {
                request = request.header(REFERER, referer);
            }
            let response = request.send().await
                .map_err(|err| ApiError::UpstreamFailed(format!("failed to open upstream stream: {err}")))?
                .error_for_status()
                .map_err(|err| ApiError::UpstreamFailed(format!("upstream returned an error: {err}")))?;
            let mut stream = response.bytes_stream();
            while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
                yield chunk.map_err(|err| ApiError::UpstreamFailed(format!("failed while reading upstream stream: {err}")))?;
            }
        }
    }
}

async fn fetch_bytes(client: &reqwest::Client, url: &str, referer: Option<&str>) -> Result<Bytes, ApiError> {
    let mut headers = HeaderMap::new();
    if let Some(referer) = referer {
        headers.insert(
            REFERER,
            referer
                .parse()
                .map_err(|_| ApiError::BadRequest("invalid referer header".to_owned()))?,
        );
    }
    client
        .get(url)
        .headers(headers)
        .send()
        .await
        .map_err(|err| ApiError::UpstreamFailed(format!("failed to fetch {url}: {err}")))?
        .error_for_status()
        .map_err(|err| ApiError::UpstreamFailed(format!("upstream returned an error for {url}: {err}")))?
        .bytes()
        .await
        .map_err(|err| ApiError::UpstreamFailed(format!("failed to read {url}: {err}")))
}

fn is_m3u8(url: &str) -> bool {
    Url::parse(url)
        .map(|url| url.path().to_ascii_lowercase().ends_with(".m3u8"))
        .unwrap_or(false)
}

fn content_type_for_url(url: &str) -> &'static str {
    if is_m3u8(url) {
        "video/mp2t"
    } else if url.to_ascii_lowercase().contains(".mp4") {
        "video/mp4"
    } else {
        "application/octet-stream"
    }
}

fn sanitize_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}
