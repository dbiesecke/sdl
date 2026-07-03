use actix_web::{web, HttpResponse};

use crate::api::error::ApiError;
use crate::api::sdl_wrapper;
use crate::api::types::{HealthResponse, InfoRequest, PlayRequest};

pub async fn get_play(query: web::Query<PlayRequest>) -> Result<HttpResponse, ApiError> {
    let response = sdl_wrapper::play(query.into_inner()).await?;
    Ok(HttpResponse::Ok().json(response))
}

pub async fn post_play(payload: web::Json<PlayRequest>) -> Result<HttpResponse, ApiError> {
    let response = sdl_wrapper::play(payload.into_inner()).await?;
    Ok(HttpResponse::Ok().json(response))
}

pub async fn get_info(query: web::Query<InfoRequest>) -> Result<HttpResponse, ApiError> {
    let response = sdl_wrapper::info(query.into_inner().url).await?;
    Ok(HttpResponse::Ok().json(response))
}

pub async fn post_info(payload: web::Json<InfoRequest>) -> Result<HttpResponse, ApiError> {
    let response = sdl_wrapper::info(payload.into_inner().url).await?;
    Ok(HttpResponse::Ok().json(response))
}

pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(HealthResponse {
        name: "sdl-api",
        routes: &["GET /api/play", "POST /api/play", "GET /api/info", "POST /api/info"],
    })
}
