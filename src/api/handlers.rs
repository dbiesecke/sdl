use actix_web::{web, HttpResponse};

use crate::api::error::ApiError;
use crate::api::sdl_wrapper;
use crate::api::types::{
    ApiState, DownloadJson, DownloadQuery, HealthResponse, InfoRequest, InfoResolveRequest, PlayJson, PlayQuery,
};

pub async fn get_play(state: web::Data<ApiState>, query: web::Query<PlayQuery>) -> Result<HttpResponse, ApiError> {
    sdl_wrapper::play(state.get_ref().clone(), query.into_inner()).await
}

pub async fn post_play(state: web::Data<ApiState>, payload: web::Json<PlayJson>) -> Result<HttpResponse, ApiError> {
    sdl_wrapper::play(state.get_ref().clone(), payload.into_inner()).await
}

pub async fn get_download(
    state: web::Data<ApiState>,
    query: web::Query<DownloadQuery>,
) -> Result<HttpResponse, ApiError> {
    sdl_wrapper::download(state.get_ref().clone(), query.into_inner()).await
}

pub async fn post_download(
    state: web::Data<ApiState>,
    payload: web::Json<DownloadJson>,
) -> Result<HttpResponse, ApiError> {
    sdl_wrapper::download(state.get_ref().clone(), payload.into_inner()).await
}

pub async fn get_info(state: web::Data<ApiState>, query: web::Query<InfoRequest>) -> Result<HttpResponse, ApiError> {
    let response = sdl_wrapper::info(state.get_ref().clone(), query.into_inner()).await?;
    Ok(HttpResponse::Ok().json(response))
}

pub async fn post_info(state: web::Data<ApiState>, payload: web::Json<InfoRequest>) -> Result<HttpResponse, ApiError> {
    let response = sdl_wrapper::info(state.get_ref().clone(), payload.into_inner()).await?;
    Ok(HttpResponse::Ok().json(response))
}

pub async fn get_info_resolve(
    state: web::Data<ApiState>,
    query: web::Query<InfoResolveRequest>,
) -> Result<HttpResponse, ApiError> {
    let response = sdl_wrapper::info_resolve(state.get_ref().clone(), query.into_inner()).await?;
    Ok(HttpResponse::Ok().json(response))
}

pub async fn post_info_resolve(
    state: web::Data<ApiState>,
    payload: web::Json<InfoResolveRequest>,
) -> Result<HttpResponse, ApiError> {
    let response = sdl_wrapper::info_resolve(state.get_ref().clone(), payload.into_inner()).await?;
    Ok(HttpResponse::Ok().json(response))
}

pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(HealthResponse {
        name: "sdl-api",
        routes: &[
            "GET /api/play",
            "POST /api/play",
            "GET /api/download",
            "POST /api/download",
            "GET /api/info",
            "POST /api/info",
            "GET /api/info_resolve",
            "POST /api/info_resolve",
            "GET /openapi.yaml",
        ],
    })
}

pub async fn openapi_yaml() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("application/yaml; charset=utf-8")
        .body(include_str!("openapi.yaml"))
}
