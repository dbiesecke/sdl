use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    UnsupportedUrl(String),
    #[error("{0}")]
    ExtractorFailed(String),
    #[error("{0}")]
    UpstreamFailed(String),
    #[error("{0}")]
    Timeout(String),
    #[error("{0}")]
    Overloaded(String),
    #[error("{0}")]
    Internal(String),
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    error: String,
}

impl ApiError {
    pub fn code(&self) -> &'static str {
        match self {
            ApiError::BadRequest(_) => "bad_request",
            ApiError::UnsupportedUrl(_) => "unsupported_url",
            ApiError::ExtractorFailed(_) => "extractor_failed",
            ApiError::UpstreamFailed(_) => "upstream_failed",
            ApiError::Timeout(_) => "timeout",
            ApiError::Overloaded(_) => "overloaded",
            ApiError::Internal(_) => "internal",
        }
    }
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::UnsupportedUrl(_) => StatusCode::NOT_FOUND,
            ApiError::ExtractorFailed(_) | ApiError::UpstreamFailed(_) => StatusCode::BAD_GATEWAY,
            ApiError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            ApiError::Overloaded(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(ErrorBody {
            code: self.code(),
            error: self.to_string(),
        })
    }
}
