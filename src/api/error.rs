use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use serde::Serialize;

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    UnsupportedUrl(String),
    ExtractorFailed(String),
    UpstreamFailed(String),
    Timeout(String),
    ServiceUnavailable(String),
    Internal(String),
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
    code: &'a str,
}

impl ApiError {
    fn code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "bad_request",
            Self::UnsupportedUrl(_) => "unsupported_url",
            Self::ExtractorFailed(_) => "extractor_failed",
            Self::UpstreamFailed(_) => "upstream_failed",
            Self::Timeout(_) => "timeout",
            Self::ServiceUnavailable(_) => "service_unavailable",
            Self::Internal(_) => "internal",
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message)
            | Self::UnsupportedUrl(message)
            | Self::ExtractorFailed(message)
            | Self::UpstreamFailed(message)
            | Self::Timeout(message)
            | Self::ServiceUnavailable(message)
            | Self::Internal(message) => f.write_str(message),
        }
    }
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::UnsupportedUrl(_) => StatusCode::NOT_FOUND,
            Self::ExtractorFailed(_) | Self::UpstreamFailed(_) => StatusCode::BAD_GATEWAY,
            Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            Self::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(ErrorBody {
            error: &self.to_string(),
            code: self.code(),
        })
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(value: anyhow::Error) -> Self {
        log::error!("Internal API error: {value:#}");
        Self::Internal("internal server error".to_owned())
    }
}
