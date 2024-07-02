use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("{0}")]
    Generic(#[from] anyhow::Error),
    #[error("failed to authenticate")]
    FailedAuth,
    #[error("failed to validate payload")]
    FailedValidation,
    #[error("failed to parse payload")]
    FailedJsonParse,
    #[error("not found")]
    NotFound,
    #[error("error with environment configuration")]
    EnvConfig(#[from] std::env::VarError),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            Self::Generic(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("internal server error: {}", err),
            ),
            Self::FailedAuth => (StatusCode::BAD_REQUEST, "authentication failed".into()),
            Self::FailedValidation => (StatusCode::BAD_REQUEST, "validation failed".into()),
            Self::FailedJsonParse => (StatusCode::BAD_REQUEST, "json parse failed".into()),
            Self::NotFound => (StatusCode::NOT_FOUND, "not found".into()),
            Self::EnvConfig(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("environment variable error: {}", err),
            ),
        };

        let body = Json(json!({
            "error": error_message,
        }));

        (status, body).into_response()
    }
}