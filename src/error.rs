use axum::{
    response::{IntoResponse, Response}, 
    http::StatusCode
};
use log::warn;
use s3::error::S3Error;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Not modified: {0}")]
    NotModified(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Access forbidden: {0}")]
    Forbidden(String),
    #[error("Origin unavailable: {0}")]
    Unavailable(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error)
}

/// Custom IntoResponse impl for Error type
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        // convert error to status code
        let status = match self {
            Error::NotModified(_) => StatusCode::NOT_MODIFIED,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Forbidden(_) => StatusCode::FORBIDDEN,
            Error::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Error::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Config(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let msg = self.to_string();
        warn!("return response with status: {status}, message: {msg}"); 
        // calling other implementations    
        (status, msg).into_response()
    }
}

/// Convert from S3 client errors
impl From<S3Error> for Error {
    fn from(e: S3Error) -> Self {
        match e {
            S3Error::Http(304, e)   => Error::NotModified(e),
            S3Error::Http(404, _)   => Error::NotFound(e.to_string()),
            S3Error::Http(403, _)   => Error::Forbidden(e.to_string()),
            S3Error::Credentials(_) => Error::Forbidden(e.to_string()),
            S3Error::MaxExpiry(_)   => Error::Unavailable(e.to_string()),
            S3Error::HttpFail       => Error::Unavailable(e.to_string()),
            S3Error::Reqwest(e)     => Error::Unavailable(e.to_string()),
            S3Error::Io(e)   => Error::Io(e),
            _ => Error::Internal(e.to_string())
        }
    }
}

/// Convert from serde errors
impl From<serde_json::error::Error> for Error {
    fn from(e: serde_json::error::Error) -> Self {
        Error::Internal(e.to_string())
    }
}