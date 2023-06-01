use s3::error::S3Error;
use thiserror::Error;

#[derive(Debug, Error, Responder)]
pub enum Error {
    #[error("Not modified: {0}")]
    #[response(status = 304)]
    NotModified(String),
    #[error("Not found: {0}")]
    #[response(status = 404)]
    NotFound(String),
    #[error("Access forbidden: {0}")]
    #[response(status = 403)]
    Forbidden(String),
    #[error("Service unavailable: {0}")]
    #[response(status = 503)]
    Unavailable(String),
    #[error("Internal error: {0}")]
    #[response(status = 500)]
    Internal(String),
    #[error("Configuration error: {0}")]
    #[response(status = 500)]
    Config(String),
    #[error("IO error: {0}")]
    #[response(status = 500)]
    Io(#[from] std::io::Error)
}

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
            _ => Error::Internal(e.to_string())
        }
    }
}