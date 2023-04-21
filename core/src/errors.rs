//! Custom errors types for PGMQ
use thiserror::Error;
use url::ParseError;

#[derive(Error, Debug)]
pub enum PgmqError {
    /// a json parsing error
    #[error("json parsing error {0}")]
    JsonParsingError(#[from] serde_json::error::Error),

    /// a url parsing error
    #[error("url parsing error {0}")]
    UrlParsingError(#[from] ParseError),

    /// a database error
    #[error("database error {0}")]
    DatabaseError(#[from] sqlx::Error),

    /// a queue name error
    /// queue names must be alphanumeric and start with a letter
    #[error("naming error: {name}")]
    InvalidQueueName { name: String },
}
