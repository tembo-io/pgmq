use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgmqError {
    /// a parsing error
    #[error("parsing error {0}")]
    ParsingError(#[from] serde_json::error::Error),

    /// a database error
    #[error("database error {0}")]
    DatabaseError(#[from] sqlx::Error),
}
