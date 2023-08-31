use pgmq_core::errors::PgmqError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgmqExtError {
    #[error("unexpected error")]
    SqlError(#[from] pgrx::spi::Error),

    #[error("{0}")]
    Core(#[from] PgmqError),

    #[error("{0} invalid types")]
    TypeErrorError(String),

    #[error("missing dependency: {0}")]
    MissingDependency(String),

    #[error("invalid delay: {0}, must be an integer >= 0")]
    InvalidDelay(i32),
}
