use pgmq_crate::errors::PgmqError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgmqExtError {
    #[error("unexpected error")]
    SqlError(#[from] pgrx::spi::Error),

    #[error("invalid queue name: \"{0}\"")]
    InvalidQueueName(String),

    #[error("queue error")]
    QueueError,

    #[error("{0} invalid types")]
    TypeErrorError(String),

    #[error("missing dependency: {0}")]
    MissingDependency(String),
}

impl From<PgmqError> for PgmqExtError {
    fn from(error: PgmqError) -> PgmqExtError {
        match error {
            PgmqError::InvalidQueueName { name } => PgmqExtError::InvalidQueueName(name),
            _ => PgmqExtError::QueueError,
        }
    }
}
