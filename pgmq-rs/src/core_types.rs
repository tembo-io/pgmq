use serde::Deserialize;
use sqlx::types::chrono::{DateTime, Utc};
use std::time::Duration;

use sqlx::FromRow;

pub const VT_DEFAULT: i32 = 30;
pub const READ_LIMIT_DEFAULT: i32 = 1;
pub const POLL_TIMEOUT_DEFAULT: Duration = Duration::from_secs(5);
pub const POLL_INTERVAL_DEFAULT: Duration = Duration::from_millis(250);

use chrono::serde::ts_seconds::deserialize as from_ts;

pub const QUEUE_PREFIX: &str = r#"q"#;
pub const ARCHIVE_PREFIX: &str = r#"a"#;
pub const PGMQ_SCHEMA: &str = "pgmq";

pub struct PGMQueueMeta {
    pub queue_name: String,
    pub is_partitioned: bool,
    pub created_at: DateTime<Utc>,
}

/// Message struct received from the queue
///
/// It is an "envelope" for the message that is stored in the queue.
/// It contains both the message body but also metadata about the message.
#[derive(Clone, Debug, Deserialize, FromRow)]
pub struct Message<T = serde_json::Value> {
    /// unique identifier for the message
    pub msg_id: i64,
    #[serde(deserialize_with = "from_ts")]
    /// "visibility time". The UTC timestamp at which the message will be available for reading again.
    pub vt: chrono::DateTime<Utc>,
    /// UTC timestamp that the message was sent to the queue
    pub enqueued_at: chrono::DateTime<Utc>,
    /// The number of times the message has been read. Increments on read.
    pub read_ct: i32,
    /// The message body.
    pub message: T,
}
