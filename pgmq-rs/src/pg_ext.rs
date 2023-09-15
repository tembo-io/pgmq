use log::info;
use pgmq_core::{
    errors::PgmqError,
    types::{Message, QUEUE_PREFIX},
    util::{check_input, connect},
};
use serde::{Deserialize, Serialize};
use sqlx::types::chrono::Utc;
use sqlx::{Executor, Pool, Postgres};

const DEFAULT_POLL_TIMEOUT_S: i32 = 5;
const DEFAULT_POLL_INTERVAL_MS: i32 = 250;

/// Main controller for interacting with a managed by the PGMQ Postgres extension.
#[derive(Clone, Debug)]
pub struct PGMQueueExt {
    pub url: String,
    pub connection: Pool<Postgres>,
}

pub struct PGMQueueMeta {
    pub queue_name: String,
    pub created_at: chrono::DateTime<Utc>,
}
impl PGMQueueExt {
    /// Initialize a connection to PGMQ/Postgres
    pub async fn new(url: String, max_connections: u32) -> Result<PGMQueueExt, PgmqError> {
        Ok(PGMQueueExt {
            connection: connect(&url, max_connections).await?,
            url,
        })
    }

    /// BYOP  - bring your own pool
    /// initialize a PGMQ connection with your own SQLx Postgres connection pool
    pub async fn new_with_pool(pool: Pool<Postgres>) -> Result<PGMQueueExt, PgmqError> {
        Ok(PGMQueueExt {
            url: "".to_owned(),
            connection: pool,
        })
    }

    pub async fn init(&self) -> Result<bool, PgmqError> {
        sqlx::query!("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")
            .execute(&self.connection)
            .await
            .map(|_| true)
            .map_err(PgmqError::from)
    }

    /// Errors when there is any database error and Ok(false) when the queue already exists.
    pub async fn create(&self, queue_name: &str) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        sqlx::query!("SELECT * from pgmq.create($1::text);", queue_name)
            .execute(&self.connection)
            .await?;
        Ok(true)
    }

    /// Create a new partitioned queue.
    /// Errors when there is any database error and Ok(false) when the queue already exists.
    pub async fn create_partitioned(&self, queue_name: &str) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let queue_table = format!("pgmq.{QUEUE_PREFIX}_{queue_name}");
        // we need to check whether the queue exists first
        // pg_partman create operations are currently unable to be idempotent
        let exists_stmt = format!(
            "SELECT EXISTS(SELECT * from part_config where parent_table = '{queue_table}');",
            queue_table = queue_table
        );
        let exists = sqlx::query_scalar(&exists_stmt)
            .fetch_one(&self.connection)
            .await?;
        if exists {
            info!("queue: {} already exists", queue_name);
            Ok(false)
        } else {
            sqlx::query!(
                "SELECT * from pgmq.create_partitioned($1::text);",
                queue_name
            )
            .execute(&self.connection)
            .await?;
            Ok(true)
        }
    }

    /// Drop an existing queue table.
    pub async fn drop_queue(&self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        self.connection
            .execute(sqlx::query!(
                "SELECT * from pgmq.drop_queue($1::text);",
                queue_name
            ))
            .await?;

        Ok(())
    }

    /// Drop an existing queue table.
    pub async fn purge_queue(&self, queue_name: &str) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let purged = sqlx::query!("SELECT * from pgmq.purge_queue($1::text);", queue_name)
            .fetch_one(&self.connection)
            .await?;

        Ok(purged.purge_queue.expect("no purged count"))
    }

    /// List all queues in the Postgres instance.
    pub async fn list_queues(&self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        let queues = sqlx::query!("SELECT * from pgmq.list_queues();")
            .fetch_all(&self.connection)
            .await?;
        if queues.is_empty() {
            Ok(None)
        } else {
            let queues = queues
                .into_iter()
                .map(|q| PGMQueueMeta {
                    queue_name: q.queue_name.expect("queue_name missing"),
                    created_at: q.created_at.expect("created_at missing"),
                })
                .collect();
            Ok(Some(queues))
        }
    }

    // Set the visibility time on an existing message.
    pub async fn set_vt<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        msg_id: i64,
        vt: i32,
    ) -> Result<Message<T>, PgmqError> {
        check_input(queue_name)?;
        let updated = sqlx::query!(
            "SELECT * from pgmq.set_vt($1::text, $2::bigint, $3::integer);",
            queue_name,
            msg_id,
            vt
        )
        .fetch_one(&self.connection)
        .await?;
        let raw_msg = updated.message.expect("no message");
        let parsed_msg = serde_json::from_value::<T>(raw_msg)?;

        Ok(Message {
            msg_id: updated.msg_id.expect("msg_id missing"),
            vt: updated.vt.expect("vt missing"),
            read_ct: updated.read_ct.expect("read_ct missing"),
            enqueued_at: updated.enqueued_at.expect("enqueued_at missing"),
            message: parsed_msg,
        })
    }

    pub async fn send<T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let msg = serde_json::json!(&message);
        let sent = sqlx::query!(
            "SELECT send as msg_id from pgmq.send($1::text, $2::jsonb, 0::integer);",
            queue_name,
            msg
        )
        .fetch_one(&self.connection)
        .await?;
        Ok(sent.msg_id.expect("no message id"))
    }

    pub async fn send_delay<T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
        delay: u32,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let msg = serde_json::json!(&message);
        let sent = sqlx::query!(
            "SELECT send as msg_id from pgmq.send($1::text, $2::jsonb, $3::int);",
            queue_name,
            msg,
            delay as i32
        )
        .fetch_one(&self.connection)
        .await?;
        Ok(sent.msg_id.expect("no message id"))
    }

    pub async fn read<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: i32,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let row = sqlx::query!(
            "SELECT * from pgmq.read($1::text, $2, $3)",
            queue_name,
            vt,
            1
        )
        .fetch_optional(&self.connection)
        .await?;
        match row {
            Some(row) => {
                // happy path - successfully read a message
                let raw_msg = row.message.expect("no message");
                let parsed_msg = serde_json::from_value::<T>(raw_msg)?;
                Ok(Some(Message {
                    msg_id: row.msg_id.expect("msg_id missing from queue table"),
                    vt: row.vt.expect("vt missing from queue table"),
                    read_ct: row.read_ct.expect("read_ct missing from queue table"),
                    enqueued_at: row
                        .enqueued_at
                        .expect("enqueued_at missing from queue table"),
                    message: parsed_msg,
                }))
            }
            None => {
                // no message found
                Ok(None)
            }
        }
    }

    pub async fn read_batch_with_poll<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: i32,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Vec<Message<T>>>, PgmqError> {
        check_input(queue_name)?;
        let poll_timeout_s = poll_timeout.map_or(DEFAULT_POLL_TIMEOUT_S, |t| t.as_secs() as i32);
        let poll_interval_ms =
            poll_interval.map_or(DEFAULT_POLL_INTERVAL_MS, |i| i.as_millis() as i32);
        let result = sqlx::query!(
            "SELECT * from pgmq.read_with_poll($1::text, $2, $3, $4, $5)",
            queue_name,
            vt,
            max_batch_size,
            poll_timeout_s,
            poll_interval_ms
        )
        .fetch_all(&self.connection)
        .await;

        match result {
            Err(sqlx::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e)?,
            Ok(rows) => {
                // happy path - successfully read messages
                let mut messages: Vec<Message<T>> = Vec::new();
                for row in rows.iter() {
                    let raw_msg = row.message.clone().expect("no message");
                    let parsed_msg = serde_json::from_value::<T>(raw_msg);
                    if let Err(e) = parsed_msg {
                        return Err(PgmqError::JsonParsingError(e));
                    } else if let Ok(parsed_msg) = parsed_msg {
                        messages.push(Message {
                            msg_id: row.msg_id.expect("msg_id missing from queue table"),
                            vt: row.vt.expect("vt missing from queue table"),
                            read_ct: row.read_ct.expect("read_ct missing from queue table"),
                            enqueued_at: row
                                .enqueued_at
                                .expect("enqueued_at missing from queue table"),
                            message: parsed_msg,
                        })
                    }
                }
                Ok(Some(messages))
            }
        }
    }

    /// Move a message to the archive table.
    pub async fn archive(&self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let arch = sqlx::query!(
            "SELECT * from pgmq.archive($1::text, $2::bigint)",
            queue_name,
            msg_id
        )
        .fetch_one(&self.connection)
        .await?;
        Ok(arch.archive.expect("no archive result"))
    }

    /// Move a message to the archive table.
    pub async fn archive_batch(
        &self,
        queue_name: &str,
        msg_ids: &[i64],
    ) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let arch = sqlx::query!(
            "SELECT * from pgmq.archive($1::text, $2::bigint[])",
            queue_name,
            msg_ids
        )
        .fetch_one(&self.connection)
        .await?;
        Ok(arch.archive.expect("no archive result"))
    }

    // Read and message and immediately delete it.
    pub async fn pop<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let row = sqlx::query!("SELECT * from pgmq.pop($1::text)", queue_name,)
            .fetch_optional(&self.connection)
            .await?;
        match row {
            Some(row) => {
                // happy path - successfully read a message
                let raw_msg = row.message.expect("no message");
                let parsed_msg = serde_json::from_value::<T>(raw_msg)?;
                Ok(Some(Message {
                    msg_id: row.msg_id.expect("msg_id missing from queue table"),
                    vt: row.vt.expect("vt missing from queue table"),
                    read_ct: row.read_ct.expect("read_ct missing from queue table"),
                    enqueued_at: row
                        .enqueued_at
                        .expect("enqueued_at missing from queue table"),
                    message: parsed_msg,
                }))
            }
            None => {
                // no message found
                Ok(None)
            }
        }
    }

    // Delete a message by message id.
    pub async fn delete(&self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        let row = sqlx::query!(
            "SELECT * from pgmq.delete($1::text, $2::bigint)",
            queue_name,
            msg_id
        )
        .fetch_one(&self.connection)
        .await?;
        Ok(row.delete.expect("no delete result"))
    }

    // Delete with a slice of message ids
    pub async fn delete_batch(&self, queue_name: &str, msg_id: &[i64]) -> Result<bool, PgmqError> {
        let row = sqlx::query!(
            "SELECT * from pgmq.delete($1::text, $2::bigint[])",
            queue_name,
            msg_id
        )
        .fetch_one(&self.connection)
        .await?;
        Ok(row.delete.expect("no delete result"))
    }
}
