use crate::errors::PgmqError;
use crate::types::{Message, QUEUE_PREFIX};
use crate::util::{check_input, connect};
use log::info;
use serde::{Deserialize, Serialize};
use sqlx::types::chrono::Utc;
use sqlx::{Pool, Postgres};

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
    pub is_unlogged: bool,
    pub is_partitioned: bool,
}
impl PGMQueueExt {
    /// Initialize a connection to PGMQ/Postgres
    pub async fn new(url: String, max_connections: u32) -> Result<Self, PgmqError> {
        Ok(Self {
            connection: connect(&url, max_connections).await?,
            url,
        })
    }

    /// BYOP  - bring your own pool
    /// initialize a PGMQ connection with your own SQLx Postgres connection pool
    pub async fn new_with_pool(pool: Pool<Postgres>) -> Self {
        Self {
            url: "".to_owned(),
            connection: pool,
        }
    }

    pub async fn init_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        executor: E,
    ) -> Result<bool, PgmqError> {
        sqlx::query!("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")
            .execute(executor)
            .await
            .map(|_| true)
            .map_err(PgmqError::from)
    }

    pub async fn init(&self) -> Result<bool, PgmqError> {
        self.init_with_cxn(&self.connection).await
    }

    pub async fn create_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        sqlx::query!(
            "SELECT * from pgmq.create(queue_name=>$1::text);",
            queue_name
        )
        .execute(executor)
        .await?;
        Ok(true)
    }
    /// Errors when there is any database error and Ok(false) when the queue already exists.
    pub async fn create(&self, queue_name: &str) -> Result<bool, PgmqError> {
        self.create_with_cxn(queue_name, &self.connection).await?;
        Ok(true)
    }

    pub async fn create_unlogged_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        sqlx::query!(
            "SELECT * from pgmq.create_unlogged(queue_name=>$1::text);",
            queue_name
        )
        .execute(executor)
        .await?;
        Ok(true)
    }

    /// Errors when there is any database error and Ok(false) when the queue already exists.
    pub async fn create_unlogged(&self, queue_name: &str) -> Result<bool, PgmqError> {
        self.create_unlogged_with_cxn(queue_name, &self.connection)
            .await?;
        Ok(true)
    }

    pub async fn create_partitioned_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres> + std::marker::Copy,
    >(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let queue_table = format!("pgmq.{QUEUE_PREFIX}_{queue_name}");
        // we need to check whether the queue exists first
        // pg_partman create operations are currently unable to be idempotent
        let exists_stmt = "SELECT EXISTS(SELECT * from part_config where parent_table = $1);";
        let exists = sqlx::query_scalar(exists_stmt)
            .bind(queue_table)
            .fetch_one(executor)
            .await?;
        if exists {
            info!("queue: {} already exists", queue_name);
            Ok(false)
        } else {
            sqlx::query!(
                "SELECT * from pgmq.create_partitioned(queue_name=>$1::text);",
                queue_name
            )
            .execute(executor)
            .await?;
            Ok(true)
        }
    }

    /// Create a new partitioned queue.
    /// Errors when there is any database error and Ok(false) when the queue already exists.
    pub async fn create_partitioned(&self, queue_name: &str) -> Result<bool, PgmqError> {
        self.create_partitioned_with_cxn(queue_name, &self.connection)
            .await
    }

    pub async fn drop_queue_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        executor
            .execute(sqlx::query!(
                "SELECT * from pgmq.drop_queue(queue_name=>$1::text);",
                queue_name
            ))
            .await?;

        Ok(())
    }

    /// Drop an existing queue table.
    pub async fn drop_queue(&self, queue_name: &str) -> Result<(), PgmqError> {
        self.drop_queue_with_cxn(queue_name, &self.connection).await
    }

    /// Drop an existing queue table.
    pub async fn purge_queue_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let purged = sqlx::query!(
            "SELECT * from pgmq.purge_queue(queue_name=>$1::text);",
            queue_name
        )
        .fetch_one(executor)
        .await?;

        Ok(purged.purge_queue.expect("no purged count"))
    }

    /// Drop an existing queue table.
    pub async fn purge_queue(&self, queue_name: &str) -> Result<i64, PgmqError> {
        self.purge_queue_with_cxn(queue_name, &self.connection)
            .await
    }

    pub async fn list_queues_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        executor: E,
    ) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        let queues = sqlx::query!("SELECT * from pgmq.list_queues();")
            .fetch_all(executor)
            .await?;
        if queues.is_empty() {
            Ok(None)
        } else {
            let queues = queues
                .into_iter()
                .map(|q| PGMQueueMeta {
                    queue_name: q.queue_name.expect("queue_name missing"),
                    created_at: q.created_at.expect("created_at missing"),
                    is_unlogged: q.is_unlogged.expect("is_unlogged missing"),
                    is_partitioned: q.is_partitioned.expect("is_partitioned missing"),
                })
                .collect();
            Ok(Some(queues))
        }
    }

    /// List all queues in the Postgres instance.
    pub async fn list_queues(&self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        self.list_queues_with_cxn(&self.connection).await
    }

    pub async fn set_vt_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        msg_id: i64,
        vt: i32,
        executor: E,
    ) -> Result<Message<T>, PgmqError> {
        check_input(queue_name)?;
        let updated = sqlx::query!(
            "SELECT * from pgmq.set_vt(queue_name=>$1::text, msg_id=>$2::bigint, vt=>$3::integer);",
            queue_name,
            msg_id,
            vt
        )
        .fetch_one(executor)
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
    // Set the visibility time on an existing message.
    pub async fn set_vt<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        msg_id: i64,
        vt: i32,
    ) -> Result<Message<T>, PgmqError> {
        self.set_vt_with_cxn(queue_name, msg_id, vt, &self.connection)
            .await
    }

    pub async fn send_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>, T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
        executor: E,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let msg = serde_json::json!(&message);
        let prepared = sqlx::query!(
            "SELECT send as msg_id from pgmq.send(queue_name=>$1::text, msg=>$2::jsonb, delay=>0::integer);",
            queue_name,
            msg
        );
        let sent = prepared.fetch_one(executor).await?;
        Ok(sent.msg_id.expect("no message id"))
    }

    pub async fn send<T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
    ) -> Result<i64, PgmqError> {
        self.send_with_cxn(queue_name, message, &self.connection)
            .await
    }

    pub async fn send_delay_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: Serialize,
    >(
        &self,
        queue_name: &str,
        message: &T,
        delay: u32,
        executor: E,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let msg = serde_json::json!(&message);
        let sent = sqlx::query!(
            "SELECT send as msg_id from pgmq.send(queue_name=>$1::text, msg=>$2::jsonb, delay=>$3::int);",
            queue_name,
            msg,
            delay as i32
        )
        .fetch_one(executor)
        .await?;
        Ok(sent.msg_id.expect("no message id"))
    }

    pub async fn send_delay<T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
        delay: u32,
    ) -> Result<i64, PgmqError> {
        self.send_delay_with_cxn(queue_name, message, delay, &self.connection)
            .await
    }

    pub async fn read_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: i32,
        executor: E,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let row = sqlx::query!(
            "SELECT * from pgmq.read(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer)",
            queue_name,
            vt,
            1
        )
        .fetch_optional(executor)
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
    pub async fn read<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: i32,
    ) -> Result<Option<Message<T>>, PgmqError> {
        self.read_with_cxn(queue_name, vt, &self.connection).await
    }

    pub async fn read_batch_with_poll_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: i32,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
        executor: E,
    ) -> Result<Option<Vec<Message<T>>>, PgmqError> {
        check_input(queue_name)?;
        let poll_timeout_s = poll_timeout.map_or(DEFAULT_POLL_TIMEOUT_S, |t| t.as_secs() as i32);
        let poll_interval_ms =
            poll_interval.map_or(DEFAULT_POLL_INTERVAL_MS, |i| i.as_millis() as i32);
        let result = sqlx::query!(
            "SELECT * from pgmq.read_with_poll(
                queue_name=>$1::text,
                vt=>$2::integer,
                qty=>$3::integer,
                max_poll_seconds=>$4::integer,
                poll_interval_ms=>$5::integer
            )",
            queue_name,
            vt,
            max_batch_size,
            poll_timeout_s,
            poll_interval_ms
        )
        .fetch_all(executor)
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

    pub async fn read_batch_with_poll<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: i32,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Vec<Message<T>>>, PgmqError> {
        self.read_batch_with_poll_with_cxn(
            queue_name,
            vt,
            max_batch_size,
            poll_timeout,
            poll_interval,
            &self.connection,
        )
        .await
    }

    pub async fn archive_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        msg_id: i64,
        executor: E,
    ) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let arch = sqlx::query!(
            "SELECT * from pgmq.archive(queue_name=>$1::text, msg_id=>$2::bigint)",
            queue_name,
            msg_id
        )
        .fetch_one(executor)
        .await?;
        Ok(arch.archive.expect("no archive result"))
    }
    /// Move a message to the archive table.
    pub async fn archive(&self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        self.archive_with_cxn(queue_name, msg_id, &self.connection)
            .await
    }

    /// Move a slice of messages to the archive table.
    pub async fn archive_batch_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        msg_ids: &[i64],
        executor: E,
    ) -> Result<usize, PgmqError> {
        check_input(queue_name)?;
        let qty = sqlx::query!(
            "SELECT * from pgmq.archive(queue_name=>$1::text, msg_ids=>$2::bigint[])",
            queue_name,
            msg_ids
        )
        .fetch_all(executor)
        .await?
        .len();

        Ok(qty)
    }

    /// Move a slice of messages to the archive table.
    pub async fn archive_batch(
        &self,
        queue_name: &str,
        msg_ids: &[i64],
    ) -> Result<usize, PgmqError> {
        self.archive_batch_with_cxn(queue_name, msg_ids, &self.connection)
            .await
    }

    pub async fn pop_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let row = sqlx::query!("SELECT * from pgmq.pop(queue_name=>$1::text)", queue_name,)
            .fetch_optional(executor)
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
    // Read and message and immediately delete it.
    pub async fn pop<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError> {
        self.pop_with_cxn(queue_name, &self.connection).await
    }

    pub async fn delete_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        msg_id: i64,
        executor: E,
    ) -> Result<bool, PgmqError> {
        let row = sqlx::query!(
            "SELECT * from pgmq.delete(queue_name=>$1::text, msg_id=>$2::bigint)",
            queue_name,
            msg_id
        )
        .fetch_one(executor)
        .await?;
        Ok(row.delete.expect("no delete result"))
    }

    // Delete a message by message id.
    pub async fn delete(&self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        self.delete_with_cxn(queue_name, msg_id, &self.connection)
            .await
    }

    pub async fn delete_batch_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        msg_id: &[i64],
        executor: E,
    ) -> Result<usize, PgmqError> {
        let qty = sqlx::query!(
            "SELECT * from pgmq.delete(queue_name=>$1::text, msg_ids=>$2::bigint[])",
            queue_name,
            msg_id
        )
        .fetch_all(executor)
        .await?
        .len();

        // FIXME: change function signature to Vec<i64> and return rows
        Ok(qty)
    }

    // Delete with a slice of message ids
    pub async fn delete_batch(&self, queue_name: &str, msg_id: &[i64]) -> Result<usize, PgmqError> {
        self.delete_batch_with_cxn(queue_name, msg_id, &self.connection)
            .await
    }
}
