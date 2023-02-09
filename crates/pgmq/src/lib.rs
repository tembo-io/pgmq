//! # Postgres Message Queue
//!
//! A lightweight messaging queue for Rust, using Postgres as the backend.
//! Inspired by the [RSMQ project](https://github.com/smrchy/rsmq).
//!
//! ## Examples
//!
//! First, start any Postgres instance. It is the only external dependency.
//!
//! ```bash
//! docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
//! ```
//!
//! Example of sending, receiving, and deleting messages from the queue. Typically applications sending messages
//! will not be the same application reading the message.
//!
//! ```rust
//! use pgmq::{Message, PGMQueue};
//! use serde::{Serialize, Deserialize};
//! use serde_json::Value;
//!
//! #[tokio::main]
//! async fn main() {
//!     // CREATE A QUEUE
//!     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned()).await.expect("failed to connect to postgres");
//!     let myqueue = "myqueue".to_owned();
//!     queue.create(&myqueue).await.expect("Failed to create queue");
//!
//!     // SEND A `serde_json::Value` MESSAGE
//!     let msg1 = serde_json::json!({
//!         "foo": "bar"
//!     });
//!     let msg_id1: i64 = queue.send(&myqueue, &msg1).await.expect("Failed to enqueue message");
//!
//!     // SEND A STRUCT
//!     #[derive(Serialize, Debug, Deserialize)]
//!     struct MyMessage {
//!         foo: String,
//!     }
//!     let msg2 = MyMessage {
//!         foo: "bar".to_owned(),
//!     };
//!     let msg_id2: i64  = queue.send(&myqueue, &msg2).await.expect("Failed to enqueue message");
//!
//!     // READ A MESSAGE as `serde_json::Value`
//!     let vt: i32 = 30;
//!     let read_msg1: Message<Value> = queue.read::<Value>(&myqueue, Some(&vt)).await.unwrap().expect("no messages in the queue!");
//!     assert_eq!(read_msg1.msg_id, msg_id1);
//!
//!     // READ A MESSAGE as a struct
//!     let read_msg2: Message<MyMessage> = queue.read::<MyMessage>(&myqueue, Some(&vt)).await.unwrap().expect("no messages in the queue!");
//!     assert_eq!(read_msg2.msg_id, msg_id2);
//!
//!     // READ MULTIPLE MESSAGES as `serde_json::Value`
//!     let num_msgs = 2;
//!     let batch: Vec<Message<Value>> = queue.read_batch::<Value>(&myqueue, Some(&vt), &num_msgs).await.unwrap().expect("no messages in the queue!");
//!     for (i, message) in batch.iter().enumerate() {
//!         let index = i + 1;
//!         assert_eq!(message.msg_id.to_string(), index.to_string());
//!     }
//!
//!     // READ MULTIPLE MESSAGES as a struct
//!     let batch: Vec<Message<MyMessage>> = queue.read_batch::<MyMessage>(&myqueue, Some(&vt), &num_msgs).await.unwrap().expect("no messages in the queue!");
//!     for (i, message) in batch.iter().enumerate() {
//!         let index = i + 1;
//!         assert_eq!(message.msg_id.to_string(), index.to_string());
//!     }
//!
//!     // DELETE THE MESSAGE WE SENT
//!     let deleted = queue.delete(&myqueue, &read_msg1.msg_id).await.expect("Failed to delete message");
//!     let deleted = queue.delete(&myqueue, &read_msg2.msg_id).await.expect("Failed to delete message");
//!
//!     // No messages present after we've deleted all of them
//!     let no_msg: Option<Message<Value>> = queue.read::<Value>(&myqueue, Some(&vt)).await.unwrap();
//!     assert!(no_msg.is_none());
//!
//!     // SEND MULTIPLE MESSAGES as `serde_json::Value`
//!     let msgs1 = vec![
//!         serde_json::json!({"foo": "bar1"}),
//!         serde_json::json!({"foo": "bar2"}),
//!         serde_json::json!({"foo": "bar3"}),
//!     ];
//!     let msg_ids1 = queue.send_batch(&myqueue, &msgs1).await.expect("Failed to enqueue messages");
//!
//!     // SEND MULTIPLE MESSAGES as a struct
//!     let msgs2 = vec![
//!         MyMessage {foo: "bar1".to_owned()},
//!         MyMessage {foo: "bar2".to_owned()},
//!         MyMessage {foo: "bar3".to_owned()},
//!     ];
//!     let msg_ids2 = queue.send_batch(&myqueue, &msgs2).await.expect("Failed to enqueue messages");
//! }
//! ```
//! ## Sending messages
//!
//! `queue.send()` can be passed any type that implements `serde::Serialize`. This means you can prepare your messages as JSON or as a struct.
//!
//! ## Reading messages
//!
//! Reading a message will make it invisible (unavailable for consumption) for the duration of the visibility timeout (vt).
//! No messages are returned when the queue is empty or all messages are invisible.
//!
//! Messages can be parsed as serde_json::Value or into a struct. `queue.read()` returns an `Result<Option<Message<T>>, PGMQError>`
//! where `T` is the type of the message on the queue. It returns an error when there is an issue parsing the message or if PGMQ is unable to reach postgres.
//! Note that when parsing into a `struct`, the operation will return an error if
//! parsed as the type specified. For example, if the message expected is
//! `MyMessage{foo: "bar"}` but` {"hello": "world"}` is received, the application will panic.
//!
//! ## Archive or Delete a message
//!
//! Remove the message from the queue when you are done with it. You can either completely `.delete()`, or `.archive()` the message. Archived messages are deleted from the queue and inserted to the queue's archive table. Deleted messages are just deleted.

#![doc(html_root_url = "https://docs.rs/pgmq/")]

use log::LevelFilter;
use serde::{Deserialize, Serialize};
use sqlx::error::Error;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow};
use sqlx::types::chrono::Utc;
use sqlx::{ConnectOptions, FromRow};
use sqlx::{Pool, Postgres, Row};
use url::{ParseError, Url};

pub mod errors;
pub mod query;
use chrono::serde::ts_seconds::deserialize as from_ts;

const VT_DEFAULT: i32 = 30;
const READ_LIMIT_DEFAULT: i32 = 1;

#[derive(Debug, Deserialize, FromRow)]
pub struct Message<T = serde_json::Value> {
    pub msg_id: i64,
    #[serde(deserialize_with = "from_ts")]
    pub vt: chrono::DateTime<Utc>,
    pub enqueued_at: chrono::DateTime<Utc>,
    pub read_ct: i32,
    pub message: T,
}

#[derive(Debug)]
pub struct PGMQueue {
    pub url: String,
    pub connection: Pool<Postgres>,
}

impl PGMQueue {
    pub async fn new(url: String) -> Result<PGMQueue, errors::PgmqError> {
        let con = PGMQueue::connect(&url).await?;
        Ok(PGMQueue {
            url,
            connection: con,
        })
    }

    /// Connect to the database
    async fn connect(url: &str) -> Result<Pool<Postgres>, errors::PgmqError> {
        let options = conn_options(url)?;
        let pgp = PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(10))
            .max_connections(5)
            .connect_with(options)
            .await?;
        Ok(pgp)
    }

    /// Create a queue
    pub async fn create(&self, queue_name: &str) -> Result<(), errors::PgmqError> {
        let mut tx = self.connection.begin().await?;
        let setup = query::init_queue(queue_name);
        for q in setup {
            sqlx::query(&q).execute(&mut tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// destroy a queue
    pub async fn destroy(&self, queue_name: &str) -> Result<(), errors::PgmqError> {
        let mut tx = self.connection.begin().await?;
        let setup = query::destory_queue(queue_name);
        for q in setup {
            sqlx::query(&q).execute(&mut tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Send a message to the queue
    pub async fn send<T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
    ) -> Result<i64, errors::PgmqError> {
        let mut msgs: Vec<serde_json::Value> = Vec::new();
        let msg = serde_json::json!(&message);
        msgs.push(msg);
        let row: PgRow = sqlx::query(&query::enqueue(queue_name, &msgs))
            .fetch_one(&self.connection)
            .await?;
        let msg_id: i64 = row.get("msg_id");
        Ok(msg_id)
    }

    /// Send multiple messages to the queue
    pub async fn send_batch<T: Serialize>(
        &self,
        queue_name: &str,
        messages: &Vec<T>,
    ) -> Result<Vec<i64>, errors::PgmqError> {
        let mut msgs: Vec<serde_json::Value> = Vec::new();
        let mut msg_ids: Vec<i64> = Vec::new();
        for msg in messages.iter() {
            let binding = serde_json::json!(&msg);
            msgs.push(binding)
        }
        let rows: Vec<PgRow> = sqlx::query(&query::enqueue(queue_name, &msgs))
            .fetch_all(&self.connection)
            .await?;
        for row in rows.iter() {
            msg_ids.push(row.get("msg_id"));
        }
        Ok(msg_ids)
    }

    /// Reads a single message from the queue. If the queue is empty or all messages are invisible, `None` is returned.
    /// If a message is returned, it is made invisible for the duration of the visibility timeout (vt) in seconds.
    pub async fn read<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: Option<&i32>,
    ) -> Result<Option<Message<T>>, errors::PgmqError> {
        // map vt or default VT
        let vt_ = match vt {
            Some(t) => t,
            None => &VT_DEFAULT,
        };
        let limit = &READ_LIMIT_DEFAULT;
        let query = &query::read(queue_name, vt_, limit);
        let message = fetch_one_message::<T>(query, &self.connection).await?;
        Ok(message)
    }

    /// Reads a given number of messages (num_msgs) from the queue. If the queue is empty or all messages are invisible, `None` is returned.
    /// If messages are returned, they are made invisible for the duration of the visibility timeout (vt) in seconds.
    pub async fn read_batch<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: Option<&i32>,
        num_msgs: &i32,
    ) -> Result<Option<Vec<Message<T>>>, errors::PgmqError> {
        // map vt or default VT
        let vt_ = match vt {
            Some(t) => t,
            None => &VT_DEFAULT,
        };
        let query = &query::read(queue_name, vt_, num_msgs);
        let messages = fetch_messages::<T>(query, &self.connection).await?;
        Ok(messages)
    }

    /// Delete a message from the queue
    pub async fn delete(&self, queue_name: &str, msg_id: &i64) -> Result<u64, Error> {
        let query = &query::delete(queue_name, msg_id);
        let row = sqlx::query(query).execute(&self.connection).await?;
        let num_deleted = row.rows_affected();
        Ok(num_deleted)
    }

    /// move message from queue table to archive table
    pub async fn archive(&self, queue_name: &str, msg_id: &i64) -> Result<u64, Error> {
        let query = query::archive(queue_name, msg_id);
        let row = sqlx::query(&query).execute(&self.connection).await?;
        let num_deleted = row.rows_affected();
        Ok(num_deleted)
    }

    /// Reads a single message from the queue. The message is deleted from the queue immediately.
    /// If no messages are available, `None` is returned.
    pub async fn pop<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, errors::PgmqError> {
        let query = &query::pop(queue_name);
        let message = fetch_one_message::<T>(query, &self.connection).await?;
        Ok(message)
    }
}

// Executes a query and returns a single row
// If the query returns no rows, None is returned
async fn fetch_one_message<T: for<'de> Deserialize<'de>>(
    query: &str,
    connection: &Pool<Postgres>,
) -> Result<Option<Message<T>>, errors::PgmqError> {
    // explore: .fetch_optional()
    let row: Result<PgRow, Error> = sqlx::query(query).fetch_one(connection).await;
    match row {
        Ok(row) => {
            // happy path - successfully read a message
            let raw_msg = row.get("message");
            let parsed_msg = serde_json::from_value::<T>(raw_msg);
            match parsed_msg {
                Ok(parsed_msg) => Ok(Some(Message {
                    msg_id: row.get("msg_id"),
                    vt: row.get("vt"),
                    read_ct: row.get("read_ct"),
                    enqueued_at: row.get("enqueued_at"),
                    message: parsed_msg,
                })),
                Err(e) => Err(errors::PgmqError::JsonParsingError(e)),
            }
        }
        Err(sqlx::error::Error::RowNotFound) => Ok(None),
        Err(e) => Err(e)?,
    }
}

// Executes a query and returns multiple rows
// If the query returns no rows, None is returned
async fn fetch_messages<T: for<'de> Deserialize<'de>>(
    query: &str,
    connection: &Pool<Postgres>,
) -> Result<Option<Vec<Message<T>>>, errors::PgmqError> {
    let mut messages: Vec<Message<T>> = Vec::new();
    let rows: Result<Vec<PgRow>, Error> = sqlx::query(query).fetch_all(connection).await;
    if let Err(sqlx::error::Error::RowNotFound) = rows {
        return Ok(None);
    } else if let Err(e) = rows {
        return Err(e)?;
    } else if let Ok(rows) = rows {
        // happy path - successfully read messages
        for row in rows.iter() {
            let raw_msg = row.get("message");
            let parsed_msg = serde_json::from_value::<T>(raw_msg);
            if let Err(e) = parsed_msg {
                return Err(errors::PgmqError::JsonParsingError(e));
            } else if let Ok(parsed_msg) = parsed_msg {
                messages.push(Message {
                    msg_id: row.get("msg_id"),
                    vt: row.get("vt"),
                    read_ct: row.get("read_ct"),
                    enqueued_at: row.get("enqueued_at"),
                    message: parsed_msg,
                })
            }
        }
    }
    Ok(Some(messages))
}

// Configure connection options
pub fn conn_options(url: &str) -> Result<PgConnectOptions, ParseError> {
    // Parse url
    let parsed = Url::parse(url)?;
    let mut options = PgConnectOptions::new()
        .host(parsed.host_str().ok_or(ParseError::EmptyHost)?)
        .port(parsed.port().ok_or(ParseError::InvalidPort)?)
        .username(parsed.username())
        .password(parsed.password().ok_or(ParseError::IdnaError)?);
    options.log_statements(LevelFilter::Debug);
    Ok(options)
}
