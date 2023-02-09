//! # Postgres Message Queue (PGMQ)
//!
//! A lightweight distributed message queue for Rust. Like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but on Postgres.
//!
//! Not building in Rust? Try the [CoreDB pgmq extension](https://github.com/CoreDB-io/coredb/tree/main/extensions/pgx_pgmq).
//!
//! ## Features
//!
//! - Lightweight - Rust and Postgres only
//! - Guaranteed delivery of messages to exactly one consumer within a visibility timeout
//! - API parity with [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq)
//! - Messages stay in the queue until deleted
//! - Messages can be archived, instead of deleted, for long-term retention and replayability
//! - Completely asynchronous API
//!
//! ## Quick start
//!
//! - First, you will need Postgres. We use a container in this example.
//!
//! ```bash
//! docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
//! ```
//!
//! - If you don't have Docker installed, it can be found [here](https://docs.docker.com/get-docker/).
//!
//! - Make sure you have the Rust toolchain installed:
//!
//! ```bash
//! cargo --version
//! ```
//!
//! - This example was written with version 1.67.0, but the latest stable should work. You can go [here](https://www.rust-lang.org/tools/install) to install Rust if you don't have it already, then run `rustup install stable` to install the latest, stable toolchain.
//!
//! - Next, let's create a Rust project for the demo.
//!
//! ```bash
//! # Create a new Rust project
//! cargo new basic
//!
//! # Change directory into the new project
//! cd basic
//! ```
//!
//! - Add PGMQ to the project
//!
//! ```bash
//! cargo add pgmq
//! ```
//!
//! - Add other dependencies to the project
//!
//! ```bash
//! cargo add tokio serde serde_json
//! ```
//!
//! - Replace the contents of `src/main.rs` with this:
//!
//! ```rust
//! use pgmq::{errors::PgmqError, Message, PGMQueue};
//! use serde::{Deserialize, Serialize};
//! use serde_json::Value;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), PgmqError> {
//!
//!     // Initialize a connection to Postgres
//!     println!("Connecting to Postgres");
//!     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
//!         .await
//!         .expect("Failed to connect to postgres");
//!
//!     // Create a queue
//!     println!("Creating a queue 'my_queue'");
//!     let my_queue = "my_queue".to_owned();
//!     queue.create(&my_queue)
//!         .await
//!         .expect("Failed to create queue");
//!
//!     // Send a message as JSON
//!     let json_message = serde_json::json!({
//!         "foo": "bar"
//!     });
//!     println!("Enqueueing a JSON message: {json_message}");
//!     let json_message_id: i64 = queue
//!         .send(&my_queue, &json_message)
//!         .await
//!         .expect("Failed to enqueue message");
//!
//!     // Messages can also be sent from structs
//!     #[derive(Serialize, Debug, Deserialize)]
//!     struct MyMessage {
//!         foo: String,
//!     }
//!     let struct_message = MyMessage {
//!         foo: "bar".to_owned(),
//!     };
//!     println!("Enqueueing a struct message: {:?}", struct_message);
//!     let struct_message_id: i64 = queue
//!         .send(&my_queue, &struct_message)
//!         .await
//!         .expect("Failed to enqueue message");
//!
//!     // Use a visibility timeout of 30 seconds.
//!     //
//!     // Messages that are not deleted within the
//!     // visibility timeout will return to the queue.
//!     let visibility_timeout_seconds: i32 = 30;
//!
//!     // Read the JSON message
//!     let received_json_message: Message<Value> = queue
//!         .read::<Value>(&my_queue, Some(&visibility_timeout_seconds))
//!         .await
//!         .unwrap()
//!         .expect("No messages in the queue");
//!     println!("Received a message: {:?}", received_json_message);
//!
//!     // Compare message IDs
//!     assert_eq!(received_json_message.msg_id, json_message_id);
//!
//!     // Read the struct message
//!     let received_struct_message: Message<MyMessage> = queue
//!         .read::<MyMessage>(&my_queue, Some(&visibility_timeout_seconds))
//!         .await
//!         .unwrap()
//!         .expect("No messages in the queue");
//!     println!("Received a message: {:?}", received_struct_message);
//!
//!     assert_eq!(received_struct_message.msg_id, struct_message_id);
//!
//!     // Delete the messages to remove them from the queue
//!     let _ = queue.delete(&my_queue, &received_json_message.msg_id)
//!         .await
//!         .expect("Failed to delete message");
//!     let _ = queue.delete(&my_queue, &received_struct_message.msg_id)
//!         .await
//!         .expect("Failed to delete message");
//!     println!("Deleted the messages from the queue");
//!
//!     // No messages are remaining
//!     let no_message: Option<Message<Value>> = queue.read::<Value>(&my_queue, Some(&visibility_timeout_seconds))
//!         .await
//!         .unwrap();
//!     assert!(no_message.is_none());
//!
//!     // We can also send and receive messages in batches
//!
//!     // Send a batch of JSON messages
//!     let json_message_batch = vec![
//!         serde_json::json!({"foo": "bar1"}),
//!         serde_json::json!({"foo": "bar2"}),
//!         serde_json::json!({"foo": "bar3"}),
//!     ];
//!     println!("Enqueuing a batch of messages: {:?}", json_message_batch);
//!     let json_message_batch_ids = queue.send_batch(&my_queue, &json_message_batch)
//!         .await
//!         .expect("Failed to enqueue messages");
//!
//!     // Receive a batch of JSON messages
//!     let batch_size = 3;
//!     let batch: Vec<Message<Value>> = queue.read_batch::<Value>(&my_queue, Some(&visibility_timeout_seconds), &batch_size)
//!       .await
//!       .unwrap()
//!       .expect("no messages in the queue!");
//!     println!("Received a batch of messages: {:?}", batch);
//!     for (_, message) in batch.iter().enumerate() {
//!         assert!(json_message_batch_ids.contains(&message.msg_id));
//!         let _ = queue.delete(&my_queue, &message.msg_id)
//!             .await
//!             .expect("Failed to delete message");
//!         println!("Deleted message {}", message.msg_id);
//!     }
//!
//!     // Send a batch of struct messages
//!     let struct_message_batch = vec![
//!         MyMessage {foo: "bar1".to_owned()},
//!         MyMessage {foo: "bar2".to_owned()},
//!         MyMessage {foo: "bar3".to_owned()},
//!     ];
//!     println!("Enqueuing a batch of messages: {:?}", struct_message_batch);
//!     let struct_message_batch_ids = queue.send_batch(&my_queue, &struct_message_batch)
//!         .await
//!         .expect("Failed to enqueue messages");
//!
//!     // Receive a batch of struct messages
//!     let batch_size = 3;
//!     let batch: Vec<Message<MyMessage>> = queue.read_batch::<MyMessage>(&my_queue, Some(&visibility_timeout_seconds), &batch_size)
//!       .await
//!       .unwrap()
//!       .expect("no messages in the queue!");
//!     println!("Received a batch of messages: {:?}", batch);
//!     for (_, message) in batch.iter().enumerate() {
//!         assert!(struct_message_batch_ids.contains(&message.msg_id));
//!         let _ = queue.delete(&my_queue, &message.msg_id)
//!             .await
//!             .expect("Failed to delete message");
//!         println!("Deleted message {}", message.msg_id);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! - Run the program
//!
//! - This example is present in the examples/basic directory
//!
//! ```bash
//! cargo run
//! ```
//!
//! ## Sending messages
//!
//! You can send one message at a time with `queue.send()` or several with `queue.send_batch()`.
//! These methods can be passed any type that implements `serde::Serialize`. This means you can prepare your messages as JSON or as a struct.
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
//! `MyMessage{foo: "bar"}` but `{"hello": "world"}` is received, the application will panic.
//!
//! Read a single message with `queue.read()` or as many as you want with `queue.read_batch()`.
//!
//! ## Archive or Delete a message
//!
//! Remove the message from the queue when you are done with it. You can either completely `.delete()`, or `.archive()` the message. Archived messages are deleted from the queue and inserted to the queue's archive table. Deleted messages are just deleted.
//!
//! Read messages from the queue archive with SQL:
//!
//! ```sql
//! SELECT *
//! FROM pgmq_{your_queue_name}_archive;
//! ```
//!

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
