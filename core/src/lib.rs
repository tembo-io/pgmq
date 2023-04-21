//! # Postgres Message Queue (PGMQ)
//!
//! [![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)
//!
//! PGMQ is a lightweight, distributed message queue.
//! It's like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but native to Postgres.
//!
//! Message queues allow you to decouple and connect microservices.
//! Send, store, and receive messages between components scalably, without dropping messages or
//! needing other services to be available.
//!
//! PGMQ was created by CoreDB. Our goal is to make the full Postgres ecosystem accessible to everyone.
//! We're building a radically simplified Postgres platform designed to be developer-first and easily extensible.
//! PGMQ is a part of that project.
//!
//! Not building in Rust? Try the [CoreDB pgmq Postgres extension](https://github.com/CoreDB-io/coredb/tree/main/extensions/pgmq).
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
//! - Change directory to the example project:
//! ```bash
//! cd examples/basic
//!```
//!
//! - Run the project!
//!
//! ```bash
//! cargo run
//! ```
//!
//! ## Minimal example at a glance
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
//!     let my_queue = "my_example_queue".to_owned();
//!     queue.create(&my_queue)
//!         .await
//!         .expect("Failed to create queue");
//!
//!     // Structure a message
//!     #[derive(Serialize, Debug, Deserialize)]
//!     struct MyMessage {
//!         foo: String,
//!     }
//!     let message = MyMessage {
//!         foo: "bar".to_owned(),
//!     };
//!     // Send the message
//!     let message_id: i64 = queue
//!         .send(&my_queue, &message)
//!         .await
//!         .expect("Failed to enqueue message");
//!
//!     // Use a visibility timeout of 30 seconds
//!     // Once read, the message will be unable to be read
//!     // until the visibility timeout expires
//!     let visibility_timeout_seconds: i32 = 30;
//!
//!     // Read a message
//!     let received_message: Message<MyMessage> = queue
//!         .read::<MyMessage>(&my_queue, Some(&visibility_timeout_seconds))
//!         .await
//!         .unwrap()
//!         .expect("No messages in the queue");
//!     println!("Received a message: {:?}", received_message);
//!
//!     assert_eq!(received_message.msg_id, message_id);
//!
//!     // archive the messages
//!     let _ = queue.archive(&my_queue, &received_message.msg_id)
//!         .await
//!         .expect("Failed to archive message");
//!     println!("archived the messages from the queue");
//!     Ok(())
//!
//! }
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

use errors::PgmqError;
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

/// Main controller for interacting with a queue.
#[derive(Clone, Debug)]
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

    /// Create a queue. This sets up the queue's tables, indexes, and metadata.
    /// It is idempotent, but does not check if the queue already exists.
    /// Amounts to `IF NOT EXISTS` statements in Postgres.
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{errors::PgmqError, PGMQueue};
    /// use serde::{Deserialize, Serialize};
    /// use serde_json::Value;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///    let my_queue = "my_queue";
    ///    queue.create(my_queue).await?;
    ///    Ok(())
    /// }
    pub async fn create(&self, queue_name: &str) -> Result<(), errors::PgmqError> {
        let mut tx = self.connection.begin().await?;
        let setup = query::init_queue(queue_name)?;
        for q in setup {
            sqlx::query(&q).execute(&mut tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Destroy a queue. This deletes the queue's tables, indexes, and metadata.
    /// Does not delete any data related to adjacent queues.
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{errors::PgmqError, PGMQueue};
    /// use serde::{Deserialize, Serialize};
    /// use serde_json::Value;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue_destroy".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///     queue.destroy("my_queue_destroy").await.expect("Failed to destroy queue!");
    ///     Ok(())
    /// }
    pub async fn destroy(&self, queue_name: &str) -> Result<(), errors::PgmqError> {
        let mut tx = self.connection.begin().await?;
        let setup = query::destroy_queue(queue_name)?;
        for q in setup {
            sqlx::query(&q).execute(&mut tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Send a single message to a queue.
    /// Messages can be any implementor of the [`serde::Serialize`] trait.
    /// The message id, unique to the queue, is returned. Typically,
    /// the message sender does not consume the message id but may use it for
    /// logging and tracing purposes.
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{errors::PgmqError, PGMQueue};
    /// use serde::{Deserialize, Serialize};
    /// use serde_json::Value;
    ///
    /// #[derive(Debug, Deserialize, Serialize)]
    /// struct MyMessage {
    ///    foo: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///
    ///     let struct_message = MyMessage {
    ///         foo: "bar".to_owned(),
    ///     };
    ///
    ///     let struct_message_id: i64 = queue
    ///        .send(&my_queue, &struct_message)
    ///        .await
    ///        .expect("Failed to enqueue message");
    ///     println!("Struct Message id: {}", struct_message_id);
    ///
    ///     let json_message = serde_json::json!({
    ///         "foo": "bar"
    ///     });
    ///     let json_message_id: i64 = queue
    ///         .send(&my_queue, &json_message)
    ///         .await
    ///         .expect("Failed to enqueue message");
    ///     println!("Json Message id: {}", json_message_id);
    ///     Ok(())
    /// }
    pub async fn send<T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
    ) -> Result<i64, errors::PgmqError> {
        let msg = serde_json::json!(&message);
        let msgs: [serde_json::Value; 1] = [msg];
        let row: PgRow = sqlx::query(&query::enqueue(queue_name, &msgs, &0)?)
            .fetch_one(&self.connection)
            .await?;
        let msg_id: i64 = row.get("msg_id");
        Ok(msg_id)
    }

    /// Send a single message to a queue with a delay.
    /// Specify your delay in seconds.
    /// Messages can be any implementor of the [`serde::Serialize`] trait.
    /// The message id, unique to the queue, is returned. Typically,
    /// the message sender does not consume the message id but may use it for
    /// logging and tracing purposes.
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{errors::PgmqError, PGMQueue};
    /// use serde::{Deserialize, Serialize};
    /// use serde_json::Value;
    ///
    /// #[derive(Debug, Deserialize, Serialize)]
    /// struct MyMessage {
    ///    foo: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///
    ///     let struct_message = MyMessage {
    ///         foo: "bar".to_owned(),
    ///     };
    ///
    ///     let struct_message_id: i64 = queue
    ///        .send_delay(&my_queue, &struct_message, 15)
    ///        .await
    ///        .expect("Failed to enqueue message");
    ///     println!("Struct Message id: {}", struct_message_id);
    ///
    ///     let json_message = serde_json::json!({
    ///         "foo": "bar"
    ///     });
    ///     let json_message_id: i64 = queue
    ///         .send_delay(&my_queue, &json_message, 15)
    ///         .await
    ///         .expect("Failed to enqueue message");
    ///     println!("Json Message id: {}", json_message_id);
    ///     Ok(())
    /// }
    pub async fn send_delay<T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
        delay: u64,
    ) -> Result<i64, errors::PgmqError> {
        let mut msgs: Vec<serde_json::Value> = Vec::new();
        let msg = serde_json::json!(&message);
        msgs.push(msg);
        let row: PgRow = sqlx::query(&query::enqueue(queue_name, &msgs, &delay)?)
            .fetch_one(&self.connection)
            .await?;
        let msg_id: i64 = row.get("msg_id");
        Ok(msg_id)
    }

    /// Send multiple messages to a queue.
    /// Same as send(), messages can be any implementor of the [`serde::Serialize`] trait.
    /// A vector of message ids are returned in the call. These message ids are in the
    /// same order as the messages in the input vector.
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{errors::PgmqError, PGMQueue};
    /// use serde::{Deserialize, Serialize};
    /// use serde_json::Value;
    ///
    /// #[derive(Debug, Deserialize, Serialize)]
    /// struct MyMessage {
    ///    foo: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///    let struct_message_batch = vec![
    ///        MyMessage {foo: "bar1".to_owned()},
    ///        MyMessage {foo: "bar2".to_owned()},
    ///        MyMessage {foo: "bar3".to_owned()},
    ///    ];
    ///
    ///    let struct_message_batch_ids = queue.send_batch(&my_queue, &struct_message_batch)
    ///        .await
    ///        .expect("Failed to enqueue messages");
    ///     println!("Struct Message ids: {:?}", struct_message_batch_ids);
    ///     Ok(())
    /// }
    pub async fn send_batch<T: Serialize>(
        &self,
        queue_name: &str,
        messages: &[T],
    ) -> Result<Vec<i64>, errors::PgmqError> {
        let mut msgs: Vec<serde_json::Value> = Vec::new();
        let mut msg_ids: Vec<i64> = Vec::new();
        for msg in messages.iter() {
            let binding = serde_json::json!(&msg);
            msgs.push(binding)
        }
        let rows: Vec<PgRow> = sqlx::query(&query::enqueue(queue_name, &msgs, &0)?)
            .fetch_all(&self.connection)
            .await?;
        for row in rows.iter() {
            msg_ids.push(row.get("msg_id"));
        }
        Ok(msg_ids)
    }

    /// Reads a single message from the queue. If the queue is empty or all messages are invisible, [`Option::None`] is returned.
    /// If a message is returned, it is made invisible for the duration of the visibility timeout (vt) in seconds.
    ///
    /// Reading a message returns a [`Message`] struct.
    /// Typically, the application reading messages is most interested in the message body but will use the
    /// message id in order to either delete or archive the message when it is done processing it.
    ///
    /// Refer to the [`Message`] struct for more details.
    ///
    /// You can specify the message structure you are expecting to read from the queue by using the type parameters.
    /// Any implementor of the [`serde::Deserialize`] trait can be used.
    /// If you do not know the type of the message, it will default to [`serde_json::Value`].
    ///
    /// [`Message`]: struct@crate::Message
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{Message, errors::PgmqError, PGMQueue};
    /// use serde::{Deserialize, Serialize};
    /// use serde_json::Value;
    ///
    /// #[derive(Debug, Deserialize, Serialize)]
    /// struct MyMessage {
    ///    foo: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///     let struct_message_batch = vec![
    ///        MyMessage {foo: "bar1".to_owned()},
    ///        MyMessage {foo: "bar2".to_owned()},
    ///        MyMessage {foo: "bar3".to_owned()},
    ///     ];
    ///
    ///     let struct_message_batch_ids = queue.send_batch(&my_queue, &struct_message_batch)
    ///        .await
    ///        .expect("Failed to enqueue messages");
    ///     println!("Struct Message ids: {:?}", struct_message_batch_ids);
    ///
    ///     let visibility_timeout_seconds = 30;
    ///     let known_message_structure: Message<MyMessage> = queue.read::<MyMessage>(&my_queue, Some(&visibility_timeout_seconds))
    ///         .await
    ///         .unwrap()
    ///         .expect("no messages in the queue!");
    ///     println!("Received known : {known_message_structure:?}");
    ///
    ///     let unknown_message_structure: Message = queue.read(&my_queue, Some(&visibility_timeout_seconds))
    ///         .await
    ///         .unwrap()
    ///         .expect("no messages in the queue!");
    ///     println!("Received known : {unknown_message_structure:?}");
    ///     Ok(())
    /// }
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
        let query = &query::read(queue_name, vt_, limit)?;
        let message = fetch_one_message::<T>(query, &self.connection).await?;
        Ok(message)
    }

    /// Reads a specified number of messages (num_msgs) from the queue.
    /// Any messages that are returned are made invisible for the duration of the visibility timeout (vt) in seconds.
    ///
    /// If the queue is empty or all messages are invisible,[`Option::None`] is returned. If there are messages,
    /// it is returned as a vector of [`Message`] structs (it will never be an empty vector).
    ///
    /// Refer to the [`Message`] struct for more details.
    ///
    /// You can specify the message structure you are expecting to read from the queue by using the type parameters.
    /// Any implementor of the [`serde::Deserialize`] trait can be used.
    /// If you do not know the type of the message, it will default to [`serde_json::Value`].
    ///
    /// [`Message`]: struct@crate::Message
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{Message, errors::PgmqError, PGMQueue};
    /// use serde::{Deserialize, Serialize};
    /// use serde_json::Value;
    ///
    /// #[derive(Debug, Deserialize, Serialize)]
    /// struct MyMessage {
    ///    foo: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///
    ///     let struct_message_batch = vec![
    ///        MyMessage {foo: "bar1".to_owned()},
    ///        MyMessage {foo: "bar2".to_owned()},
    ///        MyMessage {foo: "bar3".to_owned()},
    ///     ];
    ///
    ///     let struct_message_batch_ids = queue.send_batch(&my_queue, &struct_message_batch)
    ///        .await
    ///        .expect("Failed to enqueue messages");
    ///     println!("Struct Message ids: {struct_message_batch_ids:?}");
    ///
    ///     let visibility_timeout_seconds = 30;
    ///     let batch_size = 1;
    ///     let batch: Vec<Message<MyMessage>> = queue.read_batch::<MyMessage>(&my_queue, Some(&visibility_timeout_seconds), &batch_size)
    ///         .await
    ///         .unwrap()
    ///         .expect("no messages in the queue!");
    ///     println!("Received a batch of messages: {batch:?}");
    ///
    ///     let batch_size = 2;
    ///     let unknown_message_structure: Message = queue.read(&my_queue, Some(&visibility_timeout_seconds))
    ///         .await
    ///         .unwrap()
    ///         .expect("no messages in the queue!");
    ///     println!("Received known : {unknown_message_structure:?}");
    ///     Ok(())
    /// }
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
        let query = &query::read(queue_name, vt_, num_msgs)?;
        let messages = fetch_messages::<T>(query, &self.connection).await?;
        Ok(messages)
    }

    /// Delete a message from the queue.
    /// This is a permanent delete and cannot be undone. If you want to retain a log of the message,
    /// use the [archive](#method.archive) method.
    ///
    /// Deletes happen by message id, so you must have the message id to delete the message.
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{errors::PgmqError, PGMQueue};
    /// use serde::Serialize;
    /// use serde_json::Value;
    ///
    /// #[derive(Debug, Serialize)]
    /// struct MyMessage {
    ///    foo: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///
    ///     let struct_message = MyMessage {
    ///         foo: "bar".to_owned(),
    ///     };
    ///
    ///     let message_id: i64 = queue
    ///        .send(&my_queue, &struct_message)
    ///        .await
    ///        .expect("Failed to enqueue message");
    ///     println!("Struct Message id: {message_id}");
    ///
    ///     queue.delete(&my_queue, &message_id).await.expect("failed to delete message");
    ///
    ///     Ok(())
    /// }
    pub async fn delete(&self, queue_name: &str, msg_id: &i64) -> Result<u64, PgmqError> {
        let query = &query::delete(queue_name, msg_id)?;
        let row = sqlx::query(query).execute(&self.connection).await?;
        let num_deleted = row.rows_affected();
        Ok(num_deleted)
    }

    /// Delete multiple messages from the queue.
    /// This is a permanent delete and cannot be undone. If you want to retain a log of the message,
    /// use the [archive](#method.archive) method.
    ///
    /// Deletes happen by message id, so you must have the message id to delete the message.
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{errors::PgmqError, PGMQueue};
    /// use serde::Serialize;
    /// use serde_json::Value;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///
    ///     let msgs = vec![
    ///         serde_json::json!({"foo": "bar1"}),
    ///         serde_json::json!({"foo": "bar2"}),
    ///         serde_json::json!({"foo": "bar3"}),
    ///     ];
    ///     let msg_ids = queue
    ///        .send_batch(&my_queue, &msgs)
    ///        .await
    ///        .expect("Failed to enqueue messages");
    ///     let del = queue
    ///         .delete_batch(&my_queue, &msg_ids)
    ///         .await
    ///         .expect("Failed to delete messages from queue");
    ///
    ///     Ok(())
    /// }
    pub async fn delete_batch(&self, queue_name: &str, msg_ids: &[i64]) -> Result<u64, PgmqError> {
        let query = &query::delete_batch(queue_name, msg_ids)?;
        let row = sqlx::query(query).execute(&self.connection).await?;
        let num_deleted = row.rows_affected();
        Ok(num_deleted)
    }

    /// Moves a message, by message id, from the queue table to archive table
    /// View messages on the archive table with sql:
    /// ```sql
    /// SELECT * FROM pgmq_<queue_name>_archive;
    /// ```
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{errors::PgmqError, PGMQueue};
    /// use serde::Serialize;
    /// use serde_json::Value;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///
    ///     let message = serde_json::json!({"foo": "bar1"});
    ///
    ///     let message_id: i64 = queue
    ///        .send(&my_queue, &message)
    ///        .await
    ///        .expect("Failed to enqueue message");
    ///
    ///     queue.archive(&my_queue, &message_id).await.expect("failed to archive message");
    ///
    ///     Ok(())
    /// }
    pub async fn archive(&self, queue_name: &str, msg_id: &i64) -> Result<u64, PgmqError> {
        let query = query::archive(queue_name, msg_id)?;
        let row = sqlx::query(&query).execute(&self.connection).await?;
        let num_deleted = row.rows_affected();
        Ok(num_deleted)
    }

    /// Reads single message from the queue and delete it at the same time.
    /// Similar to [read](#method.read) and [read_batch](#method.read_batch),
    /// if no messages are available, [`Option::None`] is returned. Unlike these methods,
    /// the visibility timeout does not apply. This is because the message is immediately deleted.
    ///
    /// Example:
    ///
    /// ```rust
    /// use pgmq::{Message, errors::PgmqError, PGMQueue};
    /// use serde::{Deserialize, Serialize};
    /// use serde_json::Value;
    ///
    /// #[derive(Debug, Deserialize, Serialize)]
    /// struct MyMessage {
    ///    foo: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///     let send_message = MyMessage {foo: "bar1".to_owned()};
    ///
    ///     let struct_message_batch_ids = queue.send(&my_queue, &send_message)
    ///        .await
    ///        .expect("Failed to send message");
    ///
    ///     let popped_message: Message<MyMessage> = queue.pop::<MyMessage>(&my_queue)
    ///         .await
    ///         .unwrap()
    ///         .expect("no messages in the queue!");
    ///     println!("Received popped message : {popped_message:?}");
    ///
    ///     Ok(())
    /// }
    pub async fn pop<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError> {
        let query = &query::pop(queue_name)?;
        let message = fetch_one_message::<T>(query, &self.connection).await?;
        Ok(message)
    }

    /// Set the visibility time for a single message. This is useful when you want
    /// change when a message becomes visible again (able to be read with .read() methods).
    /// For example, in task execution use cases or job scheduling.
    ///
    /// Example:
    ///
    /// ```rust
    /// use chrono::{Utc, DateTime, Duration};
    /// use pgmq::{errors::PgmqError, PGMQueue};
    /// use serde::{Deserialize, Serialize};
    /// use serde_json::Value;

    /// #[derive(Debug, Deserialize, Serialize)]
    /// struct MyMessage {
    ///    foo: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), PgmqError> {
    ///
    ///     println!("Connecting to Postgres");
    ///     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
    ///         .await
    ///         .expect("Failed to connect to postgres");
    ///     let my_queue = "my_queue".to_owned();
    ///     queue.create(&my_queue)
    ///         .await
    ///         .expect("Failed to create queue");
    ///
    ///     let struct_message = MyMessage {
    ///         foo: "bar".to_owned(),
    ///     };
    ///
    ///     let message_id: i64 = queue
    ///        .send(&my_queue, &struct_message)
    ///        .await
    ///        .expect("Failed to enqueue message");
    ///     println!("Struct Message id: {message_id}");
    ///
    ///     let utc_24h_from_now = Utc::now() + Duration::hours(24);
    ///
    ///     queue.set_vt::<MyMessage>(&my_queue, &message_id, &utc_24h_from_now).await.expect("failed to set vt");
    ///
    ///     Ok(())
    /// }
    pub async fn set_vt<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        msg_id: &i64,
        vt: &chrono::DateTime<Utc>,
    ) -> Result<Option<Message<T>>, errors::PgmqError> {
        let query = &query::set_vt(queue_name, msg_id, vt)?;
        let updated_message = fetch_one_message::<T>(query, &self.connection).await?;
        Ok(updated_message)
    }
}

// Executes a query and returns a single row
// If the query returns no rows, None is returned
// This function is intended for internal use.
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
// This function is intended for internal use.
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
