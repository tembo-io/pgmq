//! # Postgres Message Queue
//!
//! A lightweight messaging queue for Rust, using Postgres as the backend.
//! Inspired by the [RSMQ project](https://github.com/smrchy/rsmq).
//!
//! # Examples
//!
//! First, start any Postgres instance. It is the only external dependency.
//!
//! ```bash
//! docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
//! ```
//! ## Create a queue
//!
//! ```rust
//! use pgmq::{Message, PGMQueue};
//! let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned()).await.expect("Failed to connect to Postgres");
//!
//! let myqueue = "myqueue".to_owned();
//! queue.create(&myqueue).await.expect("Failed to create queue");
//! ```
//!
//! ## Sending messages
//!
//! `queue.enqueue()` can be passed any type that implements `serde::Serialize`. This means you can prepare your messages as JSON or as a struct.
//!
//! #### as serde_json::Value
//! ```rust
//! let msg = serde_json::json!({
//!     "foo": "bar"
//! });
//! let msg_id = queue.enqueue(&myqueue, &msg).await.expect("Failed to enqueue message");
//! ```
//! #### as a struct
//! ```rust
//! use serde::{Serialize, Deserialize};
//! #[derive(Serialize, Debug, Deserialize)]
//! struct MyMessage {
//!     foo: String,
//! }
//! let msg = MyMessage {
//!     foo: "bar".to_owned(),
//! };
//! let msg_id: i64  = queue.enqueue(&myqueue, &msg).await.expect("Failed to enqueue message");
//! ```
//!
//! ## Reading messages
//! Reading a message will make it invisible for the duration of the visibility timeout (vt).
//! No messages are returned when the queue is empty or all messages are invisible.
//! Messages can be parsed as JSON or as into a struct. `queue.read()` returns an `Option<Message<T>>`
//! where `T` is the type of the message on the queue. It can be parsed as JSON or as a struct.
//! Note that when parsing into a `struct`, the application will panic if the message cannot be
//! parsed as the type specified. For example, if the message expected is
//! `MyMessage{foo: "bar"}` but` {"hello": "world"}` is received, the application will panic.
//! #### as serde_json::Value
//! ```rust
//! use serde_json::Value;
//! let vt: u32 = 30;
//! let read_msg: Message<Value> = queue.read::<Value>(&myqueue, Some(&vt)).await.expect("no messages in the queue!");
//! ```
//! #### as a Struct
//! Reading a message will make it invisible for the duration of the visibility timeout (vt).
//! No messages are returned when the queue is empty or all messages are invisible.
//! ```rust
//! use serde_json::Value;
//! let vt: u32 = 30;
//! let read_msg: Message<MyMessage> = queue.read::<MyMessage>(&myqueue, Some(&vt)).await.expect("no messages in the queue!");
//! ```
//! ## Delete a message
//! Remove the message from the queue when you are done with it.
//! ```rust
//! let deleted = queue.delete(&read_msg.msg_id).await;
//! ```

#![doc(html_root_url = "https://docs.rs/pgmq/")]

use serde::{Deserialize, Serialize};
use sqlx::error::Error;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::types::chrono::Utc;
use sqlx::FromRow;
use sqlx::{Pool, Postgres, Row};

mod query;
use chrono::serde::ts_seconds::deserialize as from_ts;

const VT_DEFAULT: u32 = 30;

#[derive(Debug, Deserialize, FromRow)]
pub struct Message<T = serde_json::Value> {
    pub msg_id: i64,
    #[serde(deserialize_with = "from_ts")]
    pub vt: chrono::DateTime<Utc>,
    pub message: T,
}

#[derive(Debug)]
pub struct PGMQueue {
    pub url: String,
    pub connection: Pool<Postgres>,
}

impl PGMQueue {
    pub async fn new(url: String) -> PGMQueue {
        let con = PGMQueue::connect(&url).await;
        PGMQueue {
            url,
            connection: con,
        }
    }

    async fn connect(url: &str) -> Pool<Postgres> {
        PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(10))
            .max_connections(5)
            .connect(url)
            .await
            .expect("connection failed")
    }

    pub async fn create(&self, queue_name: &str) -> Result<(), Error> {
        let create = query::create(queue_name);
        let index: String = query::create_index(queue_name);
        sqlx::query(&create).execute(&self.connection).await?;
        sqlx::query(&index).execute(&self.connection).await?;
        Ok(())
    }

    pub async fn enqueue<T: Serialize>(&self, queue_name: &str, message: &T) -> Result<i64, Error> {
        let msg = &serde_json::json!(&message);
        let row: PgRow = sqlx::query(&query::enqueue(queue_name, msg))
            .fetch_one(&self.connection)
            .await?;

        Ok(row.try_get("msg_id").unwrap())
    }

    pub async fn read<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: Option<&u32>,
    ) -> Option<Message<T>> {
        // map vt or default VT
        let vt_ = match vt {
            Some(t) => t,
            None => &VT_DEFAULT,
        };
        let query = &query::read(queue_name, vt_);
        let row: Result<PgRow, Error> = sqlx::query(query).fetch_one(&self.connection).await;

        match row {
            Ok(row) => {
                let b = row.get("message");
                let a = serde_json::from_value::<T>(b).unwrap();
                Some(Message {
                    msg_id: row.get("msg_id"),
                    vt: row.get("vt"),
                    message: a,
                })
            }
            Err(_) => None,
        }
    }

    pub async fn delete(&self, queue_name: &str, msg_id: &i64) -> Result<u64, Error> {
        let query = &&query::delete(queue_name, msg_id);
        let row = sqlx::query(query).execute(&self.connection).await?;
        let num_deleted = row.rows_affected();
        Ok(num_deleted)
    }

    // pub async fn pop(self) -> Message{
    //     // TODO: returns a struct
    // }
}
