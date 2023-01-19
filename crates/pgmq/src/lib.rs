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
//!     let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned()).await;
//!     let myqueue = "myqueue".to_owned();
//!     queue.create(&myqueue).await.expect("Failed to create queue");
//!
//!     // SEND A `serde_json::Value` MESSAGE
//!     let msg1 = serde_json::json!({
//!         "foo": "bar"
//!     });
//!     let msg_id1: i64 = queue.enqueue(&myqueue, &msg1).await.expect("Failed to enqueue message");
//!
//!     // SEND A STRUCT
//!     #[derive(Serialize, Debug, Deserialize)]
//!     struct MyMessage {
//!         foo: String,
//!     }
//!     let msg2 = MyMessage {
//!         foo: "bar".to_owned(),
//!     };
//!     let msg_id2: i64  = queue.enqueue(&myqueue, &msg2).await.expect("Failed to enqueue message");
//!     
//!     // READ A MESSAGE as `serde_json::Value`
//!     let vt: u32 = 30;
//!     let read_msg1: Message<Value> = queue.read::<Value>(&myqueue, Some(&vt)).await.expect("no messages in the queue!");
//!     assert_eq!(read_msg1.msg_id, msg_id1);
//!
//!     // READ A MESSAGE as a struct
//!     let read_msg2: Message<MyMessage> = queue.read::<MyMessage>(&myqueue, Some(&vt)).await.expect("no messages in the queue!");
//!     assert_eq!(read_msg2.msg_id, msg_id2);
//!
//!     // DELETE THE MESSAGE WE SENT
//!     let deleted = queue.delete(&myqueue, &read_msg1.msg_id).await.expect("Failed to delete message");
//!     let deleted = queue.delete(&myqueue, &read_msg2.msg_id).await.expect("Failed to delete message");
//! }
//! ```
//! ## Sending messages
//!
//! `queue.enqueue()` can be passed any type that implements `serde::Serialize`. This means you can prepare your messages as JSON or as a struct.
//!
//! ## Reading messages
//! Reading a message will make it invisible (unavailable for consumption) for the duration of the visibility timeout (vt).
//! No messages are returned when the queue is empty or all messages are invisible.
//!
//! Messages can be parsed as JSON or into a struct. `queue.read()` returns an `Option<Message<T>>`
//! where `T` is the type of the message on the queue. It can be parsed as JSON or as a struct.
//! Note that when parsing into a `struct`, the application will panic if the message cannot be
//! parsed as the type specified. For example, if the message expected is
//! `MyMessage{foo: "bar"}` but` {"hello": "world"}` is received, the application will panic.
//!
//! #### as a Struct
//! Reading a message will make it invisible for the duration of the visibility timeout (vt).
//! No messages are returned when the queue is empty or all messages are invisible.
//!
//! ## Delete a message
//! Remove the message from the queue when you are done with it.

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
