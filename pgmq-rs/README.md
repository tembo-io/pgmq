# Postgres Message Queue (PGMQ)

[![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)

PGMQ is a lightweight, distributed message queue.
It's like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but native to Postgres.

Message queues allow you to decouple and connect microservices.
Send, store, and receive messages between components scalably, without dropping messages or
needing other services to be available.

PGMQ was created by [Tembo](https://tembo.io/). Our goal is to make the full Postgres ecosystem accessible to everyone.
We're building a radically simplified Postgres platform designed to be developer-first and easily extensible.
PGMQ is a part of that project.

This project contains two APIs, a pure Rust client side library and the Rust SDK wrapped around the Postgres extension.

`The Rust client for the Postgres extension`. This gives the you the an ORM-like experience with the Postgres extension and makes managing connection pools, transactions, and serialization/deserialization much easier.

```rust
use pgmq::PGMQueueExt;
```

`The pure Rust client`. This provides minimal functionality but can be used on any existing Postgres instance.

```rust
use pgmq::PGMQueue;
```

## Quick start

- First, you will need Postgres. We use a container in this example.

```bash
docker run -d --name pgmq-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pg16-pgmq:latest
```

- If you don't have Docker installed, it can be found [here](https://docs.docker.com/get-docker/).

- Make sure you have the [Rust toolchain](https://www.rust-lang.org/tools/install) installed:

- Clone the project and run the [basic example](./examples/basic.rs):

```bash
git clone https://github.com/tembo-io/pgmq.git

cd pgmq-rs

cargo run --example basic
```

## Minimal example at a glance

```rust
use pgmq::{errors::PgmqError, Message, PGMQueue};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), PgmqError> {

    // Initialize a connection to Postgres
    println!("Connecting to Postgres");
    let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned())
        .await
        .expect("Failed to connect to postgres");

    // Create a queue
    println!("Creating a queue 'my_queue'");
    let my_queue = "my_example_queue".to_owned();
    queue.create(&my_queue)
        .await
        .expect("Failed to create queue");

    // Structure a message
    #[derive(Serialize, Debug, Deserialize)]
    struct MyMessage {
        foo: String,
    }
    let message = MyMessage {
        foo: "bar".to_owned(),
    };
    // Send the message
    let message_id: i64 = queue
        .send(&my_queue, &message)
        .await
        .expect("Failed to enqueue message");

    // Use a visibility timeout of 30 seconds
    // Once read, the message will be unable to be read
    // until the visibility timeout expires
    let visibility_timeout_seconds: i32 = 30;

    // Read a message
    let received_message: Message<MyMessage> = queue
        .read::<MyMessage>(&my_queue, Some(&visibility_timeout_seconds))
        .await
        .unwrap()
        .expect("No messages in the queue");
    println!("Received a message: {:?}", received_message);

    assert_eq!(received_message.msg_id, message_id);

    // archive the messages
    let _ = queue.archive(&my_queue, &received_message.msg_id)
        .await
        .expect("Failed to archive message");
    println!("archived the messages from the queue");
    Ok(())

}
```

## Transactions

You can execute all of PGMQ's operations within a transaction along with other database operations. See the [transaction example](./examples/transaction.rs).

## Sending messages

You can send one message at a time with `queue.send()` or several with `queue.send_batch()`.
These methods can be passed any type that implements `serde::Serialize`. This means you can prepare your messages as JSON or as a struct.

## Reading messages

Reading a message will make it invisible (unavailable for consumption) for the duration of the visibility timeout (vt).
No messages are returned when the queue is empty or all messages are invisible.

Messages can be parsed as serde_json::Value or into a struct. `queue.read()` returns an `Result<Option<Message<T>>, PgmqError>`
where `T` is the type of the message on the queue. It returns an error when there is an issue parsing the message (`PgmqError::JsonParsingError`) or if PGMQ is unable to reach postgres (`PgmqError::DatabaseError`).
Note that when parsing into a `struct` (say, you expect `MyMessage{foo: "bar"}`), and the data of the message does not correspond to the struct definition (e.g. `{"hello": "world"}`), an error will be returned and unwrapping this result the way it is done for demo purposes in the [example](#minimal-example-at-a-glance) above will cause a panic, so you will rather want to handle this case properly.

Read a single message with `queue.read()` or as many as you want with `queue.read_batch()`.

## Archive or Delete a message

Remove the message from the queue when you are done with it. You can either completely `.delete()`, or `.archive()` the message. Archived messages are deleted from the queue and inserted to the queue's archive table. Deleted messages are just deleted.

Read messages from the queue archive with SQL:

```sql
SELECT *
FROM pgmq.a_{your_queue_name};
```

License: [PostgreSQL](LICENSE)
