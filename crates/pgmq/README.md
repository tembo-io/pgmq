# Postgres Message Queue (PGMQ)

[![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)

PGMQ is a lightweight, distributed message queue.
It's like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but native to Postgres.

Message queues allow you to decouple and connect microservices.
Send, store, and receive messages between components scalably, without dropping messages or
needing other services to be available.

PGMQ was created by CoreDB. Our goal is to make the full Postgres ecosystem accessible to everyone.
We're building a radically simplified Postgres platform designed to be developer-first and easily extensible.
PGMQ is a part of that project.

Not building in Rust? Try the [CoreDB pgmq Postgres extension](https://github.com/CoreDB-io/coredb/tree/main/extensions/pgx_pgmq).

## Features

- Lightweight - Rust and Postgres only
- Guaranteed delivery of messages to exactly one consumer within a visibility timeout
- API parity with [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq)
- Messages stay in the queue until deleted
- Messages can be archived, instead of deleted, for long-term retention and replayability
- Completely asynchronous API

## Quick start

- First, you will need Postgres. We use a container in this example.

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
```

- If you don't have Docker installed, it can be found [here](https://docs.docker.com/get-docker/).

- Make sure you have the Rust toolchain installed:

```bash
cargo --version
```

- This example was written with version 1.67.0, but the latest stable should work. You can go [here](https://www.rust-lang.org/tools/install) to install Rust if you don't have it already, then run `rustup install stable` to install the latest, stable toolchain.

- Change directory to the example project:
```bash
cd examples/basic
```

- Run the project!

```bash
cargo run
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

## Sending messages

You can send one message at a time with `queue.send()` or several with `queue.send_batch()`.
These methods can be passed any type that implements `serde::Serialize`. This means you can prepare your messages as JSON or as a struct.

## Reading messages

Reading a message will make it invisible (unavailable for consumption) for the duration of the visibility timeout (vt).
No messages are returned when the queue is empty or all messages are invisible.

Messages can be parsed as serde_json::Value or into a struct. `queue.read()` returns an `Result<Option<Message<T>>, PGMQError>`
where `T` is the type of the message on the queue. It returns an error when there is an issue parsing the message or if PGMQ is unable to reach postgres.
Note that when parsing into a `struct`, the operation will return an error if
parsed as the type specified. For example, if the message expected is
`MyMessage{foo: "bar"}` but `{"hello": "world"}` is received, the application will panic.

Read a single message with `queue.read()` or as many as you want with `queue.read_batch()`.

## Archive or Delete a message

Remove the message from the queue when you are done with it. You can either completely `.delete()`, or `.archive()` the message. Archived messages are deleted from the queue and inserted to the queue's archive table. Deleted messages are just deleted.

Read messages from the queue archive with SQL:

```sql
SELECT *
FROM pgmq_{your_queue_name}_archive;
```


License: Apache-2.0
