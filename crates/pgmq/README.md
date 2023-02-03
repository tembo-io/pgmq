# Postgres Message Queue

A lightweight messaging queue for Rust, using Postgres as the backend.
Inspired by the [RSMQ project](https://github.com/smrchy/rsmq).

# Examples

First, start any Postgres instance. It is the only external dependency.

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
```

Example of sending, receiving, and deleting messages from the queue. Typically applications sending messages
will not be the same application reading the message.

```rust
use pgmq::{Message, PGMQueue};
use serde::{Serialize, Deserialize};
use serde_json::Value;

#[tokio::main]
async fn main() {
    // CREATE A QUEUE
    let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned()).await.expect("failed to connect to postgres");
    let myqueue = "myqueue".to_owned();
    queue.create(&myqueue).await.expect("Failed to create queue");

    // SEND A `serde_json::Value` MESSAGE
    let msg1 = serde_json::json!({
        "foo": "bar"
    });
    let msg_id1: i64 = queue.enqueue(&myqueue, &msg1).await.expect("Failed to enqueue message");

    // SEND A STRUCT
    #[derive(Serialize, Debug, Deserialize)]
    struct MyMessage {
        foo: String,
    }
    let msg2 = MyMessage {
        foo: "bar".to_owned(),
    };
    let msg_id2: i64  = queue.enqueue(&myqueue, &msg2).await.expect("Failed to enqueue message");

    // READ A MESSAGE as `serde_json::Value`
    let vt: i32 = 30;
    let read_msg1: Message<Value> = queue.read::<Value>(&myqueue, Some(&vt)).await.unwrap().expect("no messages in the queue!");
    assert_eq!(read_msg1.msg_id, msg_id1);

    // READ A MESSAGE as a struct
    let read_msg2: Message<MyMessage> = queue.read::<MyMessage>(&myqueue, Some(&vt)).await.unwrap().expect("no messages in the queue!");
    assert_eq!(read_msg2.msg_id, msg_id2);

    // DELETE THE MESSAGE WE SENT
    let deleted = queue.delete(&myqueue, &read_msg1.msg_id).await.expect("Failed to delete message");
    let deleted = queue.delete(&myqueue, &read_msg2.msg_id).await.expect("Failed to delete message");

    // No messages present aftwe we've deleted all of them
    let no_msg: Option<Message<Value>> = queue.read::<Value>(&myqueue, Some(&vt)).await.unwrap();
    assert!(no_msg.is_none());
}
```
## Sending messages

`queue.enqueue()` can be passed any type that implements `serde::Serialize`. This means you can prepare your messages as JSON or as a struct.

## Reading messages
Reading a message will make it invisible (unavailable for consumption) for the duration of the visibility timeout (vt).
No messages are returned when the queue is empty or all messages are invisible.

Messages can be parsed as serde_json::Value or into a struct. `queue.read()` returns an `Result<Option<Message<T>>, PGMQError>`
where `T` is the type of the message on the queue. It returns an error when there is an issue parsing the message or if PGMQ is unable to reach postgres.
Note that when parsing into a `struct`, the operation will return an error if
parsed as the type specified. For example, if the message expected is
`MyMessage{foo: "bar"}` but` {"hello": "world"}` is received, the application will panic.

#### as a Struct
Reading a message will make it invisible for the duration of the visibility timeout (vt).
No messages are returned when the queue is empty or all messages are invisible.

## Delete a message
Remove the message from the queue when you are done with it.

License: MIT
