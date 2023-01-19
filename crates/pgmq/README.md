# Postgres Message Queue

A lightweight messaging queue for Rust, using Postgres as the backend.
Inspired by the [RSMQ project](https://github.com/smrchy/rsmq).

# Examples

First, start any Postgres instance. It is the only external dependency.

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
```
## Create a queue

```rust
use pgmq::{Message, PGMQueue};
let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned()).await.expect("Failed to connect to Postgres");

let myqueue = "myqueue".to_owned();
queue.create(&myqueue).await.expect("Failed to create queue");
```

## Sending messages

`queue.enqueue()` can be passed any type that implements `serde::Serialize`. This means you can prepare your messages as JSON or as a struct.

#### as serde_json::Value
```rust
let msg = serde_json::json!({
    "foo": "bar"
});
let msg_id = queue.enqueue(&myqueue, &msg).await.expect("Failed to enqueue message");
```
#### as a struct
```rust
use serde::{Serialize, Deserialize};
#[derive(Serialize, Debug, Deserialize)]
struct MyMessage {
    foo: String,
}
let msg = MyMessage {
    foo: "bar".to_owned(),
};
let msg_id: i64  = queue.enqueue(&myqueue, &msg).await.expect("Failed to enqueue message");
```

## Reading messages
Reading a message will make it invisible for the duration of the visibility timeout (vt).
No messages are returned when the queue is empty or all messages are invisible.
Messages can be parsed as JSON or as into a struct. `queue.read()` returns an `Option<Message<T>>`
where `T` is the type of the message on the queue. It can be parsed as JSON or as a struct.
Note that when parsing into a `struct`, the application will panic if the message cannot be
parsed as the type specified. For example, if the message expected is
`MyMessage{foo: "bar"}` but` {"hello": "world"}` is received, the application will panic.
#### as serde_json::Value
```rust
use serde_json::Value;
let vt: u32 = 30;
let read_msg: Message<Value> = queue.read::<Value>(&myqueue, Some(&vt)).await.expect("no messages in the queue!");
```
#### as a Struct
Reading a message will make it invisible for the duration of the visibility timeout (vt).
No messages are returned when the queue is empty or all messages are invisible.
```rust
use serde_json::Value;
let vt: u32 = 30;
let read_msg: Message<MyMessage> = queue.read::<MyMessage>(&myqueue, Some(&vt)).await.expect("no messages in the queue!");
```
## Delete a message
Remove the message from the queue when you are done with it.
```rust
let deleted = queue.delete(&read_msg.msg_id).await;
```

License: MIT
