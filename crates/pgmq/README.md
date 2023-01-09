# Postgres Message Queue


### Start any Postgres instance
```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
```

## Initialize a queue connection

```rust
use pgmq::{Message, PGMQueue};


let queue: PGMQueue = PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned()).await;

```

## Create the queue

```rust
let myqueue = "myqueue".to_owned();
queue.create(&myqueue).await?;
```

## Pusblish a message
```rust
let msg = serde_json::json!({
    "foo": "bar"
});
let msg_id = queue.enqueue(&myqueue, &msg).await;
```

## Read a message
Reading a message will make it invisible for the duration of the visibility timeout (vt).

No messages are returned when the queue is empty or all messages are invisible.
```rust
let vt: u32 = 30;
let read_msg: Message = queue.read(&myqueue, Some(&vt)).await.expect("no messages in the queue!");
```

## Delete a message
Remove the message from the queue when you are done with it.
```rust
let deleted = queue.delete(&read_msg.msg_id).await;
```
