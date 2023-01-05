# Postgres Message Queue




### Start any PG instance
```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
```


## Initialize a queue connection

```rust
use pgmq::{Message, PGMQueue, PGMQueueConfig};

let qconfig = PGMQueueConfig {
    queue_name: "myqueue".to_owned(),
    url: "postgres://postgres:postgres@0.0.0.0:5432".to_owned(),
    vt: 30,
    delay: 0,
};

let queue: PGMQueue = qconfig.init().await;
```

## Create the queue

```rust
queue.create().await?;
```

## Pusblish a message
```rust
let msg = serde_json::json!({
    "foo": "bar"
});
let msg_id = queue.enqueue(&msg).await;
```

## Read a message
No messages are returned when the queue is empty or all messages are invisible.

Reading a message will make it invisible for the duration of the visibility timeout (vt).

```rust
let read_msg: Message = queue.read().await.unwrap();
```

## Delete a message
Remove the message from the queue when you are done with it.
```rust
let deleted = queue.delete(&read_msg.msg_id).await;
```
