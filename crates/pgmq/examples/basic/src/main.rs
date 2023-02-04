use pgmq::{Message, PGMQueue};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[tokio::main]
async fn main() {
    // CREATE A QUEUE
    let queue: PGMQueue =
        PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned()).await;
    let myqueue = "myqueue".to_owned();
    queue
        .create(&myqueue)
        .await
        .expect("Failed to create queue");

    // SEND A `serde_json::Value` MESSAGE
    let msg1 = serde_json::json!({
        "foo": "bar"
    });
    let _msg_id1: i64 = queue
        .send(&myqueue, &msg1)
        .await
        .expect("Failed to enqueue message");

    // SEND A STRUCT
    #[derive(Serialize, Debug, Deserialize)]
    struct MyMessage {
        foo: String,
    }
    let msg2 = MyMessage {
        foo: "bar".to_owned(),
    };
    let _msg_id2: i64 = queue
        .send(&myqueue, &msg2)
        .await
        .expect("Failed to enqueue message");

    // READ A MESSAGE as `serde_json::Value`
    let vt: i32 = 30;
    let read_msg1: Message<Value> = queue
        .read::<Value>(&myqueue, Some(&vt))
        .await
        .expect("no messages in the queue!");

    // READ A MESSAGE as a struct
    let read_msg2: Message<MyMessage> = queue
        .read::<MyMessage>(&myqueue, Some(&vt))
        .await
        .expect("no messages in the queue!");

    // DELETE THE MESSAGE WE SENT
    queue
        .delete(&myqueue, &read_msg1.msg_id)
        .await
        .expect("Failed to delete message");
    queue
        .delete(&myqueue, &read_msg2.msg_id)
        .await
        .expect("Failed to delete message");
}
