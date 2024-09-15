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
    let my_queue = "my_basic_queue".to_owned();
    queue
        .create(&my_queue)
        .await
        .expect("Failed to create queue");

    // Send a message as JSON
    let json_message = serde_json::json!({
        "foo": "bar"
    });
    println!("Enqueueing a JSON message: {json_message}");
    let json_message_id: i64 = queue
        .send(&my_queue, &json_message)
        .await
        .expect("Failed to enqueue message");

    // Messages can also be sent from structs
    #[derive(Serialize, Debug, Deserialize)]
    struct MyMessage {
        foo: String,
    }
    let struct_message = MyMessage {
        foo: "bar".to_owned(),
    };
    println!("Enqueueing a struct message: {struct_message:?}");
    let struct_message_id: i64 = queue
        .send(&my_queue, &struct_message)
        .await
        .expect("Failed to enqueue message");

    // Use a visibility timeout of 30 seconds.
    //
    // Messages that are not deleted within the
    // visibility timeout will return to the queue.
    let visibility_timeout_seconds: i32 = 30;

    // Read the JSON message
    let received_json_message: Message<Value> = queue
        .read::<Value>(&my_queue, Some(visibility_timeout_seconds))
        .await
        .unwrap()
        .expect("No messages in the queue");
    println!("Received a message: {received_json_message:?}");

    // Compare message IDs
    assert_eq!(received_json_message.msg_id, json_message_id);

    // Read the struct message
    let received_struct_message: Message<MyMessage> = queue
        .read::<MyMessage>(&my_queue, Some(visibility_timeout_seconds))
        .await
        .unwrap()
        .expect("No messages in the queue");
    println!("Received a message: {received_struct_message:?}");

    assert_eq!(received_struct_message.msg_id, struct_message_id);

    // Delete the messages to remove them from the queue
    let _ = queue
        .delete(&my_queue, received_json_message.msg_id)
        .await
        .expect("Failed to delete message");
    let _ = queue
        .delete(&my_queue, received_struct_message.msg_id)
        .await
        .expect("Failed to delete message");
    println!("Deleted the messages from the queue");

    // No messages are remaining
    let no_message: Option<Message<Value>> = queue
        .read::<Value>(&my_queue, Some(visibility_timeout_seconds))
        .await
        .unwrap();
    assert!(no_message.is_none());

    // We can also send and receive messages in batches

    // Send a batch of JSON messages
    let json_message_batch = vec![
        serde_json::json!({"foo": "bar1"}),
        serde_json::json!({"foo": "bar2"}),
        serde_json::json!({"foo": "bar3"}),
    ];
    println!("Enqueuing a batch of messages: {json_message_batch:?}");
    let json_message_batch_ids = queue
        .send_batch(&my_queue, &json_message_batch)
        .await
        .expect("Failed to enqueue messages");

    // Receive a batch of JSON messages
    let batch_size = 3;
    let batch: Vec<Message<Value>> = queue
        .read_batch::<Value>(&my_queue, Some(visibility_timeout_seconds), batch_size)
        .await
        .unwrap()
        .expect("no messages in the queue!");
    println!("Received a batch of messages: {batch:?}");
    for (_, message) in batch.iter().enumerate() {
        assert!(json_message_batch_ids.contains(&message.msg_id));
        let _ = queue
            .delete(&my_queue, message.msg_id)
            .await
            .expect("Failed to delete message");
        println!("Deleted message {}", message.msg_id);
    }

    // Send a batch of struct messages
    let struct_message_batch = vec![
        MyMessage {
            foo: "bar1".to_owned(),
        },
        MyMessage {
            foo: "bar2".to_owned(),
        },
        MyMessage {
            foo: "bar3".to_owned(),
        },
    ];
    println!("Enqueuing a batch of messages: {struct_message_batch:?}");
    let struct_message_batch_ids = queue
        .send_batch(&my_queue, &struct_message_batch)
        .await
        .expect("Failed to enqueue messages");

    // Receive a batch of struct messages
    let batch_size = 3;
    let batch: Vec<Message<MyMessage>> = queue
        .read_batch::<MyMessage>(&my_queue, Some(visibility_timeout_seconds), batch_size)
        .await
        .unwrap()
        .expect("no messages in the queue!");
    println!("Received a batch of messages: {batch:?}");
    for (_, message) in batch.iter().enumerate() {
        assert!(struct_message_batch_ids.contains(&message.msg_id));
        let _ = queue
            .delete(&my_queue, message.msg_id)
            .await
            .expect("Failed to delete message");
        println!("Deleted message {}", message.msg_id);
    }

    Ok(())
}
