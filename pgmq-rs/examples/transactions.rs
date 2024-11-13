use pgmq::{Message, PGMQueueExt};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

#[derive(Serialize, Debug, Deserialize, Eq, PartialEq)]
struct MyMessage {
    foo: String,
    num: u64,
}

#[tokio::main]
async fn main() {
    let db_url = "postgres://postgres:postgres@localhost:5432/postgres";
    let example_queue = "example_tx_queue";

    // regular postgres connection pool
    let pool = PgPool::connect(&db_url)
        .await
        .expect("failed to connect to postgres");
    // a separate pool is created in the queue
    let queue: PGMQueueExt =
        PGMQueueExt::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned(), 2)
            .await
            .expect("Failed to connect to postgres");

    // Create the queue
    queue
        .create(&example_queue)
        .await
        .expect("Failed to create queue");

    // start a transaction
    let mut tx = pool.begin().await.expect("failed to start transaction");

    // send message using the open transaction
    let msg = MyMessage {
        foo: "bar".to_owned(),
        num: 42,
    };
    let sent = queue
        .send_with_cxn(&example_queue, &msg, &mut *tx)
        .await
        .expect("failed to send message");
    println!("message sent. id: {sent}, msg: {msg:?}");

    // get row count from a new connection (not the transaction)
    let rows = sqlx::query("SELECT queue_length FROM pgmq.metrics($1)")
        .bind(example_queue)
        .fetch_one(&pool)
        .await
        .expect("failed to fetch row")
        .get::<i64, usize>(0);
    // queue empty because transaction not committed
    assert_eq!(rows, 0);
    println!("queue length: {rows}");

    // reading from queue returns no messages
    let received: Option<Message<MyMessage>> = queue
        .read(&example_queue, 10)
        .await
        .expect("failed to read message");
    assert!(received.is_none());

    // commit the transaction
    tx.commit().await.expect("failed to commit transaction");
    println!("transaction committed");

    let rows = sqlx::query("SELECT queue_length FROM pgmq.metrics($1)")
        .bind(example_queue)
        .fetch_one(&pool)
        .await
        .expect("failed to fetch row")
        .get::<i64, usize>(0);
    // queue empty because transaction not committed
    assert_eq!(rows, 1);
    println!("queue length: {rows}");

    // reading from queue returns no messages
    let received: Message<MyMessage> = queue
        .read(&example_queue, 10)
        .await
        .expect("failed to read message")
        .expect("expected message");

    // can now read the message because transaction was committed
    assert!(received.msg_id == sent);
    assert_eq!(received.message, msg);
    println!(
        "message received. id: {}, msg: {:?}",
        received.msg_id, received.message
    );

    queue
        .drop_queue(&example_queue)
        .await
        .expect("failed to drop queue");
}
