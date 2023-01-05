use pgmq::{Message, PGMQueue, PGMQueueConfig};

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let qconfig = PGMQueueConfig {
        queue_name: "myqueue".to_owned(),
        url: "postgres://postgres:postgres@0.0.0.0:5432".to_owned(),
        vt: 30,
        delay: 0,
    };

    let queue: PGMQueue = qconfig.init().await;

    queue.create().await?;

    let msg = serde_json::json!({
        "foo": "bar"
    });
    let msg_id = queue.enqueue(&msg).await;
    println!("msg_id: {:?}", msg_id);

    let read_msg: Message = queue.read().await.unwrap();
    print!("read_msg: {:?}", read_msg);

    let deleted = queue.delete(&read_msg.msg_id).await;
    println!("deleted: {:?}", deleted);

    Ok(())
}
