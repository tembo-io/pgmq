use pgmq::{Message, PGMQueue};

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let queue: PGMQueue =
        PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned()).await;

    let myqueue = "myqueue1".to_owned();
    queue.create(&myqueue).await?;

    let msg = serde_json::json!({
        "foo": "bar"
    });
    let msg_id = queue.enqueue(&myqueue, &msg).await;
    println!("msg_id: {:?}", msg_id);

    // read a message from myqueue. Set it to become visible 30 seconds from now.
    let read_msg: Message = queue.read(&myqueue, Some(&30_u32)).await.unwrap();
    print!("read_msg: {:?}", read_msg);

    // we're done with that message, so let's delete it from myqueue by passing the msg_id
    let deleted = queue.delete(&myqueue, &read_msg.msg_id).await;
    println!("deleted: {:?}", deleted);

    Ok(())
}
