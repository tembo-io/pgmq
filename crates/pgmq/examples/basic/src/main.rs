use pgmq::{Message, PGMQueue};
use serde_json;
use serde::{Serialize, Deserialize};


#[tokio::main]
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let queue: PGMQueue =
        PGMQueue::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned()).await;

    // CREATE A QUEUE
    let myqueue = "demo_queue_0".to_owned();
    queue.create(&myqueue).await?;

    // // SEND A MESSAGE
    let msg = serde_json::json!({
        "foo1": "bar"
    });
    let msg_id = queue.enqueue(&myqueue, &msg).await;
    println!("msg_id: {:?}", msg_id);

    // READ A MESSAGE - set it to be invisible for 30 seconds
    let read_msg: Message = queue.read(&myqueue, Some(&30_u32)).await.unwrap();
    println!("read_json_msg: {:?}", read_msg);

    // DELETE A MESSAGE - we're done with that message, so let's delete it from myqueue by passing the msg_id
    let deleted = queue.delete(&myqueue, &read_msg.msg_id).await;
    println!("deleted: {:?}", deleted);

    // SEND A STRUCT
    #[derive(Serialize, Debug, Deserialize)]
    struct MyMessage {
        foo: String,
    }

    let mymsg = MyMessage {
        foo: "bar".to_owned(),
    };
    let msg_id = queue.enqueue(&myqueue, &mymsg).await;
    println!("msg_id: {:?}", msg_id);

    // READ A STRUCT
    let read_msg: Message<MyMessage> = queue.read::<MyMessage>(&myqueue, None).await.unwrap();
    println!("read_struct_msg: {:?}", read_msg);

    Ok(())
}

fn main() {
    run();
}

