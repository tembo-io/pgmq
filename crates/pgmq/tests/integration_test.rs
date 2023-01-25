use pgmq::{self, Message};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Pool, Postgres, Row};
use std::env;

async fn init_queue(qname: &str) -> pgmq::PGMQueue {
    let pgpass = env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_owned());
    let queue = pgmq::PGMQueue::new(format!("postgres://postgres:{}@0.0.0.0:5432", pgpass))
        .await
        .expect("failed to connect to postgres");

    // drop the test table at beginning of test
    sqlx::query(format!("DROP TABLE IF EXISTS pgmq_{}", qname).as_str())
        .execute(&queue.connection)
        .await
        .unwrap();

    // CREATE QUEUE
    let q_success = queue.create(qname).await;
    println!("q_success: {:?}", q_success);
    assert!(q_success.is_ok());
    queue
}

#[derive(Serialize, Debug, Deserialize)]
struct MyMessage {
    foo: String,
    num: u64,
}

impl Default for MyMessage {
    fn default() -> Self {
        MyMessage {
            foo: "bar".to_owned(),
            num: rand::thread_rng().gen_range(0..100),
        }
    }
}

#[derive(Serialize, Debug, Deserialize)]
struct YoloMessage {
    yolo: String,
}

async fn rowcount(qname: &str, connection: &Pool<Postgres>) -> i64 {
    let row_ct_query = format!("SELECT count(*) as ct FROM pgmq_{}", qname);
    sqlx::query(&row_ct_query)
        .fetch_one(connection)
        .await
        .unwrap()
        .get::<i64, usize>(0)
}

#[tokio::test]
async fn test_lifecycle() {
    let test_queue = "test_queue_0".to_owned();

    let queue = init_queue(&test_queue).await;

    // CREATE QUEUE
    let q_success = queue.create(&test_queue).await;
    assert!(q_success.is_ok());
    let num_rows = rowcount(&test_queue, &queue.connection).await;
    // no records on init
    assert_eq!(num_rows, 0);

    // SEND MESSAGE
    let msg = serde_json::json!({
        "foo": "bar"
    });
    let msg_id = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id, 1);
    let num_rows = rowcount(&test_queue, &queue.connection).await;

    // one record after one record sent
    assert_eq!(num_rows, 1);

    // READ MESSAGE
    let vt = 2;
    let msg1 = queue
        .read::<Value>(&test_queue, Some(&vt))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg1.msg_id, 1);
    // no messages returned, and the one record on the table is invisible
    let no_messages = queue.read::<Value>(&test_queue, Some(&vt)).await.unwrap();
    assert!(no_messages.is_none());
    // still one invisible record on the table
    let num_rows = rowcount(&test_queue, &queue.connection).await;

    // still one invisible record
    assert_eq!(num_rows, 1);

    // WAIT FOR VISIBILITY TIMEOUT TO EXPIRE
    tokio::time::sleep(std::time::Duration::from_secs(vt as u64)).await;
    let msg2 = queue
        .read::<Value>(&test_queue, Some(&vt))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg2.msg_id, 1);

    // DELETE MESSAGE
    let deleted = queue.delete(&test_queue, &msg1.msg_id).await.unwrap();
    assert_eq!(deleted, 1);
    let msg3 = queue.read::<Value>(&test_queue, Some(&vt)).await.unwrap();
    assert!(msg3.is_none());
    let num_rows = rowcount(&test_queue, &queue.connection).await;

    // table empty
    assert_eq!(num_rows, 0);
}

#[tokio::test]
async fn test_fifo() {
    let test_queue = "test_fifo_queue".to_owned();

    let queue = init_queue(&test_queue).await;

    // PUBLISH THREE MESSAGES
    let msg = serde_json::json!({
        "foo": "bar1"
    });
    let msg_id1 = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id1, 1);
    let msg_id2 = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id2, 2);
    let msg_id3 = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id3, 3);

    let vt: u32 = 1;
    // READ FIRST TWO MESSAGES
    let read1 = queue
        .read::<Value>(&test_queue, Some(&vt))
        .await
        .unwrap()
        .unwrap();
    let read2 = queue
        .read::<Value>(&test_queue, Some(&vt))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read2.msg_id, 2);
    assert_eq!(read1.msg_id, 1);
    // WAIT FOR VISIBILITY TIMEOUT TO EXPIRE
    tokio::time::sleep(std::time::Duration::from_secs(vt as u64)).await;
    tokio::time::sleep(std::time::Duration::from_secs(vt as u64)).await;

    // READ ALL, must still be in order
    let read1 = queue
        .read::<Value>(&test_queue, Some(&vt))
        .await
        .unwrap()
        .unwrap();
    let read2 = queue
        .read::<Value>(&test_queue, Some(&vt))
        .await
        .unwrap()
        .unwrap();
    let read3 = queue
        .read::<Value>(&test_queue, Some(&vt))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read1.msg_id, 1);
    assert_eq!(read2.msg_id, 2);
    assert_eq!(read3.msg_id, 3);
}

#[tokio::test]
async fn test_serde() {
    // series of tests serializing to queue and deserializing from queue
    let mut rng = rand::thread_rng();
    let test_queue = "test_ser_queue".to_owned();
    let queue = init_queue(&test_queue).await;

    // STRUCT => STRUCT
    // enqueue a struct and read a struct
    let msg = MyMessage {
        foo: "bar".to_owned(),
        num: rng.gen_range(0..100000),
    };
    let msg1 = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg1, 1);

    let msg_read = queue
        .read::<MyMessage>(&test_queue, Some(&30_u32))
        .await
        .unwrap()
        .unwrap();
    let _ = queue.delete(&test_queue, &msg_read.msg_id).await;
    assert_eq!(msg_read.message.num, msg.num);

    // JSON => JSON
    // enqueue json, read json
    let msg = serde_json::json!({
        "foo": "bar",
        "num": rng.gen_range(0..100000)
    });
    let msg2 = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg2, 2);

    let msg_read = queue
        .read::<Value>(&test_queue, Some(&30_u32))
        .await
        .unwrap()
        .unwrap();
    let _ = queue.delete(&test_queue, &msg_read.msg_id).await.unwrap();
    assert_eq!(msg_read.message["num"], msg["num"]);
    assert_eq!(msg_read.message["foo"], msg["foo"]);

    // JSON => STRUCT
    // enqueue json, read struct
    let msg = serde_json::json!({
        "foo": "bar",
        "num": rng.gen_range(0..100000)
    });

    let msg3 = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg3, 3);

    let msg_read = queue
        .read::<MyMessage>(&test_queue, Some(&30_u32))
        .await
        .unwrap()
        .unwrap();
    queue.delete(&test_queue, &msg_read.msg_id).await.unwrap();
    assert_eq!(msg_read.message.foo, msg["foo"].to_owned());
    assert_eq!(msg_read.message.num, msg["num"].as_u64().unwrap());

    // STRUCT => JSON
    let msg = MyMessage {
        foo: "bar".to_owned(),
        num: rng.gen_range(0..100000),
    };
    let msg4 = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg4, 4);
    let msg_read = queue
        .read::<Value>(&test_queue, Some(&30_u32))
        .await
        .unwrap()
        .unwrap();
    let _ = queue.delete(&test_queue, &msg_read.msg_id).await;
    assert_eq!(msg_read.message["foo"].to_owned(), msg.foo);
    assert_eq!(msg_read.message["num"].as_u64().unwrap(), msg.num);

    // DEFAULT json => json
    // no turbofish .read<T>()
    // no Message<T> annotation on assignment
    let msg = serde_json::json!( {
        "foo": "bar".to_owned(),
        "num": rng.gen_range(0..100000),
    });
    let msg5 = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg5, 5);
    let msg_read: crate::pgmq::Message = queue
        .read(&test_queue, Some(&30_u32)) // no turbofish on this line
        .await
        .unwrap()
        .unwrap();
    let _ = queue.delete(&test_queue, &msg_read.msg_id).await.unwrap();
    assert_eq!(msg_read.message["foo"].to_owned(), msg["foo"].to_owned());
    assert_eq!(
        msg_read.message["num"].as_u64().unwrap(),
        msg["num"].as_u64().unwrap()
    );
}

#[tokio::test]
async fn test_pop() {
    let test_queue = "test_pop_queue".to_owned();
    let queue = init_queue(&test_queue).await;
    let msg = MyMessage::default();
    let msg = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg, 1);
    let popped_msg = queue.pop::<MyMessage>(&test_queue).await.unwrap().unwrap();
    assert_eq!(popped_msg.msg_id, 1);
    let num_rows = rowcount(&test_queue, &queue.connection).await;
    // popped record is deleted on read
    assert_eq!(num_rows, 0);
}

/// test db operations that should produce errors
#[tokio::test]
async fn test_database_error_modes() {
    let pgpass = env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_owned());
    let queue = pgmq::PGMQueue::new(format!("postgres://postgres:{}@0.0.0.0:5432", pgpass))
        .await
        .expect("failed to connect to postgres");
    // let's not create the queues and make sure we get an error
    let msg_id = queue.enqueue("doesNotExist", &"foo").await;
    assert!(msg_id.is_err());

    // read from a queue that does not exist should error
    let read_msg = queue.read::<Message>("doesNotExist", Some(&10_u32)).await;
    assert!(read_msg.is_err());

    // connect to a postgres instance that doesnt exist should error
    let queue = pgmq::PGMQueue::new("postgres://DNE:5432".to_owned()).await;
    // we expect a database error
    match queue {
        Err(e) => {
            if let pgmq::errors::PgmqError::DatabaseError { .. } = e {
                // got the db error. good.
            } else {
                // got some other error. bad.
                panic!("expected a db error, got {:?}", e);
            }
        }
        // didnt get an error. bad.
        _ => panic!("expected a db error, got {:?}", read_msg),
    }
}

/// test parsing operations that should produce errors
#[tokio::test]
async fn test_parsing_error_modes() {
    let test_queue = "test_parsing_queue".to_owned();
    let queue = init_queue(&test_queue).await;
    let msg = MyMessage::default();
    let _ = queue.enqueue(&test_queue, &msg).await.unwrap();

    // we sent MyMessage, so trying to parse into YoloMessage should error
    let read_msg = queue.read::<YoloMessage>(&test_queue, Some(&10_u32)).await;

    // we expect a parse error
    match read_msg {
        Err(e) => {
            if let pgmq::errors::PgmqError::ParsingError { .. } = e {
                // got the parsing error. good.
            } else {
                // got some other error. bad.
                panic!("expected a parse error, got {:?}", e);
            }
        }
        // didnt get an error. bad.
        _ => panic!("expected a parse error, got {:?}", read_msg),
    }
}
