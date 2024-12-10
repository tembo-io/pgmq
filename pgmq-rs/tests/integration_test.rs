use chrono::{Duration, Utc};
use pgmq::{
    errors::PgmqError,
    types::{Message, ARCHIVE_PREFIX, PGMQ_SCHEMA, QUEUE_PREFIX},
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Pool, Postgres, Row};
use std::env;

async fn init_queue(qname: &str) -> pgmq::PGMQueue {
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let queue = pgmq::PGMQueue::new(db_url)
        .await
        .expect("failed to connect to postgres");
    // make sure queue doesn't exist before the test
    queue.destroy(qname).await.unwrap();
    // CREATE QUEUE
    // jiggle to mitigate race condition in concurrent `create if not exists` statements
    let random_sleep_ms = rand::thread_rng().gen_range(0..1000);
    tokio::time::sleep(std::time::Duration::from_millis(random_sleep_ms)).await;
    let q_success = queue.create(qname).await;
    println!("q_success: {q_success:?}");
    assert!(q_success.is_ok());
    queue
}

#[derive(Serialize, Debug, Deserialize, Eq, PartialEq)]
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
    let row_ct_query = format!("SELECT count(*) as ct FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{qname}");
    sqlx::query(&row_ct_query)
        .fetch_one(connection)
        .await
        .unwrap()
        .get::<i64, usize>(0)
}

async fn archive_rowcount(qname: &str, connection: &Pool<Postgres>) -> i64 {
    let row_ct_query = format!("SELECT count(*) as ct FROM {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{qname}");
    sqlx::query(&row_ct_query)
        .fetch_one(connection)
        .await
        .unwrap()
        .get::<i64, usize>(0)
}

// we need to check whether table exists
// simple solution: our existing rowcount() helper will fail
// wrap it in a Result<> so we can use it
async fn fallible_rowcount(qname: &str, connection: &Pool<Postgres>) -> Result<i64, PgmqError> {
    let row_ct_query = format!("SELECT count(*) as ct FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{qname}");
    Ok(sqlx::query(&row_ct_query)
        .fetch_one(connection)
        .await?
        .get::<i64, usize>(0))
}

async fn fallible_archive_rowcount(
    qname: &str,
    connection: &Pool<Postgres>,
) -> Result<i64, PgmqError> {
    let row_ct_query = format!("SELECT count(*) as ct FROM {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{qname}");
    Ok(sqlx::query(&row_ct_query)
        .fetch_one(connection)
        .await?
        .get::<i64, usize>(0))
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
    let msg_id = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id, 1);
    let num_rows = rowcount(&test_queue, &queue.connection).await;

    // one record after one record sent
    assert_eq!(num_rows, 1);

    // READ MESSAGE
    let vt = 2;
    let msg1 = queue
        .read::<Value>(&test_queue, Some(vt))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg1.msg_id, 1);
    // no messages returned, and the one record on the table is invisible
    let no_messages = queue.read::<Value>(&test_queue, Some(vt)).await.unwrap();
    assert!(no_messages.is_none());
    // still one invisible record on the table
    let num_rows = rowcount(&test_queue, &queue.connection).await;

    // still one invisible record
    assert_eq!(num_rows, 1);

    // WAIT FOR VISIBILITY TIMEOUT TO EXPIRE
    tokio::time::sleep(std::time::Duration::from_secs(vt as u64)).await;
    let msg2 = queue
        .read::<Value>(&test_queue, Some(vt))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg2.msg_id, 1);

    // DELETE MESSAGE
    let deleted = queue.delete(&test_queue, msg1.msg_id).await.unwrap();
    assert_eq!(deleted, 1);
    let msg3 = queue.read::<Value>(&test_queue, Some(vt)).await.unwrap();
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
    let msg_id1 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id1, 1);
    let msg_id2 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id2, 2);
    let msg_id3 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id3, 3);

    let vt: i32 = 1;
    // READ FIRST TWO MESSAGES
    let read1 = queue
        .read::<Value>(&test_queue, Some(vt))
        .await
        .unwrap()
        .unwrap();
    let read2 = queue
        .read::<Value>(&test_queue, Some(vt))
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
        .read::<Value>(&test_queue, Some(vt))
        .await
        .unwrap()
        .unwrap();
    let read2 = queue
        .read::<Value>(&test_queue, Some(vt))
        .await
        .unwrap()
        .unwrap();
    let read3 = queue
        .read::<Value>(&test_queue, Some(vt))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read1.msg_id, 1);
    assert_eq!(read2.msg_id, 2);
    assert_eq!(read3.msg_id, 3);
}

#[tokio::test]
async fn test_send_delay() {
    let vt: i32 = 1;

    let test_queue = "test_send_delay_queue".to_owned();

    let queue = init_queue(&test_queue).await;

    // CREATE QUEUE
    let q_success = queue.create(&test_queue).await;
    assert!(q_success.is_ok());
    let num_rows = rowcount(&test_queue, &queue.connection).await;
    // no records on init
    assert_eq!(num_rows, 0);

    // SEND MESSAGE WITH 5 SECOND DELAY
    let msg = serde_json::json!({
        "foo": "bar"
    });
    let _ = queue.send_delay(&test_queue, &msg, 5).await.unwrap();
    let num_rows = rowcount(&test_queue, &queue.connection).await;
    assert_eq!(num_rows, 1);
    let no_messages = queue.read::<Value>(&test_queue, Some(vt)).await.unwrap();
    assert!(no_messages.is_none());
    // SLEEP 5 SECONDS
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    // GET MESSAGE
    let one_messages = queue.read::<Value>(&test_queue, Some(vt)).await.unwrap();
    assert!(one_messages.is_some());
}

#[tokio::test]
async fn test_read_batch_with_poll() {
    let test_queue = "test_read_batch_with_poll".to_owned();

    let queue = init_queue(&test_queue).await;

    // PUBLISH THREE MESSAGES
    let msg = serde_json::json!({
        "foo": "bar1"
    });
    let msg_id1 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id1, 1);
    let msg_id2 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id2, 2);
    let msg_id3 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id3, 3);

    // Reading from queue with a 5 seconds VT
    let read_message_1 = queue
        .read_batch_with_poll::<Value>(
            &test_queue,
            Some(5),
            5,
            Some(std::time::Duration::from_secs(6)),
            None,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(read_message_1.len(), 3);

    let starting_time = std::time::Instant::now();

    // Since VT is 5 seconds, if we poll the queue, it takes around 5 seconds
    // to return the result, and returns all 3 messages
    let read_message_2 = queue
        .read_batch_with_poll::<Value>(
            &test_queue,
            Some(5),
            5,
            Some(std::time::Duration::from_secs(6)),
            None,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(read_message_2.len(), 3);
    assert!(starting_time.elapsed() > std::time::Duration::from_secs(3));

    // If we don't poll for long enough, we get none
    let read_message_3 = queue
        .read_batch_with_poll::<Value>(
            &test_queue,
            Some(3),
            5,
            Some(std::time::Duration::from_secs(1)),
            None,
        )
        .await
        .unwrap();

    assert!(read_message_3.is_none());
}

#[tokio::test]
async fn test_read_batch() {
    let test_queue = "test_read_batch".to_owned();

    let queue = init_queue(&test_queue).await;

    // PUBLISH THREE MESSAGES
    let msg = serde_json::json!({
        "foo": "bar1"
    });
    let msg_id1 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id1, 1);
    let msg_id2 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id2, 2);
    let msg_id3 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id3, 3);

    let vt: i32 = 1;
    let num_msgs = 3;

    let batch = queue
        .read_batch::<Value>(&test_queue, Some(vt), num_msgs)
        .await
        .unwrap()
        .unwrap();

    for (i, message) in batch.iter().enumerate() {
        let index = i + 1;
        assert_eq!(message.msg_id.to_string(), index.to_string());
    }

    let msg = queue.read::<Value>(&test_queue, Some(vt)).await.unwrap();
    // assert no messages read because they are invisible
    assert!(msg.is_none());
    let num_rows = rowcount(&test_queue, &queue.connection).await;
    // assert there are still 3 (invisible) entries on the table
    assert_eq!(num_rows, 3);
}

#[tokio::test]
async fn test_send_batch() {
    let test_queue = "test_send_batch".to_owned();

    let queue = init_queue(&test_queue).await;

    // Send 3 messages to queue as batch
    let msgs = vec![
        serde_json::json!({"foo": "bar1"}),
        serde_json::json!({"foo": "bar2"}),
        serde_json::json!({"foo": "bar3"}),
    ];
    let msg_ids = queue
        .send_batch(&test_queue, &msgs)
        .await
        .expect("Failed to enqueue messages");
    for (i, id) in msg_ids.iter().enumerate() {
        assert_eq!(id.to_string(), msg_ids[i].to_string());
    }

    // Send 3 messages in struct form to queue as batch
    #[derive(Serialize, Debug, Deserialize)]
    struct MyMessage {
        foo: String,
    }
    let msgs2 = vec![
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
    let msg_ids2 = queue
        .send_batch(&test_queue, &msgs2)
        .await
        .expect("Failed to enqueue messages");
    for (i, id) in msg_ids2.iter().enumerate() {
        assert_eq!(id.to_string(), msg_ids2[i].to_string());
    }

    // Read 3 messages from queue as batch
    let vt: i32 = 1;
    let num_msgs = 3;
    let batch = queue
        .read_batch::<Value>(&test_queue, Some(vt), num_msgs)
        .await
        .unwrap()
        .unwrap();

    for (i, message) in batch.iter().enumerate() {
        let index = i + 1;
        assert_eq!(message.msg_id.to_string(), index.to_string());
    }
}

#[tokio::test]
async fn test_delete_batch() {
    let test_queue = "test_delete_batch".to_owned();
    let queue = init_queue(&test_queue).await;
    let vt: i32 = 1;
    let mut msg_id_first_last: Vec<i64> = Vec::new();

    // Send 1 message to the queue
    let msg_first = serde_json::json!({
        "foo": "first"
    });
    let msg_id1 = queue.send(&test_queue, &msg_first).await.unwrap();
    assert_eq!(msg_id1, 1);
    msg_id_first_last.push(msg_id1);

    // Send 3 messages to queue as batch
    let msgs = vec![
        serde_json::json!({"foo": "bar1"}),
        serde_json::json!({"foo": "bar2"}),
        serde_json::json!({"foo": "bar3"}),
    ];
    let msg_ids = queue
        .send_batch(&test_queue, &msgs)
        .await
        .expect("Failed to enqueue messages");
    for (i, id) in msg_ids.iter().enumerate() {
        assert_eq!(id.to_string(), msg_ids[i].to_string());
    }

    // Send 1 message to the queue
    let msg_last = serde_json::json!({
        "foo": "last"
    });
    let msg_id2 = queue.send(&test_queue, &msg_last).await.unwrap();
    assert_eq!(msg_id2, 5);
    msg_id_first_last.push(msg_id2);

    // Delete 3 messages from queue as batch
    let del = queue
        .delete_batch(&test_queue, &msg_ids)
        .await
        .expect("Failed to delete messages from queue");

    // Assert the number of messages deleted is equal to the number of messages we passed to delete_batch()
    assert_eq!(del.to_string(), msg_ids.len().to_string());

    // Assert first message is still present and readable
    let first = queue
        .read::<Value>(&test_queue, Some(vt))
        .await
        .expect("Failed to read message");
    assert_eq!(first.unwrap().msg_id, msg_id1);

    // Assert last message is still present and readable
    let last = queue
        .read::<Value>(&test_queue, Some(vt))
        .await
        .expect("Failed to read message");
    assert_eq!(last.unwrap().msg_id, msg_id2);

    // Delete first and last message as batch
    let del_first_last = queue
        .delete_batch(&test_queue, &msg_id_first_last)
        .await
        .expect("Failed to delete messages from queue");

    // Assert the number of messages deleted is equal to the number of messages we passed to delete_batch()
    assert_eq!(
        del_first_last.to_string(),
        msg_id_first_last.len().to_string()
    );

    // Assert there are no messages to read from queue
    let msg = queue.read::<Value>(&test_queue, Some(vt)).await.unwrap();
    assert!(msg.is_none());
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
    let msg1 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg1, 1);

    let msg_read = queue
        .read::<MyMessage>(&test_queue, Some(30_i32))
        .await
        .unwrap()
        .unwrap();
    let _ = queue.delete(&test_queue, msg_read.msg_id).await;
    assert_eq!(msg_read.message.num, msg.num);

    // JSON => JSON
    // enqueue json, read json
    let msg = serde_json::json!({
        "foo": "bar",
        "num": rng.gen_range(0..100000)
    });
    let msg2 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg2, 2);

    let msg_read = queue
        .read::<Value>(&test_queue, Some(30_i32))
        .await
        .unwrap()
        .unwrap();
    let _ = queue.delete(&test_queue, msg_read.msg_id).await.unwrap();
    assert_eq!(msg_read.message["num"], msg["num"]);
    assert_eq!(msg_read.message["foo"], msg["foo"]);

    // JSON => STRUCT
    // enqueue json, read struct
    let msg = serde_json::json!({
        "foo": "bar",
        "num": rng.gen_range(0..100000)
    });

    let msg3 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg3, 3);

    let msg_read = queue
        .read::<MyMessage>(&test_queue, Some(30_i32))
        .await
        .unwrap()
        .unwrap();
    queue.delete(&test_queue, msg_read.msg_id).await.unwrap();
    assert_eq!(msg_read.message.foo, msg["foo"].to_owned());
    assert_eq!(msg_read.message.num, msg["num"].as_u64().unwrap());

    // STRUCT => JSON
    let msg = MyMessage {
        foo: "bar".to_owned(),
        num: rng.gen_range(0..100000),
    };
    let msg4 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg4, 4);
    let msg_read = queue
        .read::<Value>(&test_queue, Some(30_i32))
        .await
        .unwrap()
        .unwrap();
    let _ = queue.delete(&test_queue, msg_read.msg_id).await;
    assert_eq!(msg_read.message["foo"].to_owned(), msg.foo);
    assert_eq!(msg_read.message["num"].as_u64().unwrap(), msg.num);

    // DEFAULT json => json
    // no turbofish .read<T>()
    // no Message<T> annotation on assignment
    let msg = serde_json::json!( {
        "foo": "bar".to_owned(),
        "num": rng.gen_range(0..100000),
    });
    let msg5 = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg5, 5);
    let msg_read: Message = queue
        .read(&test_queue, Some(30_i32)) // no turbofish on this line
        .await
        .unwrap()
        .unwrap();
    let _ = queue.delete(&test_queue, msg_read.msg_id).await.unwrap();
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
    let msg = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg, 1);
    let popped_msg = queue.pop::<MyMessage>(&test_queue).await.unwrap().unwrap();
    assert_eq!(popped_msg.msg_id, 1);
    let num_rows = rowcount(&test_queue, &queue.connection).await;
    // popped record is deleted on read
    assert_eq!(num_rows, 0);
}

#[tokio::test]
async fn test_archive() {
    let test_queue = "test_archive_queue".to_owned();
    let queue = init_queue(&test_queue).await;
    let msg = MyMessage::default();
    let msg = queue.send(&test_queue, &msg).await.unwrap();
    assert_eq!(msg, 1);
    let num_moved = queue.archive(&test_queue, msg).await.unwrap();
    assert_eq!(num_moved, 1);
    let num_rows_queue = rowcount(&test_queue, &queue.connection).await;
    // archived record is no longer on the queue
    assert_eq!(num_rows_queue, 0);
    let num_rows_archive = archive_rowcount(&test_queue, &queue.connection).await;
    // archived record is now on the archive table
    assert_eq!(num_rows_archive, 1);
}

#[tokio::test]
async fn test_archive_batch() {
    let test_queue = "test_archive_batch_queue".to_owned();
    let queue = init_queue(&test_queue).await;
    let msg = MyMessage::default();

    let msg_1 = queue.send(&test_queue, &msg).await.unwrap();
    let msg_2 = queue.send(&test_queue, &msg).await.unwrap();
    let msg_3 = queue.send(&test_queue, &msg).await.unwrap();

    let num_moved = queue
        .archive_batch(&test_queue, &[msg_1, msg_2, msg_3])
        .await
        .unwrap();
    assert_eq!(num_moved, 3);

    let num_rows_queue = rowcount(&test_queue, &queue.connection).await;
    assert_eq!(num_rows_queue, 0);

    let num_rows_archive = archive_rowcount(&test_queue, &queue.connection).await;
    assert_eq!(num_rows_archive, 3);
}

/// test db operations that should produce errors
#[tokio::test]
async fn test_database_error_modes() {
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let queue = pgmq::PGMQueue::new(db_url)
        .await
        .expect("failed to connect to postgres");
    // let's not create the queues and make sure we get an error
    let msg_id = queue.send("doesNotExist", &"foo").await;
    assert!(msg_id.is_err());

    // read from a queue that does not exist should error
    let read_msg = queue.read::<Message>("doesNotExist", Some(10_i32)).await;
    assert!(read_msg.is_err());

    // connect to a postgres instance with a malformed connection string should error
    let queue = pgmq::PGMQueue::new("postgres://DNE:5432".to_owned()).await;
    // we expect a url parsing error
    match queue {
        Err(e) => {
            if let PgmqError::UrlParsingError { .. } = e {
                // got the url parsing error error. good.
            } else {
                // got some other error. bad.
                panic!("expected a url parsing error, got {e:?}");
            }
        }
        // didn't get an error. bad.
        _ => panic!("expected a url parsing error, got {read_msg:?}"),
    }

    // connect to a postgres instance that doesn't exist should error
    let queue = pgmq::PGMQueue::new("postgres://user:pass@badhost:5432".to_owned()).await;
    // we expect a database error
    match queue {
        Err(e) => {
            if let PgmqError::DatabaseError { .. } = e {
                // got the db error. good.
            } else {
                // got some other error. bad.
                panic!("expected a db error, got {e:?}");
            }
        }
        // didn't get an error. bad.
        _ => panic!("expected a db error, got {read_msg:?}"),
    }
}

#[tokio::test]
async fn test_purge() {
    let test_queue = format!("test_purge_{}", rand::thread_rng().gen_range(0..100000));
    let queue = init_queue(&test_queue).await;
    let msg = MyMessage::default();
    let _ = queue.send(&test_queue, &msg).await.unwrap();
    let _ = queue.send(&test_queue, &msg).await.unwrap();
    let _ = queue.send(&test_queue, &msg).await.unwrap();

    let purged_count = queue.purge(&test_queue).await.expect("purge queue error");

    assert_eq!(purged_count, 3);
    let post_purge_rowcount = rowcount(&test_queue, &queue.connection).await;
    assert_eq!(post_purge_rowcount, 0);
}

/// test parsing operations that should produce errors
#[tokio::test]
async fn test_parsing_error_modes() {
    let test_queue = "test_parsing_queue".to_owned();
    let queue = init_queue(&test_queue).await;
    let msg = MyMessage::default();
    let _ = queue.send(&test_queue, &msg).await.unwrap();

    // we sent MyMessage, so trying to parse into YoloMessage should error
    let read_msg = queue.read::<YoloMessage>(&test_queue, Some(10_i32)).await;

    // we expect a parse error
    match read_msg {
        Err(e) => {
            if let PgmqError::JsonParsingError { .. } = e {
                // got the parsing error. good.
            } else {
                // got some other error. bad.
                panic!("expected a parse error, got {e:?}");
            }
        }
        // didn't get an error. bad.
        _ => panic!("expected a parse error, got {read_msg:?}"),
    }
}

/// destroy queue should clean up appropriately
#[tokio::test]
async fn test_destroy() {
    let test_queue = "test_destroy_queue".to_owned();
    let queue = init_queue(&test_queue).await;
    let msg = MyMessage::default();
    // send two messages
    let msg1 = queue.send(&test_queue, &msg).await.unwrap();
    let msg2 = queue.send(&test_queue, &msg).await.unwrap();

    // archive one, so there's messages in queue and in archive
    let _ = queue.archive(&test_queue, msg1).await.unwrap();
    // read one to make sure messages are on the queue
    let read: Message = queue
        .read(&test_queue, Some(30_i32))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.msg_id, msg2);

    // must not panic
    queue.destroy(&test_queue).await.unwrap();

    // the queue and the queue archive should no longer exist
    let queue_table = fallible_rowcount(&test_queue, &queue.connection).await;
    assert!(queue_table.is_err());
    let archive_table = fallible_archive_rowcount(&test_queue, &queue.connection).await;
    assert!(archive_table.is_err());

    // queue must not be present on pgmq_meta
    let pgmq_meta_query = format!(
        "SELECT count(*) as ct
        FROM {PGMQ_SCHEMA}.meta
        WHERE queue_name = '{test_queue}'",
    );
    let rowcount = sqlx::query(&pgmq_meta_query)
        .fetch_one(&queue.connection)
        .await
        .unwrap()
        .get::<i64, usize>(0);
    assert_eq!(rowcount, 0);
}

#[tokio::test]
async fn test_set_vt() {
    let test_queue = "test_set_vt_queue".to_owned();
    let queue = init_queue(&test_queue).await;
    let msg = MyMessage::default();

    let num_rows_queue = rowcount(&test_queue, &queue.connection).await;
    assert_eq!(num_rows_queue, 0);

    let msg_id = queue.send(&test_queue, &msg).await.unwrap();
    // read the message
    let read: Message = queue.read(&test_queue, Some(0_i32)).await.unwrap().unwrap();
    assert_eq!(read.msg_id, msg_id);

    // read again
    let read: Message = queue.read(&test_queue, Some(0_i32)).await.unwrap().unwrap();
    assert_eq!(read.msg_id, msg_id);
    assert_eq!(read.msg_id, msg_id);

    // set the vt
    let utc_24h_from_now = Utc::now() + Duration::hours(24);
    let _ = queue
        .set_vt::<MyMessage>(&test_queue, msg_id, utc_24h_from_now)
        .await
        .unwrap();

    // try read again
    let read: Option<Message> = queue.read(&test_queue, Some(0_i32)).await.unwrap();
    // we set the vt to tomorrow, should not be read
    assert!(read.is_none());

    // set the vt to now
    let now = Utc::now();
    let _ = queue
        .set_vt::<MyMessage>(&test_queue, msg_id, now)
        .await
        .unwrap();

    let num_rows_queue = rowcount(&test_queue, &queue.connection).await;
    assert_eq!(num_rows_queue, 1);
}
