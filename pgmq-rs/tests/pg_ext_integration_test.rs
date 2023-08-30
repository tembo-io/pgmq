use pgmq_core::{types::TABLE_PREFIX, util::connect};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres, Row};
use std::env;

async fn init_queue_ext(qname: &str) -> pgmq::PGMQueueExt {
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/pgmq".to_owned());
    let queue = pgmq::PGMQueueExt::new(db_url, 2)
        .await
        .expect("failed to connect to postgres");
    queue.init().await.expect("failed to init pgmq");
    // make sure queue doesn't exist before the test
    let _ = queue.drop_queue(qname).await;
    // CREATE QUEUE
    let q_success = queue.create(qname).await;
    println!("q_success: {:?}", q_success);
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
    let row_ct_query = format!("SELECT count(*) as ct FROM {TABLE_PREFIX}_{qname}");
    sqlx::query(&row_ct_query)
        .fetch_one(connection)
        .await
        .unwrap()
        .get::<i64, usize>(0)
}

#[tokio::test]
async fn test_ext_create_list_drop() {
    let test_queue = format!(
        "test_ext_create_list_drop_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;

    let q_names = queue
        .list_queues()
        .await
        .expect("error listing queues")
        .expect("test queue was not created")
        .iter()
        .map(|q| q.queue_name.clone())
        .collect::<Vec<String>>();

    assert!(q_names.contains(&test_queue));

    let _ = queue
        .drop_queue(&test_queue)
        .await
        .expect("error dropping queue");

    let post_drop_q_names = queue
        .list_queues()
        .await
        .expect("error listing queues")
        .expect("test queue was not created")
        .iter()
        .map(|q| q.queue_name.clone())
        .collect::<Vec<String>>();

    assert!(!post_drop_q_names.contains(&test_queue));
}

#[tokio::test]
async fn test_ext_send_read_delete() {
    let test_queue = format!(
        "test_ext_send_read_delete_{}",
        rand::thread_rng().gen_range(0..100000)
    );

    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();
    let num_rows_queue = rowcount(&test_queue, &queue.connection).await;
    assert_eq!(num_rows_queue, 0);

    let msg_id = queue.send(&test_queue, &msg).await.unwrap();
    assert!(msg_id >= 1);

    let read_message = queue
        .read::<MyMessage>(&test_queue, 5)
        .await
        .expect("error reading message");
    assert!(read_message.is_some());
    let read_message = read_message.unwrap();
    assert_eq!(read_message.msg_id, msg_id);
    assert_eq!(read_message.message, msg);

    // read again, assert no messages visible
    let read_message = queue
        .read::<MyMessage>(&test_queue, 2)
        .await
        .expect("error reading message");
    assert!(read_message.is_none());

    // read with poll, blocks until message visible
    let start_poll = std::time::Instant::now();
    let read_with_poll = queue
        .read_batch_with_poll::<MyMessage>(
            &test_queue,
            5,
            1,
            Some(std::time::Duration::from_secs(6)),
            None,
        )
        .await
        .expect("error reading message")
        .expect("no message");

    let poll_duration = start_poll.elapsed();

    assert!(poll_duration.as_millis() > 1000);
    assert_eq!(read_with_poll.len(), 1);
    assert_eq!(read_with_poll[0].msg_id, msg_id);

    // change the VT to now
    let _vt_set = queue
        .set_vt::<MyMessage>(&test_queue, msg_id, 0)
        .await
        .expect("failed to set VT");
    let read_message = queue
        .read::<MyMessage>(&test_queue, 1)
        .await
        .expect("error reading message")
        .expect("expected a message");
    assert_eq!(read_message.msg_id, msg_id);

    // delete message
    let msg_id_del = queue.send(&test_queue, &msg).await.unwrap();

    let deleted = queue
        .delete(&test_queue, msg_id_del)
        .await
        .expect("failed to delete");
    assert!(deleted);

    // try to delete a message that doesn't exist
    let deleted = queue
        .delete(&test_queue, msg_id_del)
        .await
        .expect("failed to delete");
    assert!(!deleted);
}

#[tokio::test]
async fn test_ext_send_pop() {
    let test_queue = format!(
        "test_ext_send_pop_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();

    let _ = queue.send(&test_queue, &msg).await.unwrap();

    let popped = queue
        .pop::<MyMessage>(&test_queue)
        .await
        .expect("failed to pop")
        .expect("no message to pop");
    assert_eq!(popped.message, msg);
}

#[tokio::test]
async fn test_ext_send_archive() {
    let test_queue = format!(
        "test_ext_send_archive_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();

    let msg_id = queue.send(&test_queue, &msg).await.unwrap();

    let archived = queue
        .archive(&test_queue, msg_id)
        .await
        .expect("failed to archive");
    assert!(archived);
}

#[tokio::test]
async fn test_ext_archive_batch() {
    let test_queue = format!(
        "test_ext_archive_batch_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let test_queue_archive = format!("{}_archive", test_queue);
    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();

    let m1 = queue.send(&test_queue, &msg).await.unwrap();
    let m2 = queue.send(&test_queue, &msg).await.unwrap();
    let m3 = queue.send(&test_queue, &msg).await.unwrap();

    let archive_result = queue
        .archive_batch(&test_queue, &[m1, m2, m3])
        .await
        .expect("archive batch error");

    let post_archive_rowcount = rowcount(&test_queue, &queue.connection).await;

    assert_eq!(post_archive_rowcount, 0);
    assert_eq!(archive_result, true);

    let post_archive_archive_rowcount = rowcount(&test_queue_archive, &queue.connection).await;
    assert_eq!(post_archive_archive_rowcount, 3);
}

#[tokio::test]
async fn test_ext_delete_batch() {
    let test_queue = format!(
        "test_ext_delete_batch{}",
        rand::thread_rng().gen_range(0..100000)
    );

    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();
    let m1 = queue.send(&test_queue, &msg).await.unwrap();
    let m2 = queue.send(&test_queue, &msg).await.unwrap();
    let m3 = queue.send(&test_queue, &msg).await.unwrap();
    let delete_result = queue
        .delete_batch(&test_queue, &[m1, m2, m3])
        .await
        .expect("delete batch error");
    let post_delete_rowcount = rowcount(&test_queue, &queue.connection).await;
    assert_eq!(post_delete_rowcount, 0);
    assert_eq!(delete_result, true);
}

#[tokio::test]
async fn test_pgmq_init() {
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/pgmq".to_owned());
    let queue = pgmq::PGMQueueExt::new(db_url.clone(), 2)
        .await
        .expect("failed to connect to postgres");
    let init = queue.init().await.expect("failed to create extension");
    assert!(init, "failed to create extension");

    // error mode on queue partitioned create but already exists
    let qname = format!("test_dup_{}", rand::thread_rng().gen_range(0..100));
    println!("db_url: {}, qname: {:?}", db_url, qname);
    let created = queue
        .create_partitioned(&qname)
        .await
        .expect("failed attempting to create queue");
    assert!(created, "did not create queue");
    // create again
    let created = queue
        .create_partitioned(&qname)
        .await
        .expect("failed attempting to create the duplicate queue");
    assert!(!created, "failed to detect duplicate queue");
}

/// test "bring your own pool"
#[tokio::test]
async fn test_byop() {
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/pgmq".to_owned());

    let conn = connect(&db_url, 2).await.expect("failed to connect");

    let queue = pgmq::PGMQueueExt::new_with_pool(conn)
        .await
        .expect("failed to connect to postgres");

    let init = queue.init().await.expect("failed to create extension");
    assert!(init, "failed to create extension");

    let created = queue
        .create("test_byop")
        .await
        .expect("failed to create queue");
    assert!(created);
}
