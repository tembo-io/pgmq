use pgmq;
use sqlx::Row;
use std::env;

async fn init_queue(qname: &str) -> pgmq::PGMQueue {
    let pgpass = env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_owned());
    let queue = pgmq::PGMQueue::new(format!("postgres://postgres:{}@0.0.0.0:5432", pgpass)).await;

    // drop the test table at beginning of test
    sqlx::query(format!("DROP TABLE IF EXISTS pgmq_{}", qname).as_str())
        .execute(&queue.connection)
        .await
        .unwrap();

    // CREATE QUEUE
    let q_success = queue.create(qname).await;
    assert!(q_success.is_ok());
    queue
}

#[tokio::test]
async fn test_lifecycle() {
    let test_queue = "test_queue_0".to_owned();

    let queue = init_queue(&test_queue).await;

    let row_ct_query = format!("SELECT count(*) as ct FROM pgmq_{}", test_queue);

    // CREATE QUEUE
    let q_success = queue.create(&test_queue).await;
    assert!(q_success.is_ok());
    let results = sqlx::query(&row_ct_query)
        .fetch_one(&queue.connection)
        .await
        .unwrap()
        .get::<i64, usize>(0);
    // no records on init
    assert_eq!(results, 0);

    // SEND MESSAGE
    let msg = serde_json::json!({
        "foo": "bar"
    });
    let msg_id = queue.enqueue(&test_queue, &msg).await.unwrap();
    assert_eq!(msg_id, 1);
    let results = sqlx::query(&row_ct_query)
        .fetch_one(&queue.connection)
        .await
        .unwrap()
        .get::<i64, usize>(0);
    // one record after one record sent
    assert_eq!(results, 1);

    // READ MESSAGE
    let vt = 2;
    let msg1 = queue.read(&test_queue, Some(&vt)).await.unwrap();
    assert_eq!(msg1.msg_id, 1);
    // no messages returned, and the one record on the table is invisible
    let no_messages = queue.read(&test_queue, Some(&vt)).await;
    assert!(no_messages.is_none());
    // still one invisible record on the table
    let results = sqlx::query(&row_ct_query)
        .fetch_one(&queue.connection)
        .await
        .unwrap()
        .get::<i64, usize>(0);
    // still one invisible record
    assert_eq!(results, 1);

    // WAIT FOR VISIBILITY TIMEOUT TO EXPIRE
    tokio::time::sleep(std::time::Duration::from_secs(vt as u64)).await;
    let msg2 = queue.read(&test_queue, Some(&vt)).await.unwrap();
    assert_eq!(msg2.msg_id, 1);

    // DELETE MESSAGE
    let deleted = queue.delete(&test_queue, &msg1.msg_id).await.unwrap();
    assert_eq!(deleted, 1);
    let msg3 = queue.read(&test_queue, Some(&vt)).await;
    assert!(msg3.is_none());
    let results = sqlx::query(&row_ct_query)
        .fetch_one(&queue.connection)
        .await
        .unwrap()
        .get::<i64, usize>(0);
    // table empty
    assert_eq!(results, 0);
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
    let read1 = queue.read(&test_queue, Some(&vt)).await.unwrap();
    let read2 = queue.read(&test_queue, Some(&vt)).await.unwrap();
    assert_eq!(read1.msg_id, 1);
    assert_eq!(read2.msg_id, 2);
    // WAIT FOR VISIBILITY TIMEOUT TO EXPIRE
    tokio::time::sleep(std::time::Duration::from_secs(vt as u64)).await;
    tokio::time::sleep(std::time::Duration::from_secs(vt as u64)).await;

    // READ ALL, must still be in order
    let read1 = queue.read(&test_queue, Some(&vt)).await.unwrap();
    let read2 = queue.read(&test_queue, Some(&vt)).await.unwrap();
    let read3 = queue.read(&test_queue, Some(&vt)).await.unwrap();
    assert_eq!(read1.msg_id, 1);
    assert_eq!(read2.msg_id, 2);
    assert_eq!(read3.msg_id, 3);
}
