use pgmq;
use sqlx::Row;
use std::env;

#[tokio::test]
async fn test_lifecycle() {
    let pgpass = env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_owned());
    let queue = pgmq::PGMQueue::new(format!("postgres://postgres:{}@0.0.0.0:5432", pgpass)).await;
    let test_queue = "test_queue_0".to_owned();

    // drop the test table at beginning of integration test
    sqlx::query(format!("DROP TABLE IF EXISTS pgmq_{}", test_queue).as_str())
        .execute(&queue.connection)
        .await
        .unwrap();

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
