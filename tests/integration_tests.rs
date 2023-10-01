use pgmq_core::types::{ARCHIVE_PREFIX, PGMQ_SCHEMA, QUEUE_PREFIX};
use pgmq_core::util::{conn_options, fetch_one_message};
use rand::Rng;
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, Pool, Postgres, Row};

#[allow(dead_code)]
#[derive(FromRow)]
struct MetricsRow {
    queue_name: String,
    queue_length: i64,
    newest_msg_age_sec: Option<i32>,
    oldest_msg_age_sec: Option<i32>,
    total_messages: i64,
    scrape_time: chrono::DateTime<chrono::Utc>,
}

#[allow(dead_code)]
#[derive(FromRow, Debug)]
struct QueueMeta {
    queue_name: String,
}

#[allow(dead_code)]
#[derive(FromRow)]
struct ResultSet {
    num_partmans: i64,
}

async fn connect(url: &str) -> Pool<Postgres> {
    let options = conn_options(url).expect("failed to parse url");
    println!("URL: {}", url);
    PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(10))
        .max_connections(5)
        .connect_with(options)
        .await
        .expect("failed to connect to pg")
}

async fn init_database() -> Pool<Postgres> {
    let username = whoami::username();

    let conn00 = connect(&format!(
        "postgres://{username}:postgres@localhost:28815/pgmq"
    ))
    .await;
    // ignore the error if the db already exists!
    let _ = sqlx::query("CREATE DATABASE pgmq_test;")
        .execute(&conn00)
        .await;
    conn00.close().await;

    let conn = connect(&database_name()).await;

    // DROP EXTENSION
    // requires pg_partman to already be installed in the instance
    let _ = sqlx::query("DROP EXTENSION IF EXISTS pgmq CASCADE")
        .execute(&conn)
        .await
        .expect("failed to drop extension");

    // CREATE EXTENSION
    let _ = sqlx::query("CREATE EXTENSION pgmq")
        .execute(&conn)
        .await
        .expect("failed to create extension");

    // CREATE EXTENSION
    let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_partman")
        .execute(&conn)
        .await
        .expect("failed to create extension");

    conn
}

// Integration tests are ignored by default
#[ignore]
#[tokio::test]
async fn test_unlogged() {
    let conn = init_database().await;
    let mut rng = rand::thread_rng();
    let test_num = rng.gen_range(0..100000);

    // CREATE with default retention and partition strategy
    let test_queue = format!("test_unlogged_{test_num}");
    let _ = sqlx::query(&format!(
        "SELECT {PGMQ_SCHEMA}.create_unlogged('{test_queue}');"
    ))
    .execute(&conn)
    .await
    .expect("failed to create queue");

    let msg_id = sqlx::query(&format!(
        "SELECT * from {PGMQ_SCHEMA}.send('{test_queue}', '{{\"hello\": \"world\"}}');"
    ))
    .fetch_one(&conn)
    .await
    .expect("failed send")
    .get::<i64, usize>(0);
    assert_eq!(msg_id, 1);

    let query = &format!("SELECT * from {PGMQ_SCHEMA}.read('{test_queue}', 2, 1);");

    let message = fetch_one_message::<serde_json::Value>(query, &conn)
        .await
        .expect("failed reading message")
        .expect("expected message");
    assert_eq!(message.msg_id, msg_id);
}

// Integration tests are ignored by default
#[ignore]
#[tokio::test]
async fn test_lifecycle() {
    let conn = init_database().await;
    let mut rng = rand::thread_rng();
    let test_num = rng.gen_range(0..100000);

    // CREATE with default retention and partition strategy
    let test_default_queue = format!("test_default_{test_num}");
    let _ = sqlx::query(&format!(
        "SELECT {PGMQ_SCHEMA}.create('{test_default_queue}');"
    ))
    .execute(&conn)
    .await
    .expect("failed to create queue");

    // creating a queue must be idempotent
    // create with same name again, must be no  error
    let _ = sqlx::query(&format!(
        "SELECT {PGMQ_SCHEMA}.create('{test_default_queue}');"
    ))
    .execute(&conn)
    .await
    .expect("failed to create queue");

    let msg_id = sqlx::query(&format!(
        "SELECT * from {PGMQ_SCHEMA}.send('{test_default_queue}', '{{\"hello\": \"world\"}}');"
    ))
    .fetch_one(&conn)
    .await
    .expect("failed send")
    .get::<i64, usize>(0);
    assert_eq!(msg_id, 1);

    // read message
    // vt=2, limit=1
    let query = &format!("SELECT * from {PGMQ_SCHEMA}.read('{test_default_queue}', 2, 1);");

    let message = fetch_one_message::<serde_json::Value>(query, &conn)
        .await
        .expect("failed reading message")
        .expect("expected message");
    assert_eq!(message.msg_id, 1);

    // set VT to in 10 seconds
    let query =
        &format!("SELECT * from {PGMQ_SCHEMA}.set_vt('{test_default_queue}', {msg_id}, 5);");
    let message = fetch_one_message::<serde_json::Value>(query, &conn)
        .await
        .expect("failed reading message")
        .expect("expected message");
    assert_eq!(message.msg_id, 1);
    let now = chrono::offset::Utc::now();
    // closish to 10 seconds from now
    assert!(message.vt > now + chrono::Duration::seconds(4));

    // read again, assert no messages because we just set VT to the future
    let query = &format!("SELECT * from {PGMQ_SCHEMA}.read('{test_default_queue}', 2, 1);");
    let message = fetch_one_message::<serde_json::Value>(query, &conn)
        .await
        .expect("failed reading message");
    assert!(message.is_none());

    // read again, now using poll to block until message is ready
    let query =
        &format!("SELECT * from {PGMQ_SCHEMA}.read_with_poll('{test_default_queue}', 10, 1, 10);");
    let message = fetch_one_message::<serde_json::Value>(query, &conn)
        .await
        .expect("failed reading message")
        .expect("expected message");
    assert_eq!(message.msg_id, 1);

    // after reading it, set VT to now
    let query =
        &format!("SELECT * from {PGMQ_SCHEMA}.set_vt('{test_default_queue}', {msg_id}, 0);");
    let message = fetch_one_message::<serde_json::Value>(query, &conn)
        .await
        .expect("failed reading message")
        .expect("expected message");
    assert_eq!(message.msg_id, 1);

    // read again, should have msg_id 1 again
    let query = &format!("SELECT * from {PGMQ_SCHEMA}.read('{test_default_queue}', 2, 1);");
    let message = fetch_one_message::<serde_json::Value>(query, &conn)
        .await
        .expect("failed reading message")
        .expect("expected message");
    assert_eq!(message.msg_id, 1);

    // send a batch of 2 messages
    let batch_queue = format!("test_batch_{test_num}");
    create_queue(&batch_queue.to_string(), &conn).await;

    let msg_ids = sqlx::query(
        &format!("select {PGMQ_SCHEMA}.send_batch('{batch_queue}', ARRAY['{{\"hello\": \"world_0\"}}'::jsonb, '{{\"hello\": \"world_1\"}}'::jsonb])")
    )
    .fetch_all(&conn).await.expect("failed to send batch");
    assert_eq!(msg_ids.len(), 2);
    assert_eq!(msg_ids[0].get::<i64, usize>(0), 1);
    assert_eq!(msg_ids[1].get::<i64, usize>(0), 2);
    assert_eq!(get_queue_size(&batch_queue, &conn).await, 2);

    let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_partman")
        .execute(&conn)
        .await
        .expect("failed to create extension");

    // CREATE with 5 seconds per partition, 10 seconds retention
    let test_duration_queue = format!("test_duration_{test_num}");
    let q = format!("SELECT {PGMQ_SCHEMA}.create_partitioned('{test_duration_queue}'::text, '5 seconds'::text, '10 seconds'::text);");
    let _ = sqlx::query(&q)
        .execute(&conn)
        .await
        .expect("failed creating duration queue");

    // CREATE with 10 messages per partition, 20 messages retention
    let test_numeric_queue = format!("test_numeric_{test_num}");
    let _ = sqlx::query(&format!(
        "SELECT {PGMQ_SCHEMA}.create_partitioned('{test_numeric_queue}'::text, '10'::text, '20'::text);"
    ))
    .execute(&conn)
    .await
    .expect("failed creating numeric interval queue");

    // get metrics
    let rows = sqlx::query_as::<_, MetricsRow>(&format!(
        "SELECT * from {PGMQ_SCHEMA}.metrics('{test_duration_queue}'::text);"
    ))
    .fetch_all(&conn)
    .await
    .expect("failed creating numeric interval queue");
    assert_eq!(rows.len(), 1);

    // get metrics all
    let rows =
        sqlx::query_as::<_, MetricsRow>(&format!("SELECT * from {PGMQ_SCHEMA}.metrics_all();"))
            .fetch_all(&conn)
            .await
            .expect("failed creating numeric interval queue");
    assert!(rows.len() > 1);

    // delete all the queues
    // delete partitioned queues
    for queue in [test_duration_queue, test_numeric_queue].iter() {
        sqlx::query(&format!(
            "select {PGMQ_SCHEMA}.drop_queue('{}', true);",
            &queue
        ))
        .execute(&conn)
        .await
        .expect("failed to drop partitioned queues");
    }

    let queues = sqlx::query_as::<_, QueueMeta>(&format!(
        "select queue_name from {PGMQ_SCHEMA}.list_queues();"
    ))
    .fetch_all(&conn)
    .await
    .expect("failed to list queues");

    // drop the rest of the queues
    for queue in queues {
        let q = queue.queue_name;
        sqlx::query(&format!("select {PGMQ_SCHEMA}.drop_queue('{}');", &q))
            .execute(&conn)
            .await
            .expect("failed to drop standard queues");
    }

    let queues = sqlx::query_as::<_, QueueMeta>(&format!(
        "select queue_name from {PGMQ_SCHEMA}.list_queues();"
    ))
    .fetch_all(&conn)
    .await
    .expect("failed to list queues");
    assert!(queues.is_empty());
}

// Integration tests are ignored by default
#[ignore]
#[tokio::test]
async fn test_archive() {
    let conn = init_database().await;
    let mut rng = rand::thread_rng();
    let test_num = rng.gen_range(0..100000);

    let queue_name = format!("test_archive_{test_num}");

    create_queue(&queue_name.to_string(), &conn).await;

    // no messages in the queue
    assert_eq!(get_queue_size(&queue_name, &conn).await, 0);

    // no messages in queue archive
    assert_eq!(get_archive_size(&queue_name, &conn).await, 0);

    // put messages on the queue
    let msg_id1 = send_sample_message(&queue_name, &conn).await;
    let msg_id2 = send_sample_message(&queue_name, &conn).await;

    // two messages in the queue
    assert_eq!(get_queue_size(&queue_name, &conn).await, 2);

    // archive the message. The first two exist so the id should be returned, the
    // last one doesn't
    let archived = sqlx::query(&format!(
        "SELECT * FROM {PGMQ_SCHEMA}.archive('{queue_name}', ARRAY[{msg_id1}, {msg_id2}])"
    ))
    .fetch_all(&conn)
    .await
    .expect("failed to archive");

    assert_eq!(archived.len(), 2);
    assert_eq!(archived[0].get::<i64, usize>(0), 1);
    assert_eq!(archived[1].get::<i64, usize>(0), 2);

    // should be no messages left on the queue table
    assert_eq!(get_queue_size(&queue_name, &conn).await, 0);

    // should be two messages in archive
    assert_eq!(get_archive_size(&queue_name, &conn).await, 2);

    let msg_id3 = send_sample_message(&queue_name, &conn).await;

    assert_eq!(get_queue_size(&queue_name, &conn).await, 1);

    let archived_single = sqlx::query(&format!(
        "SELECT * FROM {PGMQ_SCHEMA}.archive('{queue_name}', {msg_id3})"
    ))
    .fetch_one(&conn)
    .await
    .expect("failed to archive")
    .get::<bool, usize>(0);

    assert_eq!(archived_single, true);

    assert_eq!(get_queue_size(&queue_name, &conn).await, 0);
    assert_eq!(get_archive_size(&queue_name, &conn).await, 3);
}

// Integration tests are ignored by default
#[ignore]
#[tokio::test]
async fn test_read_read_with_poll() {
    let conn = init_database().await;
    let mut rng = rand::thread_rng();
    let test_num = rng.gen_range(0..100000);

    let queue_name = format!("test_read_{test_num}");

    // Creating queue
    create_queue(&queue_name.to_string(), &conn).await;

    // Sending 3 messages to the queue
    let msg_id1 = send_sample_message(&queue_name, &conn).await;
    let msg_id2 = send_sample_message(&queue_name, &conn).await;
    let msg_id3 = send_sample_message(&queue_name, &conn).await;

    // Reading with limit respects the limit
    let read_batch = sqlx::query(&format!(
        "SELECT * FROM {PGMQ_SCHEMA}.read('{queue_name}', 5, 1)"
    ))
    .fetch_all(&conn)
    .await
    .expect("failed to read");

    assert_eq!(read_batch.len(), 1);
    assert_eq!(read_batch[0].get::<i64, usize>(0), msg_id1);

    // Reading respects the VT
    let read_batch2 = sqlx::query(&format!(
        "SELECT * FROM {PGMQ_SCHEMA}.read('{queue_name}', 10, 5)"
    ))
    .fetch_all(&conn)
    .await
    .expect("failed to read");

    assert_eq!(read_batch2.len(), 2);
    assert_eq!(read_batch2[0].get::<i64, usize>(0), msg_id2);
    assert_eq!(read_batch2[1].get::<i64, usize>(0), msg_id3);

    // Read with poll will poll until the first message is available
    let start = std::time::Instant::now();

    let read_batch3 = sqlx::query(&format!(
        "SELECT * FROM {PGMQ_SCHEMA}.read_with_poll('{queue_name}', 10, 5, 5, 100)"
    ))
    .fetch_all(&conn)
    .await
    .expect("failed to read");

    // Asserting it took more than 3 seconds:
    assert!(start.elapsed().as_secs() > 3);
    assert_eq!(read_batch3.len(), 1);
    assert_eq!(read_batch3[0].get::<i64, usize>(0), msg_id1);
}

// Integration tests are ignored by default
#[ignore]
#[tokio::test]
async fn test_partitioned_delete() {
    let conn = init_database().await;
    let mut rng = rand::thread_rng();
    let test_num = rng.gen_range(0..100000);

    let queue_name = format!("test_partitioned_{test_num}");
    let partition_interval = 2;
    let retention_interval = 2;

    // We first will drop pg_partman and assert that create fails without the
    // extension installed
    let _ = sqlx::query("DROP EXTENSION IF EXISTS pg_partman")
        .execute(&conn)
        .await
        .unwrap();

    let create_result = sqlx::query(&format!(
        "select *
                     from {PGMQ_SCHEMA}.create_partitioned(
                         '{queue_name}',
                         '{partition_interval}',
                         '{retention_interval}'
                     )"
    ))
    .execute(&conn)
    .await;

    assert!(create_result.is_err());

    // With the extension existing, the queue is created successfully
    let _ = sqlx::query("CREATE EXTENSION pg_partman")
        .execute(&conn)
        .await
        .unwrap();

    let create_result = sqlx::query(&format!(
        "select *
                     from {PGMQ_SCHEMA}.create_partitioned(
                         '{queue_name}',
                         '{partition_interval}',
                         '{retention_interval}'
                     )"
    ))
    .execute(&conn)
    .await;

    assert!(create_result.is_ok());

    // queue shows up in list queues
    let list_queues_result = sqlx::query_as::<_, QueueMeta>(&format!(
        "select queue_name from {PGMQ_SCHEMA}.list_queues();"
    ))
    .fetch_all(&conn)
    .await;

    match list_queues_result {
        Ok(qm) => {
            let q = qm.iter().find(|m| m.queue_name == queue_name);
            assert!(q.is_some());
        }
        Err(err) => panic!("{}", err),
    }

    // Sending 3 messages to the queue
    let msg_id1 = send_sample_message(&queue_name, &conn).await;
    let msg_id2 = send_sample_message(&queue_name, &conn).await;
    let msg_id3 = send_sample_message(&queue_name, &conn).await;

    assert_eq!(get_queue_size(&queue_name, &conn).await, 3);

    // Deleting message 3
    let deleted_single = sqlx::query(&format!(
        "SELECT * FROM {PGMQ_SCHEMA}.delete('{queue_name}', {msg_id3})"
    ))
    .fetch_one(&conn)
    .await
    .expect("failed to delete")
    .get::<bool, usize>(0);

    assert_eq!(deleted_single, true);
    assert_eq!(get_queue_size(&queue_name, &conn).await, 2);

    // Deleting batch
    let deleted_batch = sqlx::query(&format!(
        "SELECT * FROM {PGMQ_SCHEMA}.archive('{queue_name}', ARRAY[{msg_id1}, {msg_id2}, {msg_id3}, -3])"
    ))
    .fetch_all(&conn)
    .await
    .expect("failed to archive");

    assert_eq!(deleted_batch.len(), 2);
    assert_eq!(deleted_batch[0].get::<i64, usize>(0), 1);
    assert_eq!(deleted_batch[1].get::<i64, usize>(0), 2);
}
// Integration tests are ignored by default
#[ignore]
#[tokio::test]
async fn test_transaction_create() {
    // Queue creation is reverted if transaction is rolled back
    let _ = init_database().await;
    let queue_name = "transaction_test_queue";
    let conn1 = connect(&database_name()).await;
    let mut tx1 = conn1.begin().await.unwrap();

    sqlx::query(&format!("select from {PGMQ_SCHEMA}.create('{queue_name}')"))
        .fetch_one(&mut *tx1)
        .await
        .unwrap();

    tx1.rollback().await.unwrap();

    let table_exists = sqlx::query(&format!(
        "select from pg_tables where schemaname = 'pgmq' and tablename = 'q_{queue_name}'"
    ))
    .fetch_optional(&conn1)
    .await
    .unwrap();

    assert!(table_exists.is_none());
}

// Integration tests are ignored by default
#[ignore]
#[tokio::test]
async fn test_transaction_send() {
    // This aims to test that a message won't be visible for other transactions
    // until the transaction that published it commits
    let _ = init_database().await;
    let queue_name = "transaction_send_test_queue";
    let conn1 = connect(&database_name()).await;
    let conn2 = connect(&database_name()).await;

    create_queue(&queue_name.to_string(), &conn1).await;

    // Message can't be read for pending transaction
    let mut tx = conn1.begin().await.unwrap();

    sqlx::query(&format!(
        "select from {PGMQ_SCHEMA}.send('{queue_name}', '1')"
    ))
    .fetch_one(&mut *tx)
    .await
    .unwrap();

    let read_msg = sqlx::query(&format!(
        "select from {PGMQ_SCHEMA}.read('{queue_name}', 0, 1)"
    ))
    .fetch_optional(&conn2)
    .await
    .unwrap();

    assert!(read_msg.is_none());

    // After commiting the transaction, the message can be read
    tx.commit().await.unwrap();

    let read_msg2 = sqlx::query(&format!(
        "select from {PGMQ_SCHEMA}.read('{queue_name}', 0, 1)"
    ))
    .fetch_optional(&conn2)
    .await
    .unwrap();

    assert!(read_msg2.is_some());
}

// Integration tests are ignored by default
#[ignore]
#[tokio::test]
async fn test_transaction_read() {
    // A message read by one transaction can't be read by other concurrent transaction,
    // even if VT expired, until the other transaction is committed or rolled back.
    let _ = init_database().await;
    let queue_name = "transaction_read_test_queue";
    let conn1 = connect(&database_name()).await;
    let conn2 = connect(&database_name()).await;

    create_queue(&queue_name.to_string(), &conn1).await;

    sqlx::query(&format!(
        "select from {PGMQ_SCHEMA}.send('{queue_name}', '1')"
    ))
    .fetch_one(&conn1)
    .await
    .unwrap();

    let mut tx1 = conn1.begin().await.unwrap();
    let mut tx2 = conn2.begin().await.unwrap();

    let read_msg1 = sqlx::query(&format!(
        "select from {PGMQ_SCHEMA}.read('{queue_name}', 1, 1)"
    ))
    .fetch_optional(&mut *tx1)
    .await
    .unwrap();

    assert!(read_msg1.is_some());

    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

    let read_msg2 = sqlx::query(&format!(
        "select from {PGMQ_SCHEMA}.read('{queue_name}', 1, 1)"
    ))
    .fetch_optional(&mut *tx2)
    .await
    .unwrap();

    assert!(read_msg2.is_none());

    tx1.rollback().await.unwrap();

    let read_msg3 = sqlx::query(&format!(
        "select from {PGMQ_SCHEMA}.read('{queue_name}', 1, 1)"
    ))
    .fetch_optional(&mut *tx2)
    .await
    .unwrap();

    assert!(read_msg3.is_some());
}

async fn create_queue(queue_name: &String, conn: &Pool<Postgres>) {
    sqlx::query(&format!("select from {PGMQ_SCHEMA}.create('{queue_name}')"))
        .fetch_one(conn)
        .await
        .unwrap();
}

async fn get_queue_size(queue_name: &String, conn: &Pool<Postgres>) -> i64 {
    sqlx::query(&format!(
        "SELECT count(*) FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue_name}"
    ))
    .fetch_one(conn)
    .await
    .expect("failed get queue size")
    .get::<i64, usize>(0)
}

async fn get_archive_size(queue_name: &String, conn: &Pool<Postgres>) -> i64 {
    sqlx::query(&format!(
        "SELECT count(*) FROM {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{queue_name}"
    ))
    .fetch_one(conn)
    .await
    .expect("failed get queue size")
    .get::<i64, usize>(0)
}

async fn send_sample_message(queue_name: &String, conn: &Pool<Postgres>) -> i64 {
    sqlx::query(&format!(
        "SELECT * FROM {PGMQ_SCHEMA}.send('{queue_name}', '0')"
    ))
    .fetch_one(conn)
    .await
    .expect("failed to push message")
    .get::<i64, usize>(0)
}

pub fn database_name() -> String {
    let username = whoami::username();
    format!("postgres://{username}:postgres@localhost:28815/pgmq_test")
}
