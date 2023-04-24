use pgmq_crate::{conn_options, fetch_one_message};
use rand::Rng;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Row};

async fn connect(url: &str) -> Pool<Postgres> {
    let options = conn_options(url).expect("failed to parse url");
    PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(10))
        .max_connections(5)
        .connect_with(options)
        .await
        .unwrap()
}

#[tokio::test]
async fn test_lifecycle() {
    let username = whoami::username();
    let conn = connect(&format!("postgres://{username}:postgres@localhost:28815/pgmq")).await;
    let mut rng = rand::thread_rng();
    let test_num = rng.gen_range(0..100000);

    // DROP EXTENSION
    // requires pg_partman to already be installed in the instance
    let _ = sqlx::query("DROP EXTENSION IF EXISTS pgmq")
        .execute(&conn)
        .await
        .expect("failed to drop extension");

    // CREATE EXTENSION
    let _ = sqlx::query("CREATE EXTENSION pgmq cascade")
        .execute(&conn)
        .await
        .expect("failed to create extension");

    // CREATE with default retention and partition strategy
    let _ = sqlx::query(&format!("SELECT pgmq_create('test_default_{test_num}');"))
        .execute(&conn)
        .await
        .expect("failed to create queue");

    let msg_id = sqlx::query(&format!(
        "SELECT * from pgmq_send('test_default_{test_num}', '{{\"hello\": \"world\"}}');"
    ))
    .fetch_one(&conn)
    .await
    .expect("failed send")
    .get::<i64, usize>(0);
    assert_eq!(msg_id, 1);

    // read message
    let query = &format!("SELECT * from pgmq_read('test_default_{test_num}', 2, 1);");

    let message = fetch_one_message::<serde_json::Value>(query, &conn)
        .await
        .expect("failed reading message")
        .expect("expected message");
    assert_eq!(message.msg_id, 1);

    // CREATE with 5 seconds per partition, 10 seconds retention
    let q = format!("SELECT \"pgmq_create\"('test_duration_{test_num}'::text, '5 seconds'::text, '10 seconds'::text);");
    println!("q: {q}");
    let _ = sqlx::query(&q)
        .execute(&conn)
        .await
        .expect("failed creating duration queue");

    // CREATE with 10 messages per partition, 20 messages retention
    let _ = sqlx::query(&format!(
        "SELECT \"pgmq_create\"('test_numeric_{test_num}'::text, '10'::text, '20'::text);"
    ))
    .execute(&conn)
    .await
    .expect("failed creating numeric interval queue");
}
