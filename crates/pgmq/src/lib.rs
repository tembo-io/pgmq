use sqlx::types::chrono::Utc;

use sqlx::error::Error;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{Pool, Postgres, Row};

mod query;

const VT_DEFAULT: u32 = 30;

#[derive(Debug)]
pub struct Message {
    pub msg_id: i64,
    pub vt: chrono::DateTime<Utc>,
    pub message: serde_json::Value,
}

#[derive(Debug)]
pub struct PGMQueue {
    pub url: String,
    pub connection: Pool<Postgres>,
}

impl PGMQueue {
    pub async fn new(url: String) -> PGMQueue {
        let con = PGMQueue::connect(&url).await;
        PGMQueue {
            url,
            connection: con,
        }
    }

    async fn connect(url: &str) -> Pool<Postgres> {
        PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(10))
            .max_connections(5)
            .connect(url)
            .await
            .expect("connection failed")
    }

    pub async fn create(&self, queue_name: &str) -> Result<(), Error> {
        let create = query::create(queue_name);
        let index: String = query::create_index(queue_name);
        sqlx::query(&create).execute(&self.connection).await?;
        sqlx::query(&index).execute(&self.connection).await?;
        Ok(())
    }

    pub async fn enqueue(
        &self,
        queue_name: &str,
        message: &serde_json::Value,
    ) -> Result<i64, Error> {
        // TODO: sends any struct that can be serialized to json
        // struct will need to derive serialize
        let row: PgRow = sqlx::query(&query::enqueue(queue_name, message))
            .fetch_one(&self.connection)
            .await?;

        Ok(row.try_get("msg_id").unwrap())
    }

    pub async fn read(&self, queue_name: &str, vt: Option<&u32>) -> Option<Message> {
        // map vt or default VT
        let vt_ = match vt {
            Some(t) => t,
            None => &VT_DEFAULT,
        };
        let query = &query::read(queue_name, vt_);
        let row = sqlx::query(query).fetch_one(&self.connection).await;

        match row {
            Ok(row) => Some(Message {
                msg_id: row.get("msg_id"),
                vt: row.get("vt"),
                message: row.try_get("message").unwrap(),
            }),
            Err(_) => None,
        }
    }

    pub async fn delete(&self, queue_name: &str, msg_id: &i64) -> Result<u64, Error> {
        let query = &&query::delete(queue_name, msg_id);
        let row = sqlx::query(query).execute(&self.connection).await?;
        let num_deleted = row.rows_affected();
        println!("num_deleted: {}", num_deleted);
        Ok(num_deleted)
    }

    // pub async fn pop(self) -> Message{
    //     // TODO: returns a struct
    // }
}

pub struct PGMQueueConfig {
    pub url: String,
    pub queue_name: String,
    pub vt: u32,
    pub delay: u32,
}
