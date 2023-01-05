use chrono;
use sqlx::types::chrono::Utc;

use sqlx::postgres::PgPoolOptions;
use sqlx::Pool;
use sqlx::Postgres;
use sqlx::Row;

use sqlx::error::Error;
use sqlx::postgres::PgRow;

mod query;

#[derive(Debug)]
pub struct Message {
    pub msg_id: i64,
    pub vt: chrono::DateTime<Utc>,
    pub message: serde_json::Value,
}

pub struct PGMQueue {
    pub config: PGMQueueConfig,
    pub connection: Option<Pool<Postgres>>,
}

impl PGMQueue {
    pub async fn connect(&mut self) {
        let con = PgPoolOptions::new()
            .max_connections(5)
            .connect(&self.config.url)
            .await
            .unwrap();
        self.connection = Some(con);
    }

    pub async fn create(&self) -> Result<(), Error> {
        let create = query::create(&self.config.queue_name);
        let index: String = query::create_index(&self.config.queue_name);
        sqlx::query(&create)
            .execute(self.connection.as_ref().unwrap())
            .await?;
        sqlx::query(&index)
            .execute(self.connection.as_ref().unwrap())
            .await?;
        Ok(())
    }

    pub async fn enqueue(&self, message: &serde_json::Value) -> Result<i64, Error> {
        // TODO: sends any struct that can be serialized to json
        // struct will need to derive serialize
        let row: PgRow = sqlx::query(&query::enqueue(&self.config.queue_name, &message))
            .fetch_one(self.connection.as_ref().unwrap())
            .await?;

        Ok(row.try_get("msg_id").unwrap())
    }

    pub async fn read(&self) -> Option<Message> {
        let query = &query::read(&self.config.queue_name, &self.config.vt);
        let row = sqlx::query(query)
            .fetch_one(self.connection.as_ref().unwrap())
            .await;

        match row {
            Ok(row) => Some(Message {
                msg_id: row.get("msg_id"),
                vt: row.get("vt"),
                message: row.try_get("message").unwrap(),
            }),
            Err(_) => None,
        }
    }

    pub async fn delete(&self, msg_id: &i64) -> Result<u64, Error> {
        let query = &&query::delete(&self.config.queue_name, &msg_id);
        let row = sqlx::query(query)
            .execute(self.connection.as_ref().unwrap())
            .await?;
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

impl PGMQueueConfig {
    pub fn new() -> PGMQueueConfig {
        PGMQueueConfig {
            url: "postgres://postgres:postgres@0.0.0.0:5432".to_owned(),
            queue_name: "default".to_owned(),
            vt: 30,
            delay: 0,
        }
    }

    pub async fn init(self) -> PGMQueue {
        let mut q = PGMQueue {
            config: self,
            connection: None,
        };
        q.connect().await;
        q
    }
}
