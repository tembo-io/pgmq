use serde::{Deserialize, Serialize};
use sqlx::error::Error;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::types::chrono::Utc;
use sqlx::FromRow;
use sqlx::{Pool, Postgres, Row};

mod query;
use chrono::serde::ts_seconds::deserialize as from_ts;

const VT_DEFAULT: u32 = 30;

#[derive(Debug, Deserialize, FromRow)]
pub struct Message<T = serde_json::Value> {
    pub msg_id: i64,
    #[serde(deserialize_with = "from_ts")]
    pub vt: chrono::DateTime<Utc>,
    pub message: T,
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

    pub async fn enqueue<T: Serialize>(&self, queue_name: &str, message: &T) -> Result<i64, Error> {
        let msg = &serde_json::json!(&message);
        let row: PgRow = sqlx::query(&query::enqueue(queue_name, msg))
            .fetch_one(&self.connection)
            .await?;

        Ok(row.try_get("msg_id").unwrap())
    }

    pub async fn read<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: Option<&u32>,
    ) -> Option<Message<T>> {
        // map vt or default VT
        let vt_ = match vt {
            Some(t) => t,
            None => &VT_DEFAULT,
        };
        let query = &query::read(queue_name, vt_);
        let row: Result<PgRow, Error> = sqlx::query(query).fetch_one(&self.connection).await;

        match row {
            Ok(row) => {
                let b = row.get("message");
                let a = serde_json::from_value::<T>(b).unwrap();
                Some(Message {
                    msg_id: row.get("msg_id"),
                    vt: row.get("vt"),
                    message: a,
                })
            }
            Err(_) => None,
        }
    }

    pub async fn delete(&self, queue_name: &str, msg_id: &i64) -> Result<u64, Error> {
        let query = &&query::delete(queue_name, msg_id);
        let row = sqlx::query(query).execute(&self.connection).await?;
        let num_deleted = row.rows_affected();
        Ok(num_deleted)
    }

    // pub async fn pop(self) -> Message{
    //     // TODO: returns a struct
    // }
}
