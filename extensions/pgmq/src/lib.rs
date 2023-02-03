use pgx::prelude::*;
use pgx::spi::SpiTupleTable;
use pgx::warning;

pgx::pg_module_magic!();

use pgmq::query::{create, delete, enqueue_str, pop, read};

#[pg_extern]
fn pgmq_create(queue_name: &str) -> Result<(), pgx::spi::Error> {
    Spi::run(&create(queue_name))
}

// puts messages onto the queue
#[pg_extern]
fn pgmq_enqueue(queue_name: &str, message: pgx::Json) -> Result<Option<i64>, spi::Error> {
    let m = serde_json::to_string(&message.0).unwrap();
    Spi::get_one(&enqueue_str(queue_name, &m))
}

// check message out of the queue using default timeout
#[pg_extern]
fn pgmq_read(queue_name: &str, vt: i32) -> Result<Option<pgx::Json>, spi::Error> {
    Spi::connect(|mut client| {
        let tup_table: SpiTupleTable = client.update(&read(queue_name, &vt), None, None)?.first();
        let row = tup_table.get_heap_tuple()?;
        match row {
            Some(row) => {
                let msg_id = row["msg_id"].value::<i64>()?.expect("no msg_id");
                let vt = row["vt"]
                    .value::<pgx::TimestampWithTimeZone>()?
                    .expect("no vt");
                let message = row["message"].value::<pgx::Json>()?.expect("no message");

                Ok(Some(pgx::Json(serde_json::json!({
                    "msg_id": msg_id,
                    "vt": vt,
                    "message": message
                }))))
            }
            None => Ok(None),
        }
    })
}

#[pg_extern(volatile)]
fn pgmq_delete(queue_name: &str, msg_id: i64) -> Result<Option<bool>, spi::Error> {
    let mut num_deleted = 0;

    Spi::connect(|mut client| {
        let tup_table = client.update(&delete(queue_name, &msg_id), None, None);
        match tup_table {
            Ok(tup_table) => num_deleted = tup_table.len(),
            Err(e) => {
                error!("error deleting message: {}", e);
            }
        }
    });
    match num_deleted {
        1 => Ok(Some(true)),
        0 => {
            warning!("no message found with msg_id: {}", msg_id);
            Ok(Some(false))
        }
        _ => {
            error!("multiple messages found with msg_id: {}", msg_id);
        }
    }
}

// reads and deletes at same time
#[pg_extern]
fn pgmq_pop(queue_name: &str) -> Result<Option<pgx::Json>, spi::Error> {
    let (msg_id, vt, message) =
        Spi::get_three::<i64, pgx::TimestampWithTimeZone, pgx::Json>(&pop(queue_name))?;
    match msg_id {
        Some(msg_id) => Ok(Some(pgx::Json(serde_json::json!({
            "msg_id": msg_id,
            "vt": vt,
            "message": message
        })))),
        None => Ok(None),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::*;
    use pgmq::query::TABLE_PREFIX;
    use pgx::prelude::*;
    #[pg_test]
    fn test_create() {
        let qname = r#"test_queue"#;
        let _ = pgmq_create(&qname).unwrap();
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 0);
        let _ = pgmq_enqueue(&qname, pgx::Json(serde_json::json!({"x":"y"}))).unwrap();
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 1);
    }

    // assert an invisible message is not readable
    #[pg_test]
    fn test_default() {
        let qname = r#"test_queue"#;
        let _ = pgmq_create(&qname);
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        // should not be any messages initially
        assert_eq!(init_count.unwrap(), 0);

        // put a message on the queue
        let _ = pgmq_enqueue(&qname, pgx::Json(serde_json::json!({"x":"y"})));
        // read the message off queue
        let msg = pgmq_read(&qname, 10_i32).unwrap();
        assert!(msg.is_some());
        // should be no messages left
        let nomsgs = pgmq_read(&qname, 10_i32).unwrap();
        assert!(nomsgs.is_none());
        // but still one record on the table
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 1);
    }
}

#[cfg(test)]
pub mod pg_test {
    // pg_test module with both the setup and postgresql_conf_options functions are required
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
