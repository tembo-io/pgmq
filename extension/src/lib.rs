use pgrx::prelude::*;
use pgrx::spi;
use pgrx::spi::SpiTupleTable;
use pgrx::warning;

pgrx::pg_module_magic!();

pub mod api;
pub mod metrics;
pub mod partition;

use pgmq_crate::errors::PgmqError;
use pgmq_crate::query::{archive, check_input, delete, init_queue, pop, read, TABLE_PREFIX};
use thiserror::Error;

#[derive(Error, Debug)]
enum PgmqExtError {
    #[error("")]
    SqlError(#[from] pgrx::spi::Error),

    #[error("")]
    QueueError(#[from] PgmqError),

    #[error("{0} invalid types")]
    TypeErrorError(String),
}

#[pg_extern]
fn pgmq_create_non_partitioned(queue_name: &str) -> Result<(), PgmqExtError> {
    let setup = init_queue(queue_name)?;
    let ran: Result<_, spi::Error> = Spi::connect(|mut c| {
        for q in setup {
            let _ = c.update(&q, None, None)?;
        }
        Ok(())
    });
    Ok(ran?)
}

#[pg_extern]
fn pgmq_create(
    queue_name: &str,
    partition_interval: default!(String, "'10000'"),
    retention_interval: default!(String, "'100000'"),
) -> Result<(), PgmqExtError> {
    validate_same_type(&partition_interval, &retention_interval)?;
    let setup =
        partition::init_partitioned_queue(queue_name, &partition_interval, &retention_interval)?;
    let ran: Result<_, spi::Error> = Spi::connect(|mut c| {
        for q in setup {
            let _ = c.update(&q, None, None)?;
        }
        Ok(())
    });
    Ok(ran?)
}

fn validate_same_type(a: &str, b: &str) -> Result<(), PgmqExtError> {
    // either both can be ints, or not not ints
    match (a.parse::<i32>(), b.parse::<i32>()) {
        (Ok(_), Ok(_)) => Ok(()),
        (Err(_), Err(_)) => Ok(()),
        _ => Err(PgmqExtError::TypeErrorError("".to_owned())),
    }
}

#[pg_extern]
fn pgmq_send(queue_name: &str, message: pgrx::JsonB) -> Result<Option<i64>, PgmqExtError> {
    let query = enqueue_str(queue_name)?;
    Ok(Spi::get_one_with_args(
        &query,
        vec![(PgBuiltInOids::JSONBOID.oid(), message.into_datum())],
    )?)
}

fn enqueue_str(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        INSERT INTO {TABLE_PREFIX}_{name} (vt, message)
        VALUES (now() at time zone 'utc', $1)
        RETURNING msg_id;
        "
    ))
}

#[pg_extern]
fn pgmq_read(
    queue_name: &str,
    vt: i32,
    limit: i32,
) -> Result<
    TableIterator<
        'static,
        (
            name!(msg_id, i64),
            name!(read_ct, i32),
            name!(vt, TimestampWithTimeZone),
            name!(enqueued_at, TimestampWithTimeZone),
            name!(message, pgrx::JsonB),
        ),
    >,
    spi::Error,
> {
    let results = readit(queue_name, vt, limit)?;
    Ok(TableIterator::new(results.into_iter()))
}

fn readit(
    queue_name: &str,
    vt: i32,
    limit: i32,
) -> Result<
    Vec<(
        i64,
        i32,
        TimestampWithTimeZone,
        TimestampWithTimeZone,
        pgrx::JsonB,
    )>,
    spi::Error,
> {
    let mut results: Vec<(
        i64,
        i32,
        TimestampWithTimeZone,
        TimestampWithTimeZone,
        pgrx::JsonB,
    )> = Vec::new();
    let _: Result<(), PgmqExtError> = Spi::connect(|mut client| {
        let query = read(queue_name, &vt, &limit)?;
        let mut tup_table: SpiTupleTable = client.update(&query, None, None)?;
        while let Some(row) = tup_table.next() {
            let msg_id = row["msg_id"].value::<i64>()?.expect("no msg_id");
            let read_ct = row["read_ct"].value::<i32>()?.expect("no read_ct");
            let vt = row["vt"].value::<TimestampWithTimeZone>()?.expect("no vt");
            let enqueued_at = row["enqueued_at"]
                .value::<TimestampWithTimeZone>()?
                .expect("no enqueue time");
            let message = row["message"].value::<pgrx::JsonB>()?.expect("no message");
            results.push((msg_id, read_ct, vt, enqueued_at, message));
        }
        Ok(())
    });
    Ok(results)
}

#[pg_extern]
fn pgmq_delete(queue_name: &str, msg_id: i64) -> Result<Option<bool>, PgmqExtError> {
    let mut num_deleted = 0;
    let query = delete(queue_name, &msg_id)?;
    Spi::connect(|mut client| {
        let tup_table = client.update(&query, None, None);
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

/// archive a message forever instead of deleting it
#[pg_extern]
fn pgmq_archive(queue_name: &str, msg_id: i64) -> Result<Option<bool>, PgmqExtError> {
    let mut num_deleted = 0;
    let query = archive(queue_name, &msg_id)?;
    Spi::connect(|mut client| {
        let tup_table = client.update(&query, None, None);
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
fn pgmq_pop(
    queue_name: &str,
) -> Result<
    TableIterator<
        'static,
        (
            name!(msg_id, i64),
            name!(read_ct, i32),
            name!(vt, TimestampWithTimeZone),
            name!(enqueued_at, TimestampWithTimeZone),
            name!(message, pgrx::JsonB),
        ),
    >,
    PgmqExtError,
> {
    let results = popit(queue_name)?;
    Ok(TableIterator::new(results.into_iter()))
}

fn popit(
    queue_name: &str,
) -> Result<
    Vec<(
        i64,
        i32,
        TimestampWithTimeZone,
        TimestampWithTimeZone,
        pgrx::JsonB,
    )>,
    PgmqExtError,
> {
    let mut results: Vec<(
        i64,
        i32,
        TimestampWithTimeZone,
        TimestampWithTimeZone,
        pgrx::JsonB,
    )> = Vec::new();
    let _: Result<(), PgmqExtError> = Spi::connect(|mut client| {
        let query = pop(queue_name)?;
        let tup_table: SpiTupleTable = client.update(&query, None, None)?;
        for row in tup_table {
            let msg_id = row["msg_id"].value::<i64>()?.expect("no msg_id");
            let read_ct = row["read_ct"].value::<i32>()?.expect("no read_ct");
            let vt = row["vt"].value::<TimestampWithTimeZone>()?.expect("no vt");
            let enqueued_at = row["enqueued_at"]
                .value::<TimestampWithTimeZone>()?
                .expect("no enqueue time");
            let message = row["message"].value::<pgrx::JsonB>()?.expect("no message");
            results.push((msg_id, read_ct, vt, enqueued_at, message));
        }
        Ok(())
    });
    Ok(results)
}

/// change the visibility time on an existing message
/// vt_offset is a time relative to now that the message will be visible
/// accepts positive or negative integers
#[pg_extern]
fn pgmq_set_vt(
    queue_name: &str,
    msg_id: i64,
    vt_offset: i32,
) -> Result<
    TableIterator<
        'static,
        (
            name!(msg_id, i64),
            name!(read_ct, i32),
            name!(enqueued_at, TimestampWithTimeZone),
            name!(vt, TimestampWithTimeZone),
            name!(message, pgrx::JsonB),
        ),
    >,
    PgmqExtError,
> {
    check_input(queue_name)?;
    let mut results: Vec<(
        i64,
        i32,
        TimestampWithTimeZone,
        TimestampWithTimeZone,
        pgrx::JsonB,
    )> = Vec::new();

    let query = format!(
        "
        UPDATE {TABLE_PREFIX}_{queue_name}
        SET vt = (now() at time zone 'utc' + interval '{vt_offset} seconds')
        WHERE msg_id = $1
        RETURNING *;
        "
    );
    let args = vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())];
    let res: Result<(), spi::Error> = Spi::connect(|mut client| {
        let tup_table: SpiTupleTable = client.update(&query, None, Some(args))?;
        for row in tup_table {
            let msg_id = row["msg_id"].value::<i64>()?.expect("no msg_id");
            let read_ct = row["read_ct"].value::<i32>()?.expect("no read_ct");
            let vt = row["vt"].value::<TimestampWithTimeZone>()?.expect("no vt");
            let enqueued_at = row["enqueued_at"]
                .value::<TimestampWithTimeZone>()?
                .expect("no enqueue time");
            let message = row["message"].value::<pgrx::JsonB>()?.expect("no message");
            results.push((msg_id, read_ct, enqueued_at, vt, message));
        }
        Ok(())
    });
    res?;
    Ok(TableIterator::new(results.into_iter()))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::*;
    use pgmq_crate::query::TABLE_PREFIX;

    #[pg_test]
    fn test_creat_non_partitioned() {
        let qname = r#"test_queue"#;
        let _ = pgmq_create_non_partitioned(&qname).unwrap();
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 0);
        let _ = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":"y"}))).unwrap();
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 1);
    }

    // assert an invisible message is not readable
    #[pg_test]
    fn test_default() {
        let qname = r#"test_default"#;
        let _ = pgmq_create_non_partitioned(&qname);
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        // should not be any messages initially
        assert_eq!(init_count.unwrap(), 0);

        // put a message on the queue
        let _ = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":"y"})));

        // read the message with the pg_extern, sets message invisible
        let _ = pgmq_read(&qname, 10_i32, 1_i32);
        // but still one record on the table
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 1);

        // pop the message, must not panic
        let popped = pgmq_pop(&qname);
        assert!(popped.is_ok());
    }

    // validate all internal functions
    // e.g. readit, popit, listit
    #[pg_test]
    fn test_internal() {
        let qname = r#"test_internal"#;
        let _ = pgmq_create_non_partitioned(&qname).unwrap();

        let queues = api::listit().unwrap();
        assert_eq!(queues.len(), 1);

        // put two message on the queue
        let msg_id1 = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":1})))
            .unwrap()
            .unwrap();
        let msg_id2 = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":2})))
            .unwrap()
            .unwrap();
        assert_eq!(msg_id1, 1);
        assert_eq!(msg_id2, 2);

        // read first message
        let msg1 = readit(&qname, 1_i32, 1_i32).unwrap();
        // pop the second message
        let msg2 = popit(&qname).unwrap();
        assert_eq!(msg1.len(), 1);
        assert_eq!(msg2.len(), 1);
        assert_eq!(msg1[0].0, msg_id1);
        assert_eq!(msg2[0].0, msg_id2);

        // read again, should be no messages
        let nothing = readit(&qname, 2_i32, 1_i32).unwrap();
        assert_eq!(nothing.len(), 0);

        // but still one record on the table
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 1);

        //  delete the messages
        let delete1 = pgmq_delete(&qname, msg_id1).unwrap().unwrap();
        assert!(delete1);

        //  delete when message is gone returns False
        let delete1 = pgmq_delete(&qname, msg_id1).unwrap().unwrap();
        assert!(!delete1);

        // no records after delete
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 0);
    }

    /// lifecycle test for partitioned queues
    #[pg_test]
    fn test_partitioned() {
        let qname = r#"test_internal"#;

        let partition_interval = "2".to_owned();
        let retention_interval = "2".to_owned();

        let _ = pgmq_create(&qname, partition_interval, retention_interval).unwrap();

        let queues = api::listit().unwrap();
        assert_eq!(queues.len(), 1);

        // put two message on the queue
        let msg_id1 = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":1})))
            .unwrap()
            .unwrap();
        let msg_id2 = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":2})))
            .unwrap()
            .unwrap();
        assert_eq!(msg_id1, 1);
        assert_eq!(msg_id2, 2);

        // read first message
        let msg1 = readit(&qname, 1_i32, 1_i32).unwrap();
        // pop the second message
        let msg2 = popit(&qname).unwrap();
        assert_eq!(msg1.len(), 1);
        assert_eq!(msg2.len(), 1);
        assert_eq!(msg1[0].0, msg_id1);
        assert_eq!(msg2[0].0, msg_id2);

        // read again, should be no messages
        let nothing = readit(&qname, 2_i32, 1_i32).unwrap();
        assert_eq!(nothing.len(), 0);

        // but still one record on the table
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 1);

        //  delete the messages
        let delete1 = pgmq_delete(&qname, msg_id1).unwrap().unwrap();
        assert!(delete1);

        //  delete when message is gone returns False
        let delete1 = pgmq_delete(&qname, msg_id1).unwrap().unwrap();
        assert!(!delete1);

        // no records after delete
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 0);
    }

    #[pg_test]
    fn test_archive() {
        let qname = r#"test_archive"#;
        let _ = pgmq_create_non_partitioned(&qname).unwrap();
        // no messages in the queue
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 0);
        // no messages in queue archive
        let retval = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM {TABLE_PREFIX}_{qname}_archive"
        ))
        .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 0);
        // put a message on the queue
        let msg_id = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":"y"}))).unwrap();
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 1);

        // archive the message
        let archived = pgmq_archive(&qname, msg_id.unwrap()).unwrap().unwrap();
        assert!(archived);
        // should be no messages left on the queue table
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 0);
        // but one on the archive table
        let retval = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM {TABLE_PREFIX}_{qname}_archive"
        ))
        .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 1);
    }

    #[pg_test]
    fn test_validate_same_type() {
        let invalid = validate_same_type("10", "daily");
        assert!(invalid.is_err());
        let invalid = validate_same_type("daily", "10");
        assert!(invalid.is_err());

        let valid = validate_same_type("10", "10");
        assert!(valid.is_ok());
        let valid = validate_same_type("daily", "weekly");
        assert!(valid.is_ok());
    }
}

#[cfg(test)]
pub mod pg_test {
    // pg_test module with both the setup and postgresql_conf_options functions are required

    use std::vec;

    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        // uncomment this when there are tests for the partman background worker
        // vec!["shared_preload_libraries = 'pg_partman_bgw'"]
        vec![]
    }
}
