use pgrx::prelude::*;
use pgrx::spi;
use pgrx::spi::SpiTupleTable;
use pgrx::warning;

pgrx::pg_module_magic!();

pub mod api;
pub mod errors;
pub mod metrics;
pub mod partition;
pub mod util;

use pgmq_core::{
    query::{archive_batch, delete_batch, enqueue, init_queue, pop, read},
    types::TABLE_PREFIX,
    util::check_input,
};

use errors::PgmqExtError;
use std::time::Duration;

extension_sql!(
    "
CREATE TABLE public.pgmq_meta (
    queue_name VARCHAR UNIQUE NOT NULL,
    is_partitioned BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    WHERE has_table_privilege('pg_monitor', 'public.pgmq_meta', 'SELECT')
  ) THEN
    EXECUTE 'GRANT SELECT ON public.pgmq_meta TO pg_monitor';
  END IF;
END;
$$ LANGUAGE plpgsql;
",
    name = "bootstrap"
);

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
fn pgmq_create_partitioned(
    queue_name: &str,
    partition_interval: default!(String, "'10000'"),
    retention_interval: default!(String, "'100000'"),
) -> Result<(), PgmqExtError> {
    // validate pg_partman is installed
    match Spi::get_one::<bool>(&partition::partman_installed())?
        .expect("could not query extensions table")
    {
        true => (),
        false => {
            warning!("pg_partman not installed. Install https://github.com/pgpartman/pg_partman and then run `CREATE EXTENSION pg_partman;`");
            return Err(PgmqExtError::MissingDependency("pg_partman".to_owned()));
        }
    };
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

#[pg_extern]
fn pgmq_create(queue_name: &str) -> Result<(), PgmqExtError> {
    pgmq_create_non_partitioned(queue_name)
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
fn pgmq_send(
    queue_name: &str,
    message: pgrx::JsonB,
    delay: default!(i32, 0),
) -> Result<Option<i64>, PgmqExtError> {
    let delay = util::delay_to_u64(delay)?;
    let query = enqueue(queue_name, &[message.0], &delay)?;
    Spi::connect(|mut client| {
        let tup_table: SpiTupleTable = client.update(&query, None, None)?;
        Ok(tup_table.first().get_one::<i64>()?)
    })
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
            name!(enqueued_at, TimestampWithTimeZone),
            name!(vt, TimestampWithTimeZone),
            name!(message, pgrx::JsonB),
        ),
    >,
    spi::Error,
> {
    let results = readit(queue_name, vt, limit)?;
    Ok(TableIterator::new(results))
}

#[pg_extern]
fn pgmq_read_with_poll(
    queue_name: &str,
    vt: i32,
    limit: i32,
    poll_timeout_s: default!(i32, 5),
    poll_interval_ms: default!(i32, 250),
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
    spi::Error,
> {
    let start_time = std::time::Instant::now();
    let poll_timeout_ms = (poll_timeout_s * 1000) as u128;
    loop {
        let results = readit(queue_name, vt, limit)?;
        if results.is_empty() && start_time.elapsed().as_millis() < poll_timeout_ms {
            std::thread::sleep(Duration::from_millis(poll_interval_ms.try_into().unwrap()));
            continue;
        } else {
            break Ok(TableIterator::new(results));
        }
    }
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
        let query = read(queue_name, vt, limit)?;
        let tup_table: SpiTupleTable = client.update(&query, None, None)?;
        results.reserve_exact(tup_table.len());

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
    Ok(results)
}

#[pg_extern]
fn pgmq_delete(queue_name: &str, msg_id: i64) -> Result<Option<bool>, PgmqExtError> {
    pgmq_delete_batch(queue_name, vec![msg_id]).map(|mut iter| iter.next().map(|b| b.0))
}

#[pg_extern(name = "pgmq_delete")]
fn pgmq_delete_batch(
    queue_name: &str,
    msg_ids: Vec<i64>,
) -> Result<TableIterator<'static, (name!(pgmq_delete, bool),)>, PgmqExtError> {
    let query = delete_batch(queue_name)?;

    let mut deleted: Vec<i64> = Vec::new();
    let _: Result<(), spi::Error> = Spi::connect(|mut client| {
        let tup_table = client.update(
            &query,
            None,
            Some(vec![(
                PgBuiltInOids::INT8ARRAYOID.oid(),
                msg_ids.clone().into_datum(),
            )]),
        )?;

        deleted.reserve_exact(tup_table.len());

        for row in tup_table {
            let msg_id = row["msg_id"].value::<i64>()?.expect("no msg_id");
            deleted.push(msg_id);
        }
        Ok(())
    });

    let results = msg_ids
        .iter()
        .map(|msg_id| {
            if deleted.contains(msg_id) {
                (true,)
            } else {
                (false,)
            }
        })
        .collect::<Vec<(bool,)>>();

    Ok(TableIterator::new(results))
}

/// archive a message forever instead of deleting it
#[pg_extern]
fn pgmq_archive(queue_name: &str, msg_id: i64) -> Result<Option<bool>, PgmqExtError> {
    pgmq_archive_batch(queue_name, vec![msg_id]).map(|mut iter| iter.next().map(|b| b.0))
}

#[pg_extern(name = "pgmq_archive")]
fn pgmq_archive_batch(
    queue_name: &str,
    msg_ids: Vec<i64>,
) -> Result<TableIterator<'static, (name!(pgmq_archive, bool),)>, PgmqExtError> {
    let query = archive_batch(queue_name)?;

    let mut archived: Vec<i64> = Vec::new();

    let _: Result<(), spi::Error> = Spi::connect(|mut client| {
        let tup_table: SpiTupleTable = client.update(
            &query,
            None,
            Some(vec![(
                PgBuiltInOids::INT8ARRAYOID.oid(),
                msg_ids.clone().into_datum(),
            )]),
        )?;

        archived.reserve_exact(tup_table.len());

        for row in tup_table {
            let msg_id = row["msg_id"].value::<i64>()?.expect("no msg_id");
            archived.push(msg_id);
        }
        Ok(())
    });

    let results = msg_ids
        .iter()
        .map(|msg_id| {
            if archived.contains(&msg_id) {
                (true,)
            } else {
                (false,)
            }
        })
        .collect::<Vec<(bool,)>>();

    Ok(TableIterator::new(results))
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
            name!(enqueued_at, TimestampWithTimeZone),
            name!(vt, TimestampWithTimeZone),
            name!(message, pgrx::JsonB),
        ),
    >,
    PgmqExtError,
> {
    let results = popit(queue_name)?;
    Ok(TableIterator::new(results))
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
        results.reserve_exact(tup_table.len());
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
        SET vt = (now() + interval '{vt_offset} seconds')
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
    Ok(TableIterator::new(results))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::*;
    use pgmq_core::types::TABLE_PREFIX;

    #[pg_test]
    fn test_create_non_partitioned() {
        let qname = r#"test_queue"#;
        let _ = pgmq_create_non_partitioned(&qname).unwrap();
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 0);
        let _ = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":"y"})), 0).unwrap();
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
        let _ = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":"y"})), 0);

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
        let msg_id1 = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":1})), 0)
            .unwrap()
            .unwrap();
        let msg_id2 = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":2})), 0)
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

        let _ =
            Spi::run("DROP EXTENSION IF EXISTS pg_partman").expect("Failed dropping pg_partman");

        let failed = pgmq_create_partitioned(
            &qname,
            partition_interval.clone(),
            retention_interval.clone(),
        );
        assert!(failed.is_err());

        let _ = Spi::run("CREATE EXTENSION IF NOT EXISTS pg_partman")
            .expect("Failed creating pg_partman");
        let _ = pgmq_create_partitioned(&qname, partition_interval, retention_interval).unwrap();

        let queues = api::listit().unwrap();
        assert_eq!(queues.len(), 1);

        // put two message on the queue
        let msg_id1 = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":1})), 0)
            .unwrap()
            .unwrap();
        let msg_id2 = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":2})), 0)
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
        let msg_id = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":"y"})), 0).unwrap();
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
    fn test_archive_batch() {
        let qname = r#"test_archive_batch"#;
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
        // put messages on the queue
        let msg_id1 = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":1})), 0)
            .unwrap()
            .unwrap();
        let msg_id2 = pgmq_send(&qname, pgrx::JsonB(serde_json::json!({"x":2})), 0)
            .unwrap()
            .unwrap();
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 2);

        // archive the message. The first two exist so should return true, the
        // last one doesn't so should return false.
        let mut archived = pgmq_archive_batch(&qname, vec![msg_id1, msg_id2, -1]).unwrap();
        assert!(archived.next().unwrap().0);
        assert!(archived.next().unwrap().0);
        assert!(!archived.next().unwrap().0);

        // should be no messages left on the queue table
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 0);
        // but two on the archive table
        let retval = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM {TABLE_PREFIX}_{qname}_archive"
        ))
        .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 2);
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
