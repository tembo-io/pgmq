use pgrx::prelude::*;
use pgrx::spi;
use pgrx::spi::SpiTupleTable;
use pgrx::warning;

use crate::errors::PgmqExtError;
use crate::partition;
use crate::partition::PARTMAN_SCHEMA;

use pgmq_core::{
    query::{destroy_queue, init_queue},
    types::{PGMQ_SCHEMA, QUEUE_PREFIX},
    util::check_input,
};

#[pg_extern(name = "drop_queue")]
fn pgmq_drop_queue(
    queue_name: String,
    partitioned: default!(bool, false),
) -> Result<bool, PgmqExtError> {
    delete_queue(queue_name, partitioned)?;
    Ok(true)
}

#[pg_extern(name = "_detach_archive_old")]
fn pgmq_detach_archive(_queue_name: String) -> Result<(), PgmqExtError> {
    todo!()
}

pub fn delete_queue(queue_name: String, partitioned: bool) -> Result<(), PgmqExtError> {
    // TODO: we should keep track whether queue is partitioned in pgmq_meta
    // then read that to determine we want to delete the part_config entries
    // this should go out before 1.0
    let mut queries = destroy_queue(&queue_name)?;
    if partitioned {
        let queue_table = format!("{PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue_name}");
        queries.push(format!(
            "DELETE FROM {PARTMAN_SCHEMA}.part_config where parent_table = '{queue_table}';"
        ))
    }
    let _: Result<(), spi::Error> = Spi::connect(|mut client| {
        for q in queries {
            client.update(q.as_str(), None, None)?;
        }
        Ok(())
    });
    Ok(())
}

#[pg_extern(name = "_list_queues_old")]
fn pgmq_list_queues() -> Result<
    TableIterator<
        'static,
        (
            name!(queue_name, String),
            name!(created_at, TimestampWithTimeZone),
            name!(is_partitioned, bool),
            name!(is_unlogged, bool),
        ),
    >,
    spi::Error,
> {
    todo!()
}

#[pg_extern(name = "_purge_queue_removed")]
fn pgmq_purge_queue(_queue_name: String) -> Result<i64, PgmqExtError> {
    todo!()
}

#[pg_extern(name = "create_non_partitioned")]
fn pgmq_create_non_partitioned(queue_name: &str) -> Result<(), PgmqExtError> {
    let setup = init_queue(queue_name, false)?;
    let ran: Result<_, spi::Error> = Spi::connect(|mut c| {
        for q in setup {
            let _ = c.update(&q, None, None)?;
        }
        Ok(())
    });
    Ok(ran?)
}

#[pg_extern(name = "create_unlogged")]
fn pgmq_create_unlogged(queue_name: &str) -> Result<(), PgmqExtError> {
    let setup = init_queue(queue_name, true)?;
    let ran: Result<_, spi::Error> = Spi::connect(|mut c| {
        for q in setup {
            let _ = c.update(&q, None, None)?;
        }
        Ok(())
    });
    Ok(ran?)
}

#[pg_extern(name = "create_partitioned")]
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

#[pg_extern(name = "create")]
fn pgmq_create(queue_name: &str) -> Result<(), PgmqExtError> {
    pgmq_create_non_partitioned(queue_name)
}

pub fn validate_same_type(a: &str, b: &str) -> Result<(), PgmqExtError> {
    // either both can be ints, or not not ints
    match (a.parse::<i32>(), b.parse::<i32>()) {
        (Ok(_), Ok(_)) => Ok(()),
        (Err(_), Err(_)) => Ok(()),
        _ => Err(PgmqExtError::TypeErrorError("".to_owned())),
    }
}

pub fn readit(
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
        let query = pgmq_core::query::read(queue_name, vt, limit)?;
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

// reads and deletes at same time
#[pg_extern(name = "_pop_old_removed")]
fn pgmq_pop(
    _queue_name: &str,
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
    todo!()
}

/// change the visibility time on an existing message
/// vt_offset is a time relative to now that the message will be visible
/// accepts positive or negative integers
#[pg_extern(name = "set_vt")]
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
        UPDATE {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue_name}
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
    use super::*;

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
