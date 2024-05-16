use pgrx::prelude::*;
use pgrx::spi;

use crate::errors::PgmqExtError;

#[pg_extern(name = "_drop_queue_old")]
fn pgmq_drop_queue(
    _queue_name: String,
    _partitioned: default!(bool, false),
) -> Result<bool, PgmqExtError> {
    todo!()
}

#[pg_extern(name = "_detach_archive_old")]
fn pgmq_detach_archive(_queue_name: String) -> Result<(), PgmqExtError> {
    todo!()
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

#[pg_extern(name = "_old_create_non_partitioned")]
fn pgmq_create_non_partitioned(_queue_name: &str) -> Result<(), PgmqExtError> {
    todo!()
}

#[pg_extern(name = "_create_unlogged_old")]
fn pgmq_create_unlogged(_queue_name: &str) -> Result<(), PgmqExtError> {
    todo!()
}

#[pg_extern(name = "_create_partitioned_old")]
fn pgmq_create_partitioned(
    _queue_name: &str,
    _partition_interval: default!(String, "'10000'"),
    _retention_interval: default!(String, "'100000'"),
) -> Result<(), PgmqExtError> {
    todo!()
}

#[pg_extern(name = "create_old")]
fn pgmq_create(_queue_name: &str) -> Result<(), PgmqExtError> {
    todo!()
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
#[pg_extern(name = "_set_vt_old")]
fn pgmq_set_vt(
    _queue_name: &str,
    _msg_id: i64,
    _vt_offset: i32,
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
    todo!();
}
