use pgrx::prelude::*;
use pgrx::spi;
use pgrx::spi::SpiTupleTable;

use crate::errors::PgmqExtError;
use crate::partition::PARTMAN_SCHEMA;
use crate::util;
use pgmq_core::{
    query::{destroy_queue, enqueue, purge_queue},
    types::{PGMQ_SCHEMA, TABLE_PREFIX},
};

#[pg_extern]
fn pgmq_drop_queue(
    queue_name: String,
    partitioned: default!(bool, false),
) -> Result<bool, PgmqExtError> {
    delete_queue(queue_name, partitioned)?;
    Ok(true)
}

pub fn delete_queue(queue_name: String, partitioned: bool) -> Result<(), PgmqExtError> {
    // TODO: we should keep track whether queue is partitioned in pgmq_meta
    // then read that to determine we want to delete the part_config entries
    // this should go out before 1.0
    let mut queries = destroy_queue(&queue_name)?;
    if partitioned {
        let queue_table = format!("{PGMQ_SCHEMA}.{TABLE_PREFIX}_{queue_name}");
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

#[pg_extern]
fn pgmq_list_queues() -> Result<
    TableIterator<
        'static,
        (
            name!(queue_name, String),
            name!(created_at, TimestampWithTimeZone),
        ),
    >,
    spi::Error,
> {
    let results = listit()?;
    Ok(TableIterator::new(results))
}

pub fn listit() -> Result<Vec<(String, TimestampWithTimeZone)>, spi::Error> {
    let mut results: Vec<(String, TimestampWithTimeZone)> = Vec::new();
    let query = format!("SELECT * FROM {PGMQ_SCHEMA}.pgmq_meta");
    let _: Result<(), spi::Error> = Spi::connect(|client| {
        let tup_table: SpiTupleTable = client.select(&query, None, None)?;
        for row in tup_table {
            let queue_name = row["queue_name"].value::<String>()?.expect("no queue_name");
            let created_at = row["created_at"]
                .value::<TimestampWithTimeZone>()?
                .expect("no created_at");
            results.push((queue_name, created_at));
        }
        Ok(())
    });
    Ok(results)
}

#[pg_extern]
fn pgmq_send_batch(
    queue_name: &str,
    messages: Vec<pgrx::JsonB>,
    delay: default!(i32, 0),
) -> Result<TableIterator<'static, (name!(msg_id, i64),)>, PgmqExtError> {
    let js_value: Vec<serde_json::Value> = messages.into_iter().map(|jsonb| jsonb.0).collect();
    let delay = util::delay_to_u64(delay)?;
    let query = enqueue(queue_name, &js_value, &delay)?;
    let mut results: Vec<(i64,)> = Vec::new();
    let _: Result<(), spi::Error> = Spi::connect(|mut client| {
        let tup_table: SpiTupleTable = client.update(&query, None, None)?;
        for row in tup_table {
            let msg_id = row["msg_id"].value::<i64>()?.expect("no msg_id");
            results.push((msg_id,));
        }
        Ok(())
    });
    Ok(TableIterator::new(results))
}

#[pg_extern]
fn pgmq_purge_queue(queue_name: String) -> Result<(), PgmqExtError> {
    Spi::connect(|mut client| {
        let query = purge_queue(&queue_name)?;
        client.update(query.as_str(), None, None)?;
        Ok(())
    })
}
