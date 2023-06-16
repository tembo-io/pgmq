/// Metric definitions
///
use pgrx::prelude::*;
use pgrx::spi::SpiTupleTable;
use pgrx::warning;

use crate::api::listit;
use pgmq_crate::query::{PGMQ_SCHEMA, TABLE_PREFIX};

type MetricResult = Vec<(
    String,
    i64,
    Option<i32>,
    Option<i32>,
    i64,
    TimestampWithTimeZone,
)>;

#[pg_extern]
fn pgmq_metrics(
    queue_name: &str,
) -> Result<
    TableIterator<
        'static,
        (
            name!(queue_name, String),
            name!(queue_length, i64),
            name!(newest_msg_age_sec, Option<i32>),
            name!(oldest_msg_age_sec, Option<i32>),
            name!(total_messages, i64),
            name!(scrape_time, TimestampWithTimeZone),
        ),
    >,
    crate::PgmqExtError,
> {
    let results = query_summary(&queue_name)?;
    Ok(TableIterator::new(results.into_iter()))
}

#[pg_extern]
fn pgmq_metrics_all() -> Result<
    TableIterator<
        'static,
        (
            name!(queue_name, String),
            name!(queue_length, i64),
            name!(newest_msg_age_sec, Option<i32>),
            name!(oldest_msg_age_sec, Option<i32>),
            name!(total_messages, i64),
            name!(scrape_time, TimestampWithTimeZone),
        ),
    >,
    crate::PgmqExtError,
> {
    let all_queueus = listit()?;
    let mut results: MetricResult = Vec::new();
    for q in all_queueus {
        let q_results = query_summary(&q.0)?;
        results.extend(q_results);
    }
    Ok(TableIterator::new(results.into_iter()))
}

fn query_summary(queue_name: &str) -> Result<MetricResult, crate::PgmqExtError> {
    let query: String = build_summary_query(queue_name);
    let results: Result<MetricResult, crate::PgmqExtError> = Spi::connect(|client| {
        let mut results: MetricResult = Vec::new();
        let tup_table: SpiTupleTable = client.select(&query, None, None)?;
        for row in tup_table {
            let queue_name = queue_name.to_owned();
            let queue_length = row["queue_length"].value::<i64>()?.expect("no msg_id");
            let newest_msg_sec = row["newest_msg_age_sec"].value::<i32>()?;
            let oldest_msg_sec = row["oldest_msg_age_sec"].value::<i32>()?;
            let total_messages = row["total_messages"]
                .value::<i64>()?
                .expect("failed to get total_messages");
            let scrape_time = row["scrape_time"]
                .value::<TimestampWithTimeZone>()?
                .expect("scrape timestamp missing");
            results.push((
                queue_name,
                queue_length,
                newest_msg_sec,
                oldest_msg_sec,
                total_messages,
                scrape_time,
            ));
        }
        Ok(results)
    });
    match results {
        Ok(results) => Ok(results),
        Err(e) => {
            warning!("error: {:?}", e);
            Err(e)
        }
    }
}

fn build_summary_query(queue_name: &str) -> String {
    let fq_table = format!("{PGMQ_SCHEMA}.{TABLE_PREFIX}_{queue_name}");
    format!(
        "SELECT * FROM
            (SELECT
                count(*) as queue_length,
                (EXTRACT(epoch FROM (SELECT (NOW() at time zone 'utc' -  max(enqueued_at)))))::int as newest_msg_age_sec,
                (EXTRACT(epoch FROM (SELECT (NOW() at time zone 'utc' -  min(enqueued_at)))))::int as oldest_msg_age_sec,
                (NOW() at time zone 'utc')::timestamp at time zone 'utc' as scrape_time
            FROM {fq_table}) as q_summary
        CROSS JOIN
            (SELECT
                last_value as total_messages
            from {fq_table}_msg_id_seq) as q_sent_summary
        "
    )
}
