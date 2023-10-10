use pgrx;
use pgrx::prelude::*;

use pgmq_core::{
    errors::PgmqError,
    query::{
        assign_archive, assign_queue, create_archive, create_archive_index, create_index,
        create_meta, insert_meta, pg_mon_grant_schema_seqs_permission,
        pg_mon_grant_schema_tables_permission,
    },
    types::{PGMQ_SCHEMA, QUEUE_PREFIX},
    util::CheckedName,
};

// for now, put pg_partman in the public PGMQ_SCHEMA
pub const PARTMAN_SCHEMA: &str = "public";

/// partitioned queues require pg_partman to be installed
pub fn init_partitioned_queue(
    name: &str,
    partition_interval: &str,
    retention_interval: &str,
) -> Result<Vec<String>, PgmqError> {
    let name = CheckedName::new(name)?;
    let partition_col = map_partition_col(partition_interval);
    Ok(vec![
        create_partitioned_queue(name, partition_col)?,
        assign_queue(name)?,
        create_partitioned_index(name, partition_col)?,
        create_index(name)?,
        create_archive(name)?,
        create_archive_index(name)?,
        assign_archive(name)?,
        create_partitioned_table(name, partition_col, partition_interval)?,
        insert_meta(name, true, false)?,
        set_retention_config(name, retention_interval)?,
    ])
}

/// partitioned queues require pg_partman to be installed
pub fn init_partitioned_queue_client_only(
    name: &str,
    partition_interval: &str,
    retention_interval: &str,
) -> Result<Vec<String>, PgmqError> {
    let name = CheckedName::new(name)?;
    let partition_col = map_partition_col(partition_interval);
    Ok(vec![
        create_meta(),
        create_partitioned_queue(name, partition_col)?,
        assign_queue(name)?,
        create_partitioned_index(name, partition_col)?,
        create_index(name)?,
        create_archive(name)?,
        create_archive_index(name)?,
        assign_archive(name)?,
        create_partitioned_table(name, partition_col, partition_interval)?,
        insert_meta(name, true, false)?,
        set_retention_config(name, retention_interval)?,
        pg_mon_grant_schema_tables_permission(),
        pg_mon_grant_schema_seqs_permission(),
    ])
}

/// maps the partition column based on partition_interval
fn map_partition_col(partition_interval: &str) -> &'static str {
    // map using msg_id when partition_interval is an integer
    // otherwise use enqueued_at (time based)
    match partition_interval.parse::<i32>() {
        Ok(_) => "msg_id",
        Err(_) => "enqueued_at",
    }
}

fn create_partitioned_queue(
    queue: CheckedName<'_>,
    partition_col: &str,
) -> Result<String, PgmqError> {
    Ok(format!(
        "
        CREATE TABLE IF NOT EXISTS {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue} (
            msg_id BIGSERIAL NOT NULL,
            read_ct INT DEFAULT 0 NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt TIMESTAMP WITH TIME ZONE NOT NULL,
            message JSONB
        ) PARTITION BY RANGE ({partition_col});
        "
    ))
}

pub fn create_partitioned_index(
    queue: CheckedName<'_>,
    partiton_col: &str,
) -> Result<String, PgmqError> {
    Ok(format!(
        "
        CREATE INDEX IF NOT EXISTS pgmq_partition_idx_{queue} ON {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue} ({partiton_col});
        "
    ))
}

fn create_partitioned_table(
    queue: CheckedName<'_>,
    partition_col: &str,
    partition_interval: &str,
) -> Result<String, PgmqError> {
    Ok(format!(
        "
        SELECT {PARTMAN_SCHEMA}.create_parent('{PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue}', '{partition_col}', 'native', '{partition_interval}');
        "
    ))
}

// set retention policy for a queue
// retention policy is only used for partition maintenance
// messages .deleted() are immediately removed from the queue
// messages .archived() will be retained forever on the `<queue_name>_archive` table
// https://github.com/pgpartman/pg_partman/blob/ca212077f66af19c0ca317c206091cd31d3108b8/doc/pg_partman.md#retention
// integer value will set that any partitions with an id value less than the current maximum id value minus the retention value will be dropped
fn set_retention_config(queue: CheckedName<'_>, retention: &str) -> Result<String, PgmqError> {
    Ok(format!(
        "
        UPDATE {PARTMAN_SCHEMA}.part_config
        SET
            retention = '{retention}',
            retention_keep_table = false,
            retention_keep_index = true,
            automatic_maintenance = 'on'
        WHERE parent_table = '{PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue}';
        "
    ))
}

pub fn partman_installed() -> String {
    "
        SELECT EXISTS (
            SELECT 1
            FROM pg_extension
            WHERE extname = 'pg_partman'
        );
        "
    .to_owned()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    #[pg_test]
    fn test_map_partition_col() {
        let query = map_partition_col("daily");
        assert!(query.contains("enqueued_at"));
        let query = map_partition_col("1 day");
        assert!(query.contains("enqueued_at"));
        let query = map_partition_col("10 days");
        assert!(query.contains("enqueued_at"));

        let query = map_partition_col("100");
        assert!(query.contains("msg_id"));
        let query = map_partition_col("1");
        assert!(query.contains("msg_id"));
        let query = map_partition_col("99");
        assert!(query.contains("msg_id"));
    }
}
