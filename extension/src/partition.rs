use pgx;
use pgx::prelude::*;

use pgmq_crate::{
    errors::PgmqError,
    query::{check_input, create_archive, create_index, create_meta, insert_meta, TABLE_PREFIX},
};

// for now, put pg_partman in the public PGMQ_SCHEMA
const PARTMAN_SCHEMA: &str = "public";
const PGMQ_SCHEMA: &str = "public";

pub fn init_partitioned_queue(
    name: &str,
    partition_interval: &str,
    retention_interval: &str,
) -> Result<Vec<String>, PgmqError> {
    check_input(name)?;
    let partition_col = map_partition_col(partition_interval);
    Ok(vec![
        create_meta(),
        create_partitioned_queue(name, &partition_col)?,
        create_partitioned_index(name, &partition_col)?,
        create_index(name)?,
        create_archive(name)?,
        create_partitioned_table(name, &partition_col, partition_interval)?,
        insert_meta(name)?,
        set_retention_config(name, retention_interval)?,
    ])
}

/// maps the partition column based on partition_interval
fn map_partition_col(partition_interval: &str) -> String {
    match partition_interval.parse::<i32>() {
        Ok(_) => "msg_id".to_owned(),
        Err(_) => "enqueued_at".to_owned(),
    }
}

fn create_partitioned_queue(queue: &str, partition_col: &str) -> Result<String, PgmqError> {
    check_input(queue)?;
    Ok(format!(
        "
        CREATE TABLE IF NOT EXISTS {PGMQ_SCHEMA}.{TABLE_PREFIX}_{queue} (
            msg_id BIGSERIAL,
            read_ct INT DEFAULT 0,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT (now() at time zone 'utc'),
            vt TIMESTAMP WITH TIME ZONE,
            message JSONB
        ) PARTITION BY RANGE ({partition_col});
        "
    ))
}

pub fn create_partitioned_index(queue: &str, partiton_col: &str) -> Result<String, PgmqError> {
    check_input(queue)?;
    Ok(format!(
        "
        CREATE INDEX IF NOT EXISTS pgmq_partition_idx_{queue} ON {PGMQ_SCHEMA}.{TABLE_PREFIX}_{queue} ({partiton_col});
        "
    ))
}

fn create_partitioned_table(
    queue: &str,
    partition_col: &str,
    partition_interval: &str,
) -> Result<String, PgmqError> {
    Ok(format!(
        "
        SELECT {PARTMAN_SCHEMA}.create_parent('{PGMQ_SCHEMA}.{TABLE_PREFIX}_{queue}', '{partition_col}', 'native', '{partition_interval}');
        "
    ))
}

// set retention policy for a queue
// retention policy is only used for partition maintenance
// messages .deleted() are immediately removed from the queue
// messages .archived() will be retained forever on the `<queue_name>_archive` table
// https://github.com/pgpartman/pg_partman/blob/ca212077f66af19c0ca317c206091cd31d3108b8/doc/pg_partman.md#retention
// integer value will set that any partitions with an id value less than the current maximum id value minus the retention value will be dropped
fn set_retention_config(queue: &str, retention: &str) -> Result<String, PgmqError> {
    check_input(queue)?;
    Ok(format!(
        "
        UPDATE {PGMQ_SCHEMA}.part_config
        SET 
            retention = '{retention}',
            retention_keep_table = false,
            retention_keep_index = true,
            automatic_maintenance = 'on'
        WHERE parent_table = '{PGMQ_SCHEMA}.{TABLE_PREFIX}_{queue}';
        "
    ))
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
        let query: String = map_partition_col("10 days");
        assert!(query.contains("enqueued_at"));

        let query: String = map_partition_col("100");
        assert!(query.contains("msg_id"));
        let query: String = map_partition_col("1");
        assert!(query.contains("msg_id"));
        let query: String = map_partition_col("99");
        assert!(query.contains("msg_id"));
    }
}
