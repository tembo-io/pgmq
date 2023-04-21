use pgmq_crate::{
    errors::PgmqError,
    query::{check_input, create_archive, create_index, create_meta, insert_meta, TABLE_PREFIX},
};

// for now, put pg_partman in the public schema
const PARTMAN_SCHEMA: &str = "public";
const SCHEMA: &str = "public";

pub fn init_partitioned_queue(name: &str, partition_size: i64) -> Result<Vec<String>, PgmqError> {
    check_input(name)?;
    Ok(vec![
        create_meta(),
        create_partitioned_queue(name)?,
        create_partitioned_index(name)?,
        create_index(name)?,
        create_archive(name)?,
        create_partman(name, partition_size)?,
        insert_meta(name)?,
    ])
}

pub fn create_partitioned_queue(queue: &str) -> Result<String, PgmqError> {
    check_input(queue)?;
    Ok(format!(
        "
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_PREFIX}_{queue} (
            msg_id BIGSERIAL,
            read_ct INT DEFAULT 0,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT (now() at time zone 'utc'),
            vt TIMESTAMP WITH TIME ZONE,
            message JSONB
        ) PARTITION BY RANGE (msg_id);;
        "
    ))
}

pub fn create_partitioned_index(queue: &str) -> Result<String, PgmqError> {
    check_input(queue)?;
    Ok(format!(
        "
        CREATE INDEX IF NOT EXISTS msg_id_idx_{queue} ON {SCHEMA}.{TABLE_PREFIX}_{queue} (msg_id);
        "
    ))
}

pub fn create_partman(queue: &str, partition_size: i64) -> Result<String, PgmqError> {
    check_input(queue)?;
    Ok(format!(
        "
        SELECT {PARTMAN_SCHEMA}.create_parent('{SCHEMA}.{TABLE_PREFIX}_{queue}', 'msg_id', 'native', '{partition_size}');
        "
    ))
}
