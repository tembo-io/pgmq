//! Query constructors

use crate::errors::PgmqError;
use sqlx::types::chrono::Utc;
pub const TABLE_PREFIX: &str = r#"pgmq"#;
pub const PGMQ_SCHEMA: &str = "public";

pub fn init_queue(name: &str) -> Result<Vec<String>, PgmqError> {
    check_input(name)?;
    Ok(vec![
        create_meta(),
        grant_pgmon_meta(),
        create_queue(name)?,
        create_index(name)?,
        create_archive(name)?,
        create_archive_index(name)?,
        insert_meta(name)?,
        grant_pgmon_queue(name)?,
    ])
}

pub fn destroy_queue(name: &str) -> Result<Vec<String>, PgmqError> {
    check_input(name)?;
    Ok(vec![
        drop_queue(name)?,
        delete_queue_index(name)?,
        drop_queue_archive(name)?,
        delete_queue_metadata(name)?,
    ])
}

pub fn create_queue(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        CREATE TABLE IF NOT EXISTS {TABLE_PREFIX}_{name} (
            msg_id BIGSERIAL NOT NULL,
            read_ct INT DEFAULT 0 NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT (now() at time zone 'utc') NOT NULL,
            vt TIMESTAMP WITH TIME ZONE NOT NULL,
            message JSONB
        );
        "
    ))
}

pub fn create_archive(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        CREATE TABLE IF NOT EXISTS {TABLE_PREFIX}_{name}_archive (
            msg_id BIGSERIAL NOT NULL,
            read_ct INT DEFAULT 0 NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT (now() at time zone 'utc') NOT NULL,
            deleted_at TIMESTAMP WITH TIME ZONE DEFAULT (now() at time zone 'utc') NOT NULL,
            vt TIMESTAMP WITH TIME ZONE NOT NULL,
            message JSONB
        );
        "
    ))
}

pub fn create_meta() -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {TABLE_PREFIX}_meta (
            queue_name VARCHAR UNIQUE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT (now() at time zone 'utc') NOT NULL
        );
        "
    )
}

// pg_monitor needs to query queue metadata
pub fn grant_pgmon_meta() -> String {
    format!(
        "
        GRANT SELECT ON {PGMQ_SCHEMA}.{TABLE_PREFIX}_meta to pg_monitor;
        "
    )
}

// pg_monitor needs to query queue tables
pub fn grant_pgmon_queue(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        GRANT SELECT ON {PGMQ_SCHEMA}.{TABLE_PREFIX}_{name} to pg_monitor;
        "
    ))
}

pub fn drop_queue(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        DROP TABLE IF EXISTS {TABLE_PREFIX}_{name};
        "
    ))
}

pub fn delete_queue_index(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        DROP INDEX IF EXISTS {TABLE_PREFIX}_{name}.vt_idx_{name};
        "
    ))
}

pub fn delete_queue_metadata(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        DO $$
        BEGIN
           IF EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = '{TABLE_PREFIX}_meta')
            THEN
              DELETE
              FROM {TABLE_PREFIX}_meta
              WHERE queue_name = '{name}';
           END IF;
        END $$;
        "
    ))
}

pub fn drop_queue_archive(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        DROP TABLE IF EXISTS {TABLE_PREFIX}_{name}_archive;
        "
    ))
}

pub fn insert_meta(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        INSERT INTO {TABLE_PREFIX}_meta (queue_name)
        VALUES ('{name}')
        ON CONFLICT
        DO NOTHING;
        "
    ))
}

pub fn create_archive_index(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        CREATE INDEX IF NOT EXISTS deleted_at_idx_{name} ON {TABLE_PREFIX}_{name}_archive (deleted_at);
        "
    ))
}

// indexes are created ascending to support FIFO
pub fn create_index(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        CREATE INDEX IF NOT EXISTS msg_id_vt_idx_{name} ON {TABLE_PREFIX}_{name} (vt ASC, msg_id ASC);
        "
    ))
}

pub fn enqueue(
    name: &str,
    messages: &[serde_json::Value],
    delay: &u64,
) -> Result<String, PgmqError> {
    // construct string of comma separated messages
    check_input(name)?;
    let mut values: String = "".to_owned();
    for message in messages.iter() {
        let full_msg = format!(
            "((now() at time zone 'utc' + interval '{delay} seconds'), '{message}'::json),"
        );
        values.push_str(&full_msg)
    }
    // drop trailing comma from constructed string
    values.pop();
    Ok(format!(
        "
        INSERT INTO {TABLE_PREFIX}_{name} (vt, message)
        VALUES {values}
        RETURNING msg_id;
        "
    ))
}

pub fn read(name: &str, vt: &i32, limit: &i32) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
    WITH cte AS
        (
            SELECT msg_id
            FROM {TABLE_PREFIX}_{name}
            WHERE vt <= now() at time zone 'utc'
            ORDER BY msg_id ASC
            LIMIT {limit}
            FOR UPDATE SKIP LOCKED
        )
    UPDATE {TABLE_PREFIX}_{name}
    SET
        vt = (now() at time zone 'utc' + interval '{vt} seconds'),
        read_ct = read_ct + 1
    WHERE msg_id in (select msg_id from cte)
    RETURNING *;
    "
    ))
}

pub fn delete(name: &str, msg_id: &i64) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        DELETE FROM {TABLE_PREFIX}_{name}
        WHERE msg_id = {msg_id};
        "
    ))
}

pub fn set_vt(name: &str, msg_id: &i64, vt: &chrono::DateTime<Utc>) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        UPDATE {TABLE_PREFIX}_{name}
        SET vt = '{t}'::timestamp
        WHERE msg_id = {msg_id}
        RETURNING *;
        ",
        t = vt.format("%Y-%m-%d %H:%M:%S%.3f %z")
    ))
}

pub fn delete_batch(name: &str, msg_ids: &[i64]) -> Result<String, PgmqError> {
    // construct string of comma separated msg_id
    check_input(name)?;
    let mut msg_id_list: String = "".to_owned();
    for msg_id in msg_ids.iter() {
        let id_str = format!("{msg_id},");
        msg_id_list.push_str(&id_str)
    }
    // drop trailing comma from constructed string
    msg_id_list.pop();
    Ok(format!(
        "
        DELETE FROM {TABLE_PREFIX}_{name}
        WHERE msg_id in ({msg_id_list});
        "
    ))
}

pub fn archive(name: &str, msg_id: &i64) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        WITH archived AS (
            DELETE FROM {TABLE_PREFIX}_{name}
            WHERE msg_id = {msg_id}
            RETURNING msg_id, vt, read_ct, enqueued_at, message
        )
        INSERT INTO {TABLE_PREFIX}_{name}_archive (msg_id, vt, read_ct, enqueued_at, message)
        SELECT msg_id, vt, read_ct, enqueued_at, message
        FROM archived;
        "
    ))
}

pub fn pop(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        WITH cte AS
            (
                SELECT msg_id
                FROM {TABLE_PREFIX}_{name}
                WHERE vt <= now() at time zone 'utc'
                ORDER BY msg_id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
        DELETE from {TABLE_PREFIX}_{name}
        WHERE msg_id = (select msg_id from cte)
        RETURNING *;
        "
    ))
}

/// panics if input is invalid. otherwise does nothing.
pub fn check_input(input: &str) -> Result<(), PgmqError> {
    let valid = input
        .as_bytes()
        .iter()
        .all(|&c| c.is_ascii_alphanumeric() || c == b'_');
    match valid {
        true => Ok(()),
        false => Err(PgmqError::InvalidQueueName {
            name: input.to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let query = create_queue("yolo");
        assert!(query.unwrap().contains("pgmq_yolo"));
    }

    #[test]
    fn test_enqueue() {
        let mut msgs: Vec<serde_json::Value> = Vec::new();
        let msg = serde_json::json!({
            "foo": "bar"
        });
        msgs.push(msg);
        let query = enqueue("yolo", &msgs, &0).unwrap();
        assert!(query.contains("pgmq_yolo"));
        assert!(query.contains("{\"foo\":\"bar\"}"));
    }

    #[test]
    fn test_read() {
        let qname = "myqueue";
        let vt: i32 = 20;
        let limit: i32 = 1;

        let query = read(&qname, &vt, &limit).unwrap();

        assert!(query.contains(&qname));
        assert!(query.contains(&vt.to_string()));
    }

    #[test]
    fn test_delete() {
        let qname = "myqueue";
        let msg_id: i64 = 42;

        let query = delete(&qname, &msg_id).unwrap();

        assert!(query.contains(&qname));
        assert!(query.contains(&msg_id.to_string()));
    }

    #[test]
    fn test_delete_batch() {
        let mut msg_ids: Vec<i64> = Vec::new();
        let qname = "myqueue";
        msg_ids.push(42);
        msg_ids.push(43);
        msg_ids.push(44);

        let query = delete_batch(&qname, &msg_ids).unwrap();

        assert!(query.contains(&qname));
        for id in msg_ids.iter() {
            assert!(query.contains(&id.to_string()));
        }
    }

    #[test]
    #[should_panic]
    fn test_check_input() {
        let invalids = vec!["bad;queue_name", "bad name", "bad--name"];
        for i in invalids.iter() {
            let is_valid = check_input(i);
            assert!(!is_valid.is_err())
        }
    }
}
