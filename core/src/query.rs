//! Query constructors

use crate::{
    errors::PgmqError,
    types::{ARCHIVE_PREFIX, PGMQ_SCHEMA, QUEUE_PREFIX},
    util::check_input,
    util::CheckedName,
};

use sqlx::types::chrono::Utc;

pub fn init_queue(name: &str, is_unlogged: bool) -> Result<Vec<String>, PgmqError> {
    let name = CheckedName::new(name)?;
    Ok(vec![
        create_queue(name, is_unlogged)?,
        assign_queue(name)?,
        create_index(name)?,
        create_archive(name)?,
        assign_archive(name)?,
        create_archive_index(name)?,
        insert_meta(name, false, is_unlogged)?,
    ])
}

pub fn destroy_queue(name: &str) -> Result<Vec<String>, PgmqError> {
    let name = CheckedName::new(name)?;
    Ok(vec![
        unassign_queue(name)?,
        unassign_archive(name)?,
        drop_queue(name)?,
        drop_queue_archive(name)?,
        delete_queue_metadata(name)?,
    ])
}

pub fn create_queue(name: CheckedName<'_>, is_unlogged: bool) -> Result<String, PgmqError> {
    let maybe_unlogged = if is_unlogged { "UNLOGGED" } else { "" };

    Ok(format!(
        "
        CREATE {maybe_unlogged} TABLE IF NOT EXISTS {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name} (
            msg_id BIGSERIAL PRIMARY KEY,
            read_ct INT DEFAULT 0 NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt TIMESTAMP WITH TIME ZONE NOT NULL,
            message JSONB
        );
        "
    ))
}

pub fn create_archive(name: CheckedName<'_>) -> Result<String, PgmqError> {
    Ok(format!(
        "
        CREATE TABLE IF NOT EXISTS {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{name} (
            msg_id BIGSERIAL PRIMARY KEY,
            read_ct INT DEFAULT 0 NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt TIMESTAMP WITH TIME ZONE NOT NULL,
            message JSONB
        );
        "
    ))
}

pub fn create_schema() -> String {
    format!("CREATE SCHEMA IF NOT EXISTS {PGMQ_SCHEMA}")
}

pub fn create_meta() -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {PGMQ_SCHEMA}.meta (
            queue_name VARCHAR UNIQUE NOT NULL,
            is_partitioned BOOLEAN NOT NULL,
            is_unlogged BOOLEAN NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
        );
        "
    )
}

pub fn drop_queue(name: CheckedName<'_>) -> Result<String, PgmqError> {
    Ok(format!(
        "
        DROP TABLE IF EXISTS {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name};
        "
    ))
}

pub fn delete_queue_metadata(name: CheckedName<'_>) -> Result<String, PgmqError> {
    Ok(format!(
        "
        DO $$
        BEGIN
           IF EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'meta' and table_schema = 'pgmq')
            THEN
              DELETE
              FROM {PGMQ_SCHEMA}.meta
              WHERE queue_name = '{name}';
           END IF;
        END $$;
        "
    ))
}

pub fn drop_queue_archive(name: CheckedName<'_>) -> Result<String, PgmqError> {
    Ok(format!(
        "
        DROP TABLE IF EXISTS {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{name};
        "
    ))
}

pub fn insert_meta(
    name: CheckedName<'_>,
    is_partitioned: bool,
    is_unlogged: bool,
) -> Result<String, PgmqError> {
    Ok(format!(
        "
        INSERT INTO {PGMQ_SCHEMA}.meta (queue_name, is_partitioned, is_unlogged)
        VALUES ('{name}', {is_partitioned}, {is_unlogged})
        ON CONFLICT
        DO NOTHING;
        ",
    ))
}

pub fn create_archive_index(name: CheckedName<'_>) -> Result<String, PgmqError> {
    Ok(format!(
        "
        CREATE INDEX IF NOT EXISTS archived_at_idx_{name} ON {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{name} (archived_at);
        "
    ))
}

// indexes are created ascending to support FIFO
pub fn create_index(name: CheckedName<'_>) -> Result<String, PgmqError> {
    Ok(format!(
        "
        CREATE INDEX IF NOT EXISTS {QUEUE_PREFIX}_{name}_vt_idx ON {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name} (vt ASC);
        "
    ))
}

pub fn purge_queue(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!("DELETE FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name};"))
}

pub fn enqueue(
    name: &str,
    messages: &[serde_json::Value],
    delay: &u64,
) -> Result<String, PgmqError> {
    // construct string of comma separated messages
    check_input(name)?;
    let mut values = "".to_owned();
    for message in messages.iter() {
        let full_msg = format!("((now() + interval '{delay} seconds'), '{message}'::json),");
        values.push_str(&full_msg)
    }
    // drop trailing comma from constructed string
    values.pop();
    Ok(format!(
        "
        INSERT INTO {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name} (vt, message)
        VALUES {values}
        RETURNING msg_id;
        "
    ))
}

pub fn read(name: &str, vt: i32, limit: i32) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
    WITH cte AS
        (
            SELECT msg_id
            FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name}
            WHERE vt <= clock_timestamp()
            ORDER BY msg_id ASC
            LIMIT {limit}
            FOR UPDATE SKIP LOCKED
        )
    UPDATE {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name} t
    SET
        vt = clock_timestamp() + interval '{vt} seconds',
        read_ct = read_ct + 1
    FROM cte
    WHERE t.msg_id=cte.msg_id
    RETURNING *;
    "
    ))
}

pub fn set_vt(name: &str, msg_id: i64, vt: chrono::DateTime<Utc>) -> Result<String, PgmqError> {
    check_input(name)?;
    Ok(format!(
        "
        UPDATE {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name}
        SET vt = '{t}'::timestamp
        WHERE msg_id = {msg_id}
        RETURNING *;
        ",
        t = vt.format("%Y-%m-%d %H:%M:%S%.3f %z")
    ))
}

pub fn delete_batch(name: &str) -> Result<String, PgmqError> {
    // construct string of comma separated msg_id
    check_input(name)?;

    Ok(format!(
        "
        DELETE FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name}
        WHERE msg_id = ANY($1)
        RETURNING msg_id;
        "
    ))
}

pub fn archive_batch(name: &str) -> Result<String, PgmqError> {
    check_input(name)?;

    Ok(format!(
        "
        WITH archived AS (
            DELETE FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name}
            WHERE msg_id = ANY($1)
            RETURNING msg_id, vt, read_ct, enqueued_at, message
        )
        INSERT INTO {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{name} (msg_id, vt, read_ct, enqueued_at, message)
        SELECT msg_id, vt, read_ct, enqueued_at, message
        FROM archived
        RETURNING msg_id;
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
                FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name}
                WHERE vt <= now()
                ORDER BY msg_id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
        DELETE from {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name}
        WHERE msg_id = (select msg_id from cte)
        RETURNING *;
        "
    ))
}

pub fn assign_queue(name: CheckedName<'_>) -> Result<String, PgmqError> {
    Ok(assign(&format!("{QUEUE_PREFIX}_{name}")))
}

pub fn assign_archive(name: CheckedName<'_>) -> Result<String, PgmqError> {
    Ok(assign(&format!("{ARCHIVE_PREFIX}_{name}")))
}

pub fn unassign_queue(name: CheckedName<'_>) -> Result<String, PgmqError> {
    Ok(format!(
        "ALTER EXTENSION pgmq DROP TABLE {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name}; "
    ))
}

pub fn unassign_archive(name: CheckedName<'_>) -> Result<String, PgmqError> {
    Ok(format!(
        "ALTER EXTENSION pgmq DROP TABLE {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{name}; "
    ))
}

// assign a table to pgmq extension, only if its not already assigned
pub fn assign(table_name: &str) -> String {
    format!(
        "
    DO $$
        BEGIN
        -- Check if the table is not yet associated with the extension
        IF NOT EXISTS (
            SELECT 1
            FROM pg_depend
            WHERE refobjid = (SELECT oid FROM pg_extension WHERE extname = 'pgmq')
            AND objid = (
                SELECT oid
                FROM pg_class
                WHERE relname = '{table_name}'
            )
        ) THEN
            EXECUTE 'ALTER EXTENSION pgmq ADD TABLE {PGMQ_SCHEMA}.{table_name}';
        END IF;
    END $$;
    "
    )
}

fn grant_stmt(table: &str) -> String {
    let grant_seq = match &table.contains("meta") {
        true => "".to_string(),
        false => {
            format!("    EXECUTE 'GRANT SELECT ON SEQUENCE {table}_msg_id_seq TO pg_monitor';")
        }
    };
    format!(
        "
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    WHERE has_table_privilege('pg_monitor', '{table}', 'SELECT')
  ) THEN
    EXECUTE 'GRANT SELECT ON {table} TO pg_monitor';
{grant_seq}
  END IF;
END;
$$ LANGUAGE plpgsql;
"
    )
}

// pg_monitor needs to query queue metadata
pub fn grant_pgmon_meta() -> String {
    let table = format!("{PGMQ_SCHEMA}.meta");
    grant_stmt(&table)
}

// pg_monitor needs to query queue tables
pub fn grant_pgmon_queue(name: CheckedName<'_>) -> Result<String, PgmqError> {
    let table = format!("{PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name}");
    Ok(grant_stmt(&table))
}

pub fn grant_pgmon_queue_seq(name: CheckedName<'_>) -> Result<String, PgmqError> {
    let table = format!("{PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name}_msg_id_seq");
    Ok(grant_stmt(&table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grant() {
        let q = grant_stmt("my_table");
        let expected = "
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    WHERE has_table_privilege('pg_monitor', 'my_table', 'SELECT')
  ) THEN
    EXECUTE 'GRANT SELECT ON my_table TO pg_monitor';
    EXECUTE 'GRANT SELECT ON SEQUENCE my_table_msg_id_seq TO pg_monitor';
  END IF;
END;
$$ LANGUAGE plpgsql;
";
        assert_eq!(q, expected);

        let q = grant_stmt("meta");
        let expected = "
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    WHERE has_table_privilege('pg_monitor', 'meta', 'SELECT')
  ) THEN
    EXECUTE 'GRANT SELECT ON meta TO pg_monitor';
  END IF;
END;
$$ LANGUAGE plpgsql;
";
        assert_eq!(q, expected)
    }

    #[test]
    fn test_assign() {
        let query = assign("a_my_queue_archive");
        assert!(query.contains("WHERE relname = 'a_my_queue_archive'"));
    }

    #[test]
    fn test_create() {
        let queue_name = CheckedName::new("yolo").unwrap();
        let query = create_queue(queue_name, false);
        assert!(query.unwrap().contains("q_yolo"));
    }

    #[test]
    fn create_unlogged() {
        let queue_name = CheckedName::new("yolo").unwrap();
        let query = create_queue(queue_name, true);
        assert!(query.unwrap().contains("CREATE UNLOGGED TABLE"));
    }

    #[test]
    fn test_enqueue() {
        let mut msgs: Vec<serde_json::Value> = Vec::new();
        let msg = serde_json::json!({
            "foo": "bar"
        });
        msgs.push(msg);
        let query = enqueue("yolo", &msgs, &0).unwrap();
        assert!(query.contains("q_yolo"));
        assert!(query.contains("{\"foo\":\"bar\"}"));
    }

    #[test]
    fn test_read() {
        let qname = "myqueue";
        let vt: i32 = 20;
        let limit: i32 = 1;

        let query = read(&qname, vt, limit).unwrap();

        assert!(query.contains(&qname));
        assert!(query.contains(&vt.to_string()));
    }

    #[test]
    fn check_input_rejects_names_too_large() {
        let table_name = "my_valid_table_name";
        assert!(check_input(table_name).is_ok());
        assert!(check_input(&"a".repeat(61)).is_ok());
        assert!(check_input(&"a".repeat(62)).is_err());
        assert!(check_input(&"a".repeat(64)).is_err());
        assert!(check_input(&"a".repeat(70)).is_err());
    }

    #[test]
    fn test_check_input() {
        let invalids = vec!["bad;queue_name", "bad name", "bad--name"];
        for i in invalids.iter() {
            let is_valid = check_input(i);
            assert!(is_valid.is_err())
        }
        let valids = vec!["good_queue", "greatqueue", "my_great_queue"];
        for i in valids.iter() {
            let is_valid = check_input(i);
            assert!(is_valid.is_ok())
        }
    }
}
