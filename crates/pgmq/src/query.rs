pub const TABLE_PREFIX: &str = r#"pgmq"#;

pub fn create(name: &str) -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {TABLE_PREFIX}_{name} (
            msg_id BIGSERIAL,
            vt TIMESTAMP WITH TIME ZONE,
            message JSON
        );
        "
    )
}

pub fn create_index(name: &str) -> String {
    format!(
        "
        CREATE INDEX IF NOT EXISTS vt_idx_{name} ON {TABLE_PREFIX}_{name} (vt ASC);
        "
    )
}

pub fn enqueue(name: &str, message: &serde_json::Value) -> String {
    // TOOO: vt should be now() + delay
    format!(
        "
        INSERT INTO {TABLE_PREFIX}_{name} (vt, message)
        VALUES (now() at time zone 'utc', '{message}'::json)
        RETURNING msg_id;
        "
    )
}
pub fn enqueue_str(name: &str, message: &str) -> String {
    // TOOO: vt should be now() + delay
    format!(
        "
        INSERT INTO {TABLE_PREFIX}_{name} (vt, message)
        VALUES (now() at time zone 'utc', '{message}'::json)
        RETURNING msg_id;
        "
    )
}

pub fn read(name: &str, vt: &i32) -> String {
    format!(
        "
    WITH cte AS
        (
            SELECT msg_id, vt, message
            FROM {TABLE_PREFIX}_{name}
            WHERE vt <= now() at time zone 'utc'
            ORDER BY msg_id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
    UPDATE {TABLE_PREFIX}_{name}
    SET vt = (now() at time zone 'utc' + interval '{vt} seconds')
    WHERE msg_id = (select msg_id from cte)
    RETURNING *;
    "
    )
}

pub fn delete(name: &str, msg_id: &i64) -> String {
    format!(
        "
        DELETE FROM {TABLE_PREFIX}_{name}
        WHERE msg_id = {msg_id};
        "
    )
}

pub fn pop(name: &str) -> String {
    format!(
        "
        WITH cte AS
            (
                SELECT msg_id, vt, message
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
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let query = create("yolo");
        assert!(query.contains("pgmq_yolo"));
    }

    #[test]
    fn test_enqueue() {
        let msg = serde_json::json!({
            "foo": "bar"
        });
        let query = enqueue("yolo", &msg);
        assert!(query.contains("pgmq_yolo"));
        assert!(query.contains("{\"foo\":\"bar\"}"));
    }

    #[test]
    fn test_read() {
        let qname = "myqueue";
        let vt: i32 = 20;

        let query = read(&qname, &vt);

        assert!(query.contains(&qname));
        assert!(query.contains(&vt.to_string()));
    }

    #[test]
    fn test_delete() {
        let qname = "myqueue";
        let msg_id: i64 = 42;

        let query = delete(&qname, &msg_id);

        assert!(query.contains(&qname));
        assert!(query.contains(&msg_id.to_string()));
    }
}
