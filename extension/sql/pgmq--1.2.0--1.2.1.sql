DROP FUNCTION pgmq.set_vt(text, bigint, integer);
CREATE FUNCTION pgmq.set_vt(queue_name TEXT, msg_id BIGINT, vt INTEGER)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    result pgmq.message_record;
BEGIN
    sql := FORMAT(
        $QUERY$
        UPDATE pgmq.q_%s
        SET vt = (now() + interval '%s seconds')
        WHERE msg_id = %s
        RETURNING *;
        $QUERY$,
        queue_name, vt, msg_id
    );
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION pgmq.pop(text);
CREATE FUNCTION pgmq.pop(queue_name TEXT)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    result pgmq.message_record;
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
            (
                SELECT msg_id
                FROM pgmq.q_%s
                WHERE vt <= now()
                ORDER BY msg_id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
        DELETE from pgmq.q_%s
        WHERE msg_id = (select msg_id from cte)
        RETURNING *;
        $QUERY$,
        queue_name, queue_name
    );
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;