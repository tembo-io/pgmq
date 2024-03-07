-- Dropping removed functions
DROP FUNCTION pgmq.send(text, jsonb, integer);
DROP FUNCTION pgmq.send_batch(text, jsonb[], integer);

-- Adding new implementations
-- send
-- sends a message to a queue, optionally with a delay
CREATE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    delay INTEGER DEFAULT 0
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pgmq.q_%s (vt, message)
        VALUES ((clock_timestamp() + interval '%s seconds'), $1)
        RETURNING msg_id;
        $QUERY$,
        queue_name, delay
    );
    RETURN QUERY EXECUTE sql USING msg;
END;
$$ LANGUAGE plpgsql;

-- send_batch
-- sends an array of list of messages to a queue, optionally with a delay
CREATE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    delay INTEGER DEFAULT 0
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pgmq.q_%s (vt, message)
        SELECT clock_timestamp() + interval '%s seconds', unnest($1)
        RETURNING msg_id;
        $QUERY$,
        queue_name, delay
    );
    RETURN QUERY EXECUTE sql USING msgs;
END;
$$ LANGUAGE plpgsql;
