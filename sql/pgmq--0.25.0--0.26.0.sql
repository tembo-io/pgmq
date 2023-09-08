-- Dropping rust impls
DROP FUNCTION pgmq.read(text, integer, integer);
DROP FUNCTION pgmq.read_with_poll(text, integer, integer, integer, integer);

-- Adding message record type, required for the functions
CREATE TYPE pgmq.message_record AS (
    msg_id BIGINT,
    read_ct INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    vt TIMESTAMP WITH TIME ZONE,
    message JSONB
);

-- Adding PL/pgSQL impls
-- Copy-pasted from pgrx schema
CREATE FUNCTION pgmq.read(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
        (
            SELECT msg_id
            FROM pgmq.queue_%s
            WHERE vt <= clock_timestamp()
            ORDER BY msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.queue_%s
        SET
            vt = clock_timestamp() + interval '$2 seconds',
            read_ct = read_ct + 1
        WHERE msg_id in (select msg_id from cte)
        RETURNING *;
        $QUERY$,
        queue_name, queue_name
    );
    RETURN QUERY EXECUTE sql USING qty, vt;
END;
$$ LANGUAGE plpgsql;

---- read_with_poll
---- reads a number of messages from a queue, setting a visibility timeout on them
CREATE FUNCTION pgmq.read_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    r pgmq.message_record;
    stop_at TIMESTAMP;
    sql TEXT;
BEGIN
    stop_at := clock_timestamp() + FORMAT('%s seconds', max_poll_seconds)::interval;
    LOOP
      IF (SELECT clock_timestamp() >= stop_at) THEN
        RETURN;
      END IF;

      sql := FORMAT(
          $QUERY$
          WITH cte AS
          (
              SELECT msg_id
              FROM pgmq.queue_%s
              WHERE vt <= clock_timestamp()
              ORDER BY msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pgmq.queue_%s
          SET
              vt = clock_timestamp() + interval '$2 seconds',
              read_ct = read_ct + 1
          WHERE msg_id in (select msg_id from cte)
          RETURNING *;
          $QUERY$,
          queue_name, queue_name
      );

      FOR r IN
        EXECUTE sql USING qty, vt
      LOOP
        RETURN NEXT r;
      END LOOP;
      IF FOUND THEN
        RETURN;
      ELSE
        PERFORM pg_sleep(poll_interval_ms / 1000);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

