------------------------------------------------------------
-- Schema, tables, records, privileges, indexes, etc
------------------------------------------------------------
-- We don't need to create the `pgmq` schema because it is automatically
-- created by postgres due to being declared in extension control file

-- Table where queues and metadata about them is stored
CREATE TABLE pgmq.meta (
    queue_name VARCHAR UNIQUE NOT NULL,
    is_partitioned BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- This type has the shape of a message in a queue, and is often returned by
-- pgmq functions that return messages
CREATE TYPE pgmq.message_record AS (
    msg_id BIGINT,
    read_ct INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    vt TIMESTAMP WITH TIME ZONE,
    message JSONB
);

-- pg_monitor should have select access to everything in pgmq schema
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    WHERE has_table_privilege('pg_monitor', 'pgmq.meta', 'SELECT')
  ) THEN
    EXECUTE 'GRANT SELECT ON pgmq.meta TO pg_monitor';
  END IF;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- Functions
------------------------------------------------------------
-- read
-- reads a number of messages from a queue, setting a visibility timeout on them
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
            FROM pgmq.q_%s
            WHERE vt <= clock_timestamp()
            ORDER BY msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.q_%s
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
              FROM pgmq.q_%s
              WHERE vt <= clock_timestamp()
              ORDER BY msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pgmq.q_%s m
          SET
              vt = clock_timestamp() + interval '$2 seconds',
              read_ct = read_ct + 1
          FROM cte
          WHERE m.msg_id = cte.msg_id
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
