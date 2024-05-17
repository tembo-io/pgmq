CREATE TYPE pgmq.queue_record AS (
    queue_name VARCHAR,
    is_partitioned BOOLEAN,
    is_unlogged BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE
);

-- list queues
DROP FUNCTION IF EXISTS pgmq."list_queues"();

CREATE OR REPLACE FUNCTION pgmq."list_queues"()
RETURNS SETOF pgmq.queue_record AS $$
BEGIN
  RETURN QUERY SELECT * FROM pgmq.meta;
END
$$ LANGUAGE plpgsql;

-- purge queue, deleting all entries in it.
CREATE OR REPLACE FUNCTION pgmq."purge_queue"(queue_name TEXT)
RETURNS BIGINT AS $$
DECLARE
  deleted_count INTEGER;
BEGIN
  EXECUTE format('DELETE FROM pgmq.q_%s', queue_name);
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END
$$ LANGUAGE plpgsql;

-- unassign archive, so it can be kept when a queue is deleted
CREATE OR REPLACE FUNCTION pgmq."detach_archive"(queue_name TEXT)
RETURNS VOID AS $$
BEGIN
  EXECUTE format('ALTER EXTENSION pgmq DROP TABLE pgmq.a_%s', queue_name);
END
$$ LANGUAGE plpgsql;

-- pop a single message
DROP FUNCTION IF EXISTS pgmq."pop"(TEXT);
CREATE OR REPLACE FUNCTION pgmq.pop(queue_name TEXT)
RETURNS pgmq.message_record AS $$
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
    EXECUTE sql INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Sets vt of a message, returns it
DROP FUNCTION IF EXISTS pgmq."set_vt"(TEXT, BIGINT, INTEGER);
CREATE OR REPLACE FUNCTION pgmq.set_vt(queue_name TEXT, msg_id BIGINT, vt INTEGER)
RETURNS pgmq.message_record AS $$
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
    EXECUTE sql INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.drop_queue(queue_name TEXT, partitioned BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN AS $$
BEGIN
    EXECUTE FORMAT(
        $QUERY$
        ALTER EXTENSION pgmq DROP TABLE pgmq.q_%s
        $QUERY$,
        queue_name
    );

    EXECUTE FORMAT(
        $QUERY$
        ALTER EXTENSION pgmq DROP TABLE pgmq.a_%s
        $QUERY$,
        queue_name
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.q_%s
        $QUERY$,
        queue_name
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.a_%s
        $QUERY$,
        queue_name
    );

     IF EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_name = 'meta' and table_schema = 'pgmq'
     ) THEN
        EXECUTE FORMAT(
            $QUERY$
            DELETE FROM pgmq.meta WHERE queue_name = '%s'
            $QUERY$,
            queue_name
        );
     END IF;

     IF partitioned THEN
        EXECUTE FORMAT(
          $QUERY$
          DELETE FROM public.part_config where parent_table = '%s'
          $QUERY$,
          queue_name
        );
     END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.validate_queue_name(queue_name TEXT)
RETURNS void AS $$
BEGIN
  IF length(queue_name) >= 48 THEN
    RAISE EXCEPTION 'queue name is too long, maximum length is 48 characters';
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq._belongs_to_pgmq(table_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_depend
    WHERE refobjid = (SELECT oid FROM pg_extension WHERE extname = 'pgmq')
    AND objid = (
        SELECT oid
        FROM pg_class
        WHERE relname = table_name
    )
  ) INTO result;
  RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.create_non_partitioned(queue_name TEXT)
RETURNS void AS $$
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.q_%s (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    )
    $QUERY$,
    queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.a_%s (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    );
    $QUERY$,
    queue_name
  );

  IF NOT pgmq._belongs_to_pgmq(FORMAT('q_%s', queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.q_%s', queue_name);
  END IF;

  IF NOT pgmq._belongs_to_pgmq(FORMAT('a_%s', queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.a_%s', queue_name);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS q_%s_vt_idx ON pgmq.q_%s (vt ASC);
    $QUERY$,
    queue_name, queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS archived_at_idx_%s ON pgmq.a_%s (archived_at);
    $QUERY$,
    queue_name, queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES ('%s', false, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.create_unlogged(queue_name TEXT)
RETURNS void AS $$
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.q_%s (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    )
    $QUERY$,
    queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.a_%s (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    );
    $QUERY$,
    queue_name
  );

  IF NOT pgmq._belongs_to_pgmq(FORMAT('q_%s', queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.q_%s', queue_name);
  END IF;

  IF NOT pgmq._belongs_to_pgmq(FORMAT('a_%s', queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.a_%s', queue_name);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS q_%s_vt_idx ON pgmq.q_%s (vt ASC);
    $QUERY$,
    queue_name, queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS archived_at_idx_%s ON pgmq.a_%s (archived_at);
    $QUERY$,
    queue_name, queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES ('%s', false, true)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq._get_partition_col(partition_interval TEXT)
RETURNS TEXT AS $$
DECLARE
  num INTEGER;
BEGIN
    BEGIN
        num := partition_interval::INTEGER;
        RETURN 'msg_id';
    EXCEPTION
        WHEN others THEN
            RETURN 'enqueued_at';
    END;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq._ensure_pg_partman_installed()
RETURNS void AS $$
DECLARE
  extension_exists BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_extension
    WHERE extname = 'pg_partman'
  ) INTO extension_exists;

  IF NOT extension_exists THEN
    RAISE EXCEPTION 'pg_partman is required for partitioned queues';
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.create_partitioned(
  queue_name TEXT,
  partition_interval TEXT DEFAULT '10000',
  retention_interval TEXT DEFAULT '100000'
)
RETURNS void AS $$
DECLARE
  partition_col TEXT;
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);
  PERFORM pgmq._ensure_pg_partman_installed();
  SELECT pgmq._get_partition_col(partition_interval) INTO partition_col;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.q_%s (
        msg_id BIGINT GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    ) PARTITION BY RANGE (%s)
    $QUERY$,
    queue_name, partition_col
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.a_%s (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    );
    $QUERY$,
    queue_name
  );

  IF NOT pgmq._belongs_to_pgmq(FORMAT('q_%s', queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.q_%s', queue_name);
  END IF;

  IF NOT pgmq._belongs_to_pgmq(FORMAT('a_%s', queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.a_%s', queue_name);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    SELECT public.create_parent('pgmq.q_%s', '%s', 'native', '%s');
    $QUERY$,
    queue_name, partition_col, partition_interval
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS q_%s_part_idx ON pgmq.q_%s (%s);
    $QUERY$,
    queue_name, queue_name, partition_col
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS archived_at_idx_%s ON pgmq.a_%s (archived_at);
    $QUERY$,
    queue_name, queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE public.part_config
    SET
        retention = '%s',
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = 'pgmq.q_%s';
    $QUERY$,
    retention_interval, queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES ('%s', true, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION pgmq.create(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pgmq.create_non_partitioned(queue_name);
END;
$$ LANGUAGE plpgsql;
