CREATE OR REPLACE pgmq.read(
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
            FROM pgmq.%I
            WHERE vt <= clock_timestamp()
            ORDER BY msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%I m
        SET
            vt = clock_timestamp() + interval '%s seconds',
            read_ct = read_ct + 1
        FROM cte
        WHERE m.msg_id = cte.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message;
        $QUERY$,
        'q_' || queue_name, 'q_' || queue_name, vt
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE pgmq.read_with_poll(
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
              FROM pgmq.%I
              WHERE vt <= clock_timestamp()
              ORDER BY msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pgmq.%I m
          SET
              vt = clock_timestamp() + interval '%s seconds',
              read_ct = read_ct + 1
          FROM cte
          WHERE m.msg_id = cte.msg_id
          RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message;
          $QUERY$,
          'q_' || queue_name, 'q_' || queue_name, vt
      );

      FOR r IN
        EXECUTE sql USING qty
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

---- archive
---- removes a message from the queue, and sends it to the archive, where its
---- saved permanently.
CREATE OR REPLACE pgmq.archive(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pgmq.%I
            WHERE msg_id = $1
            RETURNING msg_id, vt, read_ct, enqueued_at, message
        )
        INSERT INTO pgmq.%I (msg_id, vt, read_ct, enqueued_at, message)
        SELECT msg_id, vt, read_ct, enqueued_at, message
        FROM archived
        RETURNING msg_id;
        $QUERY$,
        'q_' || queue_name, 'a_' || queue_name
    );
    EXECUTE sql USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$ LANGUAGE plpgsql;

---- archive
---- removes an array of message ids from the queue, and sends it to the archive,
---- where these messages will be saved permanently.
CREATE OR REPLACE pgmq.archive(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pgmq.%I
            WHERE msg_id = ANY($1)
            RETURNING msg_id, vt, read_ct, enqueued_at, message
        )
        INSERT INTO pgmq.%I (msg_id, vt, read_ct, enqueued_at, message)
        SELECT msg_id, vt, read_ct, enqueued_at, message
        FROM archived
        RETURNING msg_id;
        $QUERY$,
        'q_' || queue_name, 'a_' || queue_name
    );
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql;

---- delete
---- deletes a message id from the queue permanently
CREATE OR REPLACE pgmq.delete(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
BEGIN
    sql := FORMAT(
        $QUERY$
        DELETE FROM pgmq.%I
        WHERE msg_id = $1
        RETURNING msg_id
        $QUERY$,
        'q_' || queue_name
    );
    EXECUTE sql USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$ LANGUAGE plpgsql;

---- delete
---- deletes an array of message ids from the queue permanently
CREATE OR REPLACE pgmq.delete(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        DELETE FROM pgmq.%I
        WHERE msg_id = ANY($1)
        RETURNING msg_id
        $QUERY$,
        'q_' || queue_name
    );
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql;

-- send
-- sends a message to a queue, optionally with a delay
CREATE OR REPLACE pgmq.send(
    queue_name TEXT,
    msg JSONB,
    delay INTEGER DEFAULT 0
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pgmq.%I (vt, message)
        VALUES ((clock_timestamp() + interval '%s seconds'), $1)
        RETURNING msg_id;
        $QUERY$,
        'q_' || queue_name, delay
    );
    RETURN QUERY EXECUTE sql USING msg;
END;
$$ LANGUAGE plpgsql;

-- send_batch
-- sends an array of list of messages to a queue, optionally with a delay
CREATE OR REPLACE pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    delay INTEGER DEFAULT 0
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pgmq.%I (vt, message)
        SELECT clock_timestamp() + interval '%s seconds', unnest($1)
        RETURNING msg_id;
        $QUERY$,
        'q_' || queue_name, delay
    );
    RETURN QUERY EXECUTE sql USING msgs;
END;
$$ LANGUAGE plpgsql;

-- returned by pgmq.metrics() and pgmq.metrics_all
CREATE TYPE pgmq.metrics_result AS (
    queue_name text,
    queue_length bigint,
    newest_msg_age_sec int,
    oldest_msg_age_sec int,
    total_messages bigint,
    scrape_time timestamp with time zone
);

-- get metrics for a single queue
CREATE OR REPLACE pgmq.metrics(queue_name TEXT)
RETURNS pgmq.metrics_result AS $$
DECLARE
    result_row pgmq.metrics_result;
    query TEXT;
BEGIN
    query := FORMAT(
        $QUERY$
        WITH q_summary AS (
            SELECT
                count(*) as queue_length,
                EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int as newest_msg_age_sec,
                EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int as oldest_msg_age_sec,
                NOW() as scrape_time
            FROM pgmq.%I
        ),
        all_metrics AS (
            SELECT CASE
                WHEN is_called THEN last_value ELSE 0
                END as total_messages
            FROM pgmq.%I
        )
        SELECT
            '%I' as queue_name,
            q_summary.queue_length,
            q_summary.newest_msg_age_sec,
            q_summary.oldest_msg_age_sec,
            all_metrics.total_messages,
            q_summary.scrape_time
        FROM q_summary, all_metrics
        $QUERY$,
        'q_' || queue_name, 'q_' || queue_name | '_msg_id_seq', queue_name
    );
    EXECUTE query INTO result_row;
    RETURN result_row;
END;
$$ LANGUAGE plpgsql;

-- get metrics for all queues
CREATE OR REPLACE pgmq."metrics_all"()
RETURNS SETOF pgmq.metrics_result AS $$
DECLARE
    row_name RECORD;
    result_row pgmq.metrics_result;
BEGIN
    FOR row_name IN SELECT queue_name FROM pgmq.meta LOOP
        result_row := pgmq.metrics(row_name.queue_name);
        RETURN NEXT result_row;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- list queues
CREATE OR REPLACE pgmq."list_queues"()
RETURNS SETOF pgmq.queue_record AS $$
BEGIN
  RETURN QUERY SELECT * FROM pgmq.meta;
END
$$ LANGUAGE plpgsql;

-- purge queue, deleting all entries in it.
CREATE OR REPLACE pgmq."purge_queue"(queue_name TEXT)
RETURNS BIGINT AS $$
DECLARE
  deleted_count INTEGER;
BEGIN
  EXECUTE format('DELETE FROM pgmq.%I', 'q_' || queue_name);
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END
$$ LANGUAGE plpgsql;

-- unassign archive, so it can be kept when a queue is deleted
CREATE OR REPLACE pgmq."detach_archive"(queue_name TEXT)
RETURNS VOID AS $$
BEGIN
  EXECUTE format('ALTER EXTENSION pgmq DROP TABLE pgmq.%I', 'a_' || queue_name);
END
$$ LANGUAGE plpgsql;

-- pop a single message
CREATE OR REPLACE pgmq.pop(queue_name TEXT)
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
                FROM pgmq.%I
                WHERE vt <= now()
                ORDER BY msg_id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
        DELETE from pgmq.%I
        WHERE msg_id = (select msg_id from cte)
        RETURNING *;
        $QUERY$,
        'q_' || queue_name, 'q_' || queue_name
    );
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

-- Sets vt of a message, returns it
CREATE OR REPLACE pgmq.set_vt(queue_name TEXT, msg_id BIGINT, vt INTEGER)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    result pgmq.message_record;
BEGIN
    sql := FORMAT(
        $QUERY$
        UPDATE pgmq.%I
        SET vt = (now() + interval '%s seconds')
        WHERE msg_id = %s
        RETURNING *;
        $QUERY$,
        'q_' || queue_name, vt, msg_id
    );
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE pgmq.drop_queue(queue_name TEXT, partitioned BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN AS $$
BEGIN
    EXECUTE FORMAT(
        $QUERY$
        ALTER EXTENSION pgmq DROP TABLE pgmq.%I
        $QUERY$,
        'q_' || queue_name
    );

    EXECUTE FORMAT(
        $QUERY$
        ALTER EXTENSION pgmq DROP TABLE pgmq.%I
        $QUERY$,
        'a_' || queue_name
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.%I
        $QUERY$,
        'q_' || queue_name
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.%I
        $QUERY$,
        'a_' || queue_name
    );

     IF EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_name = 'meta' and table_schema = 'pgmq'
     ) THEN
        EXECUTE FORMAT(
            $QUERY$
            DELETE FROM pgmq.meta WHERE queue_name = '%L'
            $QUERY$,
            queue_name
        );
     END IF;

     IF partitioned THEN
        EXECUTE FORMAT(
          $QUERY$
          DELETE FROM public.part_config where parent_table = '%L'
          $QUERY$,
          queue_name
        );
     END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE pgmq.validate_queue_name(queue_name TEXT)
RETURNS void AS $$
BEGIN
  IF length(queue_name) >= 48 THEN
    RAISE EXCEPTION 'queue name is too long, maximum length is 48 characters';
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE pgmq._belongs_to_pgmq(table_name TEXT)
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

CREATE OR REPLACE pgmq.create_non_partitioned(queue_name TEXT)
RETURNS void AS $$
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    )
    $QUERY$,
    'q_' || queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    );
    $QUERY$,
    'a_' || queue_name
  );

  IF NOT pgmq._belongs_to_pgmq(FORMAT('%I', 'q_' || queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', 'q_' || queue_name);
  END IF;

  IF NOT pgmq._belongs_to_pgmq(FORMAT('%I', 'a_' || queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', 'a_' || queue_name);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC);
    $QUERY$,
    'q_' || queue_name || '_vt_idx', 'q_' || queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, 'a_' || queue_name
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

CREATE OR REPLACE pgmq.create_unlogged(queue_name TEXT)
RETURNS void AS $$
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    )
    $QUERY$,
    'q_' || queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    );
    $QUERY$,
    'a_' || queue_name
  );

  IF NOT pgmq._belongs_to_pgmq(FORMAT('%I', 'q_' || queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', 'q_' || queue_name);
  END IF;

  IF NOT pgmq._belongs_to_pgmq(FORMAT('%I', 'a_' || queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', 'a_' || queue_name);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC);
    $QUERY$,
    'q_' || queue_name || '_vt_idx', 'q_' || queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, 'a_' || queue_name
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

CREATE OR REPLACE pgmq._get_partition_col(partition_interval TEXT)
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

CREATE OR REPLACE pgmq._ensure_pg_partman_installed()
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

CREATE OR REPLACE pgmq.create_partitioned(
  queue_name TEXT,
  partition_interval TEXT DEFAULT '10000',
  retention_interval TEXT DEFAULT '100000'
)
RETURNS void AS $$
DECLARE
  partition_col TEXT;
  a_partition_col TEXT;
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);
  PERFORM pgmq._ensure_pg_partman_installed();
  SELECT pgmq._get_partition_col(partition_interval) INTO partition_col;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    ) PARTITION BY RANGE (%L)
    $QUERY$,
    'q_' || queue_name, partition_col
  );

  IF NOT pgmq._belongs_to_pgmq(FORMAT('%I', 'q_' || queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', 'q_' || queue_name);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    SELECT public.create_parent('pgmq.%I', '%L', 'native', '%L');
    $QUERY$,
    'q_' || queue_name, partition_col, partition_interval
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (Ls);
    $QUERY$,
    'q_' || queue_name || '_part_idx', 'q_' || queue_name, partition_col
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE public.part_config
    SET
        retention = '%L',
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = 'pgmq.q_%I';
    $QUERY$,
    retention_interval, 'q_' || queue_name
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

  IF partition_col = 'enqueued_at' THEN
    a_partition_col := 'archived_at';
  ELSE
    a_partition_col := partition_col;
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    ) PARTITION BY RANGE (%L);
    $QUERY$,
    'a_' || queue_name, a_partition_col
  );

  IF NOT pgmq._belongs_to_pgmq(FORMAT('%I', 'a_' || queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', 'a_' || queue_name);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    SELECT public.create_parent('pgmq.%I', '%L', 'native', '%L');
    $QUERY$,
    'a_' || queue_name, a_partition_col, partition_interval
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE public.part_config
    SET
        retention = '%L',
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = 'pgmq.&I';
    $QUERY$,
    retention_interval, 'a_' || queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, 'a_' || queue_name
  );

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE pgmq.create(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pgmq.create_non_partitioned(queue_name);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE pgmq.convert_archive_partitioned(table_name TEXT,
                                                 partition_interval TEXT DEFAULT '10000',
                                                 retention_interval TEXT DEFAULT '100000',
                                                 leading_partition INT DEFAULT 10)
RETURNS void AS $$
DECLARE
a_table_name TEXT := 'a_' || table_name;
a_table_name_old TEXT := 'a_'|| table_name || '_old';
qualified_a_table_name TEXT := format('%I.%I', 'pgmq', 'a_' || table_name);
qualified_a_table_name_old TEXT := format ('%I.%I', 'pgmq', 'a_' || table_name || '_old');
BEGIN

  PERFORM c.relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = a_table_name
    AND c.relkind = 'p';

  IF FOUND THEN
    RAISE NOTICE 'Table %s is already partitioned', a_table_name;
    RETURN;
  END IF;

  PERFORM c.relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = a_table_name
    AND c.relkind = 'r';

  IF NOT FOUND THEN
    RAISE NOTICE 'Table %s doesnot exists', a_table_name;
    RETURN;
  END IF;

  EXECUTE 'ALTER TABLE ' || qualified_a_table_name || ' RENAME TO ' || a_table_name_old;

  EXECUTE format( 'CREATE TABLE pgmq.%I (LIKE pgmq.%I including all) PARTITION BY RANGE (msg_id)', 'a_' || table_name, 'a_'|| table_name || '_old' );

  EXECUTE 'ALTER INDEX pgmq.archived_at_idx_' || table_name || ' RENAME TO archived_at_idx_' || table_name || '_old';
  EXECUTE 'CREATE INDEX archived_at_idx_'|| table_name || ' ON ' || qualified_a_table_name ||'(archived_at)';

  PERFORM create_parent(qualified_a_table_name, 'msg_id', 'native',  partition_interval,
                         p_premake := leading_partition);

  UPDATE part_config
    SET retention = retention_interval,
    retention_keep_table = false,
    retention_keep_index = false,
    infinite_time_partitions = true
    WHERE parent_table = qualified_a_table_name;
END;
$$ LANGUAGE plpgsql;