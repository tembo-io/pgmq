-- Add conditional read
-- read
-- reads a number of messages from a queue, setting a visibility timeout on them
DROP FUNCTION IF EXISTS pgmq.read(TEXT, INTEGER, INTEGER);
CREATE FUNCTION pgmq.read(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    conditional JSONB DEFAULT '{}'
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
        (
            SELECT msg_id
            FROM pgmq.%I
            WHERE vt <= clock_timestamp() AND CASE
                WHEN %L != '{}'::jsonb THEN (message @> %2$L)::integer
                ELSE 1
            END = 1
            ORDER BY msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%I m
        SET
            vt = clock_timestamp() + %L,
            read_ct = read_ct + 1
        FROM cte
        WHERE m.msg_id = cte.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
        $QUERY$,
        qtable, conditional, qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

---- read_with_poll
---- reads a number of messages from a queue, setting a visibility timeout on them
DROP FUNCTION IF EXISTS pgmq.read_with_poll(TEXT, INTEGER, INTEGER, INTEGER, INTEGER);
CREATE FUNCTION pgmq.read_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100,
    conditional JSONB DEFAULT '{}'
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    r pgmq.message_record;
    stop_at TIMESTAMP;
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
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
              WHERE vt <= clock_timestamp() AND CASE
                  WHEN %L != '{}'::jsonb THEN (message @> %2$L)::integer
                  ELSE 1
              END = 1
              ORDER BY msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pgmq.%I m
          SET
              vt = clock_timestamp() + %L,
              read_ct = read_ct + 1
          FROM cte
          WHERE m.msg_id = cte.msg_id
          RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
          $QUERY$,
          qtable, conditional, qtable, make_interval(secs => vt)
      );

      FOR r IN
        EXECUTE sql USING qty
      LOOP
        RETURN NEXT r;
      END LOOP;
      IF FOUND THEN
        RETURN;
      ELSE
        PERFORM pg_sleep(poll_interval_ms::numeric / 1000);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS pgmq.drop_queue(TEXT, BOOLEAN);

CREATE FUNCTION pgmq.drop_queue(queue_name TEXT, partitioned BOOLEAN)
RETURNS BOOLEAN AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    fq_qtable TEXT := 'pgmq.' || qtable;
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
    fq_atable TEXT := 'pgmq.' || atable;
BEGIN
    RAISE WARNING 'drop_queue(queue_name, partitioned) is deprecated and will be removed in PGMQ v2.0. Use drop_queue(queue_name) instead.';

    PERFORM pgmq.drop_queue(queue_name);

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.drop_queue(queue_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    qtable_seq TEXT := qtable || '_msg_id_seq';
    fq_qtable TEXT := 'pgmq.' || qtable;
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
    fq_atable TEXT := 'pgmq.' || atable;
    partitioned BOOLEAN;
BEGIN
    EXECUTE FORMAT(
        $QUERY$
        SELECT is_partitioned FROM pgmq.meta WHERE queue_name = %L
        $QUERY$,
        queue_name
    ) INTO partitioned;

    EXECUTE FORMAT(
        $QUERY$
        ALTER EXTENSION pgmq DROP TABLE pgmq.%I
        $QUERY$,
        qtable
    );

    EXECUTE FORMAT(
        $QUERY$
        ALTER EXTENSION pgmq DROP SEQUENCE pgmq.%I
        $QUERY$,
        qtable_seq
    );

    EXECUTE FORMAT(
        $QUERY$
        ALTER EXTENSION pgmq DROP TABLE pgmq.%I
        $QUERY$,
        atable
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.%I
        $QUERY$,
        qtable
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.%I
        $QUERY$,
        atable
    );

     IF EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_name = 'meta' and table_schema = 'pgmq'
     ) THEN
        EXECUTE FORMAT(
            $QUERY$
            DELETE FROM pgmq.meta WHERE queue_name = %L
            $QUERY$,
            queue_name
        );
     END IF;

     IF partitioned THEN
        EXECUTE FORMAT(
          $QUERY$
          DELETE FROM %I.part_config where parent_table in (%L, %L)
          $QUERY$,
          pgmq._get_pg_partman_schema(), fq_qtable, fq_atable
        );
     END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.create_non_partitioned(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q');
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB,
        headers JSONB
    )
    $QUERY$,
    qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB,
      headers JSONB
    );
    $QUERY$,
    atable
  );

  IF NOT pgmq._belongs_to_pgmq(qtable) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', qtable);
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD SEQUENCE pgmq.%I', qtable_seq);
  END IF;

  IF NOT pgmq._belongs_to_pgmq(atable) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', atable);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC);
    $QUERY$,
    qtable || '_vt_idx', qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, false, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.create_unlogged(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q');
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);
  EXECUTE FORMAT(
    $QUERY$
    CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB,
        headers JSONB
    )
    $QUERY$,
    qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB,
      headers JSONB
    );
    $QUERY$,
    atable
  );

  IF NOT pgmq._belongs_to_pgmq(qtable) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', qtable);
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD SEQUENCE pgmq.%I', qtable_seq);
  END IF;

  IF NOT pgmq._belongs_to_pgmq(atable) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', atable);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC);
    $QUERY$,
    qtable || '_vt_idx', qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, false, true)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );
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
  a_partition_col TEXT;
  qtable TEXT := pgmq.format_table_name(queue_name, 'q');
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a');
  fq_qtable TEXT := 'pgmq.' || qtable;
  fq_atable TEXT := 'pgmq.' || atable;
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
        message JSONB,
        headers JSONB
    ) PARTITION BY RANGE (%I)
    $QUERY$,
    qtable, partition_col
  );

  IF NOT pgmq._belongs_to_pgmq(qtable) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', qtable);
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD SEQUENCE pgmq.%I', qtable_seq);
  END IF;

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  EXECUTE FORMAT(
    $QUERY$
    SELECT %I.create_parent(
      p_parent_table := %L,
      p_control := %L,
      p_interval := %L,
      p_type := case
        when pgmq._get_pg_partman_major_version() = 5 then 'range'
        else 'native'
      end
    )
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    fq_qtable,
    partition_col,
    partition_interval
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (%I);
    $QUERY$,
    qtable || '_part_idx', qtable, partition_col
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE %I.part_config
    SET
        retention = %L,
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = %L;
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    retention_interval,
    'pgmq.' || qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, true, false)
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
      msg_id BIGINT NOT NULL,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB,
      headers JSONB
    ) PARTITION BY RANGE (%I);
    $QUERY$,
    atable, a_partition_col
  );

  IF NOT pgmq._belongs_to_pgmq(atable) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.%I', atable);
  END IF;

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  EXECUTE FORMAT(
    $QUERY$
    SELECT %I.create_parent(
      p_parent_table := %L,
      p_control := %L,
      p_interval := %L,
      p_type := case
        when pgmq._get_pg_partman_major_version() = 5 then 'range'
        else 'native'
      end
    )
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    fq_atable,
    a_partition_col,
    partition_interval
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE %I.part_config
    SET
        retention = %L,
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = %L;
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    retention_interval,
    'pgmq.' || atable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

END;
$$ LANGUAGE plpgsql;

-- Add queue visible length to metrics
ALTER TYPE pgmq.metrics_result ADD ATTRIBUTE queue_visible_length bigint;

DROP FUNCTION pgmq.metrics(queue_name TEXT);

CREATE FUNCTION pgmq.metrics(queue_name TEXT)
RETURNS pgmq.metrics_result AS $$
DECLARE
    result_row pgmq.metrics_result;
    query TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    query := FORMAT(
        $QUERY$
        WITH q_summary AS (
            SELECT
                count(*) as queue_length,
                count(CASE WHEN vt <= NOW() THEN 1 END) as queue_visible_length,
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
            %L as queue_name,
            q_summary.queue_length,
            q_summary.newest_msg_age_sec,
            q_summary.oldest_msg_age_sec,
            all_metrics.total_messages,
            q_summary.scrape_time,
            q_summary.queue_visible_length
        FROM q_summary, all_metrics
        $QUERY$,
        qtable, qtable || '_msg_id_seq', queue_name
    );
    EXECUTE query INTO result_row;
    RETURN result_row;
END;
$$ LANGUAGE plpgsql;

-- Headers
-- Update types
ALTER TYPE pgmq.message_record ADD ATTRIBUTE headers JSONB;

-- Update functions
DROP FUNCTION pgmq.send(TEXT, JSONB, INTEGER);
DROP FUNCTION pgmq.send_batch(TEXT, JSONB[], INTEGER);
DROP FUNCTION pgmq.archive(TEXT, BIGINT);
DROP FUNCTION pgmq.archive(TEXT, BIGINT[]);

-- send: 2 args, no delay or headers
CREATE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send(queue_name, msg, NULL, clock_timestamp());
$$ LANGUAGE sql;

-- send: 3 args with headers
CREATE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    headers JSONB
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send(queue_name, msg, headers, clock_timestamp());
$$ LANGUAGE sql;

-- send: 3 args with integer delay
CREATE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    delay INTEGER
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send(queue_name, msg, NULL, clock_timestamp() + make_interval(secs => delay));
$$ LANGUAGE sql;

-- send: 3 args with timestamp
CREATE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    delay TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send(queue_name, msg, NULL, delay);
$$ LANGUAGE sql;

-- send: 4 args with integer delay
CREATE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    headers JSONB,
    delay INTEGER
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send(queue_name, msg, headers, clock_timestamp() + make_interval(secs => delay));
$$ LANGUAGE sql;

-- send: actual implementation
CREATE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    headers JSONB,
    delay TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pgmq.%I (vt, message, headers)
        VALUES ($2, $1, $3)
        RETURNING msg_id;
        $QUERY$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING msg, delay, headers;
END;
$$ LANGUAGE plpgsql;

-- send batch: 2 args
CREATE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[]
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send_batch(queue_name, msgs, NULL, clock_timestamp());
$$ LANGUAGE sql;

-- send batch: 3 args with headers
CREATE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    headers JSONB[]
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send_batch(queue_name, msgs, headers, clock_timestamp());
$$ LANGUAGE sql;

-- send batch: 3 args with integer delay
CREATE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    delay INTEGER
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send_batch(queue_name, msgs, NULL, clock_timestamp() + make_interval(secs => delay));
$$ LANGUAGE sql;

-- send batch: 3 args with timestamp
CREATE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    delay TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send_batch(queue_name, msgs, NULL, delay);
$$ LANGUAGE sql;

-- send_batch: 4 args with integer delay
CREATE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    headers JSONB[],
    delay INTEGER
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send_batch(queue_name, msgs, headers, clock_timestamp() + make_interval(secs => delay));
$$ LANGUAGE sql;

-- send_batch: actual implementation
CREATE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    headers JSONB[],
    delay TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pgmq.%I (vt, message, headers)
        SELECT $2, unnest($1), unnest(coalesce($3, ARRAY[]::jsonb[]))
        RETURNING msg_id;
        $QUERY$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING msgs, delay, headers;
END;
$$ LANGUAGE plpgsql;

-- archive
CREATE FUNCTION pgmq.archive(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pgmq.%I
            WHERE msg_id = ANY($1)
            RETURNING msg_id, vt, read_ct, enqueued_at, message, headers
        )
        INSERT INTO pgmq.%I (msg_id, vt, read_ct, enqueued_at, message, headers)
        SELECT msg_id, vt, read_ct, enqueued_at, message, headers
        FROM archived
        RETURNING msg_id;
        $QUERY$,
        qtable, atable
    );
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql;

-- archive
CREATE FUNCTION pgmq.archive(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pgmq.%I
            WHERE msg_id = $1
            RETURNING msg_id, vt, read_ct, enqueued_at, message, headers
        )
        INSERT INTO pgmq.%I (msg_id, vt, read_ct, enqueued_at, message, headers)
        SELECT msg_id, vt, read_ct, enqueued_at, message, headers
        FROM archived
        RETURNING msg_id;
        $QUERY$,
        qtable, atable
    );
    EXECUTE sql USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$ LANGUAGE plpgsql;

-- Update existing queues
DO $$
DECLARE
    queue_record RECORD;
    qtable TEXT;
    atable TEXT;
    qtable_seq TEXT;
BEGIN
    FOR queue_record IN SELECT queue_name FROM pgmq.meta LOOP
        qtable := pgmq.format_table_name(queue_record.queue_name, 'q');
        atable := pgmq.format_table_name(queue_record.queue_name, 'a');

        EXECUTE format(
            'ALTER TABLE pgmq.%I ADD COLUMN headers JSONB',
            qtable
        );

        EXECUTE format(
            'ALTER TABLE pgmq.%I ADD COLUMN headers JSONB',
            atable
        );

        -- Add all depending sequences to the extension to fix existing queues data
        -- See the PR https://github.com/tembo-io/pgmq/pull/352
        qtable_seq := qtable || '_msg_id_seq';

        EXECUTE FORMAT(
            $QUERY$
            ALTER EXTENSION pgmq ADD SEQUENCE pgmq.%I
            $QUERY$,
            qtable_seq
        );
    END LOOP;
END;
$$;
