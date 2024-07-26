--Create partition will also create partitioned archive
CREATE OR REPLACE FUNCTION pgmq.create_partitioned(
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

  IF NOT pgmq._belongs_to_pgmq(FORMAT('q_%s', queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.q_%s', queue_name);
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

  IF partition_col = 'enqueued_at' THEN
    a_partition_col := 'archived_at';
  ELSE
    a_partition_col := partition_col;
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.a_%s (
      msg_id BIGINT,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    ) PARTITION BY RANGE (%s);
    $QUERY$,
    queue_name, a_partition_col
  );

  IF NOT pgmq._belongs_to_pgmq(FORMAT('a_%s', queue_name)) THEN
      EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.a_%s', queue_name);
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    SELECT public.create_parent('pgmq.a_%s', '%s', 'native', '%s');
    $QUERY$,
    queue_name, a_partition_col, partition_interval
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE public.part_config
    SET
        retention = '%s',
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = 'pgmq.a_%s';
    $QUERY$,
    retention_interval, queue_name
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS archived_at_idx_%s ON pgmq.a_%s (archived_at);
    $QUERY$,
    queue_name, queue_name
  );

END;
$$ LANGUAGE plpgsql;

-- Add function to convert archive queues to partitioned tables
CREATE OR REPLACE FUNCTION pgmq.convert_archive_partitioned(table_name TEXT,
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
