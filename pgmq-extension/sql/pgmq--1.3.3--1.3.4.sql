-- Add function to convert archive queues to partitioned tables
CREATE FUNCTION pgmq.convert_archive_partitioned(table_name TEXT,
                                                 partition_interval TEXT DEFAULT '10000',
                                                 retention_interval TEXT DEFAULT '100000',
                                                 leading_partition INT DEFAULT 10)
RETURNS void AS $$
DECLARE
a_table_name TEXT;
a_table_name_old TEXT;
qualified_a_table_name TEXT;
qualified_a_table_name_old TEXT;
BEGIN
  PERFORM pgmq._ensure_pg_partman_installed();
  SELECT FORMAT('%s', 'a_'|| table_name) INTO a_table_name;
  SELECT FORMAT('%s', 'a_'|| table_name || '_old') INTO a_table_name_old;
  SELECT FORMAT('%s.%s', 'pgmq','a_'|| table_name) INTO qualified_a_table_name;
  SELECT FORMAT('%s.%s', 'pgmq','a_'|| table_name || '_old') INTO qualified_a_table_name_old;

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

  EXECUTE 'CREATE TABLE '
            || qualified_a_table_name
            || '( msg_id BIGINT PRIMARY KEY,
                  read_ct INT DEFAULT 0 NOT NULL,
                  enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
                  archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
                  vt TIMESTAMP WITH TIME ZONE NOT NULL,
                  message JSONB
                ) PARTITION BY RANGE (msg_id)';

    EXECUTE 'ALTER INDEX pgmq.archived_at_idx_' || table_name || ' RENAME TO archived_at_idx_' || table_name || '_old';
    EXECUTE 'CREATE INDEX archived_at_idx_'|| table_name || ' ON ' || qualified_a_table_name ||'(archived_at)';

    PERFORM create_parent(p_parent_table := qualified_a_table_name,
                         p_control := 'msg_id', p_interval := partition_interval,
                         p_premake := leading_partition);

    UPDATE part_config
      SET retention = retention_interval,
      retention_keep_table = false,
      retention_keep_index = false,
      infinite_time_partitions = true
      WHERE parent_table = qualified_a_table_name;
END;
$$ LANGUAGE plpgsql;
