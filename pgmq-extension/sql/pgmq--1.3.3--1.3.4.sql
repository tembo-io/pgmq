-- Add function to convert archive queues to partitioned tables
CREATE FUNCTION pgmq.convert_archive_partitioned(table_name TEXT,
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
