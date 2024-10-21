-- purge queue, deleting all entries in it.
CREATE OR REPLACE FUNCTION pgmq."purge_queue"(queue_name TEXT)
RETURNS BIGINT AS $$
DECLARE
  deleted_count INTEGER;
  qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
  -- Get the row count before truncating
  EXECUTE format('SELECT count(*) FROM pgmq.%I', qtable) INTO deleted_count;

  -- Use TRUNCATE for better performance on large tables
  EXECUTE format('TRUNCATE TABLE pgmq.%I', qtable);

  -- Return the number of purged rows
  RETURN deleted_count;
END
$$ LANGUAGE plpgsql;