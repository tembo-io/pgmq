CREATE TABLE pgmq.meta (
    queue_name VARCHAR UNIQUE NOT NULL,
    is_partitioned BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

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
