CREATE TABLE IF NOT EXISTS public.pgmq_meta (
    queue_name VARCHAR UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (now() at time zone 'utc') NOT NULL
)

IF NOT EXISTS (
  SELECT 1
  WHERE has_table_privilege('pg_monitor', 'public.pgmq_meta', 'SELECT')
) THEN
  EXECUTE 'GRANT SELECT ON public.pgmq_meta TO pg_monitor';
END IF;
