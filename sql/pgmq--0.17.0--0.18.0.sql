DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'pgmq_meta') THEN
    CREATE TABLE public.pgmq_meta (
        queue_name VARCHAR UNIQUE NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
    );

    IF NOT EXISTS (
      SELECT 1
      WHERE has_table_privilege('pg_monitor', 'public.pgmq_meta', 'SELECT')
    ) THEN
      EXECUTE 'GRANT SELECT ON public.pgmq_meta TO pg_monitor';
    END IF;
  END IF;
END;
$$ LANGUAGE plpgsql;
