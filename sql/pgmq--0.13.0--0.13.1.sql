-- pgmq_meta wasn't necessarily existent in this version, so we need to check if
-- the table exists
DO $$
DECLARE
    table_name TEXT;
BEGIN
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'pgmq_meta') THEN
      FOR table_name IN (SELECT queue_name FROM public.pgmq_meta)
      LOOP
          EXECUTE format('ALTER EXTENSION pgmq ADD TABLE %I', 'pgmq_' || table_name);
          EXECUTE format('ALTER EXTENSION pgmq ADD TABLE %I', 'pgmq_' || table_name || '_archive');
      END LOOP;
      EXECUTE format('ALTER EXTENSION pgmq ADD TABLE %I', 'pgmq_meta');
    END IF;
END $$;
