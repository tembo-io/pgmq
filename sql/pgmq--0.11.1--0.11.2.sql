-- pgmq_meta wasn't necessarily existent in this version, so we need to check if
-- the table exists
DO $$
DECLARE
    table_name TEXT;
BEGIN
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'pgmq_meta') THEN
      FOR table_name IN (SELECT queue_name FROM public.pgmq_meta)
      LOOP
          EXECUTE format('ALTER TABLE %I ALTER COLUMN enqueued_at SET DEFAULT now()', 'pgmq_' || table_name);
          EXECUTE format('ALTER TABLE %I ALTER COLUMN enqueued_at SET DEFAULT now()', 'pgmq_' || table_name || '_archive');
          EXECUTE format('ALTER TABLE %I ALTER COLUMN deleted_at SET DEFAULT now()', 'pgmq_' || table_name || '_archive');
      END LOOP;
    END IF;
END $$;
