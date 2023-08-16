DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOR table_name IN (SELECT queue_name FROM public.pgmq_meta)
    LOOP
        EXECUTE format('ALTER EXTENSION pgmq ADD TABLE %I', 'pgmq_' || table_name);
        EXECUTE format('ALTER TABLE %I set SCHEMA pgmq', 'pgmq_' || table_name);
    END LOOP;
ALTER TABLE public.pgmq_meta set SCHEMA pgmq;
END $$;