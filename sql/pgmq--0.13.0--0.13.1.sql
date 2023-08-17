DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOR table_name IN (SELECT queue_name FROM public.pgmq_meta)
    LOOP
        EXECUTE format('ALTER EXTENSION pgmq ADD TABLE %I', 'pgmq_' || table_name);
        EXECUTE format('ALTER EXTENSION pgmq ADD TABLE %I', 'pgmq_' || table_name || '_archive');
    END LOOP;
EXECUTE format('ALTER EXTENSION pgmq ADD TABLE %I', 'pgmq_meta');
END $$;