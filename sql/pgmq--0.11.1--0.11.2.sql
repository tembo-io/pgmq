DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOR table_name IN (SELECT queue_name FROM public.pgmq_meta)
    LOOP
        EXECUTE format('ALTER TABLE %I ALTER COLUMN enqueued_at SET DEFAULT now()', 'pgmq_' || table_name);
        EXECUTE format('ALTER TABLE %I ALTER COLUMN enqueued_at SET DEFAULT now()', 'pgmq_' || table_name || '_archive');
        EXECUTE format('ALTER TABLE %I ALTER COLUMN deleted_at SET DEFAULT now()', 'pgmq_' || table_name || '_archive');

    END LOOP;
END $$; 
