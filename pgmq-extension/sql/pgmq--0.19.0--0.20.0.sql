DO $$
DECLARE
    qname TEXT;
BEGIN
    FOR qname IN (SELECT queue_name FROM public.pgmq_meta)
    LOOP
        EXECUTE format('ALTER TABLE %I RENAME COLUMN deleted_at TO archived_at', 'pgmq_' || qname || '_archive');
        EXECUTE format('ALTER INDEX IF EXISTS %I RENAME TO %I', 'deleted_at_idx_' || qname, 'archived_at_idx_' || qname);
    END LOOP;
END $$;
