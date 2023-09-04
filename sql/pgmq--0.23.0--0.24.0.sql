DO $$
DECLARE
    qname TEXT;
BEGIN
    FOR qname IN (SELECT queue_name FROM public.pgmq_meta)
    LOOP
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON pgmq_%I_archive(archived_at)', 'archived_at_idx_' || qname, qname);
    END LOOP;
END
$$ LANGUAGE plpgsql;