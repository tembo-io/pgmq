DO $$
DECLARE
    qname TEXT;
    seqname TEXT;
BEGIN
    FOR qname IN (SELECT queue_name FROM pgmq_meta)
    LOOP
        EXECUTE format('ALTER TABLE public.pgmq_%I ALTER COLUMN msg_id DROP DEFAULT;', qname);
        EXECUTE format('ALTER TABLE public.pgmq_%I_archive ALTER COLUMN msg_id DROP DEFAULT;', qname);
    END LOOP;

    FOR seqname IN (SELECT relname FROM pg_class WHERE relkind = 'S' AND relname LIKE 'pgmq_%')
    LOOP
        EXECUTE format('DROP SEQUENCE %1$I CASCADE;', seqname);
    END LOOP;

    FOR qname IN (SELECT queue_name FROM pgmq_meta)
    LOOP
        EXECUTE format('ALTER TABLE public.pgmq_%I ALTER COLUMN msg_id ADD GENERATED ALWAYS AS IDENTITY;', qname);
    END LOOP;
END;
$$ LANGUAGE plpgsql;
