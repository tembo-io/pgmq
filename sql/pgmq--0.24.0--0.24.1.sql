DO $$
DECLARE
    qname TEXT;
    l_value BIGINT;
BEGIN
    FOR qname IN (SELECT queue_name FROM pgmq_meta)
    LOOP
        EXECUTE format('ALTER TABLE public.pgmq_%I ALTER COLUMN msg_id DROP DEFAULT;', qname);
        EXECUTE format('ALTER TABLE public.pgmq_%I_archive ALTER COLUMN msg_id DROP DEFAULT;', qname);
        EXECUTE format('SELECT last_value FROM public.pgmq_%I_msg_id_seq;', qname) INTO l_value;
        EXECUTE format('DROP SEQUENCE public.pgmq_%I_msg_id_seq CASCADE;', qname);
        EXECUTE format('ALTER TABLE public.pgmq_%I ALTER COLUMN msg_id ADD GENERATED ALWAYS AS IDENTITY;', qname);
        EXECUTE format('SELECT setval(pg_get_serial_sequence(''public.pgmq_%I'', ''msg_id''), %s);', qname, l_value);
    END LOOP;
END;
$$ LANGUAGE plpgsql;
