DO $$
DECLARE
    qname TEXT;
BEGIN
    ALTER TABLE pgmq_meta ADD COLUMN is_partitioned BOOLEAN;

    IF 'pg_partman' NOT IN (SELECT extname FROM pg_extension) THEN
        UPDATE pgmq_meta SET is_partitioned = false;
    ELSE
        FOR qname IN (SELECT queue_name FROM pgmq_meta)
        LOOP
            -- If qname is in public.part_config it must be a partitioned table.
            IF format('public.pgmq_%1$I', qname) IN (SELECT parent_table FROM public.part_config) THEN
                UPDATE pgmq_meta SET is_partitioned = true WHERE queue_name = qname;
            ELSE
                UPDATE pgmq_meta SET is_partitioned = false WHERE queue_name = qname;
            END IF;
        END LOOP;
    END IF;

    ALTER TABLE pgmq_meta ALTER COLUMN is_partitioned SET NOT NULL;
END $$;
