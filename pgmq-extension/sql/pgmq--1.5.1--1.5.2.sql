DROP FUNCTION pgmq."validate_queue_name";
CREATE FUNCTION pgmq.validate_queue_name(queue_name TEXT)
RETURNS void AS $$
BEGIN
  IF length(queue_name) > 47 THEN
    -- complete table identifier must be <= 63
    -- https://www.postgresql.org/docs/17/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    -- e.g. template_pgmq_q_my_queue is an identifier for my_queue when partitioned
    -- template_pgmq_q_ (16) + <a max length queue name> (47) = 63 
    RAISE EXCEPTION 'queue name is too long, maximum length is 47 characters';
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION pgmq.drop_queue(TEXT);
CREATE FUNCTION pgmq.drop_queue(queue_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    qtable_seq TEXT := qtable || '_msg_id_seq';
    fq_qtable TEXT := 'pgmq.' || qtable;
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
    fq_atable TEXT := 'pgmq.' || atable;
    partitioned BOOLEAN;
BEGIN
    EXECUTE FORMAT(
        $QUERY$
        SELECT is_partitioned FROM pgmq.meta WHERE queue_name = %L
        $QUERY$,
        queue_name
    ) INTO partitioned;

    -- check if the queue exists
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = qtable and table_schema = 'pgmq'
    ) THEN
        RAISE NOTICE 'pgmq queue `%` does not exist', queue_name;
        RETURN FALSE;
    END IF;

    IF pgmq._extension_exists('pgmq') THEN
        EXECUTE FORMAT(
            $QUERY$
            ALTER EXTENSION pgmq DROP TABLE pgmq.%I
            $QUERY$,
            qtable
        );

        EXECUTE FORMAT(
            $QUERY$
            ALTER EXTENSION pgmq DROP SEQUENCE pgmq.%I
            $QUERY$,
            qtable_seq
        );

        EXECUTE FORMAT(
            $QUERY$
            ALTER EXTENSION pgmq DROP TABLE pgmq.%I
            $QUERY$,
            atable
        );
    END IF;

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.%I
        $QUERY$,
        qtable
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.%I
        $QUERY$,
        atable
    );

     IF EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_name = 'meta' and table_schema = 'pgmq'
     ) THEN
        EXECUTE FORMAT(
            $QUERY$
            DELETE FROM pgmq.meta WHERE queue_name = %L
            $QUERY$,
            queue_name
        );
     END IF;

     IF partitioned THEN
        EXECUTE FORMAT(
          $QUERY$
          DELETE FROM %I.part_config where parent_table in (%L, %L)
          $QUERY$,
          pgmq._get_pg_partman_schema(), fq_qtable, fq_atable
        );
     END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
