------------------------------------------------------------
-- Schema, tables, records, privileges, indexes, etc
------------------------------------------------------------
-- We don't need to create the `pgmq` schema because it is automatically
-- created by postgres due to being declared in extension control file
-- Table where queues and metadata about them is stored
CREATE TABLE pgmq.meta (
    queue_name varchar UNIQUE NOT NULL,
    is_partitioned boolean NOT NULL,
    is_unlogged boolean NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

-- Grant permission to pg_monitor to all tables and sequences
GRANT USAGE ON SCHEMA pgmq TO pg_monitor;

GRANT SELECT ON ALL TABLES IN SCHEMA pgmq TO pg_monitor;

GRANT SELECT ON ALL SEQUENCES IN SCHEMA pgmq TO pg_monitor;

ALTER DEFAULT PRIVILEGES IN SCHEMA pgmq GRANT
SELECT
    ON TABLES TO pg_monitor;

ALTER DEFAULT PRIVILEGES IN SCHEMA pgmq GRANT
SELECT
    ON SEQUENCES TO pg_monitor;

-- This type has the shape of a message in a queue, and is often returned by
-- pgmq functions that return messages
CREATE TYPE pgmq.message_record AS (
    msg_id bigint,
    read_ct integer,
    enqueued_at timestamp with time zone,
    vt timestamp with time zone,
    message jsonb
);

CREATE TYPE pgmq.queue_record AS (
    queue_name varchar,
    is_partitioned boolean,
    is_unlogged boolean,
    created_at timestamp with time zone
);

------------------------------------------------------------
-- Functions
------------------------------------------------------------
-- read
-- reads a number of messages from a queue, setting a visibility timeout on them
CREATE FUNCTION pgmq.read (queue_name text, vt integer, qty integer)
    RETURNS SETOF pgmq.message_record
    AS $$
DECLARE
    sql text;
BEGIN
    sql := FORMAT($QUERY$ WITH cte AS (
            SELECT
                msg_id FROM pgmq.q_ % s
                WHERE
                    vt <= clock_timestamp()
            ORDER BY msg_id ASC LIMIT $1
            FOR UPDATE
                SKIP LOCKED)
        UPDATE
            pgmq.q_ % s m
        SET
            vt = clock_timestamp() + interval '%s seconds', read_ct = read_ct + 1 FROM cte
        WHERE
            m.msg_id = cte.msg_id
        RETURNING
            m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message;
    $QUERY$,
    queue_name,
    queue_name,
    vt);
    RETURN QUERY EXECUTE sql
    USING qty;
END;
$$
LANGUAGE plpgsql;

---- read_with_poll
---- reads a number of messages from a queue, setting a visibility timeout on them
CREATE FUNCTION pgmq.read_with_poll (queue_name text, vt integer, qty integer, max_poll_seconds integer DEFAULT 5, poll_interval_ms integer DEFAULT 100)
    RETURNS SETOF pgmq.message_record
    AS $$
DECLARE
    r pgmq.message_record;
    stop_at timestamp;
    sql text;
BEGIN
    stop_at := clock_timestamp() + FORMAT('%s seconds', max_poll_seconds)::interval;
    LOOP
        IF (
            SELECT
                clock_timestamp() >= stop_at) THEN
            RETURN;
        END IF;
        sql := FORMAT($QUERY$ WITH cte AS (
                SELECT
                    msg_id FROM pgmq.q_ % s
                    WHERE
                        vt <= clock_timestamp()
                ORDER BY msg_id ASC LIMIT $1
                FOR UPDATE
                    SKIP LOCKED)
            UPDATE
                pgmq.q_ % s m
            SET
                vt = clock_timestamp() + interval '%s seconds', read_ct = read_ct + 1 FROM cte
            WHERE
                m.msg_id = cte.msg_id
            RETURNING
                m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message;
        $QUERY$,
        queue_name,
        queue_name,
        vt);
        FOR r IN EXECUTE sql
        USING qty LOOP
            RETURN NEXT r;
        END LOOP;
        IF FOUND THEN
            RETURN;
        ELSE
            PERFORM
                pg_sleep(poll_interval_ms / 1000);
        END IF;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

---- archive
---- removes a message from the queue, and sends it to the archive, where its
---- saved permanently.
CREATE FUNCTION pgmq.archive (queue_name text, msg_id bigint)
    RETURNS boolean
    AS $$
DECLARE
    sql text;
    result bigint;
BEGIN
    sql := FORMAT($QUERY$ WITH archived AS ( DELETE FROM pgmq.q_ % s
            WHERE msg_id = $1
            RETURNING
                msg_id, vt, read_ct, enqueued_at, message)
        INSERT INTO pgmq.a_ % s (msg_id, vt, read_ct, enqueued_at, message)
        SELECT
            msg_id, vt, read_ct, enqueued_at, message FROM archived
        RETURNING
            msg_id;
    $QUERY$,
    queue_name,
    queue_name);
    EXECUTE sql
    USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$
LANGUAGE plpgsql;

---- archive
---- removes an array of message ids from the queue, and sends it to the archive,
---- where these messages will be saved permanently.
CREATE FUNCTION pgmq.archive (queue_name text, msg_ids bigint[])
    RETURNS SETOF BIGINT
    AS $$
DECLARE
    sql text;
BEGIN
    sql := FORMAT($QUERY$ WITH archived AS ( DELETE FROM pgmq.q_ % s
            WHERE msg_id = ANY ($1)
            RETURNING
                msg_id, vt, read_ct, enqueued_at, message)
        INSERT INTO pgmq.a_ % s (msg_id, vt, read_ct, enqueued_at, message)
        SELECT
            msg_id, vt, read_ct, enqueued_at, message FROM archived
        RETURNING
            msg_id;
    $QUERY$,
    queue_name,
    queue_name);
    RETURN QUERY EXECUTE sql
    USING msg_ids;
END;
$$
LANGUAGE plpgsql;

---- delete
---- deletes a message id from the queue permanently
CREATE FUNCTION pgmq.delete (queue_name text, msg_id bigint)
    RETURNS boolean
    AS $$
DECLARE
    sql text;
    result bigint;
BEGIN
    sql := FORMAT($QUERY$ DELETE FROM pgmq.q_ % s
        WHERE msg_id = $1
        RETURNING
            msg_id $QUERY$, queue_name);
    EXECUTE sql
    USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$
LANGUAGE plpgsql;

---- delete
---- deletes an array of message ids from the queue permanently
CREATE FUNCTION pgmq.delete (queue_name text, msg_ids bigint[])
    RETURNS SETOF BIGINT
    AS $$
DECLARE
    sql text;
BEGIN
    sql := FORMAT($QUERY$ DELETE FROM pgmq.q_ % s
        WHERE msg_id = ANY ($1)
        RETURNING
            msg_id $QUERY$, queue_name);
    RETURN QUERY EXECUTE sql
    USING msg_ids;
END;
$$
LANGUAGE plpgsql;

-- send
-- sends a message to a queue, optionally with a delay
CREATE FUNCTION pgmq.send (queue_name text, msg jsonb, delay integer DEFAULT 0)
    RETURNS SETOF BIGINT
    AS $$
DECLARE
    sql text;
BEGIN
    sql := FORMAT($QUERY$ INSERT INTO pgmq.q_ % s (vt, message)
            VALUES ((clock_timestamp() + interval '%s seconds'), $1)
RETURNING
    msg_id;
    $QUERY$,
    queue_name,
    delay);
    RETURN QUERY EXECUTE sql
    USING msg;
END;
$$
LANGUAGE plpgsql;

-- send_batch
-- sends an array of list of messages to a queue, optionally with a delay
CREATE FUNCTION pgmq.send_batch (queue_name text, msgs jsonb[], delay integer DEFAULT 0)
    RETURNS SETOF BIGINT
    AS $$
DECLARE
    sql text;
BEGIN
    sql := FORMAT($QUERY$ INSERT INTO pgmq.q_ % s (vt, message)
        SELECT
            clock_timestamp() + interval '%s seconds', unnest($1)
    RETURNING
        msg_id;
    $QUERY$,
    queue_name,
    delay);
    RETURN QUERY EXECUTE sql
    USING msgs;
END;
$$
LANGUAGE plpgsql;

-- returned by pgmq.metrics() and pgmq.metrics_all
CREATE TYPE pgmq.metrics_result AS (
    queue_name text,
    queue_length bigint,
    newest_msg_age_sec int,
    oldest_msg_age_sec int,
    total_messages bigint,
    scrape_time timestamp with time zone
);

-- get metrics for a single queue
CREATE OR REPLACE FUNCTION pgmq.metrics (queue_name text)
    RETURNS pgmq.metrics_result
    AS $$
DECLARE
    result_row pgmq.metrics_result;
    query text;
BEGIN
    query := FORMAT($QUERY$ WITH q_summary AS (
            SELECT
                count(*) AS queue_length, EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int AS newest_msg_age_sec, EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int AS oldest_msg_age_sec, NOW() AS scrape_time FROM pgmq.q_ % s
), all_metrics AS (
    SELECT
        CASE WHEN is_called THEN
            last_value
        ELSE
            0
        END AS total_messages FROM pgmq.q_ % s_msg_id_seq)
SELECT
    '%s' AS queue_name, q_summary.queue_length, q_summary.newest_msg_age_sec, q_summary.oldest_msg_age_sec, all_metrics.total_messages, q_summary.scrape_time FROM q_summary, all_metrics $QUERY$, queue_name, queue_name, queue_name);
    EXECUTE query INTO result_row;
    RETURN result_row;
END;
$$
LANGUAGE plpgsql;

-- get metrics for all queues
CREATE OR REPLACE FUNCTION pgmq."metrics_all" ()
    RETURNS SETOF pgmq.metrics_result
    AS $$
DECLARE
    row_name RECORD;
    result_row pgmq.metrics_result;
BEGIN
    FOR row_name IN
    SELECT
        queue_name
    FROM
        pgmq.meta LOOP
            result_row := pgmq.metrics (row_name.queue_name);
            RETURN NEXT result_row;
        END LOOP;
END;
$$
LANGUAGE plpgsql;

-- list queues
CREATE OR REPLACE FUNCTION pgmq."list_queues" ()
    RETURNS SETOF pgmq.queue_record
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        *
    FROM
        pgmq.meta;
END
$$
LANGUAGE plpgsql;

-- purge queue, deleting all entries in it.
CREATE OR REPLACE FUNCTION pgmq."purge_queue" (queue_name text)
    RETURNS bigint
    AS $$
DECLARE
    deleted_count integer;
BEGIN
    EXECUTE format('DELETE FROM pgmq.q_%s', queue_name);
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END
$$
LANGUAGE plpgsql;

-- unassign archive, so it can be kept when a queue is deleted
CREATE OR REPLACE FUNCTION pgmq."detach_archive" (queue_name text)
    RETURNS VOID
    AS $$
BEGIN
    EXECUTE format('ALTER EXTENSION pgmq DROP TABLE pgmq.a_%s', queue_name);
END
$$
LANGUAGE plpgsql;

-- pop a single message
CREATE FUNCTION pgmq.pop (queue_name text)
    RETURNS pgmq.message_record
    AS $$
DECLARE
    sql text;
    result pgmq.message_record;
BEGIN
    sql := FORMAT($QUERY$ WITH cte AS (
            SELECT
                msg_id FROM pgmq.q_ % s
                WHERE
                    vt <= now()
            ORDER BY msg_id ASC LIMIT 1
            FOR UPDATE
                SKIP LOCKED)
        DELETE FROM pgmq.q_ % s
        WHERE msg_id = (
                SELECT
                    msg_id
                FROM cte)
    RETURNING
        *;
    $QUERY$,
    queue_name,
    queue_name);
    EXECUTE sql INTO result;
    RETURN result;
END;
$$
LANGUAGE plpgsql;

-- Sets vt of a message, returns it
CREATE FUNCTION pgmq.set_vt (queue_name text, msg_id bigint, vt integer)
    RETURNS pgmq.message_record
    AS $$
DECLARE
    sql text;
    result pgmq.message_record;
BEGIN
    sql := FORMAT($QUERY$ UPDATE
            pgmq.q_ % s
        SET
            vt = (now() + interval '%s seconds')
        WHERE
            msg_id = % s
        RETURNING
            *;
    $QUERY$,
    queue_name,
    vt,
    msg_id);
    EXECUTE sql INTO result;
    RETURN result;
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION pgmq.drop_queue (queue_name text, is_partitioned boolean DEFAULT FALSE)
    RETURNS void
    AS $$
BEGIN
    EXECUTE FORMAT($QUERY$ ALTER EXTENSION pgmq
            DROP TABLE pgmq.q_ % s $QUERY$, queue_name);
    EXECUTE FORMAT($QUERY$ ALTER EXTENSION pgmq
            DROP TABLE pgmq.a_ % s $QUERY$, queue_name);
    EXECUTE FORMAT($QUERY$ DROP TABLE IF EXISTS pgmq.q_ % s $QUERY$, queue_name);
    EXECUTE FORMAT($QUERY$ DROP TABLE IF EXISTS pgmq.a_ % s $QUERY$, queue_name);
    IF EXISTS (
        SELECT
            1
        FROM
            information_schema.tables
        WHERE
            table_name = 'meta'
            AND table_schema = 'pgmq') THEN
    EXECUTE FORMAT($QUERY$ DELETE FROM pgmq.meta
        WHERE queue_name = '%s' $QUERY$, queue_name);
END IF;
    IF is_partitioned THEN
        EXECUTE FORMAT($QUERY$ DELETE FROM public.part_config
            WHERE parent_table = '%s' $QUERY$, queue_name);
    END IF;
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION pgmq.validate_queue_name (queue_name text)
    RETURNS void
    AS $$
BEGIN
    IF length(queue_name) >= 48 THEN
        RAISE EXCEPTION 'queue name is too long, maximum length is 48 characters';
    END IF;
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION pgmq._belongs_to_pgmq (table_name text)
    RETURNS boolean
    AS $$
DECLARE
    sql text;
    result boolean;
BEGIN
    SELECT
        EXISTS (
            SELECT
                1
            FROM
                pg_depend
            WHERE
                refobjid = (
                    SELECT
                        oid
                    FROM
                        pg_extension
                    WHERE
                        extname = 'pgmq')
                    AND objid = (
                        SELECT
                            oid
                        FROM
                            pg_class
                        WHERE
                            relname = table_name)) INTO result;
    RETURN result;
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION pgmq.create_non_partitioned (queue_name text)
    RETURNS void
    AS $$
BEGIN
    PERFORM
        pgmq.validate_queue_name (queue_name);
    EXECUTE FORMAT($QUERY$ CREATE TABLE IF NOT EXISTS pgmq.q_ % s (msg_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY, read_ct int DEFAULT 0 NOT NULL, enqueued_at timestamp with time zone DEFAULT now( ) NOT NULL, vt timestamp with time zone NOT NULL, message jsonb ) $QUERY$, queue_name);
    EXECUTE FORMAT($QUERY$ CREATE TABLE IF NOT EXISTS pgmq.a_ % s (msg_id bigint PRIMARY KEY, read_ct int DEFAULT 0 NOT NULL, enqueued_at timestamp with time zone DEFAULT now( ) NOT NULL, archived_at timestamp with time zone DEFAULT now( ) NOT NULL, vt timestamp with time zone NOT NULL, message jsonb
        );
    $QUERY$,
    queue_name);
    IF NOT pgmq._belongs_to_pgmq (FORMAT('q_%s', queue_name)) THEN
        EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.q_%s', queue_name);
    END IF;
    IF NOT pgmq._belongs_to_pgmq (FORMAT('a_%s', queue_name)) THEN
        EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.a_%s', queue_name);
    END IF;
    EXECUTE FORMAT($QUERY$ CREATE INDEX IF NOT EXISTS q_ % s_vt_idx ON pgmq.q_ % s (vt ASC);
    $QUERY$,
    queue_name,
    queue_name);
    EXECUTE FORMAT($QUERY$ CREATE INDEX IF NOT EXISTS archived_at_idx_ % s ON pgmq.a_ % s (archived_at);
    $QUERY$,
    queue_name,
    queue_name);
    EXECUTE FORMAT($QUERY$ INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
            VALUES ('%s', FALSE, FALSE) ON CONFLICT
            DO NOTHING;
    $QUERY$,
    queue_name);
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION pgmq.create_unlogged (queue_name text)
    RETURNS void
    AS $$
BEGIN
    PERFORM
        pgmq.validate_queue_name (queue_name);
    EXECUTE FORMAT($QUERY$ CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.q_ % s (msg_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY, read_ct int DEFAULT 0 NOT NULL, enqueued_at timestamp with time zone DEFAULT now( ) NOT NULL, vt timestamp with time zone NOT NULL, message jsonb ) $QUERY$, queue_name);
    EXECUTE FORMAT($QUERY$ CREATE TABLE IF NOT EXISTS pgmq.a_ % s (msg_id bigint PRIMARY KEY, read_ct int DEFAULT 0 NOT NULL, enqueued_at timestamp with time zone DEFAULT now( ) NOT NULL, archived_at timestamp with time zone DEFAULT now( ) NOT NULL, vt timestamp with time zone NOT NULL, message jsonb
        );
    $QUERY$,
    queue_name);
    IF NOT pgmq._belongs_to_pgmq (FORMAT('q_%s', queue_name)) THEN
        EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.q_%s', queue_name);
    END IF;
    IF NOT pgmq._belongs_to_pgmq (FORMAT('a_%s', queue_name)) THEN
        EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.a_%s', queue_name);
    END IF;
    EXECUTE FORMAT($QUERY$ CREATE INDEX IF NOT EXISTS q_ % s_vt_idx ON pgmq.q_ % s (vt ASC);
    $QUERY$,
    queue_name,
    queue_name);
    EXECUTE FORMAT($QUERY$ CREATE INDEX IF NOT EXISTS archived_at_idx_ % s ON pgmq.a_ % s (archived_at);
    $QUERY$,
    queue_name,
    queue_name);
    EXECUTE FORMAT($QUERY$ INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
            VALUES ('%s', FALSE, TRUE) ON CONFLICT
            DO NOTHING;
    $QUERY$,
    queue_name);
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION pgmq._get_partition_col (partition_interval text)
    RETURNS text
    AS $$
DECLARE
    num integer;
BEGIN
    BEGIN
        num := partition_interval::integer;
        RETURN 'msg_id';
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 'enqueued_at';
    END;
END;

$$
LANGUAGE plpgsql;

CREATE FUNCTION pgmq.ensure_pg_partman_installed ()
    RETURNS void
    AS $$
DECLARE
    extension_exists boolean;
BEGIN
    SELECT
        EXISTS (
            SELECT
                1
            FROM
                pg_extension
            WHERE
                extname = 'pg_partman') INTO extension_exists;
    IF NOT extension_exists THEN
        RAISE EXCEPTION 'pg_partman is required for partitioned queues';
    END IF;
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION pgmq.create_partitioned (queue_name text, partition_interval text DEFAULT '10000', retention_interval text DEFAULT '100000')
    RETURNS void
    AS $$
DECLARE
    partition_col text;
BEGIN
    PERFORM
        pgmq.validate_queue_name (queue_name);
    PERFORM
        pgmq.ensure_pg_partman_installed ();
    SELECT
        pgmq._get_partition_col (partition_interval) INTO partition_col;
    EXECUTE FORMAT($QUERY$ CREATE TABLE IF NOT EXISTS pgmq.q_ % s (msg_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY, read_ct int DEFAULT 0 NOT NULL, enqueued_at timestamp with time zone DEFAULT now( ) NOT NULL, vt timestamp with time zone NOT NULL, message jsonb
        )
    PARTITION BY RANGE (% s ) $QUERY$, queue_name, partition_col );
    EXECUTE FORMAT($QUERY$ CREATE TABLE IF NOT EXISTS pgmq.a_ % s (msg_id bigint PRIMARY KEY, read_ct int DEFAULT 0 NOT NULL, enqueued_at timestamp with time zone DEFAULT now( ) NOT NULL, archived_at timestamp with time zone DEFAULT now( ) NOT NULL, vt timestamp with time zone NOT NULL, message jsonb
        );
    $QUERY$,
    queue_name);
    IF NOT pgmq._belongs_to_pgmq (FORMAT('q_%s', queue_name)) THEN
        EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.q_%s', queue_name);
    END IF;
    IF NOT pgmq._belongs_to_pgmq (FORMAT('a_%s', queue_name)) THEN
        EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.a_%s', queue_name);
    END IF;
    EXECUTE FORMAT($QUERY$ CREATE INDEX IF NOT EXISTS q_ % s_part_idx ON pgmq.q_ % s (% s);
    $QUERY$,
    queue_name,
    queue_name,
    partition_col);
    EXECUTE FORMAT($QUERY$ CREATE INDEX IF NOT EXISTS archived_at_idx_ % s ON pgmq.a_ % s (archived_at);
    $QUERY$,
    queue_name,
    queue_name);
    EXECUTE FORMAT($QUERY$
        SELECT
            public.create_parent ('pgmq.q_%s', '%s', 'native', '%s');
    $QUERY$,
    queue_name,
    partition_col,
    partition_interval);
    EXECUTE FORMAT($QUERY$ UPDATE
            public.part_config
        SET
            retention = '%s', retention_keep_table = FALSE, retention_keep_index = TRUE, automatic_maintenance = 'on'
            WHERE
                parent_table = 'pgmq.q_%s';
    $QUERY$,
    retention_interval,
    queue_name);
    EXECUTE FORMAT($QUERY$ INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
            VALUES ('%s', FALSE, FALSE) ON CONFLICT
            DO NOTHING;
    $QUERY$,
    queue_name);
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION pgmq.create (queue_name text)
    RETURNS void
    AS $$
BEGIN
    PERFORM
        pgmq.create_non_partitioned (queue_name);
END;
$$
LANGUAGE plpgsql;
