-- Add function to convert archive queues to partitioned tables
CREATE pgmq.convert_archive_partitioned(table_name TEXT)
RETURNS void AS $$
BEGIN
    EXECUTE format('ALTER TABLE pgmq.a_%I RENAME TO a_%I_old', table_name, table_name);

    EXECUTE format('
    CREATE TABLE pgmq.a_%I (
        msg_id bigint NOT NULL,
        read_ct int4 NULL DEFAULT 0,
        enqueued_at timestamptz NULL DEFAULT now(),
        archived_at timestamptz NULL DEFAULT now(),
        vt timestamptz NULL,
        message jsonb NULL
    )
    PARTITION BY RANGE (archived_at)', table_name);

    EXECUTE format('ALTER INDEX pgmq.archived_at_idx_a_%I RENAME TO archived_at_idx_a_%I_old',
                   replace(table_name, 'a_', ''), replace(table_name, 'a_', ''));
    EXECUTE format('CREATE INDEX pgmqarchived_at_idx_a_%I ON pgmq.a_%I (archived_at)',
                   replace(table_name, 'a_', ''), table_name);

    EXECUTE format('
    SELECT public.create_parent(
        ''pgmq.a_%I'',
        ''archived_at'',
        ''native'',
        ''daily''
    )', table_name);

    EXECUTE format('
    UPDATE part_config
        SET retention = ''30 days'',
            retention_keep_table = false,
            retention_keep_index = false,
            infinite_time_partitions = true
        WHERE parent_table = ''pgmq.a_%I''', table_name);
END;
$$ LANGUAGE plpgsql;
