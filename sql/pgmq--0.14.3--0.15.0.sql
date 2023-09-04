-- pgmq_meta wasn't necessarily existent in this version, so we need to check if
-- the table exists
DO $$
DECLARE
    table_name TEXT;
BEGIN
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'pgmq_meta') THEN
    -- get only non-partitioned queue tables
      FOR table_name IN (SELECT queue_name FROM public.pgmq_meta LEFT JOIN pg_tables pt ON pt.tablename=format('pgmq_%1$I', queue_name) LEFT JOIN pg_class c ON c.relname=pt.tablename WHERE pt.tablename IS NOT NULL AND c.relkind='r')
      LOOP
          -- drop primary key if exists
          EXECUTE format('ALTER TABLE public.pgmq_%1$I DROP CONSTRAINT IF EXISTS pgmq_%1$I_pkey;', table_name);
          -- create primary key on msg_id
          EXECUTE format('ALTER TABLE public.pgmq_%1$I ADD PRIMARY KEY (msg_id);', table_name);
          -- create index on (vt ASC)
          -- todo ERROR:  CREATE INDEX CONCURRENTLY cannot be executed from a function
          -- todo CONTEXT:  SQL statement "CREATE INDEX CONCURRENTLY IF NOT EXISTS pgmq_my_queue_vt_idx ON my_queue(vt ASC)"
          EXECUTE format('CREATE INDEX IF NOT EXISTS pgmq_%1$I_vt_idx ON pgmq_%1$I(vt ASC)', table_name);
          -- drop the index on (vt,msg_id)
          EXECUTE format('DROP INDEX IF EXISTS msg_id_vt_idx_%1$I;', table_name);
          -- drop primary key if exists on archive_table(msg_id)
          EXECUTE format('ALTER TABLE public.pgmq_%1$I_archive DROP CONSTRAINT IF EXISTS pgmq_%1$I_archive_pkey;', table_name);
          -- create primary key on archive_table(msg_id)
          EXECUTE format('ALTER TABLE public.pgmq_%1$I_archive ADD PRIMARY KEY (msg_id);', table_name);
      END LOOP;
    END IF;
END $$;
