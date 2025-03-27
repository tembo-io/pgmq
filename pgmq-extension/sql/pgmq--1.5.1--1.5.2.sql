CREATE OR REPLACE FUNCTION pgmq.validate_queue_name(queue_name TEXT)
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