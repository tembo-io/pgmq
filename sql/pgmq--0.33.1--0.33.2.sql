

CREATE OR REPLACE FUNCTION pgmq."metrics_all"()
RETURNS SETOF pgmq.metrics_result AS $$
DECLARE
    row_name RECORD;
    result_row pgmq.metrics_result;
    query TEXT;
BEGIN
    FOR row_name IN SELECT queue_name FROM pgmq.meta LOOP
        query := FORMAT(
            $QUERY$
            WITH q_summary AS (
                SELECT
                    count(*) as queue_length,
                    EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int as newest_msg_age_sec,
                    EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int as oldest_msg_age_sec,
                    NOW() as scrape_time
                FROM pgmq.q_%I
            ),
            all_metrics AS (
                SELECT CASE
                    WHEN is_called THEN last_value ELSE 0
                    END as total_messages
                FROM pgmq.q_%I_msg_id_seq
            )
            SELECT 
                '%s' as queue_name,
                q_summary.queue_length,
                q_summary.newest_msg_age_sec,
                q_summary.oldest_msg_age_sec,
                all_metrics.total_messages,
                q_summary.scrape_time
            FROM q_summary, all_metrics
            $QUERY$,
            row_name.queue_name, row_name.queue_name, row_name.queue_name
        );
        EXECUTE query INTO result_row;
        RETURN NEXT result_row;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
