DROP FUNCTION pgmq.list_queues();

-- Copy-pasted from schema
-- pgmq::api::list_queues
CREATE  FUNCTION pgmq."list_queues"() RETURNS TABLE (
	"queue_name" TEXT,  /* alloc::string::String */
	"created_at" timestamp with time zone,  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
	"is_partitioned" bool,  /* bool */
	"is_unlogged" bool  /* bool */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_list_queues_wrapper';
