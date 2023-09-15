-- Dropping replaced functions from pgmq schema
DROP FUNCTION pgmq_archive(text, bigint);
DROP FUNCTION pgmq_archive(text, bigint[]);
DROP FUNCTION pgmq_create(text);
DROP FUNCTION pgmq_create_non_partitioned(text);
DROP FUNCTION pgmq_create_partitioned(text, text, text);
DROP FUNCTION pgmq_delete(text, bigint);
DROP FUNCTION pgmq_delete(text, bigint[]);
DROP FUNCTION pgmq_drop_queue(text, boolean);
DROP FUNCTION pgmq_list_queues();
DROP FUNCTION pgmq_metrics(text);
DROP FUNCTION pgmq_metrics_all();
DROP FUNCTION pgmq_pop(text);
DROP FUNCTION pgmq_purge_queue(text);
DROP FUNCTION pgmq_read(text, integer, integer);
DROP FUNCTION pgmq_read_with_poll(text, integer, integer, integer, integer);
DROP FUNCTION pgmq_send(text, jsonb, integer);
DROP FUNCTION pgmq_send_batch(text, jsonb[], integer);
DROP FUNCTION pgmq_set_vt(text, bigint, integer);

-- Creating the new pgmq schema
CREATE SCHEMA pgmq;

-- Moving pgmq_meta to pgmq schema
ALTER TABLE pgmq_meta SET SCHEMA pgmq;
ALTER TABLE pgmq.pgmq_meta RENAME TO meta;

-- Moving other tables to pgmq schema, and adopting new prefixes
DO $$
DECLARE q_row RECORD;
DECLARE part TEXT;
BEGIN
    FOR q_row IN (SELECT queue_name, is_partitioned FROM pgmq.meta)
    LOOP
	  EXECUTE FORMAT('ALTER TABLE public.pgmq_%s SET SCHEMA pgmq', q_row.queue_name);
	  EXECUTE FORMAT('ALTER TABLE public.pgmq_%s_archive SET SCHEMA pgmq', q_row.queue_name);
	  EXECUTE FORMAT('ALTER SEQUENCE pgmq.pgmq_%s_msg_id_seq RENAME TO q_%s_msg_id_seq', q_row.queue_name, q_row.queue_name);
	  EXECUTE FORMAT('ALTER TABLE pgmq.pgmq_%s RENAME TO q_%s', q_row.queue_name, q_row.queue_name);
	  EXECUTE FORMAT('ALTER TABLE pgmq.pgmq_%s_archive RENAME TO a_%s', q_row.queue_name, q_row.queue_name);
	  EXECUTE FORMAT('ALTER SEQUENCE pgmq.pgmq_%s_archive_msg_id_seq RENAME TO a_%s_msg_id_seq', q_row.queue_name, q_row.queue_name);
	  IF q_row.is_partitioned THEN
		UPDATE part_config
		SET
			parent_table = FORMAT('pgmq.q_%s', q_row.queue_name),
			template_table = FORMAT('pgmq.template_pgmq_q_%s', q_row.queue_name)
		WHERE parent_table = FORMAT('public.pgmq_%s', q_row.queue_name);
		EXECUTE FORMAT('ALTER TABLE public.template_public_pgmq_%s SET SCHEMA pgmq', q_row.queue_name);
		EXECUTE FORMAT('ALTER TABLE pgmq.template_public_pgmq_%s RENAME TO template_q_%s', q_row.queue_name, q_row.queue_name);
		FOR part in (SELECT partition_tablename from show_partitions(FORMAT('pgmq.q_%s', q_row.queue_name)))
		LOOP
			EXECUTE FORMAT('ALTER TABLE public.%s SET SCHEMA pgmq', part);
		END LOOP;
	  END IF;
    END LOOP;
END $$;

-- Create all functions in pgmq schema. This was copied from generated pgrx schema.
-- src/api.rs:432
-- pgmq::api::set_vt
CREATE  FUNCTION pgmq."set_vt"(
	"queue_name" TEXT, /* &str */
	"msg_id" bigint, /* i64 */
	"vt_offset" INT /* i32 */
) RETURNS TABLE (
	"msg_id" bigint,  /* i64 */
	"read_ct" INT,  /* i32 */
	"enqueued_at" timestamp with time zone,  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
	"vt" timestamp with time zone,  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
	"message" jsonb  /* pgrx::datum::json::JsonB */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_set_vt_wrapper';

-- src/api.rs:81
-- pgmq::api::send_batch
CREATE  FUNCTION pgmq."send_batch"(
	"queue_name" TEXT, /* &str */
	"messages" jsonb[], /* alloc::vec::Vec<pgrx::datum::json::JsonB> */
	"delay" INT DEFAULT 0 /* i32 */
) RETURNS TABLE (
	"msg_id" bigint  /* i64 */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_send_batch_wrapper';

-- src/api.rs:165
-- pgmq::api::send
CREATE  FUNCTION pgmq."send"(
	"queue_name" TEXT, /* &str */
	"message" jsonb, /* pgrx::datum::json::JsonB */
	"delay" INT DEFAULT 0 /* i32 */
) RETURNS bigint /* core::result::Result<core::option::Option<i64>, pgmq::errors::PgmqExtError> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_send_wrapper';

-- src/api.rs:201
-- pgmq::api::read_with_poll
CREATE  FUNCTION pgmq."read_with_poll"(
	"queue_name" TEXT, /* &str */
	"vt" INT, /* i32 */
	"limit" INT, /* i32 */
	"poll_timeout_s" INT DEFAULT 5, /* i32 */
	"poll_interval_ms" INT DEFAULT 250 /* i32 */
) RETURNS TABLE (
	"msg_id" bigint,  /* i64 */
	"read_ct" INT,  /* i32 */
	"enqueued_at" timestamp with time zone,  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
	"vt" timestamp with time zone,  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
	"message" jsonb  /* pgrx::datum::json::JsonB */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_read_with_poll_wrapper';

-- src/api.rs:179
-- pgmq::api::read
CREATE  FUNCTION pgmq."read"(
	"queue_name" TEXT, /* &str */
	"vt" INT, /* i32 */
	"limit" INT /* i32 */
) RETURNS TABLE (
	"msg_id" bigint,  /* i64 */
	"read_ct" INT,  /* i32 */
	"enqueued_at" timestamp with time zone,  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
	"vt" timestamp with time zone,  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
	"message" jsonb  /* pgrx::datum::json::JsonB */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_read_wrapper';

-- src/api.rs:102
-- pgmq::api::purge_queue
CREATE  FUNCTION pgmq."purge_queue"(
	"queue_name" TEXT /* alloc::string::String */
) RETURNS bigint /* core::result::Result<i64, pgmq::errors::PgmqExtError> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_purge_queue_wrapper';

-- src/api.rs:371
-- pgmq::api::pop
CREATE  FUNCTION pgmq."pop"(
	"queue_name" TEXT /* &str */
) RETURNS TABLE (
	"msg_id" bigint,  /* i64 */
	"read_ct" INT,  /* i32 */
	"enqueued_at" timestamp with time zone,  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
	"vt" timestamp with time zone,  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
	"message" jsonb  /* pgrx::datum::json::JsonB */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_pop_wrapper';

-- src/metrics.rs:39
-- pgmq::metrics::metrics_all
CREATE  FUNCTION pgmq."metrics_all"() RETURNS TABLE (
	"queue_name" TEXT,  /* alloc::string::String */
	"queue_length" bigint,  /* i64 */
	"newest_msg_age_sec" INT,  /* core::option::Option<i32> */
	"oldest_msg_age_sec" INT,  /* core::option::Option<i32> */
	"total_messages" bigint,  /* i64 */
	"scrape_time" timestamp with time zone  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_metrics_all_wrapper';

-- src/metrics.rs:18
-- pgmq::metrics::metrics
CREATE  FUNCTION pgmq."metrics"(
	"queue_name" TEXT /* &str */
) RETURNS TABLE (
	"queue_name" TEXT,  /* alloc::string::String */
	"queue_length" bigint,  /* i64 */
	"newest_msg_age_sec" INT,  /* core::option::Option<i32> */
	"oldest_msg_age_sec" INT,  /* core::option::Option<i32> */
	"total_messages" bigint,  /* i64 */
	"scrape_time" timestamp with time zone  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_metrics_wrapper';

-- src/api.rs:49
-- pgmq::api::list_queues
CREATE  FUNCTION pgmq."list_queues"() RETURNS TABLE (
	"queue_name" TEXT,  /* alloc::string::String */
	"created_at" timestamp with time zone  /* pgrx::datum::time_stamp_with_timezone::TimestampWithTimeZone */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_list_queues_wrapper';

-- src/api.rs:20
-- pgmq::api::drop_queue
CREATE  FUNCTION pgmq."drop_queue"(
	"queue_name" TEXT, /* alloc::string::String */
	"partitioned" bool DEFAULT false /* bool */
) RETURNS bool /* core::result::Result<bool, pgmq::errors::PgmqExtError> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_drop_queue_wrapper';

-- src/api.rs:281
-- pgmq::api::delete
CREATE  FUNCTION pgmq."delete"(
	"queue_name" TEXT, /* &str */
	"msg_ids" bigint[] /* alloc::vec::Vec<i64> */
) RETURNS TABLE (
	"delete" bool  /* bool */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_delete_batch_wrapper';

-- src/api.rs:276
-- pgmq::api::delete
CREATE  FUNCTION pgmq."delete"(
	"queue_name" TEXT, /* &str */
	"msg_id" bigint /* i64 */
) RETURNS bool /* core::result::Result<core::option::Option<bool>, pgmq::errors::PgmqExtError> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_delete_wrapper';

-- src/api.rs:123
-- pgmq::api::create_partitioned
CREATE  FUNCTION pgmq."create_partitioned"(
	"queue_name" TEXT, /* &str */
	"partition_interval" TEXT DEFAULT '10000', /* alloc::string::String */
	"retention_interval" TEXT DEFAULT '100000' /* alloc::string::String */
) RETURNS VOID /* core::result::Result<(), pgmq::errors::PgmqExtError> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_create_partitioned_wrapper';

-- src/api.rs:111
-- pgmq::api::create_non_partitioned
CREATE  FUNCTION pgmq."create_non_partitioned"(
	"queue_name" TEXT /* &str */
) RETURNS VOID /* core::result::Result<(), pgmq::errors::PgmqExtError> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_create_non_partitioned_wrapper';

-- src/api.rs:151
-- pgmq::api::create
CREATE  FUNCTION pgmq."create"(
	"queue_name" TEXT /* &str */
) RETURNS VOID /* core::result::Result<(), pgmq::errors::PgmqExtError> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_create_wrapper';

-- src/api.rs:328
-- pgmq::api::archive
CREATE  FUNCTION pgmq."archive"(
	"queue_name" TEXT, /* &str */
	"msg_ids" bigint[] /* alloc::vec::Vec<i64> */
) RETURNS TABLE (
	"archive" bool  /* bool */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_archive_batch_wrapper';

-- src/api.rs:323
-- pgmq::api::archive
CREATE  FUNCTION pgmq."archive"(
	"queue_name" TEXT, /* &str */
	"msg_id" bigint /* i64 */
) RETURNS bool /* core::result::Result<core::option::Option<bool>, pgmq::errors::PgmqExtError> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_archive_wrapper';
