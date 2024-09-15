DROP FUNCTION IF EXISTS pgmq_delete(TEXT, bigint[]);

-- src/lib.rs:215
-- pgmq::pgmq_delete
CREATE FUNCTION "pgmq_delete"(
	"queue_name" TEXT, /* &str */
	"msg_ids" bigint[] /* alloc::vec::Vec<i64> */
) RETURNS TABLE (
	"pgmq_delete" bool  /* bool */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_delete_batch_wrapper';


DROP FUNCTION IF EXISTS pgmq_archive(TEXT, bigint[]);

-- src/lib.rs:260
-- pgmq::pgmq_archive
CREATE  FUNCTION "pgmq_archive"(
	"queue_name" TEXT, /* &str */
	"msg_ids" bigint[] /* alloc::vec::Vec<i64> */
) RETURNS TABLE (
	"pgmq_archive" bool  /* bool */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_archive_batch_wrapper';
