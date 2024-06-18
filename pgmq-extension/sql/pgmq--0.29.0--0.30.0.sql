-- New function, copied from schema
-- src/api.rs:26
-- pgmq::api::detach_archive
CREATE  FUNCTION pgmq."detach_archive"(
	"queue_name" TEXT /* alloc::string::String */
) RETURNS VOID /* core::result::Result<(), pgmq::errors::PgmqExtError> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgmq_detach_archive_wrapper';
