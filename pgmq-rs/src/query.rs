//! Query constructors

use crate::core_errors;
use crate::core_query;
use crate::core_util;

pub fn init_queue_client_only(
    name: &str,
    is_unlogged: bool,
) -> Result<Vec<String>, core_errors::PgmqError> {
    let name = core_util::CheckedName::new(name)?;
    Ok(vec![
        core_query::create_schema(),
        core_query::create_meta(),
        core_query::create_queue(name, is_unlogged)?,
        core_query::create_index(name)?,
        core_query::create_archive(name)?,
        core_query::create_archive_index(name)?,
        core_query::insert_meta(name, false, is_unlogged)?,
        core_query::grant_pgmon_meta(),
        core_query::grant_pgmon_queue(name)?,
    ])
}

pub fn destroy_queue_client_only(name: &str) -> Result<Vec<String>, core_errors::PgmqError> {
    let name = core_util::CheckedName::new(name)?;
    Ok(vec![
        core_query::create_schema(),
        core_query::drop_queue(name)?,
        core_query::drop_queue_archive(name)?,
        core_query::delete_queue_metadata(name)?,
    ])
}
