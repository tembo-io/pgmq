//! Query constructors

use pgmq_core::{errors, query, util};

pub fn init_queue_client_only(
    name: &str,
    is_unlogged: bool,
) -> Result<Vec<String>, errors::PgmqError> {
    let name = util::CheckedName::new(name)?;
    Ok(vec![
        query::create_schema(),
        query::create_meta(),
        query::create_queue(name, is_unlogged)?,
        query::create_index(name)?,
        query::create_archive(name)?,
        query::create_archive_index(name)?,
        query::insert_meta(name, false, is_unlogged)?,
        query::grant_pgmon_meta(),
        query::grant_pgmon_queue(name)?,
    ])
}

pub fn destroy_queue_client_only(name: &str) -> Result<Vec<String>, errors::PgmqError> {
    let name = util::CheckedName::new(name)?;
    Ok(vec![
        query::create_schema(),
        query::drop_queue(name)?,
        query::drop_queue_archive(name)?,
        query::delete_queue_metadata(name)?,
    ])
}
