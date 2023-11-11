use pgrx::prelude::*;

// We need to keep exposing linkable wrappers for old, removed functions. This
// is required so that transitory version upgrades work as expected.
//
// For example, assume the following update chain:
// 0.22 -> 0.23 -> 0.24 -> 0.25 -> 0.26
// In the version 0.23, `pgmq_archive` was added. But if the user has the dynamic
// library from version 0.26, the transitory update from 0.22 to 0.23 won't work
// as postgres can see that `pgmq_archive` isn't available in the dynamic library.
// For this reason, we need to keep `pgmq_archive` in the dynamic library forever
// to keep update compatibility.
//
#[pg_extern(sql = "")]
pub fn pgmq_read() {}
#[pg_extern(sql = "")]
pub fn pgmq_read_with_poll() {}
#[pg_extern(sql = "")]
pub fn pgmq_archive() {}
#[pg_extern(sql = "")]
pub fn pgmq_archive_batch() {}
#[pg_extern(sql = "")]
pub fn pgmq_delete() {}
#[pg_extern(sql = "")]
pub fn pgmq_delete_batch() {}
#[pg_extern(sql = "")]
pub fn pgmq_send() {}
#[pg_extern(sql = "")]
pub fn pgmq_send_batch() {}
#[pg_extern(sql = "")]
pub fn metrics() {}
#[pg_extern(sql = "")]
pub fn metrics_all() {}
