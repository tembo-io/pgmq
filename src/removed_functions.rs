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
fn pgmq_drop_queue() {}

#[pg_extern(sql = "")]
fn pgmq_detach_archive() {}

#[pg_extern(sql = "")]
fn pgmq_list_queues() {}

#[pg_extern(sql = "")]
fn pgmq_purge_queue() {}

#[pg_extern(sql = "")]
fn pgmq_create_non_partitioned() {}

#[pg_extern(sql = "")]
fn pgmq_create_unlogged() {}

#[pg_extern(sql = "")]
fn pgmq_create_partitioned() {}

#[pg_extern(sql = "")]
fn pgmq_create() {}

// reads and deletes at same time
#[pg_extern(sql = "")]
fn pgmq_pop() {}

/// change the visibility time on an existing message
/// vt_offset is a time relative to now that the message will be visible
/// accepts positive or negative integers
#[pg_extern(sql = "")]
fn pgmq_set_vt() {}
