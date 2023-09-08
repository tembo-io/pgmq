use pgrx::prelude::*;

// We need to keep exposing linkable wrappers for old, removed functions. This
// is required so that transitional version upgrades work as expected
#[pg_extern(sql="")]
pub  fn pgmq_read() {}
#[pg_extern(sql="")]
pub fn pgmq_read_with_poll() {}
