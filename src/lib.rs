pgrx::pg_module_magic!();

pgrx::extension_sql_file!("../sql/pgmq.sql", name = "bootstrap");
