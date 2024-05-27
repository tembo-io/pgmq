pgrx::pg_module_magic!();

pgrx::extension_sql_file!("./sql_src.sql", name = "bootstrap");
