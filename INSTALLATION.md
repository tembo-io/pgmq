# Installing PGMQ

PGMQ has two installation methods: as a [Postgres extension](#postgres-extension-installation) or as a [SQL-only](#sql-only-installation) project. Using PGMQ as a Postgres extension is preferred, but the SQL-only approach can also be useful when extension installation is restricted or not available.

## Postgres extension installation

You will need access to the host's file system in order to install a Postgres extension. Installing as an extension involves placing PGMQ's [extension files](https://www.postgresql.org/docs/current/extend-extensions.html#EXTEND-EXTENSIONS-FILES) in Postgres's extension directory.

There are several tools available to install PGMQ as an extension. These commands must all be run from the same host where Postgres is installed and running.

### PGXN

Install the [pgxn CLI](https://pgxn.github.io/pgxnclient/install.html).

Use the pgxn cli to install PGMQ into Postgres using the `pg_config` on your `PATH`:

```bash
pgxn install pgmq
```

### From Source

Alternatively, clone the repository:

```bash
git clone https://github.com/pgmq/pgmq.git
cd pgmq/pgmq-extension
```

Then install the extension using [Make](https://www.gnu.org/software/make/)

```bash
make && make install
```

### Enable the Extension in Postgres

Once the extension is installed, you will be able to see it in Postgres.

```sql
select * from pg_available_extensions where name = 'pgmq';
```

```plaintext
 name | default_version | installed_version |                               comment                               
------+-----------------+-------------------+---------------------------------------------------------------------
 pgmq | 1.5.1           |                   | A lightweight message queue. Like AWS SQS and RSMQ but on Postgres.
```

`CREATE` the extension to make it available to users.

```sql
CREATE EXTENSION pgmq;
```

PGMQ is now available. You can verify the installation by running:

```sql
postgres=# \dx pgmq
```

```plaintext
                                 List of installed extensions
 Name | Version | Schema |                             Description                             
------+---------+--------+---------------------------------------------------------------------
 pgmq | 1.5.1   | pgmq   | A lightweight message queue. Like AWS SQS and RSMQ but on Postgres.
(1 row)
```

### Uninstalling the extension
To uninstall the extension, you can use the `DROP EXTENSION` command:

```sql
DROP EXTENSION pgmq;
```

This will remove all objects created by the extension, including tables, functions, and types. The `pgmq` schema will remain, but if you have any data in the queues, it will be lost.

## SQL-only installation

PGMQ consists of raw SQL objects and can also be installed directly into any Postgres instance. This method is preferred when extension installation or access to the server host is not available. This method will create a schema named `pgmq` and all SQL objects in that schema.

Simply executing the SQL definition file on your Postgres instance will create all the required objects. This can be accomplished by cloning the repo then running the following commands. This requires `psql` to be installed and available on your `PATH`.

```bash
git clone https://github.com/pgmq/pgmq.git

cd pgmq

psql -f pgmq-extension/sql/pgmq.sql postgres://postgres:postgres@localhost:5432/postgres
```

Replace the postgres user, password, and database with the appropriate values for your Postgres instance.

## Uninstalling the SQL-only install

All objects are created in the `pgmq` schema, so the simplest way to remove the project is to drop the schema:

```sql
DROP SCHEMA pgmq CASCADE;
```

## Getting Started

Once you have installed PGMQ, you can start using it. The [README](pgmq-extension/README.md#sql-examples) contains a quick start guide to get you up and running.

## Considerations

The project's core functions and features are the same regardless of how PGMQ is installed, though there are some differences in how the project is managed depending on the installation method. The following table summarizes these differences:

| Capability / Feature | Extension Install | SQL-only Install | Notes |
|----------------------|-------------------|------------------|-------|
| Version Tracking | ✅ | ❌ | Extension allows Postgres to track installed versions and users can inspect via `\dx pgmq` |
| Supported Upgrades | ✅ | ❌ | Extensions can be upgraded with `ALTER EXTENSION pgmq UPDATE` |
| No File System Access Needed | ❌ | ✅ | SQL-only installs work entirely within the database |
| Managed Cloud Support | 🟡 (limited) | ✅ | SQL-only is compatible with most managed services |
| Simple Deployment | ❌ | ✅ | SQL-only requires just executing SQL in the database |
| Best For | Full control environments | Restricted/managed environments | |

Recommendation: Use the extension installation when your environment allows it. If you want to use PGMQ in a managed service that does not support extensions, then install it as raw SQL.
