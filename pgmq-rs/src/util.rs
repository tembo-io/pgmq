use std::fmt::Display;

use crate::{errors::PgmqError, types::Message};
use log::LevelFilter;
use serde::Deserialize;
use sqlx::error::Error;
use sqlx::postgres::PgRow;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::ConnectOptions;
use sqlx::Row;
use sqlx::{Pool, Postgres};
use url::{ParseError, Url};

// Configure connection options
pub fn conn_options(url: &str) -> Result<PgConnectOptions, ParseError> {
    // Parse url
    let parsed = Url::parse(url)?;
    let options = PgConnectOptions::new()
        .host(parsed.host_str().ok_or(ParseError::EmptyHost)?)
        .port(parsed.port().ok_or(ParseError::InvalidPort)?)
        .username(parsed.username())
        .password(parsed.password().ok_or(ParseError::IdnaError)?)
        .database(parsed.path().trim_start_matches('/'))
        .log_statements(LevelFilter::Debug);
    Ok(options)
}

/// Connect to the database
pub async fn connect(url: &str, max_connections: u32) -> Result<Pool<Postgres>, PgmqError> {
    let options = conn_options(url)?;
    let pgp = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(10))
        .max_connections(max_connections)
        .connect_with(options)
        .await?;
    Ok(pgp)
}

// Executes a query and returns a single row
// If the query returns no rows, None is returned
// This function is intended for internal use.
pub async fn fetch_one_message<T: for<'de> Deserialize<'de>>(
    query: &str,
    connection: &Pool<Postgres>,
) -> Result<Option<Message<T>>, PgmqError> {
    // explore: .fetch_optional()
    let row: Result<PgRow, Error> = sqlx::query(query).fetch_one(connection).await;
    match row {
        Ok(row) => {
            // happy path - successfully read a message
            let raw_msg = row.get("message");
            let parsed_msg = serde_json::from_value::<T>(raw_msg);
            match parsed_msg {
                Ok(parsed_msg) => Ok(Some(Message {
                    msg_id: row.get("msg_id"),
                    vt: row.get("vt"),
                    read_ct: row.get("read_ct"),
                    enqueued_at: row.get("enqueued_at"),
                    message: parsed_msg,
                })),
                Err(e) => Err(PgmqError::JsonParsingError(e)),
            }
        }
        Err(sqlx::error::Error::RowNotFound) => Ok(None),
        Err(e) => Err(e)?,
    }
}

/// A string that is known to be formed of only ASCII alphanumeric or an underscore;
#[derive(Clone, Copy)]
pub struct CheckedName<'a>(&'a str);

impl<'a> CheckedName<'a> {
    /// Accepts `input` as a CheckedName if it is a valid queue identifier
    pub fn new(input: &'a str) -> Result<Self, PgmqError> {
        check_input(input)?;

        Ok(Self(input))
    }
}

impl AsRef<str> for CheckedName<'_> {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl Display for CheckedName<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// panics if input is invalid. otherwise does nothing.
pub fn check_input(input: &str) -> Result<(), PgmqError> {
    // Docs:
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS

    // Default value of `NAMEDATALEN`, set in `src/include/pg_config_manual.h`
    const NAMEDATALEN: usize = 64;

    // The maximum length of an identifier.
    // Longer names can be used in commands, but they'll be truncated
    const MAX_IDENTIFIER_LEN: usize = NAMEDATALEN - 1;
    const BIGGEST_CONCAT: &str = "archived_at_idx_";

    // The max length of the name of a PGMQ queue, considering that the biggest
    // postgres identifier created by PGMQ is the index on archived_at
    const MAX_PGMQ_QUEUE_LEN: usize = MAX_IDENTIFIER_LEN - BIGGEST_CONCAT.len();

    let is_short_enough = input.len() <= MAX_PGMQ_QUEUE_LEN;
    let has_valid_characters = input
        .as_bytes()
        .iter()
        .all(|&c| c.is_ascii_alphanumeric() || c == b'_');
    let valid = is_short_enough && has_valid_characters;
    match valid {
        true => Ok(()),
        false => Err(PgmqError::InvalidQueueName {
            name: input.to_owned(),
        }),
    }
}
