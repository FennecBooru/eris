//! eris-store: Postgres persistence – schema, writes, snapshot bootstrap, and
//! the trigger-fed event feed that keeps every node's index converged.

pub mod bootstrap;
pub mod events;
pub mod feed;
pub mod prune;
pub mod writes;

use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{ConnectOptions, Connection};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
  #[error("database error: {0}")]
  Sqlx(#[from] sqlx::Error),
  #[error("migration error: {0}")]
  Migrate(#[from] sqlx::migrate::MigrateError),
  #[error("corrupt row for post {post_id}: {reason}")]
  CorruptRow { post_id: i32, reason: String },
}

pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool, StoreError> {
  Ok(
    PgPoolOptions::new()
      .max_connections(max_connections)
      .connect(url)
      .await?,
  )
}

/// Create the database named in `url` if it doesn't exist yet, connecting to
/// the server's `postgres` maintenance database to do so.
pub async fn ensure_database(url: &str) -> Result<(), StoreError> {
  let options = PgConnectOptions::from_str(url)?;
  let Some(name) = options.get_database().map(str::to_owned) else {
    return Ok(());
  };

  let mut conn = options.database("postgres").connect().await?;
  let exists: bool =
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
      .bind(&name)
      .fetch_one(&mut conn)
      .await?;
  if !exists {
    tracing::info!(database = %name, "creating database");
    let quoted = format!("\"{}\"", name.replace('"', "\"\""));
    match sqlx::query(&format!("CREATE DATABASE {quoted}"))
      .execute(&mut conn)
      .await
    {
      // Another node created it first.
      Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P04") => {}
      result => {
        result?;
      }
    }
  }
  conn.close().await?;
  Ok(())
}

/// Apply the schema migrations (embedded at compile time).
pub async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
  sqlx::migrate!("./migrations").run(pool).await?;
  Ok(())
}
