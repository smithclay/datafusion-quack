//! Serve a DuckLake catalog (SQLite metadata) to DuckDB 2.0 over Quack. See README.md
//! for making a catalog to serve.
//!
//! ```text
//! cargo run --release -- /path/to/metadata.sqlite [--write] [--port 9494] [--token s3cret-token]
//! duckdb -c "CREATE SECRET (TYPE quack, TOKEN 's3cret-token');
//!            ATTACH 'quack:localhost:9494' AS lake; SHOW ALL TABLES;"
//! ```
//!
//! The DuckLake catalog is registered as `memory`, DuckDB's default database name, so
//! `ATTACH … AS lake` finds `lake.main.trips` and `lake.ops.cities`. Without `--write`
//! the server refuses DDL and DML.

use std::sync::Arc;

use datafusion::execution::context::SQLOptions;
use datafusion::prelude::SessionContext;
use datafusion_ducklake::{
    DuckLakeCatalog, SqliteMetadataProvider, SqliteMetadataWriter, register_ducklake_functions,
    register_snapshot_consistency,
};
use datafusion_quack::datafusion_quack_catalog::CatalogListings;
use datafusion_quack::{QuackServer, ServerOptions, duckdb_session_config};

mod listing;

const CATALOG: &str = "memory";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "datafusion_quack=info".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let mut metadata = None;
    let mut write = false;
    let mut options = ServerOptions::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--write" => write = true,
            "--port" => options = options.with_port(args.next().ok_or("--port PORT")?.parse()?),
            "--token" => options = options.with_token(args.next().ok_or("--token TOKEN")?),
            _ if metadata.is_none() => metadata = Some(arg),
            _ => return Err(format!("unexpected argument {arg}").into()),
        }
    }
    let metadata = metadata
        .ok_or("usage: ducklake-quack METADATA.sqlite [--write] [--port PORT] [--token TOKEN]")?;
    let url = if metadata.starts_with("sqlite:") {
        metadata
    } else {
        format!("sqlite://{metadata}")
    };

    let provider = SqliteMetadataProvider::new(&url).await?;
    let catalog = if write {
        let writer = Arc::new(SqliteMetadataWriter::new(&url).await?);
        DuckLakeCatalog::with_writer(Arc::new(provider), writer)?
    } else {
        options = options.with_sql_options(
            SQLOptions::new()
                .with_allow_ddl(false)
                .with_allow_dml(false),
        );
        DuckLakeCatalog::new(provider)?
    };

    let catalog = Arc::new(catalog);
    // ATTACH lists the catalog's tables in a few metadata queries, not a dozen per table
    let listings = CatalogListings::new().with(
        CATALOG,
        Arc::new(listing::DuckLakeListing::new(Arc::clone(&catalog))),
    );
    let ctx =
        SessionContext::new_with_config(duckdb_session_config().with_extension(Arc::new(listings)));
    register_ducklake_functions(&ctx, catalog.provider());
    // what DuckLakeCatalog::register does, keeping a handle for the listing
    register_snapshot_consistency(&ctx);
    ctx.register_catalog(CATALOG, catalog);

    QuackServer::new(Arc::new(ctx))
        .with_options(options)
        .serve()
        .await?;
    Ok(())
}
