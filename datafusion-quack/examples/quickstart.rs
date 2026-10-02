//! The README's quick start: serve a CSV file to DuckDB.
//!
//! ```text
//! cargo run -p datafusion-quack --example quickstart -- trips.csv
//! duckdb -c "CREATE SECRET (TYPE quack, TOKEN 's3cret-token');
//!            ATTACH 'quack:localhost:9494' AS df; FROM df.trips LIMIT 5;"
//! ```

use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_quack::{ServerOptions, duckdb_session_config, serve};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "trips.csv".into());
    let ctx = SessionContext::new_with_config(duckdb_session_config());
    ctx.register_csv("trips", &path, CsvReadOptions::new())
        .await?;
    serve(
        Arc::new(ctx),
        &ServerOptions::new().with_token("s3cret-token"),
    )
    .await?;
    Ok(())
}
