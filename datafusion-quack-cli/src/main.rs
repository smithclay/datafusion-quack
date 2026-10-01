//! `datafusion-quack`: serve files to DuckDB clients over the Quack protocol.
//!
//! ```text
//! datafusion-quack --token T --port 9494 --csv name:path --parquet name:path -d dir
//! ```
//!
//! Then, in DuckDB:
//!
//! ```sql
//! CREATE SECRET (TYPE quack, TOKEN 'T');
//! ATTACH 'quack:localhost:9494' AS df;
//! FROM df.name;
//! ```

mod seed;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{
    CsvReadOptions, JsonReadOptions, ParquetReadOptions, SessionConfig, SessionContext,
};
use datafusion_quack::{QuackServer, ServerOptions};
use tracing_subscriber::EnvFilter;

/// Serve CSV, Parquet and JSON files to DuckDB clients over the Quack protocol.
#[derive(Debug, Parser)]
#[command(name = "datafusion-quack", version, about)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1", env = "QUACK_HOST")]
    host: String,

    /// Port to listen on (0 picks a free one).
    #[arg(long, short, default_value_t = datafusion_quack::DEFAULT_PORT, env = "QUACK_PORT")]
    port: u16,

    /// Token clients must present. Without one, anyone may connect.
    #[arg(long, env = "QUACK_TOKEN", hide_env_values = true)]
    token: Option<String>,

    /// PEM certificate chain, to serve HTTPS (with --tls-key).
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// PEM private key, to serve HTTPS (with --tls-cert).
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,

    /// Register a CSV file (with a header row) as a table: NAME:PATH.
    #[arg(long, value_name = "NAME:PATH", value_parser = table_arg)]
    csv: Vec<(String, String)>,

    /// Register a Parquet file or directory as a table: NAME:PATH.
    #[arg(long, value_name = "NAME:PATH", value_parser = table_arg)]
    parquet: Vec<(String, String)>,

    /// Register a newline-delimited JSON file as a table: NAME:PATH.
    #[arg(long, value_name = "NAME:PATH", value_parser = table_arg)]
    json: Vec<(String, String)>,

    /// Register every .csv, .parquet, .json and .ndjson file in a directory, named after
    /// the file.
    #[arg(long, short = 'd', value_name = "DIR")]
    dir: Vec<PathBuf>,

    /// Preload a set of fixture tables.
    #[arg(long, value_enum)]
    seed: Vec<seed::Seed>,

    /// The default catalog, which DuckDB clients see as the database.
    #[arg(long, default_value = "memory")]
    catalog: String,

    /// The default schema. DuckDB clients look tables up in `main`.
    #[arg(long, default_value = "main")]
    schema: String,

    /// The most sessions open at once (0 = unlimited).
    #[arg(long, default_value_t = 1024)]
    max_sessions: usize,

    /// The longest heartbeat timeout a client may ask for, in seconds.
    #[arg(long, default_value_t = 300)]
    heartbeat_max: u64,

    /// How long an unread result stays open, in seconds (0 = until its session ends).
    #[arg(long, default_value_t = 300)]
    result_ttl: u64,

    /// Rows a PREPARE response carries before the client must FETCH.
    #[arg(long, default_value_t = 24_576)]
    inline_rows: u64,
}

fn table_arg(value: &str) -> std::result::Result<(String, String), String> {
    match value.split_once(':') {
        Some((name, path)) if !name.is_empty() && !path.is_empty() => {
            Ok((name.to_string(), path.to_string()))
        }
        _ => Err(format!("expected NAME:PATH, got '{value}'")),
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    match run(Args::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let config = SessionConfig::new()
        .with_default_catalog_and_schema(&args.catalog, &args.schema)
        .with_create_default_catalog_and_schema(true)
        .with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    register_tables(&ctx, &args).await?;

    let mut options = ServerOptions::new()
        .with_host(&args.host)
        .with_port(args.port)
        .with_max_sessions(args.max_sessions)
        .with_heartbeat_max(Duration::from_secs(args.heartbeat_max))
        .with_result_ttl(Duration::from_secs(args.result_ttl))
        .with_inline_rows(args.inline_rows);
    if let Some(token) = &args.token {
        options = options.with_token(token);
    }
    if let (Some(cert), Some(key)) = (&args.tls_cert, &args.tls_key) {
        options = options.with_tls(cert, key);
    }

    let listener = tokio::net::TcpListener::bind((args.host.as_str(), args.port)).await?;
    let address = listener.local_addr()?;
    tracing::info!(
        "listening on quack:{address} (ATTACH 'quack:{address}' AS df)",
    );
    QuackServer::new(Arc::new(ctx))
        .with_options(options)
        .serve_with_shutdown(listener, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    Ok(())
}

async fn register_tables(ctx: &SessionContext, args: &Args) -> Result<()> {
    for seed in &args.seed {
        seed::load(ctx, *seed).await?;
    }
    for (name, path) in &args.csv {
        ctx.register_csv(name, path, CsvReadOptions::new()).await?;
    }
    for (name, path) in &args.parquet {
        ctx.register_parquet(name, path, ParquetReadOptions::default())
            .await?;
    }
    for (name, path) in &args.json {
        ctx.register_json(name, path, JsonReadOptions::default())
            .await?;
    }
    for dir in &args.dir {
        register_dir(ctx, dir).await?;
    }
    Ok(())
}

async fn register_dir(ctx: &SessionContext, dir: &Path) -> Result<()> {
    let mut entries = std::fs::read_dir(dir)
        .map_err(|e| DataFusionError::Execution(format!("cannot read {}: {e}", dir.display())))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .collect::<Vec<_>>();
    entries.sort();
    for path in entries {
        let (Some(stem), Some(extension)) = (
            path.file_stem().and_then(|s| s.to_str()),
            path.extension().and_then(|s| s.to_str()),
        ) else {
            continue;
        };
        let location = path.to_string_lossy();
        match extension.to_ascii_lowercase().as_str() {
            "csv" => ctx.register_csv(stem, &location, CsvReadOptions::new()).await?,
            "parquet" => {
                ctx.register_parquet(stem, &location, ParquetReadOptions::default())
                    .await?
            }
            "json" | "ndjson" => {
                let options = JsonReadOptions::default().file_extension(extension);
                ctx.register_json(stem, &location, options).await?
            }
            _ => continue,
        }
        tracing::info!(table = stem, path = %path.display(), "registered");
    }
    Ok(())
}
