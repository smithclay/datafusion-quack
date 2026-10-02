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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SQLOptions;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions, SessionContext};
use datafusion_quack::{QuackServer, ResultSemantics, ServerOptions};
use datafusion_quack_cli::seed;

mod read_only;
use read_only::ReadOnlyFile;
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

    /// The most sessions open at once (0 = unlimited) [default: 1024].
    #[arg(long)]
    max_sessions: Option<usize>,

    /// The longest heartbeat timeout a client may ask for, in seconds [default: 300].
    #[arg(long)]
    heartbeat_max: Option<u64>,

    /// How long an unread result stays open, in seconds (0 = until its session ends)
    /// [default: 300].
    #[arg(long)]
    result_ttl: Option<u64>,

    /// Rows a PREPARE response carries before the client must FETCH [default: 24576].
    #[arg(long)]
    inline_rows: Option<u64>,

    /// The memory queries and unread results may use, shared by all sessions, e.g.
    /// `4G` or `512M`. Past it, queries spill or fail with an Out of Memory error.
    /// [default: unlimited]
    #[arg(long, value_name = "BYTES", value_parser = bytes_arg)]
    memory_limit: Option<usize>,

    /// Whose result types every session gets where DuckDB and DataFusion differ (`5 /
    /// 2`, `avg(DECIMAL)`, date arithmetic). [default: DuckDB's for DuckDB clients,
    /// DataFusion's for the others]
    #[arg(long, value_enum)]
    result_semantics: Option<Semantics>,

    /// Serve Prometheus metrics at http://ADDR/metrics, e.g. `127.0.0.1:9495`.
    #[arg(long, value_name = "ADDR", env = "QUACK_METRICS_ADDR")]
    metrics_addr: Option<std::net::SocketAddr>,

    /// Refuse DDL and DML from clients: no CREATE, INSERT, DROP, COPY … TO or CREATE
    /// EXTERNAL TABLE. The tables registered on the command line stay queryable.
    #[arg(long, env = "QUACK_READ_ONLY")]
    read_only: bool,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum Semantics {
    Duckdb,
    Datafusion,
}

/// Parses a byte count with an optional K, M, G or T suffix (powers of 1024).
fn bytes_arg(value: &str) -> std::result::Result<usize, String> {
    let value = value.trim();
    let digits = value.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit = value[digits.len()..].to_ascii_uppercase();
    let shift = match unit.trim_end_matches("IB").trim_end_matches('B') {
        "" => 0,
        "K" => 10,
        "M" => 20,
        "G" => 30,
        "T" => 40,
        _ => return Err(format!("unknown unit in '{value}': use K, M, G or T")),
    };
    digits
        .trim()
        .parse::<usize>()
        .ok()
        .and_then(|n| n.checked_mul(1usize << shift))
        .ok_or_else(|| format!("expected a byte count such as 512M, got '{value}'"))
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
    if let Some(address) = args.metrics_addr {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(address)
            .install()?;
        tracing::info!("serving metrics on http://{address}/metrics");
    }
    let config = datafusion_quack::duckdb_session_config()
        .with_default_catalog_and_schema(&args.catalog, &args.schema);
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(limit) = args.memory_limit {
        runtime = runtime.with_memory_limit(limit, 1.0);
    }
    let ctx = SessionContext::new_with_config_rt(config, runtime.build_arc()?);
    register_tables(&ctx, &args).await?;

    // the listener below is bound here, so host and port aren't options
    let mut options = ServerOptions::new();
    if let Some(max_sessions) = args.max_sessions {
        options = options.with_max_sessions(max_sessions);
    }
    if let Some(seconds) = args.heartbeat_max {
        options = options.with_heartbeat_max(Duration::from_secs(seconds));
    }
    if let Some(seconds) = args.result_ttl {
        options = options.with_result_ttl(Duration::from_secs(seconds));
    }
    if let Some(rows) = args.inline_rows {
        options = options.with_inline_rows(rows);
    }
    if let Some(semantics) = args.result_semantics {
        options = options.with_result_semantics(match semantics {
            Semantics::Duckdb => ResultSemantics::DuckDb,
            Semantics::Datafusion => ResultSemantics::DataFusion,
        });
    }
    if args.read_only {
        options = options.with_sql_options(
            SQLOptions::new()
                .with_allow_ddl(false)
                .with_allow_dml(false),
        );
    }
    if let Some(token) = &args.token {
        options = options.with_token(token);
    }
    if let (Some(cert), Some(key)) = (&args.tls_cert, &args.tls_key) {
        options = options.with_tls(cert, key);
    }

    let listener = tokio::net::TcpListener::bind((args.host.as_str(), args.port)).await?;
    let address = listener.local_addr()?;
    tracing::info!("listening on quack:{address} (ATTACH 'quack:{address}' AS df)",);
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
    for (extension, tables) in [
        ("csv", &args.csv),
        ("parquet", &args.parquet),
        ("json", &args.json),
    ] {
        for (name, path) in tables {
            register_file(ctx, extension, name, path).await?;
        }
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
        if register_file(
            ctx,
            &extension.to_ascii_lowercase(),
            stem,
            &path.to_string_lossy(),
        )
        .await?
        {
            tracing::info!(table = stem, path = %path.display(), "registered");
        }
    }
    Ok(())
}

/// Registers the file at `path` as table `name`, read by its `extension`: csv, parquet,
/// json or ndjson (newline-delimited JSON). Returns false for any other extension.
async fn register_file(
    ctx: &SessionContext,
    extension: &str,
    name: &str,
    path: &str,
) -> Result<bool> {
    match extension {
        "csv" => ctx.register_csv(name, path, CsvReadOptions::new()).await?,
        "parquet" => {
            ctx.register_parquet(name, path, ParquetReadOptions::default())
                .await?
        }
        "json" | "ndjson" => {
            let suffix = format!(".{extension}");
            let options = JsonReadOptions::default().file_extension(&suffix);
            ctx.register_json(name, path, options).await?
        }
        _ => return Ok(false),
    }
    // served as read, never written: wrap it so writes say how to copy it instead
    if let Some(table) = ctx.deregister_table(name)? {
        ctx.register_table(name, Arc::new(ReadOnlyFile::new(name, path, table)))?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn file_tables_refuse_writes_and_say_how_to_copy_them() {
        let dir = std::env::temp_dir().join(format!("quack-read-only-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("people.csv");
        std::fs::write(&file, "id,name\n1,a\n").unwrap();
        let ctx = SessionContext::new();
        assert!(
            register_file(&ctx, "csv", "people", &file.to_string_lossy())
                .await
                .unwrap()
        );

        for sql in [
            "INSERT INTO people VALUES (2, 'b')",
            "DELETE FROM people WHERE id = 1",
            "UPDATE people SET name = 'z'",
        ] {
            let result = async { ctx.sql(sql).await?.collect().await }.await;
            let error = result.expect_err(sql).to_string();
            assert!(error.contains("CREATE TABLE"), "{sql}: {error}");
        }
        // reads still work, the file is unchanged, and a copy is writable
        let count = |sql: &'static str| {
            let ctx = ctx.clone();
            async move {
                let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
                datafusion::arrow::util::display::array_value_to_string(batches[0].column(0), 0)
                    .unwrap()
            }
        };
        assert_eq!(count("SELECT count(*) FROM people").await, "1");
        ctx.sql("CREATE TABLE mine AS SELECT * FROM people")
            .await
            .unwrap();
        ctx.sql("INSERT INTO mine VALUES (2, 'b')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(count("SELECT count(*) FROM mine").await, "2");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "id,name\n1,a\n");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn byte_counts_take_binary_suffixes() {
        assert_eq!(bytes_arg("1024"), Ok(1024));
        assert_eq!(bytes_arg("512M"), Ok(512 << 20));
        assert_eq!(bytes_arg("4GiB"), Ok(4 << 30));
        assert_eq!(bytes_arg("2kb"), Ok(2048));
        assert!(bytes_arg("4X").is_err());
        assert!(bytes_arg("").is_err());
        assert!(bytes_arg("99999999999T").is_err());
    }
}
