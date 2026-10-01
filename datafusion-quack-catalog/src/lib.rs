//! DuckDB catalog emulation for DataFusion.
//!
//! DuckDB clients learn about a remote database with DuckDB's own catalog SQL. This
//! crate makes a DataFusion session answer it:
//!
//! - `duckdb_databases()`, `duckdb_schemas()`, `duckdb_tables()` (with the `CREATE
//!   TABLE` statement DuckDB's `ATTACH` parses), `duckdb_views()` and
//!   `duckdb_columns()`, with oids stable for the session;
//! - `information_schema.columns` with DuckDB type names (`DECIMAL(10,2)`,
//!   `TIMESTAMP WITH TIME ZONE`) and `YES`/`NO` nullability, alongside DataFusion's
//!   other `information_schema` tables;
//! - `current_database()`, `current_schema()`, a `length` that also counts list
//!   elements, `count_star()`, `array_value()`/`list_value()`, and DuckDB's
//!   system-qualified function names (`"system".main.add(…)`);
//! - DuckDB name resolution: catalogs, schemas and tables are matched
//!   case-insensitively when there is no exact match;
//! - DataFusion's `duckdb` SQL dialect.
//!
//! The gaps are closed with functions and catalog providers, not by rewriting SQL.
//!
//! ```
//! # async fn run() -> datafusion::error::Result<()> {
//! use datafusion::prelude::SessionContext;
//!
//! let ctx = SessionContext::new();
//! let ctx = SessionContext::new_with_state(datafusion_quack_catalog::duckdb_session_state(ctx.state())?);
//! ctx.sql("CREATE TABLE Mixed (id INT NOT NULL)").await?;
//! let batches = ctx
//!     .sql("SELECT data_type, is_nullable FROM information_schema.columns WHERE table_name = 'mixed'")
//!     .await?
//!     .collect()
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod duckdb_functions;
mod functions;
mod information_schema;
mod names;
mod semantics;
mod table;
mod types;
mod udfs;
mod walk;

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};

pub use functions::{CatalogFunction, DuckDbCatalogFunction, OidRegistry};
pub use information_schema::DuckDbInformationSchema;
pub use names::{DuckDbCatalog, DuckDbCatalogList, DuckDbSchema};
pub use semantics::{DuckDbExprPlanner, duckdb_client_semantics, wide_sum_udaf};
pub use udfs::{count_star_udaf, current_database_udf, current_schema_udf, length_udf};

/// `state`, made to answer DuckDB clients: DuckDB name resolution and
/// `information_schema`, the DuckDB dialect, and the functions of
/// [`register_duckdb_functions`].
pub fn duckdb_session_state(state: SessionState) -> Result<SessionState> {
    let catalog_list = DuckDbCatalogList::wrap(Arc::clone(state.catalog_list()));
    let mut state = SessionStateBuilder::new_from_existing(state)
        .with_catalog_list(catalog_list)
        .build();
    let options = state.config_mut().options_mut();
    options.set("datafusion.sql_parser.dialect", "duckdb")?;
    // DataFusion's information_schema would shadow the catalogs' DuckDB one
    options.set("datafusion.catalog.information_schema", "false")?;
    register_duckdb_functions(&mut state)?;
    Ok(state)
}

/// Registers the DuckDB catalog functions (sharing one [`OidRegistry`]),
/// `current_database()`, `current_schema()`, `length`, `count_star()`, a `sum` that
/// can't wrap ([`wide_sum_udaf`]), and the list functions DuckDB's catalog queries
/// use if the session lacks them.
pub fn register_duckdb_functions(state: &mut SessionState) -> Result<()> {
    let oids = Arc::new(OidRegistry::default());
    for function in CatalogFunction::ALL {
        state.register_udtf(
            function.name(),
            Arc::new(DuckDbCatalogFunction::new(function, Arc::clone(&oids))),
        );
    }
    state.register_udf(Arc::new(current_database_udf()))?;
    state.register_udf(Arc::new(current_schema_udf()))?;
    state.register_udf(Arc::new(length_udf()))?;
    state.register_udaf(Arc::new(count_star_udaf()))?;
    if let Some(sum) = state.aggregate_functions().get("sum").cloned() {
        state.register_udaf(Arc::new(wide_sum_udaf(sum)))?;
    }
    if state.udf("list_append").is_err() {
        state.register_udf(datafusion::functions_nested::concat::array_append_udf())?;
    }
    // DuckDB names make_array array_value and list_value
    let make_array = match state.udf("make_array") {
        Ok(udf) => udf,
        Err(_) => datafusion::functions_nested::make_array::make_array_udf(),
    };
    state.register_udf(Arc::new(
        make_array
            .as_ref()
            .clone()
            .with_aliases(["array_value", "list_value"]),
    ))?;
    // last, so the "system".main. aliases cover every function above
    duckdb_functions::register(state)
}
