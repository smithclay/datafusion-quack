//! DuckDB catalog emulation for DataFusion.
use datafusion::error::Result;
use datafusion::execution::session_state::SessionState;

/// Placeholder.
pub fn duckdb_session_state(state: SessionState) -> Result<SessionState> {
    Ok(state)
}
