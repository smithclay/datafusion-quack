//! Sessions: one per client connection, each with its own `SessionContext`.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::SessionContext;
use quack_protocol::server::HugeIntParts;
use rand::RngCore;

use crate::auth::SessionInfo;
use crate::cursor::{CancelHandle, Cursor};
use crate::error::ClientError;

/// Makes the `SessionContext` each session runs its queries in.
#[async_trait]
pub trait SessionContextProvider: Send + Sync + Debug {
    /// A context for a new session.
    async fn session_context(&self, session: &SessionInfo) -> DataFusionResult<SessionContext>;
}

/// Gives each session a context built from one base context, with DuckDB compatibility
/// installed (see [`datafusion_quack_catalog::duckdb_session_state`]).
///
/// The sessions share the base context's catalogs, functions and runtime, so a table
/// one client creates is visible to the others, as in DuckDB. Settings a session
/// changes stay in that session.
#[derive(Clone)]
pub struct SharedSessionContextProvider {
    base: Arc<SessionContext>,
}

impl Debug for SharedSessionContextProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSessionContextProvider")
            .field("session_id", &self.base.session_id())
            .finish()
    }
}

impl SharedSessionContextProvider {
    /// Sessions derived from `base`.
    pub fn new(base: Arc<SessionContext>) -> Self {
        Self { base }
    }
}

#[async_trait]
impl SessionContextProvider for SharedSessionContextProvider {
    async fn session_context(&self, _session: &SessionInfo) -> DataFusionResult<SessionContext> {
        let state = datafusion_quack_catalog::duckdb_session_state(self.base.state())?;
        Ok(SessionContext::new_with_state(state))
    }
}

/// The statement a session runs: its result cursor, or why the last one stopped.
#[derive(Default)]
pub(crate) struct StatementSlot {
    pub(crate) uuid: Option<HugeIntParts>,
    pub(crate) cursor: Option<Arc<Cursor>>,
    pub(crate) abort_error: Option<ClientError>,
}

impl StatementSlot {
    /// Stops the current cursor, keeping `reason` for a FETCH that asks after it.
    pub(crate) fn abort(&mut self, reason: ClientError) {
        if let Some(cursor) = self.cursor.take() {
            cursor.cancel();
            self.abort_error = Some(reason);
        }
    }
}

pub(crate) struct Session {
    pub(crate) info: SessionInfo,
    pub(crate) ctx: SessionContext,
    heartbeat_timeout: Duration,
    lease_renewed: Mutex<Instant>,
    /// Cancels the statement that runs now, from its PREPARE to its last FETCH.
    running: Mutex<Option<CancelHandle>>,
    /// Held for the whole of a PREPARE, so a session runs one statement at a time.
    pub(crate) statement: tokio::sync::Mutex<StatementSlot>,
}

impl Session {
    pub(crate) fn new(info: SessionInfo, ctx: SessionContext, heartbeat_timeout: Duration) -> Self {
        Self {
            info,
            ctx,
            heartbeat_timeout,
            lease_renewed: Mutex::new(Instant::now()),
            running: Mutex::new(None),
            statement: tokio::sync::Mutex::new(StatementSlot::default()),
        }
    }

    fn expired(&self, now: Instant) -> bool {
        let renewed = *self
            .lease_renewed
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        now.saturating_duration_since(renewed) >= self.heartbeat_timeout
    }

    /// Renews the lease, unless it already ran out.
    fn renew(&self, now: Instant) -> bool {
        let mut renewed = self
            .lease_renewed
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if now.saturating_duration_since(*renewed) >= self.heartbeat_timeout {
            return false;
        }
        *renewed = now;
        true
    }

    /// Starts a statement: cancels the one before it, and returns the new one's handle.
    pub(crate) fn begin_statement(&self) -> CancelHandle {
        let handle = CancelHandle::new();
        let previous = self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(handle.clone());
        if let Some(previous) = previous {
            previous.cancel();
        }
        handle
    }

    /// Cancels the statement that runs now, if any.
    pub(crate) fn cancel_running(&self) {
        if let Some(handle) = self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            handle.cancel();
        }
    }

    /// Cancels the running statement, e.g. because the session ended.
    fn abort(&self, reason: &str) {
        self.cancel_running();
        if let Ok(mut slot) = self.statement.try_lock() {
            slot.abort(ClientError::interrupted(reason));
        }
    }
}

/// Generates a connection id: 128 random bits, as 32 upper-case hex digits (as DuckDB).
pub(crate) fn new_connection_id() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

/// The open sessions.
pub(crate) struct SessionStore {
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    max_sessions: usize,
}

impl SessionStore {
    pub(crate) fn new(max_sessions: usize) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            max_sessions,
        }
    }

    fn sessions(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Session>>> {
        self.sessions.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn len(&self) -> usize {
        self.sessions().len()
    }

    pub(crate) fn insert(&self, session: Arc<Session>) -> Result<(), ClientError> {
        let mut sessions = self.sessions();
        if self.max_sessions != 0 && sessions.len() >= self.max_sessions {
            return Err(ClientError::invalid_input(format!(
                "Too many sessions: the server allows {}",
                self.max_sessions
            )));
        }
        sessions.insert(session.info.connection_id.clone(), session);
        Ok(())
    }

    /// The session, with its lease renewed.
    pub(crate) fn renew(&self, connection_id: &str) -> Result<Arc<Session>, ClientError> {
        let session = self
            .sessions()
            .get(connection_id)
            .cloned()
            .ok_or_else(|| ClientError::invalid_input("Invalid connection id"))?;
        if session.renew(Instant::now()) {
            return Ok(session);
        }
        self.remove(connection_id);
        Err(ClientError::invalid_input(
            "Connection heartbeat lease expired",
        ))
    }

    pub(crate) fn remove(&self, connection_id: &str) -> Option<Arc<Session>> {
        let session = self.sessions().remove(connection_id);
        if let Some(session) = &session {
            session.abort("the session ended");
        }
        session
    }

    /// Ends sessions whose lease ran out, and closes results unread for `result_ttl`.
    pub(crate) fn sweep(&self, result_ttl: Duration) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .sessions()
            .iter()
            .filter(|(_, session)| session.expired(now))
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            tracing::debug!(connection_id = %id, "session lease expired");
            self.remove(&id);
        }
        if result_ttl.is_zero() {
            return;
        }
        let sessions: Vec<Arc<Session>> = self.sessions().values().cloned().collect();
        for session in sessions {
            // a busy slot is in use, so its result isn't stale
            let Ok(mut slot) = session.statement.try_lock() else {
                continue;
            };
            if slot
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.idle_for(now) >= result_ttl)
            {
                tracing::debug!(connection_id = %session.info.connection_id, "result expired");
                slot.abort(ClientError::invalid_input(
                    "Result has been closed: it was not read for too long",
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(id: &str, timeout: Duration) -> Arc<Session> {
        Arc::new(Session::new(
            SessionInfo {
                connection_id: id.into(),
                client_version: String::new(),
                client_platform: String::new(),
            },
            SessionContext::new(),
            timeout,
        ))
    }

    #[test]
    fn connection_ids_are_128_random_bits() {
        let a = new_connection_id();
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase()));
        assert_ne!(a, new_connection_id());
    }

    #[test]
    fn the_session_limit_is_enforced() {
        let store = SessionStore::new(1);
        store.insert(session("a", Duration::from_secs(60))).unwrap();
        assert!(store.insert(session("b", Duration::from_secs(60))).is_err());
        store.remove("a");
        store.insert(session("b", Duration::from_secs(60))).unwrap();
    }

    #[test]
    fn expired_leases_end_the_session() {
        let store = SessionStore::new(0);
        store.insert(session("a", Duration::ZERO)).unwrap();
        assert_eq!(
            store.renew("a").err().unwrap().message,
            "Connection heartbeat lease expired"
        );
        assert_eq!(
            store.renew("a").err().unwrap().message,
            "Invalid connection id"
        );

        store.insert(session("b", Duration::ZERO)).unwrap();
        store.sweep(Duration::ZERO);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn renewed_leases_live_on() {
        let store = SessionStore::new(0);
        store.insert(session("a", Duration::from_secs(60))).unwrap();
        assert!(store.renew("a").is_ok());
        store.sweep(Duration::from_secs(1));
        assert_eq!(store.len(), 1);
    }
}
