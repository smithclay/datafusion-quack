//! Sessions: one per client connection, each with its own `SessionContext`.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Write;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::prelude::SessionContext;
use quack_protocol::server::HugeIntParts;
use rand::RngCore;

use crate::auth::SessionInfo;
use crate::cursor::{CancelHandle, Cursor};
use crate::error::ClientError;
use crate::transaction::Transaction;

/// Makes the `SessionContext` each session runs its queries in.
#[async_trait]
pub trait SessionContextProvider: Send + Sync + Debug {
    /// A context for a new session.
    async fn session_context(&self, session: &SessionInfo) -> DataFusionResult<SessionContext>;
}

/// Gives each session a context built from one base context, with DuckDB compatibility
/// installed (see [`datafusion_quack_catalog::duckdb_session_state`]). Sessions of
/// DuckDB clients also get DuckDB's result types
/// ([`datafusion_quack_catalog::duckdb_client_semantics`]): DuckDB pushes whole
/// queries and expects DuckDB's answers. Other clients, such as the DataFusion Quack
/// table provider, keep DataFusion's semantics.
///
/// The sessions share the base context's catalogs and runtime, so a table one client
/// creates is visible to the others, as in DuckDB. Settings a session changes stay in
/// that session. The DuckDB-compatible state is built once, on the first connection,
/// from the base context's functions and settings at that moment.
pub struct SharedSessionContextProvider {
    base: Arc<SessionContext>,
    /// The base state with DuckDB compatibility, and with DuckDB's result types too.
    states: tokio::sync::OnceCell<(SessionState, SessionState)>,
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
        Self {
            base,
            states: tokio::sync::OnceCell::new(),
        }
    }
}

#[async_trait]
impl SessionContextProvider for SharedSessionContextProvider {
    async fn session_context(&self, session: &SessionInfo) -> DataFusionResult<SessionContext> {
        let (plain, duckdb) = self
            .states
            .get_or_try_init(|| async {
                let plain = datafusion_quack_catalog::duckdb_session_state(self.base.state())?;
                let duckdb = datafusion_quack_catalog::duckdb_client_semantics(plain.clone())?;
                Ok::<_, datafusion::error::DataFusionError>((plain, duckdb))
            })
            .await?;
        let state = if session.is_duckdb_client() {
            duckdb
        } else {
            plain
        };
        let state = SessionStateBuilder::new_from_existing(state.clone())
            .with_session_id(session.connection_id.clone())
            .build();
        Ok(SessionContext::new_with_state(state))
    }
}

/// A query uuid of zero: "whatever runs now".
pub(crate) const ZERO_UUID: HugeIntParts = HugeIntParts { upper: 0, lower: 0 };

/// What a session's statement is doing. PREPARE, FETCH, CANCEL and the reaper all
/// read and change this one value, each under a short-held lock, so they can't
/// disagree about which statement is current. Nothing slow runs under the lock.
#[derive(Default)]
enum StatementState {
    /// No statement yet.
    #[default]
    Idle,
    /// A PREPARE is running it; `cancel` stops it.
    Running {
        uuid: HugeIntParts,
        cancel: CancelHandle,
    },
    /// Its result is being fetched.
    Streaming {
        uuid: HugeIntParts,
        cursor: Arc<Cursor>,
    },
    /// It ended; a FETCH of it gets `reason`.
    Closed {
        uuid: HugeIntParts,
        reason: ClientError,
    },
}

impl StatementState {
    /// Stops a running or streaming statement, keeping `reason` for a FETCH of it.
    fn close(&mut self, reason: ClientError) {
        let uuid = match self {
            Self::Running { uuid, cancel } => {
                cancel.cancel();
                *uuid
            }
            Self::Streaming { uuid, cursor } => {
                cursor.cancel();
                *uuid
            }
            Self::Idle | Self::Closed { .. } => return,
        };
        *self = Self::Closed { uuid, reason };
    }
}

fn result_closed() -> ClientError {
    ClientError::invalid_input("Result has been closed")
}

pub(crate) struct Session {
    pub(crate) info: SessionInfo,
    pub(crate) ctx: SessionContext,
    heartbeat_timeout: Duration,
    lease_renewed: Mutex<Instant>,
    statement: Mutex<StatementState>,
    pub(crate) transaction: Mutex<Transaction>,
}

impl Session {
    pub(crate) fn new(info: SessionInfo, ctx: SessionContext, heartbeat_timeout: Duration) -> Self {
        Self {
            info,
            ctx,
            heartbeat_timeout,
            lease_renewed: Mutex::new(Instant::now()),
            statement: Mutex::new(StatementState::Idle),
            transaction: Mutex::new(Transaction::default()),
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

    fn statement(&self) -> MutexGuard<'_, StatementState> {
        self.statement
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts statement `uuid`, stopping the one before it. The handle stops the new
    /// one, and identifies it to [`finish`](Self::finish).
    pub(crate) fn begin(&self, uuid: HugeIntParts) -> CancelHandle {
        let cancel = CancelHandle::new();
        let mut statement = self.statement();
        statement.close(ClientError::cancelled());
        *statement = StatementState::Running {
            uuid,
            cancel: cancel.clone(),
        };
        cancel
    }

    /// Ends the PREPARE of the statement `cancel` began: its result streams from
    /// `cursor`, or (`None`) it has nothing to fetch. Fails if the statement was
    /// stopped meanwhile; the cursor is then cancelled.
    pub(crate) fn finish(
        &self,
        cancel: &CancelHandle,
        cursor: Option<Arc<Cursor>>,
    ) -> Result<(), ClientError> {
        let mut statement = self.statement();
        let uuid = match &*statement {
            StatementState::Running { uuid, cancel: own } if own.same(cancel) => *uuid,
            _ => {
                if let Some(cursor) = cursor {
                    cursor.cancel();
                }
                return Err(ClientError::cancelled());
            }
        };
        *statement = match cursor {
            Some(cursor) => StatementState::Streaming { uuid, cursor },
            None => StatementState::Closed {
                uuid,
                reason: result_closed(),
            },
        };
        Ok(())
    }

    /// The cursor of result `uuid`, or why it can't be fetched.
    pub(crate) fn cursor(&self, uuid: HugeIntParts) -> Result<Arc<Cursor>, ClientError> {
        match &*self.statement() {
            StatementState::Streaming { uuid: own, cursor } if *own == uuid => {
                Ok(Arc::clone(cursor))
            }
            StatementState::Closed { uuid: own, reason } if *own == uuid => Err(reason.clone()),
            _ => Err(result_closed()),
        }
    }

    /// Cancels statement `uuid`, or whatever runs when it is [`ZERO_UUID`]. A CANCEL
    /// of an earlier statement fails, and leaves the current one alone.
    pub(crate) fn cancel(&self, uuid: HugeIntParts) -> Result<(), ClientError> {
        let mut statement = self.statement();
        if let StatementState::Running { uuid: own, .. }
        | StatementState::Streaming { uuid: own, .. } = &*statement
            && uuid != ZERO_UUID
            && *own != uuid
        {
            return Err(ClientError::invalid_input(format!(
                "Attempted to cancel a different query with id '{uuid}' instead of '{own}'"
            )));
        }
        statement.close(ClientError::cancelled());
        Ok(())
    }

    /// Stops the statement, e.g. because the session ended.
    fn abort(&self, reason: &str) {
        self.statement().close(ClientError::interrupted(reason));
    }

    /// Closes a result unread for `ttl`.
    fn expire_result(&self, now: Instant, ttl: Duration) {
        let mut statement = self.statement();
        if let StatementState::Streaming { cursor, .. } = &*statement
            && cursor.idle_for(now) >= ttl
        {
            tracing::debug!(connection_id = %self.info.connection_id, "result expired");
            statement.close(ClientError::invalid_input(
                "Result has been closed: it was not read for too long",
            ));
        }
    }
}

/// Generates a connection id: 128 random bits, as 32 upper-case hex digits (as DuckDB).
pub(crate) fn new_connection_id() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().fold(String::with_capacity(32), |mut id, b| {
        // writing to a String can't fail
        let _ = write!(id, "{b:02X}");
        id
    })
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
            session.expire_result(now, result_ttl);
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
        assert!(
            a.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase())
        );
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

    fn uuid(lower: u64) -> HugeIntParts {
        HugeIntParts { upper: 0, lower }
    }

    #[test]
    fn a_statement_replaced_while_preparing_cannot_finish() {
        let session = session("a", Duration::from_secs(60));
        let first = session.begin(uuid(1));
        let second = session.begin(uuid(2));
        assert!(first.is_cancelled());
        assert!(session.finish(&first, None).is_err());
        session.finish(&second, None).unwrap();
        assert_eq!(
            session.cursor(uuid(2)).err().unwrap().message,
            "Result has been closed"
        );
    }

    #[test]
    fn cancel_checks_the_uuid_first() {
        let session = session("a", Duration::from_secs(60));
        let running = session.begin(uuid(2));
        assert!(session.cancel(uuid(1)).is_err());
        assert!(!running.is_cancelled());
        session.cancel(ZERO_UUID).unwrap();
        assert!(running.is_cancelled());
        assert!(session.finish(&running, None).is_err());
        let error = session.cursor(uuid(2)).err().unwrap();
        assert!(error.message.contains("Interrupted"), "{error}");
        // nothing runs: a cancel succeeds and changes nothing
        session.cancel(uuid(3)).unwrap();
    }
}
