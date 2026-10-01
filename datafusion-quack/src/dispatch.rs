//! Request dispatch: one decoded message in, one encoded response out.

use std::sync::{Arc, PoisonError};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow_quack::{EncodedChunk, arrow_to_logical_type};
use bytes::Bytes;
use datafusion::sql::parser::{DFParserBuilder, Statement};
use datafusion::sql::sqlparser::dialect::{Dialect, dialect_from_str};
use std::panic::AssertUnwindSafe;

use futures::{FutureExt, StreamExt};
use quack_protocol::LogicalTypes;
use quack_protocol::server::{
    BinaryReader, HugeIntParts, MessageHeader, MessageType, QUACK_VERSION, QuackMessage,
    decode_header, decode_request, encode_prepare_response, encode_response,
};

use crate::auth::{AuthProvider, ConnectionRequest, SessionInfo};
use crate::cursor::{BatchProducer, CancelHandle, Cursor};
use crate::error::ClientError;
use crate::hooks::{QueryHook, QueryOutput};
use crate::options::ServerOptions;
use crate::session::{Session, SessionContextProvider, SessionStore, ZERO_UUID, new_connection_id};
use crate::transaction::Control;

/// The version this server reports. DuckDB clients only log it.
pub(crate) const SERVER_VERSION: &str = concat!("datafusion-quack v", env!("CARGO_PKG_VERSION"));

/// Everything a request needs: configuration, auth, hooks and the sessions.
#[derive(Debug)]
pub(crate) struct Dispatcher {
    pub(crate) options: ServerOptions,
    pub(crate) auth: Arc<dyn AuthProvider>,
    pub(crate) provider: Arc<dyn SessionContextProvider>,
    pub(crate) hooks: Vec<Arc<dyn QueryHook>>,
    pub(crate) sessions: SessionStore,
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStore")
            .field("sessions", &self.len())
            .finish()
    }
}

impl Dispatcher {
    /// Answers one request body. Never fails: errors become an ERROR_RESPONSE, and so
    /// does a panic, so a bug answers one request with an error rather than dropping
    /// the connection.
    pub(crate) async fn handle(&self, body: &[u8]) -> Bytes {
        match AssertUnwindSafe(self.dispatch(body)).catch_unwind().await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                tracing::debug!(%error, "request failed");
                error_response(&error)
            }
            Err(_) => {
                tracing::error!("panic while handling a request");
                error_response(&ClientError::new(
                    crate::error::ExceptionType::Internal,
                    "the server failed while handling the request",
                ))
            }
        }
    }

    async fn dispatch(&self, body: &[u8]) -> Result<Bytes, ClientError> {
        let header = decode_header(&mut BinaryReader::new(body))
            .map_err(|e| ClientError::invalid_input(format!("Malformed request: {e}")))?;
        if !server_supports(header.message_type) {
            return Err(ClientError::invalid_input(format!(
                "Unsupported message type for server: {:?}",
                header.message_type
            )));
        }
        let session = if header.message_type == MessageType::ConnectionRequest {
            None
        } else {
            let connection_id = header.connection_id.as_deref().unwrap_or_default();
            Some(self.sessions.renew(connection_id)?)
        };
        let message = decode_request(body)
            .map_err(|e| ClientError::invalid_input(format!("Malformed request: {e}")))?;

        match (message, session) {
            (message @ QuackMessage::ConnectionRequest { .. }, _) => self.connect(message).await,
            (
                QuackMessage::PrepareRequest {
                    sql,
                    query_uuid,
                    inline_rows,
                    ..
                },
                Some(session),
            ) => {
                self.prepare(&session, &sql, query_uuid.unwrap_or(ZERO_UUID), inline_rows)
                    .await
            }
            (
                QuackMessage::FetchRequest {
                    result_uuid,
                    batch_index,
                    ack_index,
                    ..
                },
                Some(session),
            ) => {
                self.fetch(
                    &session,
                    result_uuid,
                    batch_index.unwrap_or(0),
                    ack_index.unwrap_or(0),
                )
                .await
            }
            (QuackMessage::CancelRequest { query_uuid, .. }, Some(session)) => {
                session.cancel(query_uuid)?;
                success()
            }
            (
                QuackMessage::HeartbeatRequest { .. } | QuackMessage::Acknowledgement { .. },
                Some(_),
            ) => {
                // no result is retained for a replay, so an acknowledgement has nothing to drop
                success()
            }
            (QuackMessage::Disconnect { .. }, Some(session)) => {
                if self.sessions.remove(&session.info.connection_id).is_none() {
                    return Err(ClientError::invalid_input(
                        "Connection does not exist / already disconnected",
                    ));
                }
                success()
            }
            (other, _) => Err(ClientError::invalid_input(format!(
                "Unsupported message type for server: {:?}",
                other.message_type()
            ))),
        }
    }

    async fn connect(&self, message: QuackMessage) -> Result<Bytes, ClientError> {
        let QuackMessage::ConnectionRequest {
            auth_string,
            client_duckdb_version,
            client_platform,
            min_supported_quack_version,
            max_supported_quack_version,
            heartbeat_timeout_seconds,
            ..
        } = message
        else {
            return Err(ClientError::invalid_input("expected CONNECTION_REQUEST"));
        };
        if min_supported_quack_version > QUACK_VERSION
            || max_supported_quack_version < QUACK_VERSION
        {
            return Err(ClientError::invalid_input(format!(
                "Unsupported Quack version - server only supports version {QUACK_VERSION} of quack"
            )));
        }
        let max_seconds = self.options.heartbeat_max().as_secs().max(1);
        if heartbeat_timeout_seconds == 0 {
            return Err(ClientError::invalid_input(format!(
                "heartbeat_timeout out of range - must be between 1 and {max_seconds} seconds"
            )));
        }
        let heartbeat_seconds = heartbeat_timeout_seconds.min(max_seconds);

        let request = ConnectionRequest {
            auth_string: auth_string.unwrap_or_default(),
            client_version: client_duckdb_version.unwrap_or_default(),
            client_platform: client_platform.unwrap_or_default(),
        };
        self.auth.authenticate(&request).await?;

        let info = SessionInfo {
            connection_id: new_connection_id(),
            client_version: request.client_version,
            client_platform: request.client_platform,
        };
        let ctx = self.provider.session_context(&info).await?;
        let connection_id = info.connection_id.clone();
        self.sessions.insert(Arc::new(Session::new(
            info,
            ctx,
            Duration::from_secs(heartbeat_seconds),
        )))?;
        tracing::debug!(%connection_id, "session opened");

        Ok(Bytes::from(encode_response(
            &QuackMessage::ConnectionResponse {
                header: MessageHeader::new(MessageType::ConnectionResponse)
                    .with_connection(connection_id),
                server_duckdb_version: Some(SERVER_VERSION.to_string()),
                server_platform: Some(platform()),
                quack_version: Some(QUACK_VERSION),
                heartbeat_timeout_seconds: Some(heartbeat_seconds),
            },
        )?))
    }

    async fn prepare(
        &self,
        session: &Session,
        sql: &str,
        uuid: HugeIntParts,
        inline_rows: Option<u64>,
    ) -> Result<Bytes, ClientError> {
        self.auth.authorize(&session.info, sql).await?;
        tracing::debug!(connection_id = %session.info.connection_id, sql, "prepare");
        let cancel = session.begin(uuid);
        // a cancel stops planning and DDL too: the statement is dropped mid-run
        let prepared = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(ClientError::cancelled()),
            prepared = self.run_prepare(session, sql, uuid, inline_rows, &cancel) => prepared,
        };
        match prepared {
            Ok((response, cursor)) => {
                session.finish(&cancel, cursor)?;
                Ok(response)
            }
            Err(error) => {
                // the error is the answer; a FETCH of the statement finds it closed
                let _ = session.finish(&cancel, None);
                Err(error)
            }
        }
    }

    /// Runs `sql`, and encodes the PREPARE response with the leading batches inline.
    /// Returns the cursor for the rest, if any.
    async fn run_prepare(
        &self,
        session: &Session,
        sql: &str,
        uuid: HugeIntParts,
        inline_rows: Option<u64>,
        cancel: &CancelHandle,
    ) -> Result<(Bytes, Option<Arc<Cursor>>), ClientError> {
        let stream = match self.run_sql(session, sql, cancel).await? {
            QueryOutput::Rows(stream) => stream,
            QueryOutput::Success => {
                let response = encode_prepare_response(
                    &MessageHeader::new(MessageType::PrepareResponse),
                    &[LogicalTypes::boolean()],
                    &["Success".to_string()],
                    false,
                    &[] as &[EncodedChunk],
                    uuid,
                )?;
                return Ok((Bytes::from(response), None));
            }
        };
        let (types, names) = result_columns(&stream.schema())?;
        let mut producer =
            BatchProducer::new(stream, cancel.clone(), self.options.batch_target_bytes());

        // the leading batches go inline, so a small result takes one round trip
        // a client may ask for fewer inline rows, not more than the server allows
        let max_inline_rows = inline_rows.map_or(self.options.inline_rows(), |rows| {
            rows.min(self.options.inline_rows())
        });
        let mut chunks = Vec::new();
        let mut rows = 0u64;
        let mut consumed = 0u64;
        let mut finished = false;
        while rows < max_inline_rows {
            match producer.next().await? {
                Some(batch) => {
                    consumed += 1;
                    rows += batch.rows as u64;
                    chunks.extend(batch.chunks);
                }
                None => {
                    finished = true;
                    break;
                }
            }
        }

        let cursor = Cursor::new(
            (!finished).then_some(producer),
            consumed,
            self.options.max_inflight_batches(),
            cancel.clone(),
        );
        let response = encode_prepare_response(
            &MessageHeader::new(MessageType::PrepareResponse),
            &types,
            &names,
            !finished,
            &chunks,
            uuid,
        )?;
        Ok((Bytes::from(response), Some(Arc::new(cursor))))
    }

    /// Parses `sql` and runs its statements. Every statement before the last runs to
    /// completion; the last one's output is returned.
    async fn run_sql(
        &self,
        session: &Session,
        sql: &str,
        cancel: &CancelHandle,
    ) -> Result<QueryOutput, ClientError> {
        let mut statements = parse(session, sql)?;
        let Some(last) = statements.pop_back() else {
            return Err(ClientError::invalid_input("No statement to prepare!"));
        };
        for statement in statements {
            if let QueryOutput::Rows(mut stream) = self.run_statement(session, statement).await? {
                while let Some(batch) = stream.next().await {
                    batch?;
                    if cancel.is_cancelled() {
                        return Err(ClientError::cancelled());
                    }
                }
            }
        }
        self.run_statement(session, last).await
    }

    async fn run_statement(
        &self,
        session: &Session,
        statement: Statement,
    ) -> Result<QueryOutput, ClientError> {
        {
            let mut transaction = session
                .transaction
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match Control::of(&statement) {
                Some(control) => return transaction.apply(control).map(|()| QueryOutput::Success),
                None => transaction.run(&statement),
            }
        }
        for hook in &self.hooks {
            if let Some(output) = hook.handle(&statement, &session.ctx, &session.info).await {
                return output;
            }
        }
        let state = session.ctx.state();
        let plan = state.statement_to_plan(statement).await?;
        let frame = session.ctx.execute_logical_plan(plan).await?;
        let stream = frame.execute_stream().await?;
        if stream.schema().fields().is_empty() {
            // DDL: run it, and answer as DuckDB does
            let mut stream = stream;
            while let Some(batch) = stream.next().await {
                batch?;
            }
            return Ok(QueryOutput::Success);
        }
        Ok(QueryOutput::Rows(stream))
    }

    async fn fetch(
        &self,
        session: &Session,
        uuid: HugeIntParts,
        batch_index: u64,
        ack_index: u64,
    ) -> Result<Bytes, ClientError> {
        session.cursor(uuid)?.fetch(batch_index, ack_index).await
    }
}

/// Parses `sql` in the session's dialect.
fn parse(
    session: &Session,
    sql: &str,
) -> Result<std::collections::VecDeque<Statement>, ClientError> {
    let dialect_name = session
        .ctx
        .state_ref()
        .read()
        .config()
        .options()
        .sql_parser
        .dialect
        .to_string();
    let dialect: Box<dyn Dialect> = dialect_from_str(&dialect_name)
        .ok_or_else(|| ClientError::invalid_input(format!("unknown SQL dialect {dialect_name}")))?;
    DFParserBuilder::new(sql)
        .with_dialect(dialect.as_ref())
        .build()
        .and_then(|mut parser| parser.parse_statements())
        .map_err(ClientError::from)
}

fn server_supports(message_type: MessageType) -> bool {
    matches!(
        message_type,
        MessageType::ConnectionRequest
            | MessageType::PrepareRequest
            | MessageType::FetchRequest
            | MessageType::DisconnectMessage
            | MessageType::CancelRequest
            | MessageType::Acknowledgement
            | MessageType::HeartbeatRequest
    )
}

/// The DuckDB types and names of a result's columns.
fn result_columns(
    schema: &SchemaRef,
) -> Result<(Vec<quack_protocol::LogicalType>, Vec<String>), ClientError> {
    let types = schema
        .fields()
        .iter()
        .map(|field| {
            arrow_to_logical_type(field.data_type()).map_err(|e| {
                ClientError::from(e).with_context(&format!("column \"{}\"", field.name()))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let names = schema.fields().iter().map(|f| f.name().clone()).collect();
    Ok((types, names))
}

fn success() -> Result<Bytes, ClientError> {
    Ok(Bytes::from(encode_response(
        &QuackMessage::SuccessResponse {
            header: MessageHeader::new(MessageType::SuccessResponse),
        },
    )?))
}

/// Encodes `error` as an ERROR_RESPONSE.
pub(crate) fn error_response(error: &ClientError) -> Bytes {
    let message = QuackMessage::ErrorResponse {
        header: MessageHeader::new(MessageType::ErrorResponse),
        message: error.message.clone(),
        exception_type: Some(error.exception_type.name().to_string()),
        extra_info: Vec::new(),
        must_invalidate: false,
    };
    match encode_response(&message) {
        Ok(bytes) => Bytes::from(bytes),
        // an error response holds only strings, so this can't happen
        Err(_) => Bytes::new(),
    }
}

fn platform() -> String {
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    };
    let os = match std::env::consts::OS {
        "macos" => "osx",
        other => other,
    };
    format!("{os}_{arch}")
}
