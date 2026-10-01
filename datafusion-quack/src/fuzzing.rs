//! A request handler without HTTP, for fuzzing (the `fuzzing` feature).

use std::sync::Arc;

use bytes::Bytes;
use datafusion::prelude::SessionContext;

use crate::{QuackServer, ServerOptions};

/// Feeds request bodies straight to the dispatcher, as `POST /quack` would.
#[derive(Debug)]
pub struct Harness {
    dispatcher: Arc<crate::dispatch::Dispatcher>,
}

impl Harness {
    /// A server over `ctx` with `options`.
    pub fn new(ctx: Arc<SessionContext>, options: ServerOptions) -> Self {
        Self {
            dispatcher: QuackServer::new(ctx).with_options(options).dispatcher(),
        }
    }

    /// The response to one request body. It is always a well-formed message.
    pub async fn handle(&self, body: &[u8]) -> Bytes {
        self.dispatcher.handle(body).await
    }

    /// Whether `response` decodes as a Quack message.
    pub fn is_well_formed(response: &[u8]) -> bool {
        quack_protocol::server::decode_request(response).is_ok()
    }
}
