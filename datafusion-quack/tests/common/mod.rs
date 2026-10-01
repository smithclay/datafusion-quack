//! A server on a free port, for the integration tests.
#![allow(dead_code)]

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_quack::{QuackServer, ServerOptions};

pub const TOKEN: &str = "test-token";

pub fn options() -> ServerOptions {
    ServerOptions::new().with_token(TOKEN)
}

pub struct TestServer {
    /// `quack:127.0.0.1:<port>`
    pub uri: String,
    /// `http://127.0.0.1:<port>/quack`
    pub url: String,
    task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    pub async fn start(ctx: SessionContext, options: ServerOptions) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            QuackServer::new(Arc::new(ctx))
                .with_options(options)
                .serve_with_listener(listener)
                .await
                .unwrap();
        });
        Self {
            uri: format!("quack:{address}"),
            url: format!("http://{address}/quack"),
            task,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
