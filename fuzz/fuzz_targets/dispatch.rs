//! Fuzzes request decoding and dispatch: any body gets a well-formed response.
//!
//! Seed the corpus with real requests: `cp ../testdata/wire/golden/*.req corpus/dispatch/`.
#![no_main]

use std::sync::{Arc, LazyLock};

use datafusion::prelude::SessionContext;
use datafusion_quack::ServerOptions;
use datafusion_quack::fuzzing::Harness;
use libfuzzer_sys::fuzz_target;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
});

static HARNESS: LazyLock<Harness> = LazyLock::new(|| {
    // the dispatcher turns a panic into an error response; abort instead, so the
    // fuzzer reports it
    std::panic::set_hook(Box::new(|info| {
        eprintln!("{info}");
        std::process::abort();
    }));
    Harness::new(
        Arc::new(SessionContext::new()),
        // no token, so CONNECTION requests open sessions and reach the rest of dispatch
        ServerOptions::new().with_max_sessions(64),
    )
});

fuzz_target!(|body: &[u8]| {
    let response = RUNTIME.block_on(HARNESS.handle(body));
    // whatever came in, what goes out decodes
    assert!(Harness::is_well_formed(&response), "a malformed response");
});
