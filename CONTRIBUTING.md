# Contributing

Thanks for helping. This file lists what CI checks and what reviewers look for.

## Before you open a pull request

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo deny check
typos
```

Changes to the protocol path should also pass `tests-integration/run.sh`, which
downloads the pinned DuckDB 2.0 build and runs the real clients.

## Quality gates

- **G1, every PR.** Format, clippy (default and all features), tests on stable and
  nightly, an MSRV build (`rust-version` in `Cargo.toml`, equal to DataFusion's),
  rustdoc without warnings, a `cargo package` dry run, `cargo deny`, `typos`.
- **G2, wire conformance.** `testdata/wire/golden` holds requests and DuckDB 2.0's
  responses; our responses must be byte-identical, or the difference documented in
  `datafusion-quack/tests/wire.rs`.
- **G3, SQL replay.** `datafusion-quack-cli/tests/replay.rs` replays every statement
  DuckDB's `ATTACH`, the table provider and the `quack_protocol` client sent in
  recorded sessions. When a client starts sending something new, record it with
  `tests-integration/record_proxy.py` and add it.
- **G4, property and fuzz.** `proptest` round trips in `arrow-quack`, hostile-input
  tests, a mutation proptest per PR, and `cargo fuzz` nightly.
- **G5, real clients.** `tests-integration/run.sh`: DuckDB `ATTACH` and the TPC-H
  differential test, the `quack_protocol` live suite, and the provider suite in seeded
  mode.
- **G6, releases.** release-plz, with independent crate versions and a generated
  `CHANGELOG.md`.

## Review norms

- Extend DataFusion through hooks, functions and planner extensions, not by
  rewriting SQL text. AST rewrites are only acceptable for catalog queries.
- Put optional behavior behind a feature, and give each feature its own CI entry.
- Keep pull requests small and focused. A bug fix starts with a failing regression
  test.
- Errors are typed, never matched on their text and never swallowed. No `unwrap` or
  `expect` on data that came from a client; clippy denies `unwrap_used` and
  `expect_used` in the library crates.
- Libraries log through `tracing` and never print.
- TLS uses rustls with the `ring` provider.
- Keep the public API stable. DataFusion is upgraded in one manual PR that moves
  `datafusion` and `arrow` together; Dependabot's DataFusion major bumps are closed.
- Workspace dependencies use `default-features = false`. `Cargo.lock` is committed
  and CI builds with `--locked`.
