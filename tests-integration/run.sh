#!/usr/bin/env bash
# Real-client integration tests (gate G5), locally and in CI.
#
#   1. DuckDB 2.0 ATTACHes the server: the ATTACH scenario, then the differential
#      test (TPC-H SF0.01 and a type matrix, against native DuckDB).
#   2. The quack_protocol live suite, read-only tests, plus TLS fingerprint pinning.
#   3. The datafusion-table-providers Quack suite in seeded mode.
#
# Environment:
#   DUCKDB              a DuckDB 2.0 CLI to use instead of downloading one
#   DUCKDB_STAGED       the staged DuckDB build to download (commit/version)
#   QUACK_PROTOCOL_DIR  a quack_protocol_rs checkout to use instead of cloning
#   PROVIDERS_DIR       a datafusion-table-providers checkout to use instead of cloning
#   ONLY                run only some steps: attach,differential,client,provider
#
# Logs and server output go to tests-integration/out/.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HERE="$ROOT/tests-integration"
CACHE="$HERE/.cache"
OUT="$HERE/out"
mkdir -p "$CACHE" "$OUT"

DUCKDB_STAGED=${DUCKDB_STAGED:-09eb7f7004/v2.0.0-alpha43586}
QUACK_PROTOCOL_REPO=${QUACK_PROTOCOL_REPO:-https://github.com/smithclay/quack_protocol_rs}
# feat/server-feature
QUACK_PROTOCOL_REV=${QUACK_PROTOCOL_REV:-5c5f4f785c79d8a34beadfb172ae13f21dcdadb5}
PROVIDERS_REPO=${PROVIDERS_REPO:-https://github.com/smithclay/datafusion-table-providers}
# feat/quack-seed-mode
PROVIDERS_REV=${PROVIDERS_REV:-7e05ff28c506b2e73fa19c01c732285fd427faad}
ONLY=${ONLY:-attach,differential,client,provider}
TOKEN=integration-token
SEEDED_PORT=${SEEDED_PORT:-19494}
DATA_PORT=${DATA_PORT:-19495}
TLS_PORT=${TLS_PORT:-19496}

PIDS=()
cleanup() {
    # ${PIDS[@]+...}: bash 3.2 calls an empty array unbound under `set -u`
    for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

step() { [[ ",$ONLY," == *",$1,"* ]]; }
log() { printf '\n== %s\n' "$*"; }

# --- DuckDB 2.0 ---------------------------------------------------------------

duckdb_cli() {
    if [[ -n "${DUCKDB:-}" ]]; then
        echo "$DUCKDB"
        return
    fi
    local os arch dist
    os=$(uname -s)
    arch=$(uname -m)
    case "$os/$arch" in
        Linux/x86_64) dist=linux-amd64 ;;
        Linux/aarch64 | Linux/arm64) dist=linux-arm64 ;;
        Darwin/arm64) dist=osx-arm64 ;;
        Darwin/x86_64) dist=osx-amd64 ;;
        *) echo "no DuckDB build for $os/$arch; set DUCKDB" >&2; exit 1 ;;
    esac
    local dir="$CACHE/duckdb/${DUCKDB_STAGED//\//_}"
    if [[ ! -x "$dir/duckdb" ]]; then
        mkdir -p "$dir"
        curl -fsSL "https://duckdb-staging.duckdb.org/${DUCKDB_STAGED}/duckdb/duckdb/github_release/duckdb-cli-${dist}.tar.gz" \
            | tar -C "$dir" -xzf -
    fi
    echo "$dir/duckdb"
}

DUCKDB=$(duckdb_cli)
log "DuckDB $("$DUCKDB" --version)"

# --- the server -----------------------------------------------------------------

log "building datafusion-quack"
cargo build --release --locked -p datafusion-quack-cli --manifest-path "$ROOT/Cargo.toml"
SERVER="$ROOT/target/release/datafusion-quack"

wait_for() {
    local url=$1
    for _ in $(seq 1 120); do
        if curl -fsSk -o /dev/null "$url"; then return 0; fi
        sleep 0.5
    done
    echo "server at $url did not start" >&2
    return 1
}

start_server() {
    local name=$1
    shift
    RUST_LOG=${RUST_LOG:-info} "$SERVER" --token "$TOKEN" "$@" > "$OUT/server-$name.log" 2>&1 &
    PIDS+=($!)
}

start_server seeded --port "$SEEDED_PORT" --seed provider-fixtures
wait_for "http://127.0.0.1:$SEEDED_PORT/"

# --- 1. DuckDB ATTACH and the differential test ----------------------------------

if step attach; then
    log "DuckDB ATTACH scenario"
    python3 "$HERE/attach.py" --duckdb "$DUCKDB" --uri "quack:127.0.0.1:$SEEDED_PORT" --token "$TOKEN"
fi

if step differential; then
    DATA="$CACHE/tpch-sf0.01"
    if [[ ! -f "$DATA/lineitem.parquet" ]]; then
        log "generating TPC-H SF0.01"
        rm -rf "$DATA"
        "$DUCKDB" -init /dev/null -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf = 0.01); EXPORT DATABASE '$DATA' (FORMAT parquet);"
    fi
    python3 "$HERE/differential.py" --duckdb "$DUCKDB" --uri x --token x --data "$DATA" --make-type-matrix
    start_server data --port "$DATA_PORT" -d "$DATA"
    wait_for "http://127.0.0.1:$DATA_PORT/"
    log "differential test: TPC-H and the type matrix, native DuckDB vs ATTACH"
    python3 "$HERE/differential.py" --duckdb "$DUCKDB" --uri "quack:127.0.0.1:$DATA_PORT" \
        --token "$TOKEN" --data "$DATA" --verbose
fi

# --- checkouts ------------------------------------------------------------------

checkout() {
    local repo=$1 rev=$2 dir=$3
    if [[ ! -d "$dir/.git" ]]; then
        git clone --quiet --filter=blob:none "$repo" "$dir"
    fi
    git -C "$dir" fetch --quiet origin
    git -C "$dir" checkout --quiet --detach "$rev"
}

# --- 2. the quack_protocol live suite ------------------------------------------------

# The read-only tests that need no DuckDB-only types. Left out: the connection-model
# tests that start their own servers with DuckDB's quack_serve/quack_stop, the
# APPEND tests (writes, protocol v1), and the type tests built on HUGEINT, ENUM,
# TIMETZ, VARIANT or DuckDB's literal types.
CLIENT_TESTS=(
    live_quack_basic_query_when_configured
    live_quack_preserves_empty_result_schema
    live_quack_fetches_large_results_and_sequence_vectors
    live_quack_supports_parameterized_queries
    live_quack_surfaces_server_errors
    live_quack_reports_negotiated_version
    live_quack_round_trips_nested_types
    live_quack_pinned_tls_connects_to_self_signed_server
    live_quack_pinned_tls_rejects_other_certificates
    arrow_output::live_quack_arrow_preserves_empty_result_schema
    arrow_output::live_quack_arrow_streams_multiple_batches_with_one_schema
    connection_model::live_quack_pool_reports_server_info_and_size
    connection_model::live_quack_pool_surfaces_sql_errors_without_retiring_connections
)

if step client; then
    CLIENT_DIR=${QUACK_PROTOCOL_DIR:-$CACHE/quack_protocol_rs}
    [[ -n "${QUACK_PROTOCOL_DIR:-}" ]] || checkout "$QUACK_PROTOCOL_REPO" "$QUACK_PROTOCOL_REV" "$CLIENT_DIR"

    log "TLS server with a self-signed certificate"
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=localhost" \
        -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" \
        -keyout "$OUT/tls-key.pem" -out "$OUT/tls-cert.pem" 2> /dev/null
    FINGERPRINT=$(openssl x509 -in "$OUT/tls-cert.pem" -noout -fingerprint -sha256 | cut -d= -f2)
    start_server tls --port "$TLS_PORT" --tls-cert "$OUT/tls-cert.pem" --tls-key "$OUT/tls-key.pem"
    wait_for "https://127.0.0.1:$TLS_PORT/"

    log "quack_protocol live suite (read-only tests)"
    (
        cd "$CLIENT_DIR"
        QUACK_SERVER_URI="quack:127.0.0.1:$SEEDED_PORT" QUACK_AUTH_TOKEN="$TOKEN" \
            QUACK_TLS_SERVER_URI="quack:127.0.0.1:$TLS_PORT" QUACK_TLS_FINGERPRINT="$FINGERPRINT" \
            cargo test --features arrow --test integration -- --exact "${CLIENT_TESTS[@]}"
    ) 2>&1 | tee "$OUT/client-suite.log"
fi

# --- 3. the table provider suite ---------------------------------------------------------

if step provider; then
    PROVIDERS=${PROVIDERS_DIR:-$CACHE/datafusion-table-providers}
    [[ -n "${PROVIDERS_DIR:-}" ]] || checkout "$PROVIDERS_REPO" "$PROVIDERS_REV" "$PROVIDERS"
    log "datafusion-table-providers Quack suite, seeded mode"
    (
        cd "$PROVIDERS"
        QUACK_SERVER_URI="quack:127.0.0.1:$SEEDED_PORT" QUACK_AUTH_TOKEN="$TOKEN" QUACK_SEED_MODE=1 \
            cargo test -p datafusion-table-providers --test integration --no-default-features \
            --features quack -- quack
    ) 2>&1 | tee "$OUT/provider-suite.log"
fi

log "all integration steps passed"
