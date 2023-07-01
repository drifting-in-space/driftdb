#!/bin/bash

set -e

BASE_DIR=$(dirname "$0" | xargs realpath)

echo "Testing standalone server."

cd ${BASE_DIR}/driftdb-server
killall driftdb-server 2> /dev/null || true
cargo build
cargo run &
NATIVE_SERVER_PID=$!

echo "Testing Workers-based server."

cd ${BASE_DIR}/js-pkg/packages/driftdb
npm ci --include=dev
npm test -- --forceExit --detectOpenHandles

kill ${NATIVE_SERVER_PID}

cd ${BASE_DIR}/driftdb-worker
npm ci
npm run dev &

WORKER_SERVER_PID=$!

cd ${BASE_DIR}/js-pkg/packages/driftdb
DRIFTDB_SERVER=http://localhost:8787 npm test -- --forceExit --detectOpenHandles

kill ${WORKER_SERVER_PID}
