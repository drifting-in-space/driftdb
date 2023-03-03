#!/bin/sh

set -e

npm ci || npm ci # long story

cd driftdb-server

cargo build
cargo run &

cd ../

cd js-pkg/tests

npm test
