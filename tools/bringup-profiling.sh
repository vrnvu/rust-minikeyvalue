#!/bin/bash

kill $(pgrep -f nginx)

PORT=3001 ./volume /tmp/volume1/ &
PORT=3002 ./volume /tmp/volume2/ &
PORT=3003 ./volume /tmp/volume3/ &
PORT=3004 ./volume /tmp/volume4/ &
PORT=3005 ./volume /tmp/volume5/ &

# Ensure the profiling binary is built
cargo build --profile profiling

samply record target/profiling/rust-minikeyvalue --leveldb-path /tmp/indexdb/ --volumes localhost:3001,localhost:3002,localhost:3003,localhost:3004,localhost:3005