#!/bin/bash -eux

umask 022

RUST_TEST_THREADS=1 cargo test -p haumaru -p haumaru-api
