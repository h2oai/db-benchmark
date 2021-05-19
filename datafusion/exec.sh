#!/bin/bash
set -e

RUSTFLAGS='-C target-cpu=native' cargo +nightly run --release