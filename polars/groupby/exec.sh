#!/bin/bash
set -e

RUSTFLAGS='-C target-cpu=native' cargo run +nighlty --release
