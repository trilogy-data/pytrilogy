#!/bin/bash
# Build script for preql-import-resolver

set -e

echo "Building preql-import-resolver..."

# Check for Rust
if ! command -v cargo &> /dev/null; then
    echo "Rust is not installed. Install from https://rustup.rs/"
    exit 1
fi

# Build release binary
cargo build --release

echo ""
echo "Build successful!"
echo "Binary location: ./target/release/preql-import-resolver"
echo ""
echo "To install globally, run:"
echo "  cargo install --path ."
echo ""
echo "To test:"
echo "  ./target/release/preql-import-resolver --help"
