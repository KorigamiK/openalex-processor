#!/bin/bash

# Build script for OpenAlex Rust processor
echo "🦀 Building ultra-fast OpenAlex processor in Rust..."

# Navigate to the Rust project directory
cd "$(dirname "$0")"

# Install Rust if not already installed
if ! command -v cargo &> /dev/null; then
    echo "Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source ~/.cargo/env
fi

# Build the project in release mode for maximum performance
echo "📦 Building release version..."
cargo build --release

if [ $? -eq 0 ]; then
    echo "✅ Build successful!"
    echo "🚀 Run with: ./target/release/openalex_processor --help"
    echo ""
    echo "Example usage:"
    echo "./target/release/openalex_processor -i ../download/openalex-snapshot/data/works -o openalex_works.json -w 200"
else
    echo "❌ Build failed!"
    exit 1
fi
