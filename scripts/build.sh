#!/usr/bin/env bash
# Build DashForge single binary for the current platform.
# Usage: ./scripts/build.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "── Installing build dependencies ──"
pip install pyinstaller

echo "── Building single binary ──"
pyinstaller dashforge.spec --clean --noconfirm

BINARY="dist/dashforge"
if [[ -f "$BINARY" ]]; then
    SIZE=$(du -h "$BINARY" | cut -f1)
    echo ""
    echo "✔ Built: $BINARY ($SIZE)"
    echo ""
    echo "Install globally:"
    echo "  sudo cp dist/dashforge /usr/local/bin/"
    echo ""
    echo "Or run directly:"
    echo "  ./dist/dashforge init"
    echo "  ./dist/dashforge serve"
else
    echo "✘ Build failed — binary not found at $BINARY"
    exit 1
fi
