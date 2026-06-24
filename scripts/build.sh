#!/usr/bin/env bash
# Build Tacit single binary for the current platform.
# Usage: ./scripts/build.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "── Installing build dependencies ──"
uv sync --all-extras --dev

echo "── Building single binary ──"
uv run pyinstaller tacit.spec --clean --noconfirm

BINARY="dist/tacit"
if [[ -f "$BINARY" ]]; then
    SIZE=$(du -h "$BINARY" | cut -f1)
    echo ""
    echo "✔ Built: $BINARY ($SIZE)"
    echo ""
    echo "Install globally:"
    echo "  sudo cp dist/tacit /usr/local/bin/"
    echo ""
    echo "Or run directly:"
    echo "  ./dist/tacit init"
    echo "  ./dist/tacit serve"
else
    echo "✘ Build failed — binary not found at $BINARY"
    exit 1
fi
