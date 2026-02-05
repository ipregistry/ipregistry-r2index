#!/usr/bin/env bash
set -euo pipefail

# Build package and publish to PyPI
#
# Requirements:
#   - ~/.pypirc configured, OR
#   - PYPI_TOKEN environment variable (API token from pypi.org)
#
# Usage:
#   ./scripts/publish.sh

cd "$(dirname "$0")/.."

echo "Installing build dependencies..."
pip install --quiet build twine

echo "Cleaning previous builds..."
rm -rf dist/ build/ *.egg-info src/*.egg-info

echo "Building package..."
python -m build

echo "Publishing to PyPI..."
if [[ -n "${PYPI_TOKEN:-}" ]]; then
    twine upload \
        --username __token__ \
        --password "${PYPI_TOKEN}" \
        dist/*
else
    twine upload dist/*
fi

echo "Done!"
