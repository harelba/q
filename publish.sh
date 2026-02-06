#!/bin/bash
set -e

# Publish qtextasdata to PyPI
#
# Prerequisites:
#   pip install build twine
#
# Usage:
#   ./publish.sh              # Publish to PyPI (production)
#   ./publish.sh --test       # Publish to TestPyPI first
#   ./publish.sh --dry-run    # Build and check only, do not upload
#
# PyPI credentials:
#   Set TWINE_USERNAME and TWINE_PASSWORD environment variables, or
#   configure a ~/.pypirc file. For token-based auth (recommended):
#     TWINE_USERNAME=__token__
#     TWINE_PASSWORD=pypi-AgEI...  (your API token)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

USE_TEST_PYPI=false
DRY_RUN=false

for arg in "$@"; do
    case $arg in
        --test)  USE_TEST_PYPI=true ;;
        --dry-run) DRY_RUN=true ;;
        --help|-h)
            sed -n '3,14p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown option: $arg"
            echo "Run with --help for usage."
            exit 1
            ;;
    esac
done

# Read version
VERSION=$(python3 -c "from qtextasdata import q_version; print(q_version)")
echo "=== Publishing qtextasdata v${VERSION} ==="
echo ""

# Verify imports work
echo "Verifying package imports..."
python3 -c "from qtextasdata import QTextAsData, QInputParams, __version__; print('  Package version:', __version__)"
echo ""

# Run tests
echo "Running tests..."
python3 -m pytest test/ -q --tb=short
echo ""

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf dist/ build/ *.egg-info/
echo ""

# Build
echo "Building sdist and wheel..."
python3 -m build
echo ""

# Check
echo "Checking distribution..."
python3 -m twine check dist/*
echo ""

if [ "$DRY_RUN" = true ]; then
    echo "=== Dry run complete. Artifacts in dist/ ==="
    ls -la dist/
    exit 0
fi

# Upload
if [ "$USE_TEST_PYPI" = true ]; then
    echo "=== Uploading to TestPyPI ==="
    python3 -m twine upload --repository testpypi dist/*
    echo ""
    echo "Package uploaded to TestPyPI."
    echo "Install with: pip install --index-url https://test.pypi.org/simple/ qtextasdata"
    echo ""
    echo "To publish to production PyPI, run:"
    echo "  ./publish.sh"
else
    echo "=== Uploading to PyPI ==="
    read -p "Publish qtextasdata ${VERSION} to PyPI? [y/N] " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Upload cancelled."
        exit 0
    fi
    python3 -m twine upload dist/*
    echo ""
    echo "=== Published qtextasdata ${VERSION} to PyPI ==="
    echo "Install with: pip install qtextasdata"
fi
