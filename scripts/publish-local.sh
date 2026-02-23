#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <version> [testpypi|pypi]"
  exit 1
fi

VERSION="$1"
REPOSITORY="${2:-testpypi}"

if [[ "$REPOSITORY" != "testpypi" && "$REPOSITORY" != "pypi" ]]; then
  echo "Repository must be 'testpypi' or 'pypi'"
  exit 1
fi

if [[ -z "${TWINE_PASSWORD:-}" ]]; then
  echo "TWINE_PASSWORD is required. Create an API token in $REPOSITORY and export it as TWINE_PASSWORD."
  exit 1
fi

export TWINE_USERNAME="__token__"

if [[ "$REPOSITORY" == "testpypi" ]]; then
  REPO_URL="https://test.pypi.org/legacy/"
else
  REPO_URL="https://upload.pypi.org/legacy/"
fi

ORIGINAL_PYPROJECT="$(cat pyproject.toml)"
cleanup() {
  printf '%s' "$ORIGINAL_PYPROJECT" > pyproject.toml
}
trap cleanup EXIT

sed -E "s/^version = \".*\"$/version = \"$VERSION\"/" pyproject.toml > pyproject.toml.tmp
mv pyproject.toml.tmp pyproject.toml

rm -rf dist/

python -m pip install --upgrade build twine
python -m build
python -m twine check dist/*
python -m twine upload --repository-url "$REPO_URL" dist/*
