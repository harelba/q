# Publishing qtextasdata to PyPI

This document describes how to publish a new release of `qtextasdata` to PyPI.

## Two Publishing Methods

### Method 1: GitHub Release (Automated)

Creating a GitHub release with a version tag (e.g., `v4.0.0`) triggers the `publish-package.yaml` workflow, which:

1. Runs the full test suite across Python 3.8-3.12
2. Updates the version in `__init__.py` from the tag
3. Builds sdist and wheel
4. Publishes to PyPI

**Steps:**

1. Bump the version locally:
   ```bash
   python3 bump-version.py patch --tag   # or minor, major
   git push origin v4.0.1               # push the tag
   ```

2. Create a GitHub release from the tag at https://github.com/harelba/q/releases/new

3. The workflow publishes automatically.

**Required GitHub secret:** `PYPI_API_TOKEN` -- a PyPI API token with upload permissions for the `qtextasdata` project.

To create a token:
1. Go to https://pypi.org/manage/account/token/
2. Create a token scoped to the `qtextasdata` project
3. Add it as a repository secret named `PYPI_API_TOKEN` at https://github.com/harelba/q/settings/secrets/actions

### Method 2: Local Publishing (Manual)

Use the `publish.sh` script for local publishing.

**Prerequisites:**
```bash
pip install build twine
```

**Dry run (build and check only):**
```bash
./publish.sh --dry-run
```

**Publish to TestPyPI first:**
```bash
./publish.sh --test
```

Then verify the package:
```bash
pip install --index-url https://test.pypi.org/simple/ qtextasdata
python3 -c "from qtextasdata import QTextAsData; print('OK')"
```

**Publish to production PyPI:**
```bash
./publish.sh
```

**Authentication:**

Set environment variables before running:
```bash
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=pypi-AgEI...   # Your API token
./publish.sh
```

Or configure `~/.pypirc`:
```ini
[pypi]
username = __token__
password = pypi-AgEI...

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-AgEI...
```

## Release Checklist

Before publishing a new version:

- [ ] All tests pass: `pytest -v`
- [ ] Version bumped: `python3 bump-version.py <major|minor|patch>`
- [ ] CHANGELOG.md updated with new version section
- [ ] Package builds cleanly: `./publish.sh --dry-run`
- [ ] (Optional) Tested on TestPyPI: `./publish.sh --test`

## Version Management

Use `bump-version.py` to manage versions:

```bash
python3 bump-version.py patch          # 4.0.0 -> 4.0.1
python3 bump-version.py minor          # 4.0.0 -> 4.1.0
python3 bump-version.py major          # 4.0.0 -> 5.0.0
python3 bump-version.py prerelease     # 4.0.0 -> 4.0.0-beta.1
python3 bump-version.py --set 4.2.0    # Set explicitly (used by CI)
```

The version is stored in `qtextasdata/__init__.py` as `q_version` and is also exposed as `__version__`.
