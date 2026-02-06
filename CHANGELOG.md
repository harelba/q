# Changelog

All notable changes to q (qtextasdata) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Version bumping script (`bump-version.py`) for easier version management

## [4.0.0] - (release date)

Major release exposing q as a Python module under the package name `qtextasdata`.

### BREAKING CHANGES
- Package renamed from `q` to `qtextasdata`
- Module structure completely refactored from a single `bin/q.py` file to a proper Python package (`qtextasdata/`)
- CLI entry point changed from `bin.q:run_standalone` to `qtextasdata.cli:run_standalone`
- Removed `six` dependency (Python 2 support fully dropped)
- License updated to Apache License 2.0

### Added
- **Python module API** -- q can now be imported and used programmatically via `from qtextasdata import QTextAsData, QInputParams`
- Public API classes exported from the top-level package: `QTextAsData`, `QInputParams`, `QOutput`, `QOutputPrinter`, `QMetadata`, `QWarning`, `QError`, `DataStream`, `DataStreams`
- `__version__` attribute on the `qtextasdata` package
- `__all__` definition for explicit public API surface
- Data stream injection for in-memory data processing (`DataStream`, `data_streams_dict`)
- Query-level data reuse across multiple `execute()` calls on the same engine
- Pre-loading of data via `load_data()` method
- Per-file input parameter support via `QInputParams.merged_with()`
- Query analysis without execution via `analyze()` method
- Dedicated module-level test suite (`test/BasicModuleTests.py`)
- Comprehensive Python API reference documentation (`doc/PYTHON-API.md`)
- GitHub Actions workflows for testing and documentation
- `pytest.ini` configuration for test execution
- Version bumping script (`bump-version.py`)
- `CHANGELOG.md` following Keep a Changelog format

### Changed
- Codebase restructured into modular architecture:
  - `qtextasdata/core.py` -- Engine, API classes, and output formatting
  - `qtextasdata/cli.py` -- Command-line interface
  - `qtextasdata/sql.py` -- SQL parsing, table materialization, and SQLite operations
  - `qtextasdata/exceptions.py` -- Exception hierarchy
  - `qtextasdata/utilities.py` -- Helper functions and user-defined SQL functions
  - `qtextasdata/csv_reader.py` -- CSV parsing utilities
  - `qtextasdata/logging.py` -- Debug logging
- Test suite split from monolithic `test/test_suite.py` into focused test modules
- `setup.py` updated with proper PyPI metadata, classifiers, and project URLs
- README updated with Python module usage documentation and examples

### Removed
- `bin/q.py` monolithic single-file implementation (replaced by `qtextasdata/` package)
- `bin/__init__.py` and `bin/.qrc` (package root moved out of `bin/`)
- `bin/q.bat` Windows batch wrapper
- `six` dependency
- `pyoxidizer.bzl` build configuration
- `requirements.txt` (dependencies declared in `setup.py`)
- Old monolithic test suite `test/test_suite.py` (replaced by focused test modules)

[Unreleased]: https://github.com/harelba/q/compare/v4.0.0...HEAD
[4.0.0]: https://github.com/harelba/q/releases/tag/v4.0.0 