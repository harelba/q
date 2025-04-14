# Refactoring Tasks for `qtextasdata` Python Package

This document outlines the iterative process to refactor the existing q.py codebase into a proper Python package named `qtextasdata`.

## Project Goals

- **Deliverable**: A Python package named `qtextasdata` installable via `pip`.
- **Core Logic**: Encapsulate all existing logic within the `qtextasdata` module with 100% functional equivalence.
  - No new logic, no missing logic
  - Original comments preserved
  - No new comments or docstrings
  - Code visible exactly as it was in the original file, only moved
- **Command-Line Interface**: Preserve the `q` command as an entry point wrapper around the module.
- **Testing**: Maintain end-to-end tests, invoking the CLI tool after package installation.

## Workflow Guidelines

Each refactoring task should:
1. Move one aspect/component from q.py to a module file
2. Import the component back into q.py from the package
3. Remove the original code from q.py, making q.py gradually simpler
4. Run tests to ensure functionality is preserved
5. Proceed to the next component only after tests pass

The goal is to gradually simplify q.py by having it use the refactored code directly through imports, without any additional wrappers in q.py itself.

## Detailed Tasks

### Phase 1: Setup Package Structure

- [ ] **Task 1.1**: Create the basic `qtextasdata` package directory structure
  - Create qtextasdata/ directory
  - Create empty __init__.py file
  - Keep the original q.py functioning as is
  - Run tests to verify the original tool still works

### Phase 2: Refactor Utility Functions

- [ ] **Task 2.1**: Move basic utility functions to utilities.py
  - Move simple functions like hash functions, file utilities, etc.
  - Update q.py to import these functions from the package
  - Remove the original function definitions from q.py
  - Run tests to verify

- [ ] **Task 2.2**: Move error/exception classes to exceptions.py
  - Extract all exception classes
  - Update q.py to import these classes from the package
  - Remove the original exception class definitions from q.py
  - Test

### Phase 3: Refactor Core Components

- [ ] **Task 3.1**: Move SQL-related functionality to sql.py
  - Extract Sql class and related methods
  - Update q.py to import from the package
  - Remove the original SQL-related code from q.py
  - Test

- [ ] **Task 3.2**: Move database handling code to db.py
  - Extract Sqlite3DB and related classes
  - Update q.py to import from the package
  - Remove the original database handling code from q.py
  - Test

- [ ] **Task 3.3**: Move table parsing/creation to parsers.py
  - Extract TableColumnInferer, TableCreator, etc.
  - Update q.py to import from the package
  - Remove the original parsing/creation code from q.py
  - Test

- [ ] **Task 3.4**: Move data stream handling to streams.py
  - Extract DataStream classes and related functionality
  - Update q.py to import from the package
  - Remove the original stream handling code from q.py
  - Test

- [ ] **Task 3.5**: Move materialized state handling to state.py
  - Extract MaterializedState classes
  - Update q.py to import from the package
  - Remove the original materialized state code from q.py
  - Test

### Phase 4: Refactor Main Processing Logic

- [ ] **Task 4.1**: Move QTextAsData to core.py
  - Extract the QTextAsData class
  - Update q.py to import from the package
  - Remove the original QTextAsData class from q.py
  - Test

- [ ] **Task 4.2**: Move QOutput and related classes to output.py
  - Extract output-related classes
  - Update q.py to import from the package
  - Remove the original output-related code from q.py
  - Test

### Phase 5: CLI and Packaging (Final Phase)

- [ ] **Task 5.1**: Create full CLI interface in qtextasdata/cli.py
  - Move run_standalone() and related functions to cli.py
  - Create proper package entry point
  - Remove CLI-related code from q.py
  - Test

- [ ] **Task 5.2**: Update/create setup.py for proper pip installation
  - Finalize package metadata
  - Point entry point to qtextasdata.cli
  - Test pip installation and CLI invocation

- [ ] **Task 5.3**: Create a new lightweight q.py CLI wrapper
  - Create a simple wrapper that directly imports and uses the package
  - Ensure backward compatibility
  - Test that both the wrapper and package entry point work

- [ ] **Task 5.4**: Finalize and clean up
  - Remove any redundant imports or code
  - Ensure all tests pass with the new package structure
  - Verify both direct CLI and package usage work properly

## Testing Notes

After each task:
1. Run `./run-tests.sh` to ensure all tests pass
2. Verify that command-line functionality works as expected through the original q.py
3. If tests fail, revert changes and troubleshoot before proceeding

## Package Structure Target

Final structure should look like:

```
qtextasdata/
├── __init__.py          # Package exports
├── cli.py               # CLI functionality
├── core.py              # QTextAsData main class
├── db.py                # Database interaction
├── exceptions.py        # Custom exceptions
├── output.py            # Output handling
├── parsers.py           # Data parsing
├── sql.py               # SQL related functionality
├── state.py             # Materialized state
├── streams.py           # Data stream handling
└── utilities.py         # Utility functions

# Minimal script that imports from package
bin/q.py                 # Simplified wrapper using qtextasdata package
``` 