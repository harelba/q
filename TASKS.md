# Refactoring Tasks for `qtextasdata` Python Package

This document outlines the iterative process to refactor the existing q.py codebase into a proper Python package named `qtextasdata`.

## Project Goals

- **Deliverable**: A Python package named `qtextasdata` installable via `pip`.
- **Core Logic**: Encapsulate all existing logic within the `qtextasdata` module with 100% functional equivalence.
  - **CRITICAL**: No new logic, no missing logic - absolute code fidelity must be maintained
  - **CRITICAL**: All original comments must be preserved exactly as written
  - **CRITICAL**: No new comments or docstrings may be added
  - **CRITICAL**: Code must be preserved exactly as it appears in the original file, only moved
  - **CRITICAL**: All whitespace, indentation, and formatting must match the original code
- **Command-Line Interface**: Preserve the `q` command as an entry point wrapper around the module.
- **Testing**: Maintain end-to-end tests, invoking the CLI tool after package installation.

## Code Fidelity Requirements

The refactoring process MUST adhere to these strict requirements:

1. **Absolute Code Preservation**
   - Code must be moved verbatim without ANY modifications to logic, style, or formatting
   - All comments must be preserved with their exact wording and placement
   - No new comments, docstrings, or explanatory text may be added
   - Variable names, function signatures, and class structures must remain identical

2. **Complete Component Migration**
   - When moving a component, ALL related functions/methods/classes must be migrated together
   - No "holes" or partial implementations may be left behind
   - Completeness must be verified through thorough testing after each migration
   - Each module must contain 100% of the functionality it's responsible for

3. **Zero Behavioral Changes**
   - The refactored code must behave identically to the original in all circumstances
   - No optimizations, bug fixes, or "improvements" are to be made during refactoring
   - The ONLY change permitted is the relocation of code into the appropriate module

## Workflow Guidelines

Each refactoring task should:
1. Move one aspect/component from q.py to a module file, ensuring COMPLETE migration of all related code
2. Import the component back into q.py from the package, with IDENTICAL functionality
3. Remove the original code from q.py, making q.py gradually simpler, without altering ANY behavior
4. Run tests to ensure functionality is preserved EXACTLY as in the original
5. Proceed to the next component only after tests pass and completeness is verified

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
  - Move ALL utility functions like hash functions, file utilities, etc. COMPLETELY and EXACTLY as they appear
  - Update q.py to import these functions from the package
  - Remove the original function definitions from q.py
  - Run tests to verify EXACT behavioral equivalence

- [ ] **Task 2.2**: Move error/exception classes to exceptions.py
  - Extract ALL exception classes COMPLETELY and EXACTLY as they appear
  - Update q.py to import these classes from the package
  - Remove the original exception class definitions from q.py
  - Test for EXACT behavioral equivalence

### Phase 3: Refactor Core Components

- [ ] **Task 3.1**: Move SQL-related functionality to sql.py
  - Extract Sql class and ALL related methods COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original SQL-related code from q.py
  - Test for EXACT behavioral equivalence

- [ ] **Task 3.2**: Move database handling code to db.py
  - Extract Sqlite3DB and ALL related classes COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original database handling code from q.py
  - Test for EXACT behavioral equivalence

- [ ] **Task 3.3**: Move table parsing/creation to parsers.py
  - Extract TableColumnInferer, TableCreator, etc. COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original parsing/creation code from q.py
  - Test for EXACT behavioral equivalence

- [ ] **Task 3.4**: Move data stream handling to streams.py
  - Extract ALL DataStream classes and related functionality COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original stream handling code from q.py
  - Test for EXACT behavioral equivalence

- [ ] **Task 3.5**: Move materialized state handling to state.py
  - Extract ALL MaterializedState classes COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original materialized state code from q.py
  - Test for EXACT behavioral equivalence

### Phase 4: Refactor Main Processing Logic

- [ ] **Task 4.1**: Move QTextAsData to core.py
  - Extract the QTextAsData class and ALL related methods COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original QTextAsData class from q.py
  - Test for EXACT behavioral equivalence

- [ ] **Task 4.2**: Move QOutput and related classes to output.py
  - Extract ALL output-related classes COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original output-related code from q.py
  - Test for EXACT behavioral equivalence

### Phase 5: CLI and Packaging (Final Phase)

- [ ] **Task 5.1**: Create full CLI interface in qtextasdata/cli.py
  - Move run_standalone() and ALL related functions COMPLETELY and EXACTLY as they appear
  - Create proper package entry point
  - Remove CLI-related code from q.py
  - Test for EXACT behavioral equivalence

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
  - Verify both direct CLI and package usage work properly with IDENTICAL behavior to the original

## Testing Notes

After each task:
1. Run `./run-tests.sh` to ensure all tests pass
2. Verify that command-line functionality works EXACTLY as expected through the original q.py
3. If tests fail, revert changes and troubleshoot before proceeding
4. VERIFY that ALL functionality related to the component has been migrated COMPLETELY

**CRITICAL: Test Failure Handling**
- If tests fail for any reason, the underlying assumption MUST ALWAYS be that the issue is in the refactoring process
- NEVER assume the issue is in the test itself or the Python environment
- ALWAYS assume that the Python environment is fully correct
- If you're convinced there must be some issue in the environment, DO NOT try to fix/change it
- Instead, stop immediately and provide detailed information about the issue so it can be properly investigated

## Implementation Strategy: Granular Commit Approach

To ensure a safe, traceable refactoring process, we'll follow this granular commit strategy:

### Core Principles

1. **Atomic Commits**
   - Each commit should represent ONE logical change
   - **CRITICAL**: Each commit should happen ONLY after all tests pass
   - Each commit should leave the codebase in a working state with passing tests
   - Each commit MUST maintain EXACT code fidelity

2. **Descriptive Commit Messages**
   - Format: `[Task X.Y] Component: Specific change description`
   - Example: `[Task 2.1] Utilities: Move hash functions to utilities.py`

3. **Sequential Implementation**
   - For each component to move, follow this exact sequence:
     1. Create the new file
     2. Move the code EXACTLY as it appears in the original
     3. Add imports to q.py
     4. Remove original code from q.py
     5. Test THOROUGHLY before each commit, verifying COMPLETE migration
     6. Commit ONLY after all tests pass successfully

4. **Test-First Methodology**
   - **NEVER** commit code that has not passed the full test suite
   - Run tests before each commit to ensure functionality is preserved
   - If tests fail, fix the issues before committing
   - No exceptions to this rule are permitted

### Component Completeness Verification

Before finalizing each module:
1. Review the original q.py to ensure ALL related functions have been moved
2. Verify that the entire functionality of the component is now in the new module
3. Check for any hidden dependencies or edge cases
4. Run the entire test suite to confirm behavior is IDENTICAL

### Practical Workflow

For each task in this document:

```bash
# 1. Create the necessary file(s)
mkdir -p qtextasdata  # If not already created
touch qtextasdata/utilities.py  # Example for Task 2.1

# Run tests to ensure the empty file doesn't break anything
./run-tests.sh

# Only commit after tests pass
git add qtextasdata/utilities.py
git commit -m "[Task 2.1] Utilities: Create utilities.py file"

# 2. Move code to new file
# Copy function(s) to utilities.py EXACTLY as they appear in original

# Run tests to verify functionality is preserved
./run-tests.sh

# Only commit after tests pass
git add qtextasdata/utilities.py
git commit -m "[Task 2.1] Utilities: Add hash functions to utilities.py"

# 3. Update imports in q.py
# Edit q.py to import from package

# Run tests to verify functionality is preserved
./run-tests.sh

# Only commit after tests pass
git add bin/q.py
git commit -m "[Task 2.1] q.py: Import hash functions from qtextasdata.utilities"

# 4. Remove original code from q.py
# Remove the original functions

# Run tests to verify functionality is preserved
./run-tests.sh

# Only commit after tests pass
git add bin/q.py
git commit -m "[Task 2.1] q.py: Remove original hash function implementations"
```

### Error Recovery

If tests fail after making changes:

```bash
# Fix the issues before committing
# DO NOT commit code that fails tests

# If changes are too complex to fix immediately:
git stash  # Stash your changes
# OR
git reset --hard  # Discard your changes completely

# Try again with a smaller change
# For example, move one function instead of several
```

### Task Tracking

Mark tasks as complete in this file:

```bash
# Update TASKS.md to mark task as complete
# Only after all tests for the task have passed
git add TASKS.md
git commit -m "[Task 2.1] Update TASKS.md: Mark utilities task as complete"
```

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