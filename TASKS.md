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

- [x] **Task 1.1**: Create the basic `qtextasdata` package directory structure
  - Create qtextasdata/ directory
  - Create empty __init__.py file
  - Keep the original q.py functioning as is
  - Run tests to verify the original tool still works

### Phase 2: Refactor Utility Functions

- [x] **Task 2.1**: Move basic utility functions to utilities.py
  - Move ALL utility functions like hash functions, file utilities, etc. COMPLETELY and EXACTLY as they appear
  - Update q.py to import these functions from the package
  - Remove the original function definitions from q.py
  - Run tests to verify EXACT behavioral equivalence

- [x] **Task 2.2**: Move error/exception classes to exceptions.py
  - Extract ALL exception classes COMPLETELY and EXACTLY as they appear
  - Update q.py to import these classes from the package
  - Remove the original exception class definitions from q.py
  - Test for EXACT behavioral equivalence

### Phase 3: Refactor Core Components

- [x] **Task 3.1**: Move SQL-related functionality to sql.py
  - Extract Sql class and ALL related methods COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original SQL-related code from q.py
  - Test for EXACT behavioral equivalence

- [x] **Task 3.2**: Move database handling code to db.py
  - Extract Sqlite3DB and ALL related classes COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original database handling code from q.py
  - Test for EXACT behavioral equivalence

- [x] **Task 3.3**: Move table parsing/creation to parsers.py
  - Extract TableColumnInferer, TableCreator, etc. COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original parsing/creation code from q.py
  - Test for EXACT behavioral equivalence

- [x] **Task 3.4**: Move data stream handling to streams.py
  - Extract ALL DataStream classes and related functionality COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original stream handling code from q.py
  - Test for EXACT behavioral equivalence

- [x] **Task 3.5**: Move materialized state handling to state.py
  - Extract ALL MaterializedState classes COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original materialized state code from q.py
  - Test for EXACT behavioral equivalence

### Phase 4: Refactor Main Processing Logic

- [x] **Task 4.1**: Move QTextAsData to core.py
  - Extract the QTextAsData class and ALL related methods COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original QTextAsData class from q.py
  - Test for EXACT behavioral equivalence

- [x] **Task 4.2**: Move QOutput and related classes to output.py
  - Extract ALL output-related classes COMPLETELY and EXACTLY as they appear
  - Update q.py to import from the package
  - Remove the original output-related code from q.py
  - Test for EXACT behavioral equivalence

### Phase 5: CLI and Packaging (Final Phase)

- [x] **Task 5.1**: Create full CLI interface in qtextasdata/cli.py
  - Move run_standalone() and ALL related functions COMPLETELY and EXACTLY as they appear
  - Create proper package entry point
  - Remove CLI-related code from q.py
  - Test for EXACT behavioral equivalence

- [x] **Task 5.2**: Update/create setup.py for proper pip installation
  - Finalize package metadata
  - Point entry point to qtextasdata.cli
  - Test pip installation and CLI invocation
