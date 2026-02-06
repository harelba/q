# Python API Reference

This document provides a comprehensive reference for using q (`qtextasdata`) as a Python module.

## Quick Start

```python
from qtextasdata import QTextAsData, QInputParams

q = QTextAsData(QInputParams(skip_header=True, delimiter=','))
result = q.execute('SELECT * FROM data.csv WHERE age > 25')

for row in result.data:
    print(row)

q.done()
```

## Classes

### QTextAsData

The main engine class for executing SQL queries on text data. Each instance maintains its own in-memory SQLite database and tracks loaded table data for reuse across queries.

#### Constructor

```python
QTextAsData(default_input_params=QInputParams(), data_streams_dict=None)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `default_input_params` | `QInputParams` | `QInputParams()` | Default parameters applied to all input files unless overridden per-query. |
| `data_streams_dict` | `dict` or `None` | `None` | A dictionary mapping table names (strings) to `DataStream` objects for in-memory data injection. |

#### Methods

##### `execute(query_str, input_params=None, save_db_to_disk_filename=None)`

Execute a SQL query and return results.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query_str` | `str` | (required) | SQL query string. Table names in the query refer to file paths, data stream names, or SQLite/QSQL database paths. |
| `input_params` | `QInputParams` or `None` | `None` | Per-query input parameters. Merged with `default_input_params` (per-query values override defaults). |
| `save_db_to_disk_filename` | `str` or `None` | `None` | If provided, materializes the query data to an SQLite file at this path instead of returning results. |

**Returns:** `QOutput`

**Example:**
```python
q = QTextAsData(QInputParams(skip_header=True, delimiter=','))
result = q.execute('SELECT name, age FROM users.csv WHERE age > 30')
if result.status == 'ok':
    for row in result.data:
        print(row)
q.done()
```

##### `analyze(query_str, input_params=None)`

Analyze a query without executing it. Loads and inspects the referenced tables, returning metadata (column names, types, table structures) but no result data.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query_str` | `str` | (required) | SQL query string to analyze. |
| `input_params` | `QInputParams` or `None` | `None` | Per-query input parameters. |

**Returns:** `QOutput` (with `data` set to `None` and metadata populated)

**Example:**
```python
q = QTextAsData(QInputParams(skip_header=True, delimiter=','))
analysis = q.analyze('SELECT * FROM data.csv')
if analysis.status == 'ok':
    for table_name, structure in analysis.metadata.table_structures.items():
        print(f"Table: {table_name}")
        print(f"  Columns: {structure.column_names}")
        print(f"  Types: {structure.sqlite_column_types}")
q.done()
```

##### `load_data(filename, input_params=QInputParams(), stop_after_analysis=False)`

Pre-load data from a file or data stream into the engine. Subsequent queries referencing the same table name will reuse the already-loaded data.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `filename` | `str` | (required) | The file path or data stream name to load. |
| `input_params` | `QInputParams` | `QInputParams()` | Input parameters for this specific data source. |
| `stop_after_analysis` | `bool` | `False` | If `True`, only analyze the structure without fully loading data. |

**Returns:** Table structure object, or `None` if data was already loaded.

**Example:**
```python
q = QTextAsData()
q.load_data('data.tsv', QInputParams(skip_header=True, delimiter='\t'))
result = q.execute('SELECT * FROM data.tsv')
q.done()
```

##### `done()`

Clean up all resources held by the engine, including closing all SQLite database connections. Must be called when the engine is no longer needed.

**Example:**
```python
q = QTextAsData()
try:
    result = q.execute('SELECT * FROM data.csv')
    # ... process result ...
finally:
    q.done()
```

---

### QInputParams

Configuration for how input data is parsed.

#### Constructor

```python
QInputParams(
    skip_header=False,
    delimiter=' ',
    input_encoding='UTF-8',
    gzipped_input=False,
    with_universal_newlines=False,
    parsing_mode='relaxed',
    expected_column_count=None,
    keep_leading_whitespace_in_values=False,
    disable_double_double_quoting=False,
    disable_escaped_double_quoting=False,
    disable_column_type_detection=False,
    input_quoting_mode='minimal',
    max_column_length_limit=131072,
    read_caching=False,
    write_caching=False,
    max_attached_sqlite_databases=10
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `skip_header` | `bool` | `False` | If `True`, treat the first row as a header containing column names. |
| `delimiter` | `str` | `' '` (space) | Column delimiter character. |
| `input_encoding` | `str` | `'UTF-8'` | Character encoding of the input data. |
| `gzipped_input` | `bool` | `False` | If `True`, treat input as gzip-compressed. |
| `with_universal_newlines` | `bool` | `False` | If `True`, handle universal newlines (`\r\n`, `\r`, `\n`). |
| `parsing_mode` | `str` | `'relaxed'` | Parsing mode: `'relaxed'` (default), `'strict'`, or `'fluffy'`. |
| `expected_column_count` | `int` or `None` | `None` | If set, validates that rows have exactly this many columns. |
| `keep_leading_whitespace_in_values` | `bool` | `False` | If `True`, preserve leading whitespace in field values. |
| `disable_double_double_quoting` | `bool` | `False` | Disable double-double-quote escaping. |
| `disable_escaped_double_quoting` | `bool` | `False` | Disable backslash-escaped quote handling. |
| `disable_column_type_detection` | `bool` | `False` | If `True`, treat all columns as text. |
| `input_quoting_mode` | `str` | `'minimal'` | Input quoting mode: `'minimal'`, `'all'`, or `'none'`. |
| `max_column_length_limit` | `int` | `131072` | Maximum allowed length for a single column value. |
| `read_caching` | `bool` | `False` | Enable reading from `.qsql` cache files. |
| `write_caching` | `bool` | `False` | Enable writing `.qsql` cache files for faster subsequent access. |
| `max_attached_sqlite_databases` | `int` | `10` | Maximum number of SQLite databases to attach simultaneously. |

#### Methods

##### `merged_with(input_params)`

Create a new `QInputParams` by merging this instance with another. Values from `input_params` override values in `self`.

**Returns:** A new `QInputParams` instance.

---

### QOutput

Container for query results.

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `status` | `str` | `'ok'` if the query succeeded, `'error'` if it failed. |
| `data` | `list` of `tuple` or `None` | Query result rows. Each row is a tuple of typed values. `None` if the query failed or was an analysis-only query. |
| `metadata` | `QMetadata` or `None` | Metadata about the query results and table structures. |
| `warnings` | `list` of `QWarning` | Any warnings generated during execution. |
| `error` | `QError` or `None` | Error details if `status` is `'error'`. |

**Example:**
```python
result = q.execute('SELECT name, age FROM data.csv')

if result.status == 'ok':
    print(f"Row count: {len(result.data)}")
    print(f"Columns: {result.metadata.output_column_name_list}")
    for row in result.data:
        name, age = row
        print(f"  {name}: {age}")
elif result.status == 'error':
    print(f"Error (code {result.error.errorcode}): {result.error.msg}")
```

---

### QMetadata

Metadata about query results and table structures.

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `table_structures` | `dict` | Maps table names to their structure objects (all loaded tables). |
| `new_table_structures` | `dict` | Maps table names to structure objects for tables newly loaded by this query. |
| `output_column_name_list` | `list` of `str` or `None` | Column names in the query result. |

**Checking if data was reused:**
```python
result = q.execute('SELECT * FROM data.csv')
for table_name in result.metadata.table_structures:
    if table_name in result.metadata.new_table_structures:
        print(f"{table_name}: freshly loaded")
    else:
        print(f"{table_name}: reused from previous query")
```

---

### QError

Error information for failed queries.

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `exception` | `Exception` | The original exception that caused the error. |
| `msg` | `str` | Human-readable error message. |
| `errorcode` | `int` | Numeric error code. |
| `traceback` | `str` | Formatted traceback string. |

---

### QWarning

Warning information for queries that succeeded but with caveats.

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `exception` | `Exception` or `None` | The exception associated with the warning, if any. |
| `msg` | `str` | Human-readable warning message. |

---

### DataStream

Wraps a file-like object for use as an in-memory data source.

#### Constructor

```python
DataStream(stream_id, filename, stream)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `stream_id` | `str` | Unique identifier for this data stream. |
| `filename` | `str` | Logical filename used to reference this stream in queries. |
| `stream` | file-like object | A readable file-like object (e.g., `io.StringIO`, `codecs.open()`). |

**Example:**
```python
from io import StringIO
from qtextasdata import DataStream

csv_data = "col1,col2\nval1,val2\n"
stream = DataStream('my_stream', 'my_data', StringIO(csv_data))
```

---

### DataStreams

Collection of `DataStream` objects. Used internally by `QTextAsData`. Users typically pass a plain `dict` to `QTextAsData(data_streams_dict=...)` instead of constructing `DataStreams` directly.

#### Constructor

```python
DataStreams(data_streams_dict)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `data_streams_dict` | `dict` | Maps string names to `DataStream` instances. |

---

### QOutputPrinter

Formats and prints query results to output streams. Primarily used by the CLI, but available for programmatic use.

#### Constructor

```python
QOutputPrinter(output_params, show_tracebacks=False)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `output_params` | `QOutputParams` | Output formatting configuration. |
| `show_tracebacks` | `bool` | If `True`, print full tracebacks for errors. |

#### Methods

- `print_output(f_out, f_err, results)` -- Print formatted query results.
- `print_analysis(f_out, f_err, results)` -- Print table analysis results.
- `print_errors_and_warnings(f, results)` -- Print errors and warnings.

---

## Common Patterns

### Error Handling

```python
from qtextasdata import QTextAsData, QInputParams

q = QTextAsData(QInputParams(skip_header=True, delimiter=','))

result = q.execute('SELECT * FROM nonexistent.csv')

if result.status == 'error':
    print(f"Error code: {result.error.errorcode}")
    print(f"Message: {result.error.msg}")
    # Common error codes:
    #   30 - File not found
    #    1 - SQL query error
    #    2 - Column count mismatch
    #    3 - Encoding error
    #   91 - Query encoding error

q.done()
```

### Engine Isolation

Multiple `QTextAsData` instances are fully isolated from each other:

```python
q1 = QTextAsData(QInputParams(skip_header=True, delimiter=','))
q2 = QTextAsData(QInputParams(skip_header=True, delimiter='\t'))

r1 = q1.execute('SELECT * FROM comma_file.csv')
r2 = q2.execute('SELECT * FROM tab_file.tsv')

q1.done()
q2.done()
```

### Stdin Injection

Inject data as if it came from standard input:

```python
import codecs
from qtextasdata import QTextAsData, QInputParams, DataStream

data_streams_dict = {
    '-': DataStream('stdin', '-', codecs.open('input.csv', 'rb', encoding='utf-8'))
}

q = QTextAsData(
    QInputParams(skip_header=True, delimiter=','),
    data_streams_dict=data_streams_dict
)
result = q.execute('SELECT * FROM -')

q.done()
```

### Querying SQLite Databases

```python
from qtextasdata import QTextAsData, QInputParams

q = QTextAsData()

# Single-table SQLite database (auto-detects table name)
result = q.execute('SELECT * FROM my_database.sqlite')

# Multi-table SQLite database (specify table with :::)
result = q.execute('SELECT * FROM my_database.sqlite:::my_table')

q.done()
```

## Migration from CLI to Module

| CLI Flag | `QInputParams` Parameter | Example |
|----------|--------------------------|---------|
| `-H` | `skip_header=True` | `QInputParams(skip_header=True)` |
| `-d ,` | `delimiter=','` | `QInputParams(delimiter=',')` |
| `-e utf-8` | `input_encoding='utf-8'` | `QInputParams(input_encoding='utf-8')` |
| `-z` | `gzipped_input=True` | `QInputParams(gzipped_input=True)` |
| `-p strict` | `parsing_mode='strict'` | `QInputParams(parsing_mode='strict')` |
| `-C read` | `read_caching=True` | `QInputParams(read_caching=True)` |
| `-C readwrite` | `read_caching=True, write_caching=True` | `QInputParams(read_caching=True, write_caching=True)` |
| `-w all` | `input_quoting_mode='all'` | `QInputParams(input_quoting_mode='all')` |
