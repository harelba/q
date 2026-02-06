[![Build and Package](https://github.com/harelba/q/workflows/BuildAndPackage/badge.svg?branch=master)](https://github.com/harelba/q/actions?query=branch%3Amaster)

# q - Text as Data
q's purpose is to bring SQL expressive power to the Linux command line and to provide easy access to text as actual data.

q allows the following:

* Performing SQL-like statements directly on tabular text data, auto-caching the data in order to accelerate additional querying on the same file. 
* Performing SQL statements directly on multi-file sqlite3 databases, without having to merge them or load them into memory

The following table shows the impact of using caching:

|    Rows   | Columns | File Size | Query time without caching | Query time with caching | Speed Improvement |
|:---------:|:-------:|:---------:|:--------------------------:|:-----------------------:|:-----------------:|
| 5,000,000 |   100   |   4.8GB   |    4 minutes, 47 seconds   |       1.92 seconds      |        x149       |
| 1,000,000 |   100   |   983MB   |        50.9 seconds        |      0.461 seconds      |        x110       |
| 1,000,000 |    50   |   477MB   |        27.1 seconds        |      0.272 seconds      |        x99        |
|  100,000  |   100   |    99MB   |         5.2 seconds        |      0.141 seconds      |        x36        |
|  100,000  |    50   |    48MB   |         2.7 seconds        |      0.105 seconds      |        x25        |

Notice that for the current version, caching is **not enabled** by default, since the caches take disk space. Use `-C readwrite` or `-C read` to enable it for a query, or add `caching_mode` to `.qrc` to set a new default.
 
q's web site is [https://harelba.github.io/q/](https://harelba.github.io/q/) or [https://q.textasdata.wiki](https://q.textasdata.wiki) It contains everything you need to download and use q immediately.


## Usage Examples
q treats ordinary files as database tables, and supports all SQL constructs, such as `WHERE`, `GROUP BY`, `JOIN`s, etc. It supports automatic column name and type detection, and provides full support for multiple character encodings.

Here are some example commands to get the idea:

```bash
$ q "SELECT COUNT(*) FROM ./clicks_file.csv WHERE c3 > 32.3"

$ ps -ef | q -H "SELECT UID, COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"

$ q "select count(*) from some_db.sqlite3:::albums a left join another_db.sqlite3:::tracks t on (a.album_id = t.album_id)"
```

Detailed examples are in [here](https://harelba.github.io/q/#examples)

## Installation

### As a Python package (recommended)
```bash
pip install qtextasdata
```

This installs both the `q` command-line tool and the `qtextasdata` Python module.

### Previous versions
The previous version `2.0.19` can still be downloaded from [here](https://github.com/harelba/q/releases/tag/2.0.19).

Instructions for all OSs are [here](https://harelba.github.io/q/#installation).

## Python Module Usage

Starting from version 4.0.0, q can be used as a Python module, allowing you to run SQL queries on text data directly from your Python code.

### Basic Usage
```python
from qtextasdata import QTextAsData, QInputParams

# Create an engine instance with default input parameters
q = QTextAsData(QInputParams(skip_header=True, delimiter=','))

# Execute a query on a CSV file
result = q.execute('SELECT name, age FROM data.csv WHERE age > 25')

# Check the result
if result.status == 'ok':
    print("Columns:", result.metadata.output_column_name_list)
    for row in result.data:
        print(row)
else:
    print("Error:", result.error.msg)

# Clean up resources when done
q.done()
```

### Data Reuse Across Queries
Once a file is loaded, subsequent queries on the same file reuse the already-loaded data:
```python
from qtextasdata import QTextAsData, QInputParams

q = QTextAsData(QInputParams(skip_header=True, delimiter=','))

# First query loads the data
r1 = q.execute('SELECT COUNT(*) FROM large_file.csv')

# Second query reuses the already-loaded data (no reload)
r2 = q.execute('SELECT name FROM large_file.csv WHERE age > 30')

q.done()
```

### In-Memory Data via Data Streams
You can query in-memory data by injecting data streams:
```python
from io import StringIO
from qtextasdata import QTextAsData, QInputParams, DataStream

csv_data = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n"

data_streams_dict = {
    'my_data': DataStream('my_data', 'my_data', StringIO(csv_data))
}

q = QTextAsData(
    default_input_params=QInputParams(skip_header=True, delimiter=','),
    data_streams_dict=data_streams_dict
)

result = q.execute('SELECT name, city FROM my_data WHERE age > 28')
print(result.data)  # [('Alice', 'NYC'), ('Charlie', 'Chicago')]

q.done()
```

### Query Analysis
Analyze a query without executing it to get metadata about the tables involved:
```python
from qtextasdata import QTextAsData, QInputParams

q = QTextAsData(QInputParams(skip_header=True, delimiter=','))

analysis = q.analyze('SELECT * FROM data.csv')
if analysis.status == 'ok':
    for table_name, structure in analysis.metadata.table_structures.items():
        print(f"Table: {table_name}")
        print(f"  Columns: {structure.column_names}")
        print(f"  Types: {structure.sqlite_column_types}")

q.done()
```

### Per-File Input Parameters
Different files can have different input parameters:
```python
from qtextasdata import QTextAsData, QInputParams

q = QTextAsData(QInputParams(skip_header=True, delimiter=','))

# Load a tab-delimited file with specific parameters
q.load_data('tsv_file.tsv', QInputParams(skip_header=True, delimiter='\t'))

# Query the pre-loaded data
result = q.execute('SELECT * FROM tsv_file.tsv')

q.done()
```

For a complete API reference, see [PYTHON-API.md](doc/PYTHON-API.md).

## Version Management

q uses semantic versioning (MAJOR.MINOR.PATCH):
- MAJOR: Incompatible API changes
- MINOR: New functionality (backwards compatible)
- PATCH: Bug fixes (backwards compatible)

For version management:
- Check the [CHANGELOG.md](CHANGELOG.md) for details about each release
- Use `./bump-version.py` to bump versions (run `./bump-version.py --help` for usage information)

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Linkedin: [Harel Ben Attia](https://www.linkedin.com/in/harelba/)

Twitter [@harelba](https://twitter.com/harelba)

Email [harelba@gmail.com](mailto:harelba@gmail.com)

q on twitter: [#qtextasdata](https://twitter.com/hashtag/qtextasdata?src=hashtag_click)

Patreon: [harelba](https://www.patreon.com/harelba) - All the money received is donated to the [Center for the Prevention and Treatment of Domestic Violence](https://www.gov.il/he/departments/bureaus/molsa-almab-ramla) in my hometown - Ramla, Israel.


