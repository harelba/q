# q - Run SQL directly on CSV or TSV files

[![GitHub Stars](https://img.shields.io/github/stars/harelba/q.svg?style=social&label=GitHub Stars&maxAge=600)](https://GitHub.com/harelba/q/stargazers/)
[![GitHub forks](https://img.shields.io/github/forks/harelba/q.svg?style=social&label=GitHub Forks&maxAge=600)](https://GitHub.com/harelba/q/network/)

## Overview
q's purpose is to bring SQL expressive power to the Linux command line by providing easy access to text as actual data, and allowing direct access to multi-file sqlite3 databases.

```bash
	q <flags> <sql-query>
```

q allows the following:

* Performing SQL-like statements directly on tabular text data, auto-caching the data in order to accelerate additional querying on the same file

```bash
    # Simple query from a file, columns are named c1...cN
    q "select c1,c5 from myfile.csv"

    # -d '|' sets the input delimiter, -H says there's a header
    q -d '|' -H "select my_field from myfile.delimited-file-with-pipes"

    # -C readwrite writes a cache for the csv file
    q -d , -H "select my_field from myfile.csv" -C readwrite

    # -C read tells q to use the cache
    q -d , -H "select my_field from myfile.csv" -C read

    # Setting the default caching mode (`-C`) can be done by writing a `~/.qrc` file
```

* Performing SQL statements directly on multi-file sqlite3 databases, without having to merge them or load them into memory

```bash
    q "select * from mydatabase.sqlite:::my_table_name"

        or

    q "select * from mydatabase.sqlite"

        if the database file contains only one table

    # sqlite files are autodetected, no need for any special filename extension
```

The following table shows the impact of using caching:

|    Rows   | Columns | File Size | Query time without caching | Query time with caching | Speed Improvement |
|:---------:|:-------:|:---------:|:--------------------------:|:-----------------------:|:-----------------:|
| 5,000,000 |   100   |   4.8GB   |    4 minutes, 47 seconds   |       1.92 seconds      |        x149       |
| 1,000,000 |   100   |   983MB   |        50.9 seconds        |      0.461 seconds      |        x110       |
| 1,000,000 |    50   |   477MB   |        27.1 seconds        |      0.272 seconds      |        x99        |
|  100,000  |   100   |    99MB   |         5.2 seconds        |      0.141 seconds      |        x36        |
|  100,000  |    50   |    48MB   |         2.7 seconds        |      0.105 seconds      |        x25        |

Notice that for the current version, caching is **not enabled** by default, since the caches take disk space. Use `-C readwrite` or `-C read` to enable it for a query, or add `caching_mode` to `.qrc` to set a new default.
 
q treats ordinary files as database tables, and supports all SQL constructs, such as `WHERE`, `GROUP BY`, `JOIN`s, etc. It supports automatic column name and type detection, and provides full support for multiple character encodings.

The new features - autocaching, direct querying of sqlite database and the use of `~/.qrc` file are described in detail in [here](https://github.com/harelba/q/blob/master/QSQL-NOTES.md).

Download the tool using the links in the [installation](#installation) below and play with it.

### Encodings
|                                        |                                                 |
|:--------------------------------------:|:-----------------------------------------------:|
| 完全支持所有的字符编码                 | すべての文字エンコーディングを完全にサポート    |
| 모든 문자 인코딩이 완벽하게 지원됩니다 | все кодировки символов полностью поддерживаются |

**Non-english users:** q fully supports all types of encoding. Use `-e data-encoding` to set the input data encoding, `-Q query-encoding` to set the query encoding, and use `-E output-encoding` to set the output encoding. Sensible defaults are in place for all three parameters. Please contact me if you encounter any issues and I'd be glad to help.

**Files with BOM:** Files which contain a BOM ([Byte Order Mark](https://en.wikipedia.org/wiki/Byte_order_mark)) are not properly supported inside python's csv module. q contains a workaround that allows reading UTF8 files which contain a BOM - Use `-e utf-8-sig` for this. I plan to separate the BOM handling from the encoding itself, which would allow to support BOMs for all encodings.

## Installation

| Format | Instructions | Comments |
:---|:---|:---|
|[OSX](https://github.com/harelba/q/releases/download/v3.1.6/macos-q)|Run `brew install harelba/q/q` in order to install q (moved it to its own tap), or download the standalone executable directly from the link on the left|A man page is available, just run `man q`||
|[RPM Package](https://github.com/harelba/q/releases/download/v3.1.6/q-text-as-data-3.1.6.x86_64.rpm)| run `rpm -ivh <package-filename>` or `rpm -U <package-filename>` if you already have an older version of q.| A man page is available for this release. Just enter `man q`.|
|[DEB Package](https://github.com/harelba/q/releases/download/v3.1.6/q-text-as-data-3.1.6-1.x86_64.deb)| Run `sudo dpkg -i <package-filename>`|A man page is available for this release. Just enter `man q`. Some installations don't install the man page properly for some reason. I'll fix this soon|
|[Windows Installer](https://github.com/harelba/q/releases/download/v3.1.6/q-text-as-data-3.1.6.msi)|Run the installer executable and hit next next next... q.exe will be added to the PATH so you can access it everywhere.|Windows doesn't update the PATH retroactively for open windows, so you'll need to open a new `cmd`/`bash` window after the installation is done.|
|[Source tar.gz](https://github.com/harelba/q/archive/refs/tags/v3.1.6.tar.gz)|Full source file tree for latest stable version. Note that q.py cannot be used directly anymore, as it requires python dependencies||
|[Source zip](https://github.com/harelba/q/archive/refs/tags/v3.1.6.zip)|Full source file tree for the latest stable version. Note that q.py cannot be used directly anymore, as it requires python dependencies||

I will add packages for additional Linux Distributions if there's demand for it. If you're interested in another Linux distribution, please ping me. It's relatively easy to add new ones with the new packaging flow.

The previous version `2.0.19` can be downloaded directly from [here](https://github.com/harelba/q/releases/tag/2.0.19). Please let me know if for some reason the new version is not suitable for your needs, and you're planning on using the previous one.

## Requirements
q is packaged as a compiled standalone-executable that has no dependencies, not even python itself. This was done by using the awesome [pyoxidizer](https://github.com/indygreg/PyOxidizer) project.


## Examples

This section shows example flows that highlight the main features. For more basic examples, see [here](#getting-started-examples).

### Basic Examples:

```bash
# Prepare some data
$ seq 1 1000000 > myfile.csv

# Query it
$ q "select sum(c1),count(*) from myfile.csv where c1 % 3 = 0"
166666833333 333333

# Use q to query from stdin
$ ps -ef | q -b -H "SELECT UID, COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"
501 288
0   115
270 17
```

### Auto-caching Examples

```bash
# (time command output has been shortened for berevity)

# Prepare some data
$ seq 1 1000000 > myfile.csv

# Read from the resulting file 
$ time q "select sum(c1),count(*) from myfile.csv"
500000500000 1000000
total_time=4.108 seconds

# Running with `-C readwrite` auto-creates a cache file if there is none. The cache filename would be myfile.csv.qsql. The query runs as usual
$ time q "select sum(c1),count(*) from myfile.csv" -C readwrite
500000500000 1000000
total_time=4.057 seconds

# Now run with `-C read`. The query will run from the cache file and not the original. As the file gets bigger, the difference will be much more noticeable
$ time q "select sum(c1),count(*) from myfile.csv" -C read
500000500000 1000000
total_time=0.229 seconds

# Now let's try another query on that file. Notice the short query duration. The cache is being used for any query that uses this file, and queries on multiple files that contain caches will reuse the cache as well.
$ time q "select avg(c1) from myfile.csv" -C read
500000.5
total_time=0.217 seconds

# You can also query the qsql file directly, as it's just a standard sqlite3 DB file (see next section for q's support of reading directly from sqlite DBs)
$ time q "select sum(c1),count(*) from myfile.csv.qsql"
500000500000 1000000
total_time=0.226 seconds

# Now let's delete the original csv file (be careful when deleting original data)
$ rm -vf myfile.csv

# Running another query directly on the qsql file just works
$ time q "select sum(c1),count(*) from myfile.csv.qsql"
500000500000 1000000
total_time=0.226 seconds

# See the `.qrc` section below if you want to set the default `-C` (`--caching-mode`) to something other than `none` (the default)
```

### Direct sqlite Querying Examples

```bash
# Download example sqlite3 database from https://www.sqlitetutorial.net/sqlite-sample-database/ and unzip it. The resulting file will be chinook.db
$ curl -L https://www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip | tar -xvf -

# Now we can query the database directly, specifying the name of the table in the query (<db_name>:::<table_name>)
$ q "select count(*) from chinook.db:::albums"
347

# Let's take the top 5 longest tracks of album id 34. The -b option just beautifies the output, and -O tells q to output the column names as headers
$ q "select * from chinook.db:::tracks where albumid = '34' order by milliseconds desc limit 5" -b -O
TrackId Name                       AlbumId MediaTypeId GenreId Composer Milliseconds Bytes    UnitPrice
407     "Só Tinha De Ser Com Você" 34      1           7       Vários   389642       13085596 0.99
398     "Only A Dream In Rio"      34      1           7       Vários   371356       12192989 0.99
393     "Tarde Em Itapoã"          34      1           7       Vários   313704       10344491 0.99
401     "Momentos Que Marcam"      34      1           7       Vários   280137       9313740  0.99
391     "Garota De Ipanema"        34      1           7       Vários   279536       9141343  0.99

# Let's now copy the chinook database to another file, as if it's just another different database
$ cp chinook.db another_db.db

# Now we can run a join query between the two databases. They could have been any two different databases, using the copy of chinook is just for simplicity
# Let's get the top-5 longest albums, using albums from the first database and tracks from the second database. The track times are converted to seconds, and rounded to two digits after the decimal point.
$ q -b -O "select a.title,round(sum(t.milliseconds)/1000.0/60,2) total_album_time_seconds from chinook.db:::albums a left join another_database.db:::tracks t on (a.albumid = t.albumid) group by a.albumid order by total_album_time_seconds desc limit 5"
Title                                      total_album_time_seconds
"Lost, Season 3"                           1177.76
"Battlestar Galactica (Classic), Season 1" 1170.23
"Lost, Season 1"                           1080.92
"Lost, Season 2"                           1054.83
"Heroes, Season 1"                         996.34
```

### Analysis Examples

```bash
# Let's create a simple CSV file without a header. Make sure to copy only the three lines, press enter, and
# then press Ctrl-D to exit so the file will be written.
$ cat > some-data-without-header.csv
harel,1,2
ben,3,4
attia,5,6
<Ctrl-D>

# Let's run q on it with -A, to see the detected structure of the file. `-d ,` sets the delimiter to a comma
$ q -d , "select * from some-data-without-header.csv" -A
Table: /Users/harelben-attia/dev/harelba/q/some-data-without-header.csv
  Sources:
    source_type: file source: /Users/harelben-attia/dev/harelba/q/some-data-without-header.csv
  Fields:
    `c1` - text
    `c2` - int
    `c3` - int

# Now let's create another simple CSV file, this time with a header (-H tells q to expect a header in the file)
$ cat > some-data.csv
planet_id,name,diameter_km,length_of_day_hours
1000,Earth,12756,24
2000,Mars,6792,24.7
3000,Jupiter,142984,9.9
<Ctrl-D>

# Let's run q with -A to see the analysis results.
$ q -b -O -H -d , "select * from some-data.csv" -A
Table: /Users/harelben-attia/dev/harelba/q/some-data.csv
  Sources:
    source_type: file source: /Users/harelben-attia/dev/harelba/q/some-data.csv
  Fields:
    `planet_id` - int
    `name` - text
    `diameter_km` - int
    `length_of_day_hours` - real

# Let's run it with `-C readwrite` so a cache will be created
$ q -b -O -H -d , "select * from some-data.csv" -C readwrite
planet_id,name   ,diameter_km,length_of_day_hours
1000     ,Earth  ,12756      ,24.0
2000     ,Mars   ,6792       ,24.7
3000     ,Jupiter,142984     ,9.9

# Running another query that uses some-data.csv with -A will now show that a qsql exists for that file. The source-type 
# will be "file-with-unused-qsql". The qsql cache is not being used, since by default, q does not activate caching
# so backward compatibility is maintained
$ q -b -O -H -d , "select * from some-data.csv" -A
Table: /Users/harelben-attia/dev/harelba/q/some-data.csv
  Sources:
    source_type: file-with-unused-qsql source: /Users/harelben-attia/dev/harelba/q/some-data.csv
  Fields:
    `planet_id` - int
    `name` - text
    `diameter_km` - int
    `length_of_day_hours` - real

# Now let's run another query, this time with `-C read`, telling q to use the qsql caches. This time source-type will 
# be "qsql-file-with-original", and the cache will be used when querying:
$ q -b -O -H -d , "select * from some-data.csv" -A -C read
Table: /Users/harelben-attia/dev/harelba/q/some-data.csv
  Sources:
    source_type: qsql-file-with-original source: /Users/harelben-attia/dev/harelba/q/some-data.csv.qsql
  Fields:
    `planet_id` - int
    `name` - text
    `diameter_km` - int
    `length_of_day_hours` - real

# Let's now read directly from the qsql file. Notice the change in the table name inside the query. `-C read` is not needed
# here. The source-type will be "qsql-file" 
$ q -b -O -H -d , "select * from some-data.csv.qsql" -A
Table: /Users/harelben-attia/dev/harelba/q/some-data.csv.qsql
  Sources:
    source_type: qsql-file source: /Users/harelben-attia/dev/harelba/q/some-data.csv.qsql
  Fields:
    `planet_id` - int
    `name` - text
    `diameter_km` - int
    `length_of_day_hours` - real
```

## Usage
Query should be an SQL-like query which contains filenames instead of table names (or - for stdin). The query itself should be provided as one parameter to the tool (i.e. enclosed in quotes).

All sqlite3 SQL constructs are supported, including joins across files (use an alias for each table). Take a look at the [limitations](#limitations) section below for some rarely-used use cases which are not fully supported.

q gets a full SQL query as a parameter. Remember to double-quote the query.

Historically, q supports multiple queries on the same command-line, loading each data file only once, even if it is used by multiple queries on the same q invocation. This is still supported. However, due to the new automatic-caching capabilities, this is not really required. Activate caching, and a cache file will be automatically created for each file. q Will use the cache behind the scenes in order to speed up queries. The speed up is extremely significant, so consider using caching for large files.

The following filename types are supported:

* **Delimited-file filenames** - including relative/absolute paths. E.g. `./my_folder/my_file.csv` or `/var/tmp/my_file.csv`
* **sqlite3 database filenames**
    * **With Multiple Tables** - Add an additional `:::<table_name>` for accessing a specific table. For example `mydatabase.sqlite3:::users_table`.
    * **With One Table Only** - Just specify the database filename, no need for a table name postfix. For example `my_single_table_database.sqlite`.
* **`.qsql` cache files** - q can auto-generate cache files for delimited files, and they can be queried directly as a table, since they contain only one table, as they are essentially standard sqlite databases

Use `-H` to signify that the input contains a header line. Column names will be detected automatically in that case, and can be used in the query. If this option is not provided, columns will be named cX, starting with 1 (e.g. `q "SELECT c3,c8 from ..."`).

Use `-d` to specify the input delimiter.

Column types are auto detected by the tool, no casting is needed. Note that there's a flag `--as-text` which forces all columns to be treated as text columns.

Please note that column names that include spaces need to be used in the query with back-ticks, as per the sqlite standard. Make sure to use single-quotes around the query, so bash/zsh won't interpret the backticks.

Query/Input/Output encodings are fully supported (and q tries to provide out-of-the-box usability in that area). Please use `-e`,`-E` and `-Q` to control encoding if needed.

JOINs are supported and Subqueries are supported in the WHERE clause, but unfortunately not in the FROM clause for now. Use table aliases when performing JOINs.

The SQL syntax itself is sqlite's syntax. For details look at https://www.sqlite.org/lang.html or search the net for examples.

NOTE: When using the `-O` output header option, use column name aliases if you want to control the output column names. For example, `q -O -H "select count(*) cnt,sum(*) as mysum from -"` would output `cnt` and `mysum` as the output header column names.

``` bash
Options:
  -h, --help            show this help message and exit
  -v, --version         Print version
  -V, --verbose         Print debug info in case of problems
  -S SAVE_DB_TO_DISK_FILENAME, --save-db-to-disk=SAVE_DB_TO_DISK_FILENAME
                        Save database to an sqlite database file
  -C CACHING_MODE, --caching-mode=CACHING_MODE
                        Choose the autocaching mode (none/read/readwrite).
                        Autocaches files to disk db so further queries will be
                        faster. Caching is done to a side-file with the same
                        name of the table, but with an added extension .qsql
  --dump-defaults       Dump all default values for parameters and exit. Can
                        be used in order to make sure .qrc file content is
                        being read properly.
  --max-attached-sqlite-databases=MAX_ATTACHED_SQLITE_DATABASES
                        Set the maximum number of concurrently-attached sqlite
                        dbs. This is a compile time definition of sqlite. q's
                        performance will slow down once this limit is reached
                        for a query, since it will perform table copies in
                        order to avoid that limit.
  --overwrite-qsql=OVERWRITE_QSQL
                        When used, qsql files (both caches and store-to-db)
                        will be overwritten if they already exist. Use with
                        care.

  Input Data Options:
    -H, --skip-header   Skip header row. This has been changed from earlier
                        version - Only one header row is supported, and the
                        header row is used for column naming
    -d DELIMITER, --delimiter=DELIMITER
                        Field delimiter. If none specified, then space is used
                        as the delimiter.
    -p, --pipe-delimited
                        Same as -d '|'. Added for convenience and readability
    -t, --tab-delimited
                        Same as -d <tab>. Just a shorthand for handling
                        standard tab delimited file You can use $'\t' if you
                        want (this is how Linux expects to provide tabs in the
                        command line
    -e ENCODING, --encoding=ENCODING
                        Input file encoding. Defaults to UTF-8. set to none
                        for not setting any encoding - faster, but at your own
                        risk...
    -z, --gzipped       Data is gzipped. Useful for reading from stdin. For
                        files, .gz means automatic gunzipping
    -A, --analyze-only  Analyze sample input and provide information about
                        data types
    -m MODE, --mode=MODE
                        Data parsing mode. fluffy, relaxed and strict. In
                        strict mode, the -c column-count parameter must be
                        supplied as well
    -c COLUMN_COUNT, --column-count=COLUMN_COUNT
                        Specific column count when using relaxed or strict
                        mode
    -k, --keep-leading-whitespace
                        Keep leading whitespace in values. Default behavior
                        strips leading whitespace off values, in order to
                        provide out-of-the-box usability for simple use cases.
                        If you need to preserve whitespace, use this flag.
    --disable-double-double-quoting
                        Disable support for double double-quoting for escaping
                        the double quote character. By default, you can use ""
                        inside double quoted fields to escape double quotes.
                        Mainly for backward compatibility.
    --disable-escaped-double-quoting
                        Disable support for escaped double-quoting for
                        escaping the double quote character. By default, you
                        can use \" inside double quoted fields to escape
                        double quotes. Mainly for backward compatibility.
    --as-text           Don't detect column types - All columns will be
                        treated as text columns
    -w INPUT_QUOTING_MODE, --input-quoting-mode=INPUT_QUOTING_MODE
                        Input quoting mode. Possible values are all, minimal
                        and none. Note the slightly misleading parameter name,
                        and see the matching -W parameter for output quoting.
    -M MAX_COLUMN_LENGTH_LIMIT, --max-column-length-limit=MAX_COLUMN_LENGTH_LIMIT
                        Sets the maximum column length.
    -U, --with-universal-newlines
                        Expect universal newlines in the data. Limitation: -U
                        works only with regular files for now, stdin or .gz
                        files are not supported yet.

  Output Options:
    -D OUTPUT_DELIMITER, --output-delimiter=OUTPUT_DELIMITER
                        Field delimiter for output. If none specified, then
                        the -d delimiter is used if present, or space if no
                        delimiter is specified
    -P, --pipe-delimited-output
                        Same as -D '|'. Added for convenience and readability.
    -T, --tab-delimited-output
                        Same as -D <tab>. Just a shorthand for outputting tab
                        delimited output. You can use -D $'\t' if you want.
    -O, --output-header
                        Output header line. Output column-names are determined
                        from the query itself. Use column aliases in order to
                        set your column names in the query. For example,
                        'select name FirstName,value1/value2 MyCalculation
                        from ...'. This can be used even if there was no
                        header in the input.
    -b, --beautify      Beautify output according to actual values. Might be
                        slow...
    -f FORMATTING, --formatting=FORMATTING
                        Output-level formatting, in the format X=fmt,Y=fmt
                        etc, where X,Y are output column numbers (e.g. 1 for
                        first SELECT column etc.
    -E OUTPUT_ENCODING, --output-encoding=OUTPUT_ENCODING
                        Output encoding. Defaults to 'none', leading to
                        selecting the system/terminal encoding
    -W OUTPUT_QUOTING_MODE, --output-quoting-mode=OUTPUT_QUOTING_MODE
                        Output quoting mode. Possible values are all, minimal,
                        nonnumeric and none. Note the slightly misleading
                        parameter name, and see the matching -w parameter for
                        input quoting.
    -L, --list-user-functions
                        List all user functions

  Query Related Options:
    -q QUERY_FILENAME, --query-filename=QUERY_FILENAME
                        Read query from the provided filename instead of the
                        command line, possibly using the provided query
                        encoding (using -Q).
    -Q QUERY_ENCODING, --query-encoding=QUERY_ENCODING
                        query text encoding. Experimental. Please send your
                        feedback on this
```

### Setting the default values for parameters
It's possible to set default values for parameters which are used often by configuring them in the file `~/.qrc`.

The file format is as follows:
```bash
[options]
<setting>=<default-value>
```

It's possible to generate a default `.qrc` file by running `q --dump-defaults` and write the output into the `.qrc` file.

One valuable use-case for this could be setting the caching-mode to `read`. This will make q automatically use generated `.qsql` cache files if they exist. Whenever you want a cache file to be generated, just use `-C readwrite` and a `.qsql` file will be generated if it doesn't exist.

Here's the content of the `~/.qrc` file for enabling cache reads by default:
```bash
[options]
caching_mode=read
```
  
## Getting Started Examples
This section shows some more basic examples of simple SQL constructs. 

For some more complex use-cases, see the [examples](#examples) at the beginning of the documentation.

NOTES:

* The `-H` flag in the examples below signifies that the file has a header row which is used for naming columns.
* The `-t` flag is just a shortcut for saying that the file is a tab-separated file (any delimiter is supported - Use the `-d` flag).
* Queries are given using upper case for clarity, but actual query keywords such as SELECT and WHERE are not really case sensitive.

Basic Example List:

* [Example 1 - COUNT DISTINCT values of specific field (uuid of clicks data)](#example-1)
* [Example 2 - Filter numeric data, controlling ORDERing and LIMITing output](#example-2)
* [Example 3 - Illustrate GROUP BY](#example-3)
* [Example 4 - More complex GROUP BY (group by time expression)](#example-4)
* [Example 5 - Read input from standard input](#example-5)
* [Example 6 - Use column names from header row](#example-6)
* [Example 7 - JOIN two files](#example-7)

### Example 1
Perform a COUNT DISTINCT values of specific field (uuid of clicks data).

``` bash
q -H -t "SELECT COUNT(DISTINCT(uuid)) FROM ./clicks.csv"
```
Output
``` bash
229
```
### Example 2
Filter numeric data, controlling ORDERing and LIMITing output

Note that q understands that the column is numeric and filters according to its numeric value (real numeric value comparison, not string comparison).

``` bash
q -H -t "SELECT request_id,score FROM ./clicks.csv WHERE score > 0.7 ORDER BY score DESC LIMIT 5"
```
Output:
``` bash
2cfab5ceca922a1a2179dc4687a3b26e    1.0
f6de737b5aa2c46a3db3208413a54d64    0.986665809568
766025d25479b95a224bd614141feee5    0.977105183282
2c09058a1b82c6dbcf9dc463e73eddd2    0.703255121794
```

### Example 3
Illustrate GROUP BY

``` bash
q -t -H "SELECT hashed_source_machine,count(*) FROM ./clicks.csv GROUP BY hashed_source_machine"
```
Output:
``` bash
47d9087db433b9ba.domain.com 400000
```

### Example 4
More complex GROUP BY (group by time expression)

``` bash
q -t -H "SELECT strftime('%H:%M',date_time) hour_and_minute,count(*) FROM ./clicks.csv GROUP BY hour_and_minute"
```
Output:
``` bash
07:00   138148
07:01   140026
07:02   121826
```

### Example 5
Read input from standard input

Calculates the total size per user/group in the /tmp subtree.

``` bash
sudo find /tmp -ls | q "SELECT c5,c6,sum(c7)/1024.0/1024 AS total FROM - GROUP BY c5,c6 ORDER BY total desc"
```
Output:
``` bash
mapred hadoop   304.00390625
root   root     8.0431451797485
smith  smith    4.34389972687
```

### Example 6
Use column names from header row

Calculate the top 3 user ids with the largest number of owned processes, sorted in descending order.

Note the usage of the autodetected column name UID in the query.

``` bash
ps -ef | q -H "SELECT UID,COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"
```
Output:
``` bash
root 152
harel 119
avahi 2
```

### Example 7
JOIN two files

The following command joins an ls output (exampledatafile) and a file containing rows of group-name,email (group-emails-example) and provides a row of filename,email for each of the emails of the group. For brevity of output, there is also a filter for a specific filename called ppp which is achieved using a WHERE clause.

``` bash
q "SELECT myfiles.c8,emails.c2 FROM exampledatafile myfiles JOIN group-emails-example emails ON (myfiles.c4 = emails.c1) WHERE myfiles.c8 = 'ppp'"
```
Output:
``` bash
ppp dip.1@otherdomain.com
ppp dip.2@otherdomain.com
```

You can see that the ppp filename appears twice, each time matched to one of the emails of the group dip to which it belongs. Take a look at the files `exampledatafile` and `group-emails-example` for the data.

Column name detection is supported for JOIN scenarios as well. Just specify `-H` in the command line and make sure that the source files contain the header rows.

## Implementation
Behind the scenes q creates a "virtual" sqlite3 database that does not contain data of its own, but attaches to multiple other databases as follows:

* When reading delimited files or data from `stdin`, it will analyze the data and construct an in-memory "adhoc database" that contains it. This adhoc database will be attached to the virtual database
* When a delimited file has a `.qsql` cache, it will attach to that file directly, without having to read it into memory
* When querying a standard sqlite3 file, it will be attached to the virtual database to it as well, without reading it into memory. sqlite3 files are auto-detected, no need for any special filename extension

The user query will be executed directly on the virtual database, using the attached databases.

sqlite3 itself has a limit on the number of attached databases (usually 10). If that limit is reached, q will automatically attach databases until that limit is reached, and will load additional tables into the adhoc database's in-memory database.

Please make sure to read the [limitations](#limitations) section as well.

## Development

### Tests
The code includes a test suite runnable through `run-tests.sh`. By default, it uses the python source code for running the tests. However, it is possible to provide a path to an actual executable to the tests using the `Q_EXECUTABLE` env var. This is actually being used during the build and packaging process, in order to test the resulting binary.  

## Limitations
Here's the list of known limitations. Please contact me if you have a use case that needs any of those missing capabilities.

* Common Table Expressions (CTE) are not supported for now. Will be implemented soon - See [here](https://github.com/harelba/q/issues/67) and [here](https://github.com/harelba/q/issues/124) for details.
* `FROM <subquery>` is not supported
* Spaces in file names are not supported. Use stdin for piping the data into q, or rename the file
* Some rare cases of subqueries are not supported yet.
* Queries with more than 10 different sqlite3 databases will load some data into memory
* up to 500 tables are supported in a single query

## Rationale
Have you ever stared at a text file on the screen, hoping it would have been a database so you could ask anything you want about it? I had that feeling many times, and I've finally understood that it's not the database that I want. It's the language - SQL.

SQL is a declarative language for data, and as such it allows me to define what I want without caring about how exactly it's done. This is the reason SQL is so powerful, because it treats data as data and not as bits and bytes (and chars).

The goal of this tool is to provide a bridge between the world of text files and of SQL.

### Why aren't other Linux tools enough?
The standard Linux tools are amazing and I use them all the time, but the whole idea of Linux is mixing-and-matching the best tools for each part of job. This tool adds the declarative power of SQL to the Linux toolset, without losing any of the other tools' benefits. In fact, I often use q together with other Linux tools, the same way I pipe awk/sed and grep together all the time.

One additional thing to note is that many Linux tools treat text as text and not as data. In that sense, you can look at q as a meta-tool which provides access to all the data-related tools that SQL provides (e.g. expressions, ordering, grouping, aggregation etc.).

### Philosophy
This tool has been designed with general Linux/Unix design principles in mind. If you're interested in these general design principles, read this amazing [book](http://catb.org/~esr/writings/taoup/) and specifically [this part](http://catb.org/~esr/writings/taoup/html/ch01s06.html). If you believe that the way this tool works goes strongly against any of the principles, I would love to hear your view about it.

## Future

* Expose python as a python module - Planned as a goal after the new version `3.x` is out


