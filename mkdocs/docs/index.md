# q - Run SQL directly on CSV or TSV files

[![GitHub Stars](https://img.shields.io/github/stars/harelba/q.svg?style=social&label=GitHub Stars&maxAge=600)](https://GitHub.com/harelba/q/stargazers/)
[![GitHub forks](https://img.shields.io/github/forks/harelba/q.svg?style=social&label=GitHub Forks&maxAge=600)](https://GitHub.com/harelba/q/network/)


## Overview
q is a command line tool that allows direct execution of SQL-like queries on CSVs/TSVs (and any other tabular text files).

q treats ordinary files as database tables, and supports all SQL constructs, such as WHERE, GROUP BY, JOINs etc. It supports automatic column name and column type detection, and provides full support for multiple encodings.

``` bash
q "SELECT COUNT(*) FROM ./clicks_file.csv WHERE c3 > 32.3"
```

``` bash
ps -ef | q -H "SELECT UID,COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"
```

Look at some examples [here](#examples), or just download the tool using the links in the [installation](#installation) below and play with it.

|                                        |                                                 |
|:--------------------------------------:|:-----------------------------------------------:|
| 完全支持所有的字符编码                 | すべての文字エンコーディングを完全にサポート    |
| 모든 문자 인코딩이 완벽하게 지원됩니다 | все кодировки символов полностью поддерживаются |

**Non-english users:** q fully supports all types of encoding. Use `-e data-encoding` to set the input data encoding, `-Q query-encoding` to set the query encoding, and use `-E output-encoding` to set the output encoding. Sensible defaults are in place for all three parameters. Please contact me if you encounter any issues and I'd be glad to help.

**Files with BOM:** Files which contain a BOM ([Byte Order Mark](https://en.wikipedia.org/wiki/Byte_order_mark)) are not properly supported inside python's csv module. q contains a workaround that allows reading UTF8 files which contain a BOM - Use `-e utf-8-sig` for this. I plan to separate the BOM handling from the encoding itself, which would allow to support BOMs for all encodings.

## Installation

| Format | Instructions | Comments |
:---|:---|:---|
|[OSX](https://github.com/harelba/packages-for-q/raw/master/single-binary/Darwin/2.0.9/q)|Download the executable from the link on the left, make it executable, and use it.|`brew install q` works well too.||
|[RPM Package](https://github.com/harelba/packages-for-q/raw/master/rpms/q-text-as-data-2.0.9-1.x86_64.rpm)| run `rpm -ivh <package-filename>` or `rpm -U <package-filename>` if you already have an older version of q.| A man page is available for this release. Just enter man q.|
|[DEB Package](https://github.com/harelba/packages-for-q/raw/master/deb/q-text-as-data_2.0.9-2_amd64.deb)| Run `sudo dpkg -i <package-filename>`|A man page is available for this release. Just enter `man q`.|
|[Windows Installer](https://github.com/harelba/packages-for-q/raw/master/windows/setup-q-2.0.9.exe)|Run the installer executable and hit next next next... q.exe will be added to the PATH so you can access it everywhere.|Windows doesn't update the PATH retroactively for open windows, so you'll need to open a new cmd window after the installation is done.|
|[tar.gz](https://github.com/harelba/q/archive/2.0.9.tar.gz)|Full source file tree for latest stable version||
|[zip](https://github.com/harelba/q/archive/2.0.9.zip)|Full source file tree for the latest stable version||

**Older versions can be downloaded [here](https://github.com/harelba/packages-for-q). Please let me know if you plan on using an older version, and why - I know of no reason to use any of them.**

## Requirements
As of version `2.0.9`, there's no need for any external dependency. Python itself (3.7), and any needed libraries are self-contained inside the installation, isolated from the rest of your system.

## Usage

``` bash
q <flags> "<query>"

  Simplest execution is `q "SELECT * FROM myfile"` which prints the entire file.
```

q allows performing SQL-like statements on tabular text data. Its purpose is to bring SQL expressive power to the Linux command line and to provide easy access to text as actual data.

Query should be an SQL-like query which contains *filenames instead of table names* (or - for stdin). The query itself should be provided as one parameter to the tool (i.e. enclosed in quotes). Multiple files can be used as one table by either writing them as `filename1+filename2+...` or by using shell wildcards (e.g. `my_files*.csv`).

Use `-H` to signify that the input contains a header line. Column names will be detected automatically in that case, and can be used in the query. If this option is not provided, columns will be named cX, starting with 1 (e.g. `q "SELECT c3,c8 from ..."`).

Use `-d` to specify the input delimiter.

Column types are auto detected by the tool, no casting is needed. Note that there's a flag `--as-text` which forces all columns to be treated as text columns.

Please note that column names that include spaces need to be used in the query with back-ticks, as per the sqlite standard.

Query/Input/Output encodings are fully supported (and q tries to provide out-of-the-box usability in that area). Please use `-e`,`-E` and `-Q` to control encoding if needed.

All sqlite3 SQL constructs are supported, including joins across files (use an alias for each table). Take a look at the [limitations](#limitations) section below for some rarely-used use cases which are not fully supported.

### Query
Each parameter that q gets is a full SQL query. All queries are executed one after another, outputing the results to standard output. Note that data loading is done only once, so when passing multiple queries on the same command-line, only the first one will take a long time. The rest will starting running almost instantanously, since all the data will already have been loaded. Remeber to double-quote each of the queries - Each parameter is a full SQL query.

Any standard SQL expression, condition (both WHERE and HAVING), GROUP BY, ORDER BY etc. are allowed.

JOINs are supported and Subqueries are supported in the WHERE clause, but unfortunately not in the FROM clause for now. Use table aliases when performing JOINs.

The SQL syntax itself is sqlite's syntax. For details look at http://www.sqlite.org/lang.html or search the net for examples.

NOTE: Full type detection is implemented, so there is no need for any casting or anything.

NOTE2: When using the `-O` output header option, use column name aliases if you want to control the output column names. For example, `q -O -H "select count(*) cnt,sum(*) as mysum from -"` would output `cnt` and `mysum` as the output header column names.

### Flags

``` bash
Usage: 
        q allows performing SQL-like statements on tabular text data.

        Its purpose is to bring SQL expressive power to manipulating text data using the Linux command line.

        Basic usage is q "<sql-like query>" where table names are just regular file names (Use - to read from standard input)
            When the input contains a header row, use -H, and column names will be set according to the header row content. If there isn't a header row, then columns will automatically be named c1..cN.

        Column types are detected automatically. Use -A in order to see the column name/type analysis.

        Delimiter can be set using the -d (or -t) option. Output delimiter can be set using -D

        All sqlite3 SQL constructs are supported.

        Examples:

              Example 1: ls -ltrd * | q "select c1,count(1) from - group by c1"
            This example would print a count of each unique permission string in the current folder.

          Example 2: seq 1 1000 | q "select avg(c1),sum(c1) from -"
            This example would provide the average and the sum of the numbers in the range 1 to 1000

          Example 3: sudo find /tmp -ls | q "select c5,c6,sum(c7)/1024.0/1024 as total from - group by c5,c6 order by total desc"
            This example will output the total size in MB per user+group in the /tmp subtree


            See the help or https://github.com/harelba/q/ for more details.
    

Options:
  -h, --help            show this help message and exit
  -v, --version         Print version
  -V, --verbose         Print debug info in case of problems
  -S SAVE_DB_TO_DISK_FILENAME, --save-db-to-disk=SAVE_DB_TO_DISK_FILENAME
                        Save database to an sqlite database file
  --save-db-to-disk-method=SAVE_DB_TO_DISK_METHOD
                        Method to use to save db to disk. 'standard' does not
                        require any deps, 'fast' currenty requires manually
                        running `pip install sqlitebck` on your python
                        installation. Once packing issues are solved, the fast
                        method will be the default.

  Input Data Options:
    -H, --skip-header   Skip header row. This has been changed from earlier
                        version - Only one header row is supported, and the
                        header row is used for column naming
    -d DELIMITER, --delimiter=DELIMITER
                        Field delimiter. If none specified, then space is used
                        as the delimiter.
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

  Query Related Options:
    -q QUERY_FILENAME, --query-filename=QUERY_FILENAME
                        Read query from the provided filename instead of the
                        command line, possibly using the provided query
                        encoding (using -Q).
    -Q QUERY_ENCODING, --query-encoding=QUERY_ENCODING
                        query text encoding. Experimental. Please send your
                        feedback on this
```

## Examples
The `-H` flag in the examples below signifies that the file has a header row which is used for naming columns.

The `-t` flag is just a shortcut for saying that the file is a tab-separated file (any delimiter is supported - Use the `-d` flag).

Queries are given using upper case for clarity, but actual query keywords such as SELECT and WHERE are not really case sensitive.

Example List:

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
The current implementation is written in Python using an in-memory database, in order to prevent the need for external dependencies. The implementation itself supports SELECT statements, including JOINs (Subqueries are supported only in the WHERE clause for now). If you want to do further analysis on the data, you can use the `--save-db-to-disk` option to write the resulting tables to an sqlite database file, and then use `seqlite3` in order to perform queries on the data separately from q itself.

Please note that there is currently no checks and bounds on data size - It's up to the user to make sure things don't get too big.

Please make sure to read the [limitations](#limitations) section as well.

## Development

### Tests
The code includes a test suite runnable through `test/test-all`. If you're planning on sending a pull request, I'd appreciate if you could make sure that it doesn't fail. 

## Limitations
Here's the list of known limitations. Please contact me if you have a use case that needs any of those missing capabilities.

* `FROM <subquery>` is not supported
* Common Table Expressions (CTE) are not supported
* Spaces in file names are not supported. Use stdin for piping the data into q, or rename the file
* Some rare cases of subqueries are not supported yet.

## Rationale
Have you ever stared at a text file on the screen, hoping it would have been a database so you could ask anything you want about it? I had that feeling many times, and I've finally understood that it's not the database that I want. It's the language - SQL.

SQL is a declarative language for data, and as such it allows me to define what I want without caring about how exactly it's done. This is the reason SQL is so powerful, because it treats data as data and not as bits and bytes (and chars).

The goal of this tool is to provide a bridge between the world of text files and of SQL.

### Why aren't other Linux tools enough?
The standard Linux tools are amazing and I use them all the time, but the whole idea of Linux is mixing-and-matching the best tools for each part of job. This tool adds the declarative power of SQL to the Linux toolset, without loosing any of the other tools' benefits. In fact, I often use q together with other Linux tools, the same way I pipe awk/sed and grep together all the time.

One additional thing to note is that many Linux tools treat text as text and not as data. In that sense, you can look at q as a meta-tool which provides access to all the data-related tools that SQL provides (e.g. expressions, ordering, grouping, aggregation etc.).

### Philosophy
This tool has been designed with general Linux/Unix design principles in mind. If you're interested in these general design principles, read this amazing [book](http://catb.org/~esr/writings/taoup/) and specifically [this part](http://catb.org/~esr/writings/taoup/html/ch01s06.html). If you believe that the way this tool works goes strongly against any of the principles, I would love to hear your view about it.

## Future

* Expose python as a python module - Mostly implemented. Requires some internal API changes with regard to handling stdin before exposing it.
* Allow to use a distributed backend for scaling the computations


