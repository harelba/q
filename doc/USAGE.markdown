# q - Text as Data

## SYNOPSIS
	`q <flags> <query>`

	Example Execution for a delimited file:

		q "select * from myfile.csv"

	Example Execution for an sqlite3 database:

		q "select * from mydatabase.sqlite:::my_table_name"

            or

		q "select * from mydatabase.sqlite"

            if the database file contains only one table

	Auto-caching of delimited files can be activated through `-C readwrite` (writes new caches if needed)  or `-C read` (only reads existing cache files)

	Setting the default caching mode (`-C`) can be done by writing a `~/.qrc` file. See docs for more info.
	
## DESCRIPTION
q's purpose is to bring SQL expressive power to the Linux command line and to provide easy access to text as actual data.

q allows the following:

* Performing SQL-like statements directly on tabular text data, auto-caching the data in order to accelerate additional querying on the same file
* Performing SQL statements directly on multi-file sqlite3 databases, without having to merge them or load them into memory

Query should be an SQL-like query which contains filenames instead of table names (or - for stdin). The query itself should be provided as one parameter to the tool (i.e. enclosed in quotes).

The following filename types are supported:

* Delimited-file filenames, including relative/absolute paths
* sqlite3 database filenames, with an additional `:::<table_name>` for accessing a specific table. If a database contains only one table, then denoting the table name is not needed. Examples: `mydatabase.sqlite3:::users_table` or `my_single_table_database.sqlite`.

Use `-H` to signify that the input contains a header line. Column names will be detected automatically in that case, and can be used in the query. If this option is not provided, columns will be named cX, starting with 1 (e.g. q "SELECT c3,c8 from ...").

Use `-d` to specify the input delimiter.

Column types are auto detected by the tool, no casting is needed.

Please note that column names that include spaces need to be used in the query with back-ticks, as per the sqlite standard.

Query/Input/Output encodings are fully supported (and q tries to provide out-of-the-box usability in that area). Please use `-e`,`-E` and `-Q` to control encoding if needed.

All sqlite3 SQL constructs are supported, including joins across files (use an alias for each table), with the exception of CTE (for now).

See https://github.com/harelba/q for more details.

## QUERY
q gets one parameter - An SQL-like query. 

Any standard SQL expression, condition (both WHERE and HAVING), GROUP BY, ORDER BY etc. are allowed.

JOINs are supported and Subqueries are supported in the WHERE clause, but unfortunately not in the FROM clause for now. Use table aliases when performing JOINs.

The SQL syntax itself is sqlite's syntax. For details look at http://www.sqlite.org/lang.html or search the net for examples.

**NOTE:** Full type detection is implemented, so there is no need for any casting or anything.

**NOTE2:** When using the `-O` output header option, use column name aliases if you want to control the output column names. For example, `q -O -H "select count(*) cnt,sum(*) as mysum from -"` would output `cnt` and `mysum` as the output header column names.

## RUNTIME OPTIONS
q can also get some runtime flags. The following parameters can be used, all optional:

````
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

### Table names
The table names are the actual file names that you want to read from. Path names are allowed. Use "-" if you want to read from stdin (e.g. `q "SELECT * FROM -"`)

Wildcard matches are supported - For example: `SELECT ... FROM ... mydata*.dat`

Files with .gz extension are considered to be gzipped and decompressed on the fly.

### Parsing Modes
q supports two parsing modes:

* `relaxed` - This is the default mode. It tries to lean towards simplicity of use. When a row doesn't contains enough columns, they'll be filled with nulls, and when there are too many, the extra values will be merged to the last column. Defining the number of expected columns in this mode is done using the `-c` parameter. If it is not provided, then the number of columns is detected automatically (In most use cases, there is no need to specify `-c`)
* `strict` - Strict mode is for hardcore csv/tsv parsing. Whenever a row doesn't contain the proper number of columns, processing will stop. `-c` must be provided when using this mode

### Output formatting option
The format of F is as a list of X=f separated by commas, where X is a column number and f is a python format:

* X - column number - This is the SELECTed column (or expression) number, not the one from the original table. E.g, 1 is the first SELECTed column, 3 is the third SELECTed column.
* f - A python formatting string such as {} - See https://www.w3schools.com/python/ref_string_format.asp for details if needed.

## EXAMPLES
Example 1: `ls -ltrd * | q "select c1,count(1) from - group by c1"`

	This example would print a count of each unique permission string in the current folder.

Example 2: `seq 1 1000 | q "select avg(c1),sum(c1) from -"`

	This example would provide the average and the sum of the numbers in the range 1 to 1000

Example 3: `sudo find /tmp -ls | q "select c5,c6,sum(c7)/1024.0/1024 as total from - group by c5,c6 order by total desc"`

	This example will output the total size in MB per user+group in the /tmp subtree

Example 4: `ps -ef | q -H "select UID,count(*) cnt from - group by UID order by cnt desc limit 3"`

	This example will show process counts per UID, calculated from ps data. Note that the column names provided by ps are being used as column name in the query (The -H flag activates that option)

## AUTHOR
Harel Ben-Attia (harelba@gmail.com)

[@harelba](https://twitter.com/harelba) on Twitter

Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

## COPYRIGHT
Copyright (C) 2012--2021 Harel Ben Attia

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 3, or (at your option) any later version.

This program is distributed in the hope that it will be useful,but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details. You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc., 51 Franklin Street - Fifth Floor, Boston, MA  02110-1301, USA 


