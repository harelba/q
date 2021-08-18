# q - 直接在CSV或TSV文件上运行SQL

[![GitHub Stars](https://img.shields.io/github/stars/harelba/q.svg?style=social&label=GitHub Stars&maxAge=600)](https://GitHub.com/harelba/q/stargazers/)
[![GitHub forks](https://img.shields.io/github/forks/harelba/q.svg?style=social&label=GitHub Forks&maxAge=600)](https://GitHub.com/harelba/q/network/)


## 概述
q 是一个可以运行在CSV/TSV文件(或其他行表形式的文本文件)上运行类SQL命令的命令行工具。

q 将普通文本（如上述）作为数据库表，且支持所有的SQL语法如：WHERE、GROUP BY、各种JOIN等。此外，还拥有自动识别列名和列类型及广泛支持多种编码的特性。

``` bash
q "SELECT COUNT(*) FROM ./clicks_file.csv WHERE c3 > 32.3"
```

``` bash
ps -ef | q -H "SELECT UID,COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"
```

查看[示例](#示例)或[安装](#安装)体验.

|                                        |                                                 |
|:--------------------------------------:|:-----------------------------------------------:|
| 完全支持所有的字符编码                 | すべての文字エンコーディングを完全にサポート    |
| 모든 문자 인코딩이 완벽하게 지원됩니다 | все кодировки символов полностью поддерживаются |


**非英语用户:** q 完全支持所有类型的字符编码。 使用 `-e data-encoding` 设置输入编码; 使用 `-Q query-encoding` 设置查询编码; 使用 `-E output-encoding` 设置输出编码;
如上三个参数均设有合理的默认值。<br/>

> 如果遇到问题请与我联系，期待与您交流。

**含有BOM的文件:** python的csv模块并不能很好的支持含有[Byte Order Mark](https://en.wikipedia.org/wiki/Byte_order_mark) 的文件。针对该种情况，使用 `-e utf-8-sig` 命令参数可读取包含BOM的UTF8编码文件。

> 我们计划将BOM相关处理与编码'解耦', 这样就可以支持所有编码的BOM文件了。

## 安装

| 格式 | 说明 | 备注 |
|:---|:---|:---|
|[OSX](https://github.com/harelba/q/releases/download/2.0.19/q-x86_64-Darwin)|运行 `brew install q`| 该方式暂不支持MAN手册, 可以使用 `q --help` 查看帮助||
|[RPM Package](https://github.com/harelba/q/releases/download/2.0.19/q-text-as-data-2.0.19-1.x86_64.rpm)| 运行 `rpm -ivh <package-filename>` 如果安装过旧版则运行 `rpm -U <package-filename>` | 该方式支持MAN手册，可运行`man q`查看|
|[DEB Package](https://github.com/harelba/q/releases/download/2.0.19/q-text-as-data_2.0.19-2_amd64.deb)| 运行 `sudo dpkg -i <package-filename>`|该方式支持MAN手册，可运行`man q`查看|
|[Windows Installer](https://github.com/harelba/q/releases/download/2.0.19/q-AMD64-Windows-installer.exe)|运行安装可执行文件，一直点击下一步、下一步... q.exe 将被添加至PATH，以便于随处运行|PATH更新后并不会即时生效，重新打开cmd命令窗口便可|
|[tar.gz](https://github.com/harelba/q/archive/2.0.19.tar.gz)|最新稳定版的所有源码文件。提示，q.py 文件不能直接使用，因为它需要python依赖||
|[zip](https://github.com/harelba/q/archive/2.0.19.zip)|最新稳定版的所有源码文件。提示，q.py 文件不能直接使用，因为它需要python依赖||

**旧版本可以在这儿[下载](https://github.com/harelba/packages-for-q) 。按理说不会有人愿意用旧版本，要是您计划使用旧版，希望能与您交流。**

## 须知
从`2.0.9`版本开始，不需要任何外部依赖。Python(3.7)和其他所需的库包含在了安装文件中且与系统隔离。

## 使用

``` bash
q <flags> "<query>"

  最简单的执行语句：q "SELECT * FROM myfile" 该语句会输出文件内容
```

q 支持在行表形式的文本上执行类SQL命令。它的初衷是为Linux命令行附加SQL的表达力且实现对文本数据的轻松访问。

类SQL的查询将*文件名(或标准输入流)看作表名*。查询语句会作为命令输入的一个参数（使用引号包裹)，如果将多个文件看作一张表，可以这样写 `文件名1+文件名2....`或者使用通配符（比如：`my_files*.csv`)。

使用 `-H` 表示输入内容中包含标题行。该情况下列名会被自动识别，如果没有指定该参数，列名将会被以`cX`命名，`X`从1开始（比如: `q "SELECT c3,c8 from ..."`) 。

使用 `-d` 声明输入的分隔符。

列类型可由工具自动识别，无需强制转换。 提示，使用`--as-text` 可以强制将所有列类型转换为文本类型。

依据sqlite规范，如果列名中含有空格，需要使用反引号 (即：`) 引起来。

完全支持查询/输入/输出的编码设置（q 力争提供一种开箱即用的方法), 可以分别使用`-Q`,`-e` and `-E`来指定编码设置类型。

支持所有的sqlite3 SQL方法，包括文件之间的关联（可以为文件设置别名）操作。在下面的[限制](#限制)小节可以看到一些少有使用的、欠支持的说明。

### 查询

q 的每一个参数都是由双引号包裹的一条完整的SQL语句。所有的查询语句会依次执行，最终结果以标准输出流形式输出。 提示，在同一命令行中执行多条查询语句时，仅在执行第一条查询语句时需要耗时载入数据，其他查询语句即时执行。

支持所有标准SQL语法，条件（WHERE 和 HAVING）、GROUP BY、ORDER BY等。

在WHERE条件查询中，支持JOIN操作和子查询，但在FROM子句中并不支持。JOIN操作时，可以为文件起别名。

SQL语法同sqlite的语法，详情见 http://www.sqlite.org/lang.html 或上网找一些示例。

**注意**：
* 支持所有类型的自动识别，无需强制转换或其他操作。
* 如果重命名输出列，则需要为列指定别名并使用 `-O` 声明。如: `q -O -H "select count(*) cnt,sum(*) as mysum from -"` 便会将`cnt`和`mysum`作为列名输出。

### 指令

``` bash
使用:
        q 支持在行表形式的文本数据上执行类SQL查询。

        它的初衷是为Linux命令行附加SQL的表达力且实现对文本数据的轻松访问。

        基本操作是 q "SQL查询语句" 表名便是文件名（使用 - 从标注输入中读取数据）。若输入内容包含表头时，可以使用 -H 指定列名。若无表头，则列将会自动命名为 c1...cN。

        列类型可被自动识别。可以使用 -A 命令查看每列的名称及其类型。

        可以使用 -d (或 -t) 指定分隔符，使用 -D 指定输出分割符。

        支持所有的sqlite3 SQL方法。

        示例:
            
          例子1: ls -ltrd * | q "select c1,count(1) from - group by c1" 
          上例将会输出当前目录下，所有文件的权限表达式分组及每组数量。

          例子2: seq 1 1000 | q "select avg(c1),sum(c1) from -" 
          上例将会输出1到1000的平均数与和数。
          
          例子3: sudo find /tmp -ls | q "select c5,c6,sum(c7)/1024.0/1024 as total from - group by c5,c6 order by total desc" 
          上例将会输出在/tmp目录下，相同'用户+组'的文件所占用的MB磁盘空间。

          更多详情见 https://github.com/harelba/q/ 或查看帮助
    
选项：
  -h, --help            显示此帮助信息并退出 
  -v, --version         显示版本号
  -V, --verbose         出现问题时显示调试信息
  -S SAVE_DB_TO_DISK_FILENAME, --save-db-to-disk=SAVE_DB_TO_DISK_FILENAME
                        将数据库保存为一个 sqlite 数据库文件
  --save-db-to-disk-method=SAVE_DB_TO_DISK_METHOD
                        保存数据库到磁盘的方法
                        'standard' 不需要任何设置
                        'fast'需要手动在python的安装目录下执行`pip install sqlitebck`
                        打包的问题解决后，'fast'即被作为默认方式
  数据相关选项:
  
    -H, --skip-header   忽略表头，在早期的版本中已修改为：仅支持用于标明列名的一行表头
    -d DELIMITER, --delimiter=DELIMITER
                        列分隔符，若无特别指定，空格符作为默认分隔符
    -p, --pipe-delimited
                        作用同 -d '|'，为了方便和可读性提供该参数
    -t, --tab-delimited
                        作用同 -d <tab>，这仅是一种简写，也可以在Linux命令行中使用$'\t'
    -e ENCODING, --encoding=ENCODING
                        输入文件的编码，默认是UTF-8
    -z, --gzipped       压缩数据，对于从输入流读取文件非常高效 .gz 是自动压缩后文件扩展名
    -A, --analyze-only  简单分析：各列的数据类型
    -m MODE, --mode=MODE
                        数据解析模式: 松散, 宽松和严格。在严格模式下必须指定 -c 
                        --column-count 参数。
    -c COLUMN_COUNT, --column-count=COLUMN_COUNT
                        当使用宽松或严格模式时，用于指定列的数量
    -k, --keep-leading-whitespace
                        保留每列前的空格。为了使其开箱即用，默认去除了列前的空格。
                        如果有需要，可以指定该参数
    --disable-double-double-quoting
                        禁止一对双引号的转义。默认可以使用 "" 转义双引号。
                        主要为了向后兼容。
    --disable-escaped-double-quoting
                        Disable support for escaped double-quoting for
                        escaping the double quote character. By default, you
                        can use \" inside double quoted fields to escape
                        double quotes. Mainly for backward compatibility.
    --as-text           不识别列类型（所有列被当作文本类型）
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


