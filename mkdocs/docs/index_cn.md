# q - 直接在CSV或TSV文件上运行SQL

[![GitHub Stars](https://img.shields.io/github/stars/harelba/q.svg?style=social&label=GitHub Stars&maxAge=600)](https://GitHub.com/harelba/q/stargazers/)
[![GitHub forks](https://img.shields.io/github/forks/harelba/q.svg?style=social&label=GitHub Forks&maxAge=600)](https://GitHub.com/harelba/q/network/)


## 概述
q 是一个可以运行在 CSV / TSV 文件(或其他表格式的文本文件)上运行类SQL命令的命令行工具。

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

> 如果遇到问题请与我联系，期待与你交流。

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

**旧版本可以在这儿[下载](https://github.com/harelba/packages-for-q) 。按理说不会有人愿意用旧版本，要是你计划使用旧版，希望能与你交流。**

## 须知
从`2.0.9`版本开始，不需要任何外部依赖。Python(3.7)和其他所需的库包含在了安装文件中且与系统隔离。

## 使用

``` bash
q <flags> "<query>"

  最简单的执行语句：q "SELECT * FROM myfile" 该语句会输出文件内容
```

q 支持在表格式的文本上执行类SQL命令。它的初衷是为Linux命令行附加SQL的表达力且实现对文本数据的轻松访问。

类SQL的查询将*文件名(或标准输入流)看作表名*。查询语句会作为命令输入的一个参数（使用引号包裹)，如果将多个文件看作一张表，可以这样写 `文件名1+文件名2....`或者使用通配符（比如：`my_files*.csv`)。

使用 `-H` 表示输入内容中包含表头。该情况下列名会被自动识别，如果没有指定该参数，列名将会被以`cX`命名，`X`从1开始（比如: `q "SELECT c3,c8 from ..."`) 。

使用 `-d` 声明输入的分隔符。

列类型可由工具自动识别，无需强制转换。 提示，使用`--as-text` 可以强制将所有列类型转换为文本类型。

依据sqlite规范，如果列名中含有空格，需要使用反引号 (即：`) 引起来。

完全支持查询/输入/输出的编码设置（q 力争提供一种开箱即用的方法), 可以分别使用`-Q`,`-e` 和 `-E`来指定编码设置类型。

支持所有的sqlite3 SQL方法，包括文件之间的 JOIN（可以为文件设置别名）操作。在下面的[限制](#限制)小节可以看到一些少有使用的、欠支持的说明。

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
        q 支持在表格式的文本数据上执行类SQL查询。

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
  数据相关的选项:
  
    -H, --skip-header   忽略表头，在早期的版本中已修改为：仅支持用于标明列名的一行表头
    -d DELIMITER, --delimiter=DELIMITER
                        列分隔符，若无特别指定，默认为空格符
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
                        保留每列前的空格。为了使其开箱即用，默认去除了列前的空格
                        如果有需要，可以指定该参数
    --disable-double-double-quoting
                        禁止一对双引号的转义。默认可以使用 "" 转义双引号
                        主要为了向后兼容
    --disable-escaped-double-quoting
                        禁止转义双引号
                        默认可以在双引号字段中使用 \" 进行转义
                        主要为了向后兼容 
    --as-text           不识别列类型（所有列被当作文本类型）
    -w INPUT_QUOTING_MODE, --input-quoting-mode=INPUT_QUOTING_MODE
                        输入内容的转义模式，可选值 all、minimal、none
                        该参数稍有误导性，-W 指定输出内容的转义模式 
    -M MAX_COLUMN_LENGTH_LIMIT, --max-column-length-limit=MAX_COLUMN_LENGTH_LIMIT
                        设置列的最大长度
    -U, --with-universal-newlines
                        设置通用换行符
                        -U 参数当前仅适用于常规文件，输入流或.gz类文件暂不支持

  输出相关的选项:
    -D OUTPUT_DELIMITER, --output-delimiter=OUTPUT_DELIMITER
                        输出列间的分隔符
                        若未指定，则与 -d 指定的分隔符相同；若均为指定，则默认为空格符
    -P, --pipe-delimited-output
                        同 -D '|' 为了方便和可读性提供该参数
    -T, --tab-delimited-output
                        同 -D <tab> 这仅是一种简写，也可以在Linux命令行中使用$'\t' 
    -O, --output-header
                        输出表头，输出的列名是由查询中指定的别名
                        如: 'select name FirstName, value1/value2 MyCalculation
                        from ...' 即使输入时未指定表头仍可使用该参数。
    -b, --beautify      美化输出结果，可能较慢...
    -f FORMATTING, --formatting=FORMATTING
                        格式化输出列
                        如格式X=fmt，Y=fmt等，上述中的X、Y是指第几列（如：1 表示 SELECT 
                        的第一列)
    -E OUTPUT_ENCODING, --output-encoding=OUTPUT_ENCODING
                        输出内容的编码，默认是 'none'，跟随系统或终端的编码
    -W OUTPUT_QUOTING_MODE, --output-quoting-mode=OUTPUT_QUOTING_MODE
                        输出内容的转义模式，可选值 all、minimal、none
                        该参数稍有误导性，-w 指定输入内容的转义模式 
    -L, --list-user-functions
                        列出所有内置函数

  查询相关的参数:
    -q QUERY_FILENAME, --query-filename=QUERY_FILENAME
                        指定文件名，由文件中读取查询语句。
                        该操作常与查询编码（使用 -Q)一同使用
    -Q QUERY_ENCODING, --query-encoding=QUERY_ENCODING
                        查询编码(包含查询语句的文件编码)
                        实验性参数，对该参数的意见可反馈
```

## 示例
下述 `-H` 参数的例子，表示文件中含有表头时使用该参数。

`-t` 参数是指定文件以 tab 作为分隔符的缩写（可以使用 `-d` 参数指定任意分隔符）。

为了清楚起见，查询关键字均使用大写，实际上关键字(如 SELECT、WHERE等)对大小写并不敏感。

示例目录:

* [例1 - 统计指定列唯一值的数量](#例1)
* [例2 - 数值条件过滤、排序并限制输出数](#例2)
* [例3 - GROUP BY简单示例](#例3)
* [例4 - GROUP BY进阶示例 (以时间格式分组)](#例4)
* [例5 - 标准输入流作为输入](#例5)
* [例6 - 使用表头中列名](#例6)
* [例7 - JOIN 两个文件](#例7)

### 例1
对指定字段（点击数据中的uuid）执行 COUNT DISTINCT 

``` bash
q -H -t "SELECT COUNT(DISTINCT(uuid)) FROM ./clicks.csv"
```
输出:
``` bash
229
```

### 例2
过滤数值数据、排序并限制输出数量

注意：q 将其看作数值类型并对其进行数值过滤(数值比较而不是字符串比较)

``` bash
q -H -t "SELECT request_id,score FROM ./clicks.csv WHERE score > 0.7 ORDER BY score DESC LIMIT 5"
```
输出:
``` bash
2cfab5ceca922a1a2179dc4687a3b26e    1.0
f6de737b5aa2c46a3db3208413a54d64    0.986665809568
766025d25479b95a224bd614141feee5    0.977105183282
2c09058a1b82c6dbcf9dc463e73eddd2    0.703255121794
```

### 例3
GROUP BY 简单示例

``` bash
q -t -H "SELECT hashed_source_machine,count(*) FROM ./clicks.csv GROUP BY hashed_source_machine"
```
输出:
``` bash
47d9087db433b9ba.domain.com 400000
```

### 例4
GROUP BY进阶示例 (以时间格式分组)

``` bash
q -t -H "SELECT strftime('%H:%M',date_time) hour_and_minute,count(*) FROM ./clicks.csv GROUP BY hour_and_minute"
```
输出:
``` bash
07:00   138148
07:01   140026
07:02   121826
```

### 例5
标准输入流作为输入

计算 /tmp 目录下各 user/group 的占用空间大小

``` bash
sudo find /tmp -ls | q "SELECT c5,c6,sum(c7)/1024.0/1024 AS total FROM - GROUP BY c5,c6 ORDER BY total desc"
```
输出:
``` bash
mapred hadoop   304.00390625
root   root     8.0431451797485
smith  smith    4.34389972687
```

### 例6
使用表头中列名

计算拥有进程数最多的前3位用户名及其数量

注意: 该查询中自动识别了列名

``` bash
ps -ef | q -H "SELECT UID,COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"
```
输出:
``` bash
root 152
harel 119
avahi 2
```

### 例7
JOIN 两个文件

如下命令中JOIN一个ls命令输出内容文件（exampledatafile) 和一个包含group_name、email两列字段的文件（group-emails-example)，每一邮件组均包含filename、email列, 为了输出简便，使用WHERE条件过滤出名为 ppp 的文件

``` bash
q "SELECT myfiles.c8,emails.c2 FROM exampledatafile myfiles JOIN group-emails-example emails ON (myfiles.c4 = emails.c1) WHERE myfiles.c8 = 'ppp'"
```
输出:
``` bash
ppp dip.1@otherdomain.com
ppp dip.2@otherdomain.com
```
可以看出 ppp 文件出现了两次，每次都匹配到了它所属的dip邮件组（如例中 dip.1@... /  dip2@...)，可以在 `exampledatafile` 和 `group-emails-example` 文件中查看数据。

JOIN 的应用场景中也支持列名识别，在查询包含表头的文件时，只需指定 `-H` 参数即可。

## 声明
为了避免引用外部依赖，当前是使用由Python编写的内存数据库实现的。当前是支持 SELECT 语句及 各种JOIN （ 目前仅在 WHERE 语句中支持子查询)。
若想对数据进一步分析，可以使用 `--save-db-to-disk` 参数，以将结果输出为 sqlite 数据库文件，然后使用 `sqlite3` 语句来执行查询操作。

需要提示的是，当前并没有对数据量的大小进行检测和限制 - 也就是说，需要用户自己掌控文件大小。

请务必阅读[限制](#限制)小节。

## 开发

### 测试
源码中包含了测试用例，可以通过 `test/test-all` 来执行。若想要提交 PR的话，一定先确保其均执行成功。

## 限制
如下罗列了一些已知的限制，若你的使用场景中需要用到以下标明的限制，请联系我。

* 不支持 `FROM <subquery>` 
* 不支持公用表表达式(CTE)
* 不支持文件名中包含空格 (可以将文件以标准输入流的方式输入 q 或重命名文件)
* 不支持较少用到的子查询

## 原理
你是否曾经盯着屏幕上的文本文件发呆，希望它要是数据库就好了，这样就可以找出自己想要的内容？我曾有过很多次，最终顿悟。我想要的不是数据库，而是 SQL。

SQL 是一种面向数据声明的语言，它允许自定义数据内容而无需关心其执行过程。这也正是SQL强大之处，因为它对于数据'所见即所得'，而不是将数据看作字节码。

本工具的目的是：在文本文件和SQL之间搭建一座桥梁。

### 为什么其他Linux工具不能满足需求？
传统的Linux工具库也很酷，我也经常使用它们， 但Linux的整体理念是为任一部分搭配最好的工具。本工具为传统Linux工具集新添了 SQL 族类工具，其他工具并不会失去本来优势。
事实上，我也经常将 q 和其他Linux工具搭配使用，就如同使用管道将 awk/sed 和 grep 搭配使用一样。

另外需要注意的是,许多Linux工具就将文本看作文本，而不是数据。从这个意义上来讲，可以将 q 看作提供了 SQL 功能（如：表达式、排序、分组、聚合等）的元工具。

### 理念

本工具的设计遵从了 Linux/Unix 的传统设计原则。若你对这些设计原则感兴趣，可以阅读 [这本书](http://catb.org/~esr/writings/taoup/) ，尤其是书中 [这部分](http://catb.org/~esr/writings/taoup/html/ch01s06.html)
若你认为本工具工作方式与之背道而驰，愿洗耳恭听你的建议。

## 展望

* 主要方向：将其作为python的模块公开。 在公开之前，需要对处理标准输入流做一些内部API的完善。
* 支持分布式以提高算力。



