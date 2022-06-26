[toc]
q - 直接在CSV或TSV文件上运行SQL

[![GitHub Stars](https://img.shields.io/github/stars/harelba/q.svg?style=social&label=GitHub Stars&maxAge=600)](https://GitHub.com/harelba/q/stargazers/)
[![GitHub forks](https://img.shields.io/github/forks/harelba/q.svg?style=social&label=GitHub Forks&maxAge=600)](https://GitHub.com/harelba/q/network/)

## 概述

**q** 旨在通过将文本当作结构化数据(且支持直接访问多文件sqlite3数据库)，赋能于Linux命令行，使其具备sql的表达能力。

``` base
    q <flags> <sql-query>
```

**q** 支持如下:

* 支持在表格式的文本上执行类SQL命令, 自动缓存数据以便于在相同文件上执行其他查询。

``` bash
    # 文件内容简单查询,列将被命名为c1...cN
    q "select c1,c5 from myfile.csv"

    # -d '|' 指定输入分隔符，-H 表示包含列名
    q -d , -H "select my_field from myfile.delimited-file-with-pipes"

    # -C readwrite 为csv文件文件创建缓存
    q -d , -H "select my_field from myfile.csv" -C readwrite

    # -C read 指定由缓存中读取csv文件
    q -d , -H "select my_field from myfile.csv" -C read

    # 可以在`~/.qrc`文件中设置默认启用缓存模式(`-C`)
```

* 支持在多文件 sqlite3 数据库上执行 SQL 语句，无需合并或加载到内存中

``` bash
    q "select * from mydatabase.sqlite:::my_table_name"
        或
    q "select * from mydatabase.sqlite"
        (如果库文件中仅含有一张表)

    # sqlite文件被自动识别，无需指定扩展名

```

缓存性能表现见下表:

|    行数   | 列数| 文件大小 | 无缓存查询时长 | 有缓存查询时长 | 速度提升 |
|:---------:|:-------:|:---------:|:--------------------------:|:-----------------------:|:-----------------:|
| 5,000,000 |   100   |   4.8GB   |    4 分47 秒 |       1.92 秒|        x149       |
| 1,000,000 |   100   |   983MB   |        50.9 秒|      0.461 秒|        x110       |
| 1,000,000 |    50   |   477MB   |        27.1 秒|      0.272 秒|        x99        |
|  100,000  |   100   |    99MB   |         5.2 秒|      0.141 秒|        x36        |
|  100,000  |    50   |    48MB   |         2.7 秒|      0.105 秒|        x25        |


**提示：** 因缓存占用磁盘空间，当前版本默认**不启用**缓存。查询时指定`-C readwrite` 或 `-C read`指令启用缓存, 或者在`.qrc` 文件中设置`caching_mode`以启用缓存模式。

**q** 将普通文件看作数据库表并支持所有SQL语法结构，比如`WHERE`,`GROUP BY`, 各种`JOIN`等。此外，**q** 也支持列名、类型自动识别及多种字符编码。

**新特性** - 自动缓存、直接查询sqlite数据库及支持`~/.qrc`配置文件，详情参照[这儿](https://github.com/harelba/q/blob/master/QSQL-NOTES.md) 下载[安装](#安装)体验.

### 字符集

|                                        |                                                 |
|:--------------------------------------:|:-----------------------------------------------:|
| 完全支持所有的字符编码                 | すべての文字エンコーディングを完全にサポート    |
| 모든 문자 인코딩이 완벽하게 지원됩니다 | все кодировки символов полностью поддерживаются |


**非英语用户:** q 完全支持所有类型的字符编码。 使用 `-e data-encoding` 设置输入编码; 使用 `-Q query-encoding` 设置查询编码; 使用 `-E output-encoding` 设置输出编码;
如上三个参数均设有合理的默认值。<br/>

> 如果遇到问题请与我联系,会积极协助的～

**含有BOM的文件:** python的csv模块并不能很好的支持含有[Byte Order Mark](https://en.wikipedia.org/wiki/Byte_order_mark) 的文件。针对该种情况，使用 `-e utf-8-sig` 命令参数可读取包含BOM的UTF8编码文件。

> 后期计划将BOM相关处理与编码'解耦', 这样就可以支持所有编码的BOM文件了。

## 安装


| 格式| 说明| 备注|
:---|:---|:---|
|[OSX](https://github.com/harelba/q/releases/download/v3.1.6/macos-q)|执行 `brew install harelba/q/q`或点击左侧链接下载执行文件|执行`man q`查看帮助手册|
|[RPM Package](https://github.com/harelba/q/releases/download/v3.1.6/q-text-as-data-3.1.6.x86_64.rpm)| 若曾安装过旧版，执行`rpm -ivh <package-filename>` 或 `rpm -U <package-filename>` | 执行 `man q`查看帮助手册|
|[DEB Package](https://github.com/harelba/q/releases/download/v3.1.6/q-text-as-data-3.1.6-1.x86_64.deb)|执行 `sudo dpkg -i <package-filename>`|执行 `man q`查看帮助手册(由于某些原因，手册可能安装失败，近期将其修复)|
|[Windows Installer](https://github.com/harelba/q/releases/download/v3.1.6/q-text-as-data-3.1.6.msi)|双击安装文件，点击下一步、下一步... q.exe 将自动添加至环境变量|windows在已打开的命令行窗口中不会更新`PATH`, 所以安装完后需要重新打开`cmd`/`bash`|
|[Source tar.gz](https://github.com/harelba/q/archive/refs/tags/v3.1.6.tar.gz)|最新稳定版源码文件包.提示`q.py` 不能直接执行，它还需要一些依赖||
|[Source zip](https://github.com/harelba/q/archive/refs/tags/v3.1.6.zip)|最新稳定版源码文件包.提示`q.py` 不能直接执行，它还需要一些依赖||

如果需要一个其他Linux发行版安装包，及时联系我就好（打包并不麻烦)。

若新版不能满足您的需求，您习惯使用旧版的话。早期版本`2.0.19`可以点击[这儿](https://github.com/harelba/q/releases/tag/2.0.19)下载。

## 须知

**q** 被打包成了独立可执行文件，无需依赖python。这得益于[pyoxidizer](https://github.com/indygreg/PyOxidizer) (一个超酷的项目)。

## 示例

本小节中展示了一些新特性示例，更多基础示例，可以在[这儿](#示例)查看。

### 基础示例

```bash
# 准备一些数据
$ seq 1 1000000 > myfile.csv

# 查询
$ q "select sum(c1),count(*) from myfile.csv where c1 % 3 = 0"
166666833333 333333

# 对标准输出进行查询
$ ps -ef | q -b -H "SELECT UID, COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"
501 288
0   115
270 17
```

### 自动缓存示例

```bash
# (为了简洁起见，将耗时简易显示)

# 准备一些数据
$ seq 1 1000000 > myfile.csv

# 由文件中读取数据
$ time q "select sum(c1),count(*) from myfile.csv"
500000500000 1000000
total_time=4.108 seconds

# 执行`-C readwrite`自动创建缓存文件（若文件不存在，则自动创建一个名为myfile.csv.qsql的文件)
$ time q "select sum(c1),count(*) from myfile.csv" -C readwrite
500000500000 1000000
total_time=4.057 seconds

# 执行`-C read`, 该查询将自动由缓存中进行读取。文件越大时，效果越明显。
$ time q "select sum(c1),count(*) from myfile.csv" -C read
500000500000 1000000
total_time=0.229 seconds

# 我们再来试下另一个查询（注意耗时上的差异). 缓存可用于针对该文件的任何查询，也可用于包含该文件的多文件查询。
$ time q "select avg(c1) from myfile.csv" -C read
500000.5
total_time=0.217 seconds

# 你也可以直接查询qsql文件, 它就是一个标准的sqlite3数据库文件（下一章节中对此处进行了描述）
$ time q "select sum(c1),count(*) from myfile.csv.qsql"
500000500000 1000000
total_time=0.226 seconds

# 现在我们删除源csv文件(删除源文件需谨慎)
$ rm -vf myfile.csv

# 在qsql文件上执行下查询仍然奏效
$ time q "select sum(c1),count(*) from myfile.csv.qsql"
500000500000 1000000
total_time=0.226 seconds

# 如何设置默认`-C(--caching-mode)`覆盖默认值`none` ,可以参照下方`.qrc`小节
```

### sqlite查询示例

```bash
# 由 https://www.sqlitetutorial.net/sqlite-sample-database/下载并解压sqlite3数据库，解压后将看到一chinook.db文件。
$ curl -L https://www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip | tar -xvf -

# 现在我们可以直接由数据库中进行查询（尤其注意表名 <db_name>:::<table_name>）
$ q "select count(*) from chinook.db:::albums"
347

# 我们查询专辑ID为34的前5首最长的曲目。-b 指令为了美化输出，-O 指令表示将列名作为表头
$ q "select * from chinook.db:::tracks where albumid = '34' order by milliseconds desc limit 5" -b -O
TrackId Name                       AlbumId MediaTypeId GenreId Composer Milliseconds Bytes    UnitPrice
407     "Só Tinha De Ser Com Você" 34      1           7       Vários   389642       13085596 0.99
398     "Only A Dream In Rio"      34      1           7       Vários   371356       12192989 0.99
393     "Tarde Em Itapoã"          34      1           7       Vários   313704       10344491 0.99
401     "Momentos Que Marcam"      34      1           7       Vários   280137       9313740  0.99
391     "Garota De Ipanema"        34      1           7       Vários   279536       9141343  0.99

# 我们复制一份chinook数据库文件，将其作为另一个不同的数据库。 
$ cp chinook.db another_db.db

# 现在我们可以在两个不同数据库(使用chinook副本仅仅为了方便演示)之间执行join查询了
# 我们使用第一个数据库中的专辑和第二个数据库中的曲目来获取前5首最长的专辑。跟踪时间转换为秒，并四舍五入到小数点后两位数。
$ q -b -O "select a.title,round(sum(t.milliseconds)/1000.0/60,2) total_album_time_seconds from chinook.db:::albums a left join another_database.db:::tracks t on (a.albumid = t.albumid) group by a.albumid order by total_album_time_seconds desc limit 5"
Title                                      total_album_time_seconds
"Lost, Season 3"                           1177.76
"Battlestar Galactica (Classic), Season 1" 1170.23
"Lost, Season 1"                           1080.92
"Lost, Season 2"                           1054.83
"Heroes, Season 1"                         996.34
```

### 分析示例

```bash
# 我们创建一个没有表头的简单CSV文件，文件内容仅包含3行文本
$ cat > some-data-without-header.csv
harel,1,2
ben,3,4
attia,5,6
<Ctrl-D>

# 我们使用`-d ,`指定分隔符，使用`-A`查看文件结构
$ q -d , "select * from some-data-without-header.csv" -A
Table: /Users/harelben-attia/dev/harelba/q/some-data-without-header.csv
  Sources:
    source_type: file source: /Users/harelben-attia/dev/harelba/q/some-data-without-header.csv
  Fields:
    `c1` - text
    `c2` - int
    `c3` - int

# 现在我们再创建一个包含表头的简单CSV文件
$ cat > some-data.csv
planet_id,name,diameter_km,length_of_day_hours
1000,Earth,12756,24
2000,Mars,6792,24.7
3000,Jupiter,142984,9.9
<Ctrl-D>

# 运行`-A`查看分析结果（`-H`指令表明文件中包含表头）
$ q -b -O -H -d , "select * from some-data.csv" -A
Table: /Users/harelben-attia/dev/harelba/q/some-data.csv
  Sources:
    source_type: file source: /Users/harelben-attia/dev/harelba/q/some-data.csv
  Fields:
    `planet_id` - int
    `name` - text
    `diameter_km` - int
    `length_of_day_hours` - real

# 我们执行`-C readwrite` 创建缓存
$ q -b -O -H -d , "select * from some-data.csv" -C readwrite
planet_id,name   ,diameter_km,length_of_day_hours
1000     ,Earth  ,12756      ,24.0
2000     ,Mars   ,6792       ,24.7
3000     ,Jupiter,142984     ,9.9

# Running another query that uses some-data.csv with -A will now show that a qsql exists for that file. The source-type 
# 在somte-date.csv上执行查询，现在将显示为该源文件生成了一个类型为"file-with-unused-qsql"的qsql文件。
# 为兼容历史版本，q 默认不启用缓存，所以qsql缓存文件没有被使用。
$ q -b -O -H -d , "select * from some-data.csv" -A
Table: /Users/harelben-attia/dev/harelba/q/some-data.csv
  Sources:
    source_type: file-with-unused-qsql source: /Users/harelben-attia/dev/harelba/q/some-data.csv
  Fields:
    `planet_id` - int
    `name` - text
    `diameter_km` - int
    `length_of_day_hours` - real

# 现在我们使用`-C read`指令读取缓存，资源类型将变为"qsql-file-with-original"类型。
# 查询时，缓存将被使用了。
$ q -b -O -H -d , "select * from some-data.csv" -A -C read
Table: /Users/harelben-attia/dev/harelba/q/some-data.csv
  Sources:
    source_type: qsql-file-with-original source: /Users/harelben-attia/dev/harelba/q/some-data.csv.qsql
  Fields:
    `planet_id` - int
    `name` - text
    `diameter_km` - int
    `length_of_day_hours` - real

# 我们直接读取qsql文件（注意查询语句中的表名）. 
# 语句中并没有使用`-C read`指令，资源类型为"qsql-file"
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



## 使用说明

**q**查询是将文件（或由标准输入输出流中读取数据）名看作表名的类SQL查询。

支持包含文件之间join查询(可以使用别名)在内的所有sqlite3 SQL语法结构。在下面的[局限](#局限)小节中可以看到一些少有使用的、欠支持的说明。

**q**将由引号括起来的语句做为其中一个参数。

**q**支持一次同时执行多条查询语句且每个文件也仅会被加载一次。因**q**支持缓存功能（启用缓存时，**q**将自动为每个文件创建缓存，在执行查询操作时，自动使用缓存加速，对于大文件查询效果尤其明显），所以该用法并非最佳实践。

支持如下文件类型：

* **CSV类文件** - 支持绝对或相对路径，如: `./my_folder/my_file.csv` 或 `/var/tmp/my_file.csv`
* **sqlite3 数据库文件**
    * **包含多表** - 指定`:::<table_name>`以此方式来指定访问特定表, 如：`mydatabase.sqlite3:::users_table`
    * **仅有单表** - 仅需要指定数据库名，无需指定表名，如: `my_single_table_database.sqlite`
* **`.qsql`缓存文件** - **q**可以自动缓存CSV类文件，并将其作为表去查询（因为它本质上就是仅包含了一张表的sqlite数据库）

使用 `-H` 表示输入内容中包含表头。若没有指定该参数，列名会被自动识别，列名将会被以`cX`命名，`X`从1开始（比如: `q "SELECT c3,c8 from ..."`) 。


使用 `-d` 声明输入的分隔符。

列类型可由工具自动识别，无需强制转换。 提示，使用`--as-text` 可以强制将所有列类型转换为文本类型。

依据sqlite规范，如果列名中含有空格，需要使用反引号 (即：`) 引起来。

完全支持查询/输入/输出的编码设置（**q**力争提供一种开箱即用的方法), 可以分别使用`-Q`,`-e` 和 `-E`来指定编码设置类型。

在WHERE条件查询中，支持JOIN操作和子查询，但在FROM子句中并不支持。JOIN操作时，可以为文件起别名。

SQL语法同sqlite的语法，详情见 http://www.sqlite.org/lang.html 或上网找一些示例。

提示: 如果重命名输出列，则需要为列指定别名并使用 `-O` 声明。如: `q -O -H "select count(*) cnt,sum(*) as mysum from -"` 便会将`cnt`和`mysum`作为列名输出。



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

```bash
选项：
  -h, --help            显示此帮助信息并退出 
  -v, --version         显示版本号
  -V, --verbose         出现问题时显示调试信息
  -S SAVE_DB_TO_DISK_FILENAME, --save-db-to-disk=SAVE_DB_TO_DISK_FILENAME
                        将数据库保存为一个 sqlite 数据库文件
  -C CACHING_MODE, --caching-mode=CACHING_MODE
                        缓存模式可选值(none/read/readwrite)
                        自动缓存文件至磁盘，以便加速后续的查询。
                        缓存文件扩展名为.qsql,名称与查询表或文件名一致
  --dump-defaults       转存默认配置参数
                        以便于确认.qrc配置中的属性是否生效
  --max-attached-sqlite-databases=MAX_ATTACHED_SQLITE_DATABASES
                        设置sqlite数据库的最大并发数，该值在sqlite编译期间被定义。
                        即将达到该值时，q将执行表格复制操作，因此q的查询性能会降低。
  --overwrite-qsql=OVERWRITE_QSQL
                        指定该参数时，相同命名的qsql文件将被覆盖（无论是缓存文件或是持久化的db文件）

  输入相关的选项:
  
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
                        数据解析模式: fluffy, relaxed和strict。在strict模式下必须指定 -c 
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

### 默认值设定

在`~/.qrc`文件中定义默认值，文件格式如下:

```bash
[options]
<setting>=<default-value>
```
可以通过指定`--dump-defaults`参数生成`.qrc`文件

实用示例: 设置 caching-mode 为 `read`，该指定下，若存在`.qsql`缓存文件，q便会自动使用缓存。若缓存不存在时，可以指定`-C readwrite` 来生成`.qsql`缓存文件。如下为在`.qrc`文件中指定默认缓存模式。

```bash
[options]
caching_mode=read
```

## 新手入门

本小节提供了一些基础示例，更多进阶示例可以参照[示例](#示例)小节。

提示: 


* 下述 `-H` 参数的例子，表示文件中含有表头时使用该参数。
* `-t` 参数是指定文件以 tab 作为分隔符的缩写（可以使用 `-d` 参数指定任意分隔符）。
* 为了清楚起见，查询关键字均使用大写，实际上关键字(如 SELECT、WHERE等)对大小写并不敏感。

基础示例目录:

* [例1 - 统计指定列唯一值的数量](#1)
* [例2 - 数值条件过滤、排序并限制输出数](#2)
* [例3 - GROUP BY简单示例](#3)
* [例4 - GROUP BY进阶示例 (以时间格式分组)](#4)
* [例5 - 标准输入流作为输入](#5)
* [例6 - 使用表头中列名](#6)
* [例7 - JOIN 两个文件](#7)

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

q在后台创建了一个"虚拟"sqlite3数据库，该库中不包含任何数据，数据依赖于如下几个数据库：

* 当从类CSV文件或`stdin`中读取数据时，它将分析数据并在内存中构建"临时数据库", 此时临时数据库附加在虚拟数据库上。
* 当类CSV文件使用`.qsql`缓存时，此时直接将该文件附加在虚拟数据库上，无需将其读入内存处理。
* 当查询一个sqlite3文件时，此时也会直接将该文件附加在虚拟数据库上，也无需将其读入内存处理。sqlite3文件可以被自动识别，无需特别声明。

依赖于如上数据库，用户的查询可直接在虚拟数据库上执行。

sqlite3 对附加的数据库数量有限制（通常为10个），如果接近上限，q会在到达上限后，自动将库附加到临时数据库内存中。

请仔细阅读[局限](#局限)小节。

## 开发

### 测试
源码中包含了测试用例，可以通过 `test/test-all` 来执行。默认情况下使用python源码来执行测试，但可以使用`Q_EXECUTABLE`环境变量指定实际可执行文件的路径, 以便于在构建和打包过程中对二进制文件进行检测。

## 局限
如下罗列了一些已知的限制，若你的使用场景中需要用到以下标明的限制，请与我联系。

* 不支持公用表操作（CTE），即将支持 - 查看[这儿](https://github.com/harelba/q/issues/67)和[这儿](https://github.com/harelba/q/issues/124)获取更多细节
* 不支持 `FROM <subquery>` 
* 不支持文件名中包含空格 (可以将文件以标准输入流的方式输入 q 或重命名文件)
* 不支持较少用到的子查询
* 超过 10 个不同 sqlite3 数据库的查询时，会将一些数据加载到内存中
* 单个查询最多支持500个表

## 缘起
我曾有很多次，盯着屏幕上的文本文件发呆，希望它要是能像数据库查询一样方便就好了？最终我顿悟了: 我想要的不是数据库，而是 SQL。

SQL 是一种面向数据声明的语言，它允许自定义数据内容而无需关心其执行过程。这也正是SQL强大之处，因为它对于数据'所见即所得'，而不是将数据看作字节码。

本工具的目的是：在文本文件和SQL之间搭建一座桥梁。

### 为什么其他Linux工具不能满足需求？
传统的Linux工具库也很酷，我也经常使用它们， 但Linux的整体理念是为任一部分搭配最好的工具。本工具为传统Linux工具集新添了 SQL 族类工具，其他工具并不会失去本来优势。
事实上，我也经常将 q 和其他Linux工具搭配使用，就如同使用管道将 awk/sed 和 grep 搭配使用一样。

另外需要注意的是,许多Linux工具就将文本看作文本，而不是数据。从这个意义上来讲，可以将 q 看作提供了 SQL 功能（如：表达式、排序、分组、聚合等）的元工具。

### 理念

本工具的设计遵从了 Linux/Unix 的传统设计原则。若你对这些设计原则感兴趣，可以阅读 [这本书](http://catb.org/~esr/writings/taoup/) ，尤其是书中 [这部分](http://catb.org/~esr/writings/taoup/html/ch01s06.html)
若你认为本工具工作方式与之背道而驰，愿洗耳恭听您的意见。

## 展望

* 计划在新版本 3.x 发布后，将其能成为python的公开模块作为目标。



