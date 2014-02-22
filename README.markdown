# q - Treating Text as a Database

## Overview
Have you ever stared at a text file on the screen, hoping it would have been a database so you could ask anything you want about it? I had that feeling many times, and I've finally understood that it's not the _database_ that I want. It's the language - SQL.

SQL is a declarative language for data, and as such it allows me to define what I want without caring about how exactly it's done. This is the reason SQL is so powerful, because it treats data as data and not as bits and bytes (and chars).

The goal of this tool is to provide a bridge between the world of text files and of SQL.

```bash
"q allows performing SQL-like statements on tabular text data, including joins and subqueries"
```

## Quick example for the impatient

Command:
```bash
sudo find /tmp -ls | q "select c5,c6,sum(c7)/1024.0/1024 as total from - group by c5,c6 order by total desc"
```

Output (total size per user/group in the /tmp subtree):
```bash
mapred hadoop   304.00390625
root   root     8.0431451797485
smith  smith    4.34389972687
```
        
## Highlights

* Seamless multi-table SQL support, including joins. filenames are just used instead of table names (or - for stdin)
* Has homebrew installation support. RPM package is in beta. Debian package will follow.
* Full UTF-8 support (and other encodings)
* Handling of gzipped files
* Output delimiter matching and selection
* Output beautifier

## Requirements
* Just Python 2.5 and up or Python 2.4 with sqlite3 module installed.

## Installation
* Mac users can use homebrew to install q - Just run `brew install q` (Thanks @stuartcarnie)
* RPM Packaging is beta ready. You can download the beta RPM from the link below.
* Debian Packaging will come soon.

    __**Current version is `1.2.0`**__
    
    **Manual installation - Download the main q executable from [here](https://raw.github.com/harelba/q/1.2.0/q) into a folder in the path and make the file executable**
    
    **RPM - RPM package is ready and can be downloaded [here](https://github.com/harelba/packages-for-q/raw/master/rpms/q-1.2.0-1.noarch.rpm). I'm hardly an RPM expert, so any feedback on the RPM packaging would be greatly appreciated. The RPM package also includes a man page. Just enter `man q`**
 

    __**Previous version is `1.1.7`**__
    
    **Manual installation of previous version - [here](https://raw.github.com/harelba/q/1.1.7/q) - Just put the file in the path and make it executable
    
    **RPM - RPM package of previous version is [here](https://github.com/harelba/packages-for-q/raw/master/rpms/q-1.1.7-1.noarch.rpm). 
  
    

**NOTE:** If you're using Python 2.4, then you will have to install the sqlite3 package for q to work.

q supports an option file in ~/.qrc or in the working directory (with the name .qrc) which provides defaults for some or all of the command line options. A sample .qrc file with commented-out options is included. Just put it in your home folder and modify it according to your needs.

## Examples
Let's postpone the official usage (See below). Look at the examples, and you'll get the general idea.

1.  We'll start with a simple example and work from there. The file `exampledatafile` contains the output of an `ls -l` command, a list of files in some directory. In this example we'll do some calculations on this file list.
  * The following commands will count the lines in the file *exampledatafile*, effectively getting the number of files in the directory. The output will be exactly as if we ran the `wc -l` command.  

            q "SELECT COUNT(1) FROM exampledatafile"    

            cat exampledatafile | q "SELECT COUNT(1) FROM -"   
        
  * Now, let's assume we want to know the number of files per date in the directory. Notice that the date is in column 6.

            q "SELECT c6,COUNT(1) FROM exampledatafile GROUP BY c6"   

  * The results will show the number of files per date. However, there's a lot of "noise" - dates in which there is only one file. Let's leave only the ones which have 3 files or more:  

            q "SELECT c6,COUNT(1) AS cnt FROM exampledatafile GROUP BY c6 HAVING cnt >= 3"   

  * Now, let's see if we can get something more interesting. The following command will provide the **total size** of the files for each date. Notice that the file size is in c5.  

            q "SELECT c6,SUM(c5) AS size FROM exampledatafile GROUP BY c6"   

  * We can see the results. However, the sums are in bytes. Let's show the same results but in KB:  

            q "SELECT c6,SUM(c5)/1024.0 AS size FROM exampledatafile GROUP BY c6"  

  * The last command provided us with a list of results, but there is no order and the list is too long. Let's get the Top 5 dates:  

            q "SELECT c6,SUM(c5)/1024.0 AS size FROM exampledatafile GROUP BY c6 ORDER BY size DESC LIMIT 5"   

  * Now we'll see how we can format the output itself, so it looks better:  

            q -f "2=%4.2f" "SELECT c6,SUM(c5)/1024.0 AS size FROM exampledatafile GROUP BY c6 ORDER BY size DESC LIMIT 5"  
        
  * (An example of using JOIN will be added here - In the mean time just remember you have to use table alias for JOINed "tables")
        
2. A more complicated example, showing time manipulation. Let's assume that we have a file with a timestamp as its first column. We'll show how it's possible to get the number of rows per full minute:  

        q "SELECT DATETIME(ROUND(c1/60000)*60000/1000,'unixepoch','-05:00') as min, COUNT(1) FROM datafile*.gz GROUP BY min"  
        
   There are several things to notice here:
   
   * The timestamp value is in the first column, hence c1.
   * The timestamp is assumed to be a unix epoch timestamp, but in ms, and DATETIME accepts seconds, so we need to divide by 1000
   * The full-minute rounding is done by dividing by 60000 (ms), rounding and then multiplying by the same amount. Rounding to an hour, for example, would be the same except for having 3600000 instead of 60000.
   * We use DATETIME's capability in order to output the time in localtime format. In that case, it's converted to New York time (hence the -5 hours)
   * The filename is actually all files matching "datafile*.gz" - Multiple files can be read, and since they have a .gz extension, they are decompressed on the fly.
   * **NOTE:** For non-SQL people, the date manipulation may seem odd at first, but this is standard SQL processing for timestamps and it's easy to get used to.

## Usage
Basic usage format is `q <flags> <query>`. Simplest execution is `q "SELECT * FROM myfile"` which will actually prints the entire file.

### Query
q gets one parameter - An SQL-like query. The following applies:

* The table name is the actual file name that you want to read from. Path names are allowed. Use "-" if you want to read from stdin (e.g. `q "SELECT * FROM -"`)
  * Actually multiple files can be provided by using one of both of the following ways:
     * By separating the filenames with a + sign: `SELECT * FROM datafile1+datafile2+datefile3`.
     * By using glob matching: `SELECT * FROM mydata*.dat`
  * Files with .gz extension are considered to be gzipped and decompressed on the fly.
* The column names are in the format cX where X is the column number starting from **1**. For example, to retrieve the second and fourth columns of the file, use `q "SELECT c2,c4 FROM myfile"`
* Any standard SQL expression, condition (both WHERE and HAVING), GROUP BY, ORDER BY etc. are allowed.
  * **NOTE:** Type inference is rudimentary for now (see Limitations and Future below), so sometimes casting would be required (e.g. for inequality conditions on numbers). Once type inference is complete, this won't be necessary. See Limitations for details on working around this.
* For both consistency and for preventing shell expansion conflicts, q currently expects the entire query to be in a single command-line parameter. Here is an example standard usage: ```q "SELECT * FROM datafile"```. Notice that the entire SQL statement is enclosed in double quotes.

JOINs are supported and Subqueries are supported in the WHERE clause, but unfortunately not in the FROM clause for now. Use table alias when performing JOINs.

The SQL syntax itself is sqlite's syntax. For details look at http://www.sqlite.org/lang.html or search the net for examples.


### Runtime options and flags
q can also get some runtime flags (Linux style, before the parameter). The following parameters can be used, all optional:

* `-z` - Means that the file is gzipped. This is detected automatically if the file extension if .gz, but can be useful when reading gzipped data from stdin (since there is no content based detection for gzip).
* `-H <N>` - Tells q to skip N header lines in the beginning of the file - Used naturally for skipping a header line. This can possibly be detected automatically in the future.
* `-d` - Column/field delimiter. If it exists, then splitting lines will be done using this delimiter. If it doesn't, space will be used as the delimiter. If you need multi-character delimiters, run the tool with engine version 1 by adding `-E v1`. Using v1 will also revert to the old behavior where if no delimiter is provided, then any whitespace will be considered as a delimiter.
* `-D` - Column/field delimiter for output. If it exists, then the output will use this delimiter instead of the one used in input. Defaults to input delimiter if provided by `-d`, or space if not.
* `-b` - Beautify the output. If this flag exists, output will be aligned to the largest actual value of each column. **NOTE:** Use this only if needed, since it is slower and more CPU intensive.
* `-t` - Shorthand flag for a tab delimiter, one header line format (Same as `-d $'\t' -H 1` - The $ notation is required so Linux would escape the tab...)
* `-f <F>` - Output-formatting option. If you don't like the output formatting of a specific column, you can use python formatting in order to change the output format for that column. See below for details
* `-e <E>` - Specify the text encoding. Defaults to UTF-8. If you have ASCII only text and want a 33% speedup, use `-e none`. Unfortunately, proper encoding/decoding has its price.
* `-E <engine-version>` - Default engine version is v2. Should not be changed unless you have problems or need multi-character delimiters which have been supported in v1 but are not supported in v2 anymore. v2 supports supports quoted CSVs. Please notify me of any problem that forces you to use v1.


### Output formatting option
The format of F is as a list of X=f separated by commas, where X is a column number and f is a python format:
* X - column number - This is the SELECTed column (or expression) number, not the one from the original table. E.g, 1 is the first SELECTed column, 3 is the third SELECTed column.
* f - A python formatting string - See http://docs.python.org/release/2.4.4/lib/typesseq-strings.html for details if needed.
  * Example: `-f 3=%-10s,5=%4.3f,1=%x`

## Implementation
The current implementation is written in Python using an in-memory database, in order to prevent the need for external dependencies. The implementation itself is pretty basic and supports only SELECT statements, including JOINs (Subqueries are supported only in the WHERE clause for now). In addition, error handling is really basic. However, I do believe that it can be of service even at that state.

Please note that there is currently no checks and bounds on data size - It's up to the user to make sure things don't get too big.

Please make sure to read the limitations section as well.

### Limitations
The following limitations exist in the current implementation:  

* Simplistic Data typing and column inference - All types are strings and columns are determined according to the first line of data, having the names of c1,c2,c3 etc. There's a column count hack, which is meant for tolerating a small variation in the column count.
  * In some cases, SQL uses its own type inference (such as treating cX as a number in case there is a SUM(cX) expression), But in other cases it won't. One such example is providing a WHERE clause with inequality - such as c5 > 1000. This will not work properly out-of-the-box until we provide type inference. There is a simple (however not clean) way to get around it - Casting the value where needed. Example:  

        ```q "SELECT c5,c9 FROM mydatafile WHERE 0+c5 > 1000"```  

  * This is simple enough, but it kind of breaks the idea of treating data as data. This is the reason why the examples below avoided using a meaningful WHERE clause. Once this is fixed, the examples will be updated.
* Basic error handling only
* No checks and bounds on data size

## Future Ideas

* Column name inference for files containing a header line
* Column type inference according to actual data
* Smarter batch insertion to the database
* Faster reuse of previous data loading
* Allow working with external DB
* Real parsing of the SQL, allowing smarter execution of queries.
* Full Subquery support (will be possible once real SQL parsing is performed)
* Provide mechanisms beyond SELECT - INSERT and CREATE TABLE SELECT and such.
* Support semi structured data - e.g. log files, where there are several columns and then free text
* Better error handling

## Why aren't other Linux tools enough?
The standard Linux tools are amazing and I use them all the time, but the whole idea of Linux is mixing-and-matching the best tools for each part of job. This tool adds the declarative power of SQL to the Linux toolset, without loosing any of the other tools' benefits. In fact, I often use q together with other Linux tools, the same way I pipe awk/sed and grep together all the time.

One additional thing to note is that many Linux tools treat text as text and not as data. In that sense, you can look at q as a meta-tool which provides access to all the data-related tools that SQL provides (e.g. expressions, ordering, grouping, aggregation etc.).

## Philosophy
This tool has been designed with general Linux/Unix design principles in mind. If you're interested in these general design principles, read the amazing book http://catb.org/~esr/writings/taoup/ and specifically http://catb.org/~esr/writings/taoup/html/ch01s06.html. If you believe that the way this tool works goes strongly against any of the principles, I would love to hear your view about it.

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

