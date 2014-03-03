# q - Treating Text as a Database

## Overview
Have you ever stared at a text file on the screen, hoping it would have been a database so you could ask anything you want about it? I had that feeling many times, and I've finally understood that it's not the _database_ that I want. It's the language - SQL.

SQL is a declarative language for data, and as such it allows me to define what I want without caring about how exactly it's done. This is the reason SQL is so powerful, because it treats data as data and not as bits and bytes (and chars).

The goal of this tool is to provide a bridge between the world of text files and of SQL.


```bash
"q allows performing SQL-like statements on tabular text data, including joins and subqueries"
```

__New version `1.3.0` is out. Contains very significant improvements:__
* Added column name and type detection (Use -A to see name/type analysis for the specified input)
* Added support for multiple parsing modes - Relaxed, Strict and Fluffy (old, backward compatible behavior)
* Fixed tab delimition parameter problem
* More improvements to error reporting
* Added a test suite, in preparation for refactoring
* Solves the following bugs/pull-requests:
  - #7  - Dynamic column count support
  - #8  - Column name inference from input containing a header row
  - #9  - Automatic column type inference using sample data
  - #30 - Header lines option does nothing
  - #33 - Last column should allow for spaces? 
  - #35 - Add q.bat
  - #38 - Problem with whitespace delimiter
  - #43 - using the -t flag stopped the header flag from working
  - #44 - Space in column name on TAB separated values break q
* Breaking changes:
  - Changed -H behavior so it's now a flag an not a parameter (1 line always)
  - Removed support for multi-char delimiters


-----
## You can use [this gitter chatroom](https://gitter.im/harelba/q) for any feedback. Please leave the feedback there even if i'm not there at that moment, I promise to read it and respond as soon as I can
-----

## Quick examples for the impatient

__Command 1:__
```bash
sudo find /tmp -ls | q "select c5,c6,sum(c7)/1024.0/1024 as total from - group by c5,c6 order by total desc"
```

__Output 1 (total size per user/group in the /tmp subtree):__
```bash
mapred hadoop   304.00390625
root   root     8.0431451797485
smith  smith    4.34389972687
```

__Command 2:__
The following command _joins_ an ls output (`exampledatafile`) and a file containing rows of **group-name,email**  (`group-emails-example`) and provides a row of **filename,email** for each of the emails of the group. For brevity of output, there is also a filter for a specific filename called `ppp` which is achieved using a WHERE clause.
```bash
q "select myfiles.c8,emails.c2 from exampledatafile myfiles join group-emails-example emails on (myfiles.c4 = emails.c1) where myfiles.c8 = 'ppp'"
```

__Output 2: (rows of filename,email):__
```bash
ppp dip.1@otherdomain.com
ppp dip.2@otherdomain.com
```

You can see that the ppp filename appears twice, each time matched to one of the emails of the group `dip` to which it belongs. Take a look at the files [`exampledatafile`](exampledatafile) and [`group-emails-example`](group-emails-example) for the data.
        
## Highlights

* Seamless multi-table SQL support, including joins. filenames are just used instead of table names (use - for stdin)
* Automatic column name and column type detection (Allows working more naturally with the data)
* Multiple parsing modes - relaxed and strict. Relaxed mode allows to easily parse semi-structured data, such as log files.
* Standard installation - RPM, Homebrew (Mac). Debian package coming soon.
* Support for quoted fields 
* Full UTF-8 support (and other encodings)
* Handling of gzipped files
* Output delimiter matching and selection
* Output beautifier
* man page when installed through the RPM package

## Requirements
* Just Python 2.5 and up or Python 2.4 with sqlite3 module installed.

## Installation
* Mac users can use homebrew to install q - Just run `brew install q` (Thanks @stuartcarnie)
* RPM Packaging is ready. You can download the RPM from the link below.
* Debian Packaging will come soon.

    __**Latest version `1.3.0`**__
    
    **RPM of the latest version** - Can be downloaded **[here](https://github.com/harelba/packages-for-q/raw/master/rpms/q-1.3.0-1.noarch.rpm)**. The RPM package also includes a man page. Just enter `man q`

    **Manual installation of the latest version** - Download the main q executable from **[here](https://raw.github.com/harelba/q/1.3.0/q)** into a folder in the path and make the file executable


    __**Previous version is `1.2.0`**__
    
    **RPM of the previous version** - Can be downloaded **[here](https://github.com/harelba/packages-for-q/raw/master/rpms/q-1.2.0-1.noarch.rpm)**. Includes man page. Just enter `man q`

    **Manual installation of the previous version** - If for some reason you need the previous version, you can download the main q executable from **[here](https://raw.github.com/harelba/q/1.2.0/q)** into a folder in the path and make the file executable. Please notify me of any such case, so I can understand the reason and fix things if needed.


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
Basic usage format is `q <flags> <query>`. Simplest execution is `q "SELECT * FROM myfile"` which prints the entire file.

### Query
q gets one parameter - An SQL-like query. The following applies:

* The table name is the actual file name that you want to read from. Path names are allowed. Use "-" if you want to read from stdin (e.g. `q "SELECT * FROM -"`)
  * Actually multiple files can be provided by using one of both of the following ways:
     * By separating the filenames with a + sign: `SELECT * FROM datafile1+datafile2+datefile3`.
     * By using glob matching: `SELECT * FROM mydata*.dat`
  * Files with .gz extension are considered to be gzipped and decompressed on the fly.
* Use `-H` in order to specify that a header row exists. q will read the header row and set the column names accordingly. 
* If there is no header row, then the column names will be in the format cX where X is the column number starting from **1**. For example, to retrieve the second and fourth columns of the file, use `q "SELECT c2,c4 FROM myfile"`
* Any standard SQL expression, condition (both WHERE and HAVING), GROUP BY, ORDER BY etc. are allowed. NOTE: Full type detection is implemented, so there is no need for any casting or anything.
* For both consistency and for preventing shell expansion conflicts, q currently expects the entire query to be in a single command-line parameter. Here is an example standard usage: ```q "SELECT * FROM datafile"```. Notice that the entire SQL statement is enclosed in double quotes. Flags are obviously outside the quotes.

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

* `-A` - Analyze sample input and provide an analysis of column names and their detected types. Does not run the query itself
* `-m` - Data parsing mode. fluffy, relaxed or strict. In relaxed mode the -c column-count is optional. In strict mode, it must be provided. See separate section in the documentation about the various modes. Fluffy mode should only be used if backward compatibility (less well defined, but helpful...) to older versions of q is needed.
* `-c` - Specific column count. This parameter fixes the column count. In relaxed mode, this will cause missing columns to be null, and extra columns to be "merged" into the last column. In strict mode, any deviation from this column count will cause an error.
* `-k` - Keep leading whitespace. By default leading whitespace is removed from values in order to provide out-of-the-box usability. Using this flag instructs q to leave any leading whitespace in tact, making the output more strictly identical to the input.

### Output formatting option
The format of F is as a list of X=f separated by commas, where X is a column number and f is a python format:
* X - column number - This is the SELECTed column (or expression) number, not the one from the original table. E.g, 1 is the first SELECTed column, 3 is the third SELECTed column.
* f - A python formatting string - See http://docs.python.org/release/2.4.4/lib/typesseq-strings.html for details if needed.
  * Example: `-f 3=%-10s,5=%4.3f,1=%x`

### Parsing Modes
q supports multiple parsing modes:
* `relaxed` - This is the default mode. It tries to lean towards simplicity of use. When a row doesn't contains enough columns, they'll be filled with nulls, and when there are too many, the extra values will be merged to the last column. Defining the number of expected columns in this mode is done using the `-c` parameter.
* `strict` - Strict mode is for hardcode csv parsing. Whenever a row doesn't contain the proper number of columns, processing will stop. 
* `fluffy` - This mode should not be used, and is just some kind of "backward compatible" parsing mode which was used by q previously. It's left as a separate parsing mode on purpose, in order to accomodate existing users. If you are such a user, please open a bug for your use case, and I'll see how I can incorporate it into the other modes. It is reasonable to say that this mode will be removed in the future.

## Implementation
The current implementation is written in Python using an in-memory database, in order to prevent the need for external dependencies. The implementation itself supports SELECT statements, including JOINs (Subqueries are supported only in the WHERE clause for now). 

Please note that there is currently no checks and bounds on data size - It's up to the user to make sure things don't get too big.

Please make sure to read the limitations section as well.

Code wise, I'm planning for a big refactoring, and I have added full test suite in the latest version, so it'll be easier to do properly.

### Limitations
* No checks and bounds on data size

## Future Ideas
* Faster reuse of previous data loading
* Allow working with external DB
* Real parsing of the SQL, allowing smarter execution of queries.
* Smarter batch insertion to the database
* Full Subquery support (will be possible once real SQL parsing is performed)
* Provide mechanisms beyond SELECT - INSERT and CREATE TABLE SELECT and such.

## Change Log
**Thu Mar 03 2014 Harel Ben-Attia <harelba@gmail.com> 1.3.0-1**
- Added column name and type detection (Use -A to see name/type analysis for the specified input)
- Added support for multiple parsing modes - Relaxed, Strict and Fluffy (old, backward compatible behavior)
- Fixed tab delimition parameter problem
- More improvements to error reporting
- Added a test suite, in preparation for refactoring
- Solves the following bugs/pull-requests:
  - #7  - Dynamic column count support
  - #8  - Column name inference from input containing a header row
  - #9  - Automatic column type inference using sample data
  - #30 - Header lines option does nothing
  - #33 - Last column should allow for spaces?
  - #35 - Add q.bat
  - #38 - Problem with whitespace delimiter
  - #43 - using the -t flag stopped the header flag from working
  - #44 - Space in column name on TAB separated values break q
- Breaking changes:
  - Changed -H behavior so it's now a flag an not a parameter (1 line always)
  - Removed support for multi-char delimiters

**Thu Feb 20 2014 Harel Ben-Attia <harelba@gmail.com> 1.1.7-1**
- Better error reporting
- Fixed python invocation for non stanard locations
- Added man page

## Why aren't other Linux tools enough?
The standard Linux tools are amazing and I use them all the time, but the whole idea of Linux is mixing-and-matching the best tools for each part of job. This tool adds the declarative power of SQL to the Linux toolset, without loosing any of the other tools' benefits. In fact, I often use q together with other Linux tools, the same way I pipe awk/sed and grep together all the time.

One additional thing to note is that many Linux tools treat text as text and not as data. In that sense, you can look at q as a meta-tool which provides access to all the data-related tools that SQL provides (e.g. expressions, ordering, grouping, aggregation etc.).

## Philosophy
This tool has been designed with general Linux/Unix design principles in mind. If you're interested in these general design principles, read the amazing book http://catb.org/~esr/writings/taoup/ and specifically http://catb.org/~esr/writings/taoup/html/ch01s06.html. If you believe that the way this tool works goes strongly against any of the principles, I would love to hear your view about it.

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

