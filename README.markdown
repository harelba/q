# q - SQL power for the Command Line

## Overview
Have you ever stared at a text file on the screen, hoping it would have been a database so you could ask anything you want about it? I had that feeling many times, and I've finally understood that it's not the _database_ that I want. It's the language - SQL.

SQL is a declarative language for data, and as such it allows me to define what I want without caring about how exactly it's done. This is the reason SQL is so powerful, because it treats data as data and not as bits and bytes (and chars).

The goal of this tool is to provide a bridge between the world of text files and of SQL.

**q allows performing SQL-like statements on tabular text data.**

### Why aren't other Linux tools enough?
The standard Linux tools are amazing and I use them all the time, but the whole idea of Linux is mixing-and-matching the best tools for each part of job. This tool adds the declarative power of SQL to the Linux toolset, without loosing any of the other tools' benefits. In fact, I often use q together with other Linux tools, the same way I pipe awk/sed and grep together all the time.

One additional thing to note is that many Linux tools treat text as text and not as data. In that sense, you can look at q as a meta-tool which provides access to all the data-related tools that SQL provides (e.g. expressions, ordering, grouping, aggregation etc.).

## Requirements
* Python with the sqlite3 module installed. The module is a part of standard Python 2.5 and up.

## Installation
* No installation is required - Just put q in the PATH.

**NOTE:** If you're using Python 2.4, then you will have to install the sqlite3 package for q to work.

## Usage

### Query
q gets one parameter - An SQL-like query. The following applies:

* The table name is the actual file name that you want to read from. Path names are allowed. Use "-" if you want to read from stdin (e.g. `q "SELECT * FROM -"`)
* The column names are in the format cX where X is the column number starting from **1**. For example, to retrieve the second and fourth columns of the file, use `q "SELECT c2,c4 FROM myfile"`
* Any standard SQL expression, condition (both WHERE and HAVING), GROUP BY, ORDER BY etc. are allowed.
  * **NOTE:** Type inference is rudimentary for now (see Limitations and Future below), so sometimes casting would be required (e.g. for inequality conditions on numbers). Once type inference is complete, this won't be necessary. See Limitations for details on working around this.
* For both consistency and for preventing shell expansion conflicts, q currently expects the entire query to be in a single command-line parameter. Here is an example standard usage: ```q "SELECT * FROM datafile"```. Notice that the entire SQL statement is enclosed in double quotes.

### Runtime options and flags
q can also get some runtime flags (Linux style, before the parameter). The following parameters can be used, all optional:

* `-z` - Means that the file is gzipped. This is detected automatically if the file extension if .gz, but can be useful when reading gzipped data from stdin (since there is no content based detection for gzip).
* `-H <N>` - Tells q to skip N header lines in the beginning of the file - Used naturally for skipping a header line. This can possibly be detected automatically in the future.
* `-d` - Column/field delimiter. If it exists, then splitting lines will be done using this delimiter. If not provided, **any whitespace** will be used as a delimiter.
* `-b` - Beautify the output. If this flag exists, output will be aligned to the largest actual value of each column. **NOTE:** Use this only if needed, since it is slower and more CPU intensive.

## Examples

1. Let's start with a simple example and work from there. 
  * The following command will count the lines in the file *exampledatafile*. The output will be exactly as if we ran the `wc -l` command.  

        ```q "SELECT COUNT(1) FROM exampledatafile"```  

  * Now, let's assume we want to know the number of files per date. Notice that the date is in column 6.

        ```q "SELECT c6,COUNT(1) FROM exampledatafile GROUP BY c6"```  

  * The results will show the number of files per date. However, there's a lot of "noise" - dates in which there is only one file. Let's remove the ones which have than 3 files:

        ```q "SELECT c6,COUNT(1) AS cnt FROM exampledatafile GROUP BY c6 HAVING cnt >= 3"```  

  * Now, let's see if we can get something more interesting. The following command will provide the total size of the files for each date. Notice that the file size is in c5.  

        ```q "SELECT c6,SUM(c5) AS size FROM exampledatafile GROUP BY c6"```  

  * We can see the results. However, the sums are in bytes. Let's show the same results but in KB:  

        ```q "SELECT c6,SUM(c5)/1024.0 AS size FROM exampledatafile GROUP BY c6"```  

  * The last command provided us with a list of results, but there is no order and the list is too long. Let's get the Top 5 dates:  

        ```q "SELECT c6,SUM(c5)/1024.0 AS size FROM exampledatafile GROUP BY c6 ORDER BY size DESC LIMIT 5"```  

## Command Line Options

## Implementation

## Limitations

## Future Ideas

## Philosophy
This tool has been designed with general Linux/Unix design principles in mind. If you're interested in these general design principles, read the amazing book http://catb.org/~esr/writings/taoup/ and specifically http://catb.org/~esr/writings/taoup/html/ch01s06.html. If you believe that the way this tool works goes strongly against any of the principles, I would love to hear your view about it.

