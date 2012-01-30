

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
q gets one parameter - An SQL-like query. The following applies:
* The table name is the actual file name that you want to read from. Path names are allowed. Use "-" if you want to read from stdin (e.g. "SELECT * FROM -")
* The column names are in the format cX where X is the column number starting from **1**. For example, to retrieve the second and fourth columns of the file, use "SELECT c2,c4 FROM myfile"
* Any standard SQL expression, condition (both WHERE and HAVING), GROUP BY, ORDER BY etc. are allowed.
  * **NOTE:** Type inference is rudimentary for now (see Limitations and Future below), so sometimes casting would be required (e.g. for inequality conditions on numbers). Once type inference is complete, this won't be necessary. See Limitations for details on working around this.
* For both consistency and for preventing shell expansion conflicts, q currently expects the entire query to be in a single command-line parameter. Here is an example standard usage: ```q "SELECT * FROM datafile"```. Notice that the entire SQL statement is enclosed in double quotes.

q can also get some runtime flags (Linux style, before the parameter). The following parameters can be used, all optional:
* **-z** - Means that the file is gzipped. This is detected automatically if the file extension if .gz, but can be useful when reading gzipped data from stdin (since there is no content based detection for gzip).
* **-H <N>** - Tells q to skip N header lines in the beginning of the file - Used naturally for skipping a header line. This can possibly be detected automatically in the future.
* **-d** - Column/field delimiter. If it exists, then splitting lines will be done using this delimiter. If not provided, **any whitespace** will be used as a delimiter.
* **-b** - Beautify the output. If this flag exists, output will be aligned to the largest actual value of each column. **NOTE:** Use this only if needed, since it is slower and more CPU intensive.

## Examples
* This example demonstrates how we can use this script to do some calculations on tabular data. We'll use the output of ls as the input data (we're using --full-time so the output format will be the same on all machines).
  * Execute the following command. It will create our test data:

    ls -ltrd --full-time * > mydatafile

  * Execute the following command. It will calculate the average file size on the file list we created:

    q "SELECT AVG(c5) FROM mydatafile"

  * Now, let's assume we want the same information, but per user:

    q "SELECT c3,AVG(c5) FROM mydatafile GROUP BY c3"

  * You'll see the the output consists of lines each having "username avg". However, the avg is in bytes. Let make it in MB:

    q "SELECT c3,AVG(c5)/1024/1024 FROM mydatafile GROUP BY c3"

  * And now, if we have lots of users, then it might not be easy to see the big offenders, so let's sort it in descending order:

## Command Line Options

## Implementation

## Limitations

## Future Ideas

## Philosophy
This tool has been designed with general Linux/Unix design principles in mind. If you're interested in these general design principles, read the amazing book http://catb.org/~esr/writings/taoup/ and specifically http://catb.org/~esr/writings/taoup/html/ch01s06.html. If you believe that the way this tool works goes strongly against any of the principles, I would love to hear your view about it.

