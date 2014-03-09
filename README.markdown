# q - Treating Text as a Database 

## Overview
Have you ever stared at a text file on the screen, hoping it would have been a database so you could ask anything you want about it? I had that feeling many times, and I've finally understood that it's not the _database_ that I want. It's the language - SQL.

SQL is a declarative language for data, and as such it allows me to define what I want without caring about how exactly it's done. This is the reason SQL is so powerful, because it treats data as data and not as bits and bytes (and chars).

The goal of this tool is to provide a bridge between the world of text files and of SQL.


```bash
"q allows performing SQL-like statements on tabular text data, including joins and subqueries"
```

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

## Installation
Current stable version is `1.3.0`. RPM + Manual installation exist.

Installation instructions can be found [here](INSTALL.markdown)

## Examples and Tutorial
Some more examples can be found [here](EXAMPLES.markdown)

## Usage
q's usage is very simple. More information can be found [here](USAGE.markdown)

## Implementation
Some implementation details can be found [here](IMPLEMENTATION.markdown)

### Limitations
* No checks and bounds on data size

## Future Ideas
* Faster reuse of previous data loading
* Allow working with external DB
* Real parsing of the SQL, allowing smarter execution of queries.
* Smarter batch insertion to the database
* Full Subquery support (will be possible once real SQL parsing is performed)
* Provide mechanisms beyond SELECT - INSERT and CREATE TABLE SELECT and such.

## Rationale
Some information regarding the rationale for this tool and related philosophy can be found [here](RATIONALE.markdown)

## Change log
History of changes can be found [here](CHANGELOG.markdown)

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

