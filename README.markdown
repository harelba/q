# q - Text as a Database 
q allows direct SQL-like queries on CSVs/TSVs (and any other tabular text files), including joins and any other SQL construct, and supports automatic detection of column types and names.

```bash
"q allows performing SQL-like statements on tabular text data, including joins and subqueries"
```

## Examples
__Usage Example 1 (total size per user/group in the /tmp subtree):__

    sudo find /tmp -ls | q "select c5,c6,sum(c7)/1024.0/1024 as total from - group by c5,c6 order by total desc"

__Output 1:__
```bash
mapred hadoop   304.00390625
root   root     8.0431451797485
smith  smith    4.34389972687
```

__Usage Example 2 with autodetected column names (top 3 user ids with the largest number of owned processes):__

    ps -ef | q -H "select UID,count(*) cnt from - group by UID order by cnt desc limit 3"
    
__Output 2:__
```bash
colord 1
daemon 1
harel 118
lp 1
root 152
```

A beginner's tutorial can be found [here](EXAMPLES.markdown)

## Overview
Have you ever stared at a text file on the screen, hoping it would have been a database so you could ask anything you want about it? I had that feeling many times, and I've finally understood that it's not the _database_ that I want. It's the language - SQL.

SQL is a declarative language for data, and as such it allows me to define what I want without caring about how exactly it's done. This is the reason SQL is so powerful, because it treats data as data and not as bits and bytes (and chars).

The goal of this tool is to provide a bridge between the world of text files and of SQL.

You can use this [gitter chat room](https://gitter.im/harelba/q) for contacting me directly. I'm trying to be available at the chat room as much as possible.

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
Current stable version is `1.3.0`. 

No special requirements other than python >= 2.5 are needed.

### Mac Users
Just run `brew install q`. 

Thanks @stuartcarnie for the initial homebrew formula

### Manual installation (very simple, since there are no dependencies)

1. Download the main q executable from **[here](https://raw.github.com/harelba/q/1.3.0/q)** into a folder in the PATH.
2. Make the file executable.

For `Windows` machines, also download q.bat **[here](https://raw.github.com/harelba/q/1.3.0/q.bat)** into the same folder and use it to run q.

### RPM-Base Linux distributions
Download the RPM here **[here](https://github.com/harelba/packages-for-q/raw/master/rpms/q-1.3.0-1.noarch.rpm)**. 

Install using `rpm -ivh <rpm-name>`.

RPM Releases also contain a man page. Just enter `man q`.

### Debian-based Linux distributions
Debian packaing is in progress. In the mean time install manually. See the section below.

## Usage
q's basic usage is very simple:`q <flags> <query>`, but it has lots of features under the hood and in the flags that can be passed to the command.

Simplest execution is q "SELECT * FROM myfile" which prints the entire file.

Complete information can be found [here](USAGE.markdown)

## Implementation
Some implementation details can be found [here](IMPLEMENTATION.markdown)

## Limitations
* No checks and bounds on data size
* Spaces in file names are not supported yet. I'm working on it.

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

