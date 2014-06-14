# q - Text as Data
q is a command line tool that allows direct execution of SQL-like queries on CSVs/TSVs (and any other tabular text files).

Main features:
* Seamless multi-table SQL support, including joins. filenames are just used instead of table names (use - for stdin)
* Automatic column name and column type detection (Allows working more naturally with the data)
* Full encoding support (input, output and query)

Version `1.4.0` is out and contains the following improvements:
* 2.5x speed increase for large files - Thanks [@Gasol](https://github.com/Gasol)!
* Output header support (based on the query's SELECTed columns/aliases)
* Additional control over query and output encodings
* Bug fixes (see [change log](doc/CHANGELOG.markdown) for full details)

Please send any feedback you have on the new version and open new issues as needed.

I would love to get any requests and comments you have on the tool.

You can use this [gitter chat room](https://gitter.im/harelba/q) for contacting me directly. I'm trying to be available at the chat room as much as possible.

**Recurring visitors**
````
Recurring visitors - I would love it if you can drop me a line on gitter or through email with the reasons for your recurring visits. 

Any requests for features would be greatly appreciated as well.
````

## Examples
__Example 1:__

    q -H -t "select count(distinct(uuid)) from ./clicks.csv"
    
__Output 1:__
```bash
229
```

__Example 2:__

    q -H -t "select request_id,score from ./clicks.csv where score > 0.7 order by score desc limit 5"

__Output 2:__
```bash
2cfab5ceca922a1a2179dc4687a3b26e	1.0
f6de737b5aa2c46a3db3208413a54d64	0.986665809568
766025d25479b95a224bd614141feee5	0.977105183282
2c09058a1b82c6dbcf9dc463e73eddd2	0.703255121794
```

__Example 3:__

    q -t -H "select strftime('%H:%M',date_time) hour_and_minute,count(*) from ./clicks.csv group by hour_and_minute"

__Output 3:__
```bash
07:00	138148
07:01	140026
07:02	121826
```

__Usage Example 4:__

    q -t -H "select hashed_source_machine,count(*) from ./clicks.csv group by hashed_source_machine"
    
__Output 4:__
```bash
47d9087db433b9ba.domain.com	400000
```

__Example 5 (total size per user/group in the /tmp subtree):__

    sudo find /tmp -ls | q "select c5,c6,sum(c7)/1024.0/1024 as total from - group by c5,c6 order by total desc"

__Output 5:__
```bash
mapred hadoop   304.00390625
root   root     8.0431451797485
smith  smith    4.34389972687
```

__Example 6 (top 3 user ids with the largest number of owned processes, sorted in descending order):__

Note the usage of the autodetected column name UID in the query.

    ps -ef | q -H "select UID,count(*) cnt from - group by UID order by cnt desc limit 3"
    
__Output 6:__
```bash
root 152
harel 119
avahi 2
```

A beginner's tutorial can be found [here](examples/EXAMPLES.markdown)

## Installation
Current stable version is `1.4.0`. 

Requirements: Just Python 2.5 and up or Python 2.4 with sqlite3 module installed. Python 3.x is not supported yet.

### Mac Users
Make sure you run `brew update` first and then just run `brew install q`. 

Version 1.4.0 has been committed to homebrew and is pending their confirmation (usually in a couple of hours).

Thanks [@stuartcarnie](https://github.com/stuartcarnie) for the initial homebrew formula

### Manual installation (very simple, since there are no dependencies)

1. Download the main q executable from **[here](https://raw.github.com/harelba/q/1.4.0/bin/q)** into a folder in the PATH.
2. Make the file executable.

For `Windows` machines, also download q.bat **[here](https://raw.github.com/harelba/q/1.4.0/bin/q.bat)** into the same folder and use it to run q.

### RPM-Base Linux distributions
Download the version `1.4.0` RPM here **[here](https://github.com/harelba/packages-for-q/raw/master/rpms/q-text-as-data-1.4.0-1.noarch.rpm)**. 

Install using `rpm -ivh <rpm-name>`.

RPM Releases also contain a man page. Just enter `man q`.

**NOTE** In Version `1.4.0`, the RPM package name has been changed to `q-text-as-data`. If you already have the old version, just remove it with `rpm -e q` before installing.

### Debian-based Linux distributions
If you're interested in Debian packaing, please drop me a line to harelba@gmail.com.

## Overview
Have you ever stared at a text file on the screen, hoping it would have been a database so you could ask anything you want about it? I had that feeling many times, and I've finally understood that it's not the _database_ that I want. It's the language - SQL.

SQL is a declarative language for data, and as such it allows me to define what I want without caring about how exactly it's done. This is the reason SQL is so powerful, because it treats data as data and not as bits and bytes (and chars).

The goal of this tool is to provide a bridge between the world of text files and of SQL.

## Usage
q's basic usage is very simple:`q <flags> <query>`, but it has lots of features under the hood and in the flags that can be passed to the command.

Simplest execution is q "SELECT * FROM myfile" which prints the entire file.

Complete information can be found [here](doc/USAGE.markdown)

## Implementation
Some implementation details can be found [here](doc/IMPLEMENTATION.markdown)

## Limitations
* No checks and bounds on data size
* Spaces in file names are not supported yet. I'm working on it.
* It is possible that some rare cases of subqueries are not supported yet. Please open an issue if you find such a case. This will be fixed once the tool performs its own full-blown SQL parsing.

## Future Ideas
* Faster reuse of previous data loading
* Allow working with external DB
* Real parsing of the SQL, allowing smarter execution of queries.
* Smarter batch insertion to the database
* Provide mechanisms beyond SELECT - INSERT and CREATE TABLE SELECT and such.

## Rationale
Some information regarding the rationale for this tool and related philosophy can be found [here](doc/RATIONALE.markdown)

## Change log
History of changes can be found [here](doc/CHANGELOG.markdown)

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

