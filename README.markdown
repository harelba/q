

# q - SQL power for the Command Line

## Overview
Have you ever stared at a text file on the screen, hoping it would have been a database so you could ask anything you want about it? I had that feeling many times, and I've finally understood that it's not the database that I want. It's the language - SQL.

SQL is a declarative language for data, and as such it allows me to define what I want without caring about how exactly it's done. This is the reason SQL is so powerful, because it treats data as data and not as bits and bytes (and chars).

The goal of this tool is to provide a bridge between the world of text files and of SQL. It allows performing SQL-like statements on tabular text data. 

### Why aren't other Linux tools enough?
The standard Linux tools are amazing and I use them all the time, but the whole idea of Linux is using the right tool for the job and building complex processes out of smaller tools. This tool adds the declarative power of SQL to the Linux toolset, without loosing any of the other tools' benefits. In fact, I mostly use q together with other Linux tools, the same way I pipe awk/sed and grep together all the time.

One additional thing to note is that many Linux tools treat text as text and not as data. In that sense, you can look at q as a meta-tool which provides access to all the data-related tools that SQL provides (e.g. ordering, grouping, aggregation etc.).

## Requirements
* Python with the sqlite3 module installed. The module is a part of standard Python 2.5 and up.

## Usage Examples
* This example demonstrates how we can use this script to do some calculation on tabular data. We'll use the output of ls as the input data (we're using --full-time so the output format will be the same on all machines).
  * Execute the following command. It will create out test data:
```bash
ls -ltrd --full-time * > mydatafile
```

## Command Line Options

## Implementation

## Limitations

## Future Ideas

## Philosophy
This tool has been designed with general Linux/Unix design principles in mind. If you're interested in these general design principles, read the amazing book http://catb.org/~esr/writings/taoup/ and specifically http://catb.org/~esr/writings/taoup/html/ch01s06.html. If you believe that the way this tool works goes strongly against any of the principles, I would love to hear your view about it.

