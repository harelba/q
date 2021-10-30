[![Build Status](https://travis-ci.org/harelba/q.svg?branch=master)](https://travis-ci.org/harelba/q)

# q - Text as Data
q's purpose is to bring SQL expressive power to the Linux command line and to provide easy access to text as actual data.

q allows the following:

* Performing SQL-like statements directly on tabular text data, auto-caching the data in order to accelerate additional querying on the same file
* Performing SQL statements directly on multi-file sqlite3 databases, without having to merge them or load them into memory

q treats ordinary files as database tables, and supports all SQL constructs, such as `WHERE`, `GROUP BY`, `JOIN`s, etc. It supports automatic column name and type detection, and provides full support for multiple character encodings.

q's web site is [http://harelba.github.io/q/](http://harelba.github.io/q/) or [https://q.textasdata.wiki](https://q.textasdata.wiki) It contains everything you need to download and use q immediately.

Usage examples are in [here](http://harelba.github.io/q/#examples)

## Installation.
**New Major Version `3.1.2` is out with a lot of significant additions.**

Instructions for all OSs are [here](http://harelba.github.io/q/#installation).

The previous version `2.0.19` Can still be downloaded from [here](https://github.com/harelba/q/releases/tag/2.0.19)  

## Benchmark
TBD New Benchmark results, with and without caching

Your input about the validity of the benchmark and about the results would be greatly appreciated. More details are [here](test/BENCHMARK.md).

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Linkedin: [Harel Ben Attia](https://www.linkedin.com/in/harelba/)

Twitter [@harelba](https://twitter.com/harelba)

Email [harelba@gmail.com](mailto:harelba@gmail.com)

q on twitter: #qtextasdata

