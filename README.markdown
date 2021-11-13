![Build And Package](https://github.com/harelba/q/workflows/BuildAndPackage/badge.svg)


# q - Text as Data
q's purpose is to bring SQL expressive power to the Linux command line and to provide easy access to text as actual data.

q allows the following:

* Performing SQL-like statements directly on tabular text data, auto-caching the data in order to accelerate additional querying on the same file. 
* Performing SQL statements directly on multi-file sqlite3 databases, without having to merge them or load them into memory

The following table shows the impact of using caching:

|    Rows   | Columns | File Size | Query time without caching | Query time with caching | Speed Improvement |
|:---------:|:-------:|:---------:|:--------------------------:|:-----------------------:|:-----------------:|
| 5,000,000 |   100   |   4.8GB   |    4 minutes, 47 seconds   |       1.92 seconds      |        x149       |
| 1,000,000 |   100   |   983MB   |        50.9 seconds        |      0.461 seconds      |        x110       |
| 1,000,000 |    50   |   477MB   |        27.1 seconds        |      0.272 seconds      |        x99        |
|  100,000  |   100   |    99MB   |         5.2 seconds        |      0.141 seconds      |        x36        |
|  100,000  |    50   |    48MB   |         2.7 seconds        |      0.105 seconds      |        x25        |

Notice that for the current version, caching is **not enabled** by default, since the caches take disk space. Use `-C readwrite` or `-C read` to enable it for a query, or add `caching_mode` to `.qrc` to set a new default.
 
q's web site is [http://harelba.github.io/q/](http://harelba.github.io/q/) or [https://q.textasdata.wiki](https://q.textasdata.wiki) It contains everything you need to download and use q immediately.


## Usage Examples
q treats ordinary files as database tables, and supports all SQL constructs, such as `WHERE`, `GROUP BY`, `JOIN`s, etc. It supports automatic column name and type detection, and provides full support for multiple character encodings.

Here are some example commands to get the idea:

```bash
$ q "SELECT COUNT(*) FROM ./clicks_file.csv WHERE c3 > 32.3"

$ ps -ef | q -H "SELECT UID, COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"

$ q "select count(*) from some_db.sqlite3:::albums a left join another_db.sqlite3:::tracks t on (a.album_id = t.album_id)"
```

Detailed examples are in [here](http://harelba.github.io/q/#examples)

## Installation.
**New Major Version `3.1.2` is out with a lot of significant additions.**

Instructions for all OSs are [here](http://harelba.github.io/q/#installation).

The previous version `2.0.19` Can still be downloaded from [here](https://github.com/harelba/q/releases/tag/2.0.19)  

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Linkedin: [Harel Ben Attia](https://www.linkedin.com/in/harelba/)

Twitter [@harelba](https://twitter.com/harelba)

Email [harelba@gmail.com](mailto:harelba@gmail.com)

q on twitter: #qtextasdata

