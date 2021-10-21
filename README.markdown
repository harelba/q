[![Build Status](https://travis-ci.org/harelba/q.svg?branch=master)](https://travis-ci.org/harelba/q)

# q - Text as Data
q is a command line tool that allows direct execution of SQL-like queries on CSVs/TSVs (and any other tabular text files).

q treats ordinary files as database tables, and supports all SQL constructs, such as `WHERE`, `GROUP BY`, `JOIN`s, etc. It supports automatic column name and type detection, and q provides full support for multiple character encodings.

q's web site is [http://harelba.github.io/q/](http://harelba.github.io/q/) or [https://q.textasdata.wiki](https://q.textasdata.wiki) It contains everything you need to download and use q immediately.

## New beta version `3.1.0-beta` is available, which contains the following major changes/additions:

The version is still in early testing, for two reasons:

* Completely new build and packaging flow - Using [pyoxidizer](https://github.com/indygreg/PyOxidizer)
* A very large change in functionality, which might surface issues, new and backward compatibility ones

Please don't use it for production, until the final non-beta version is out.

Here is a list of the new/changed functionality:

* Automatic caching of data files (into `<my-csv-filename>.qsql` files), with huge speedups for large files. Enabled through `-C readwrite` or `-C read`
* Revamped `.qrc` mechanism, allows opting-in to caching without specifying it in every query. By default, caching is **disabled**, for backward compatibility and for finding usability issues.
* Direct querying of standard sqlite databases- Just use it as a table name in the query. Format is `select ... from <sqlitedb_filename>:::<table_name>`, or just `<sqlitedb_filename>` if the database contains only one table. Multiple separate sqlite databases are fully supported in the same query.
* Direct querying of the `qsql` cache files - The user can query directly from the `qsql` files, removing the need for the original files. Just use `select ... from <my-csv-filename>.qsql`. Please wait until the non-beta version is out before thinking about deleting any of your original files...
* Only python3 is supported from now on, although q is a self-contained binary executable which has its own python embedded in it, so it shouldn't matter for most users.
* `--save-db-to-disk` option (`-S`) has been enhanced to match the new capabilities. You can query the resulting file directly through q, using the method mentioned above (it's just a standard sqlite database).

For details on the changes and the new usage, see [here](QSQL-NOTES.md)

If you're testing it out, I'd be more than happy to get any feedback. Please write all your feedback in [this issue](https://github.com/harelba/q/issues/281), instead of opening separate issues. That would really help me with managing this.

## Installation.
**This will currently install the latest standard version `2.0.19`. See below if you want to download the `3.1.0-beta` version**

The current production version `2.0.19` installation is extremely simple. 

Instructions for all OSs are [here](http://harelba.github.io/q/#installation). 

### Installation of the new beta release
For now, only Linux RPM, DEB and Mac OSX are supported. Almost made the Windows version work, but there's some issue there, and the windows executable requires some external dependencies which I'm trying to eliminate.

The beta OSX version is not in `brew` yet, you'll need to take the `macos-q` executable and put it in your filesystem. DEB/RPM are working well, although for some reason showing the q manual (`man q`) does not work for Debian, even though it's packaged in the DEB file. I'll get around to fixing it later.

Download the relevant files directly from [Links Coming Soon](TBD).

## Examples

```
q "SELECT COUNT(*) FROM ./clicks_file.csv WHERE c3 > 32.3"

ps -ef | q -H "SELECT UID, COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"
```

Go [here](http://harelba.github.io/q/#examples) for more examples.

## Benchmark
I have created a preliminary benchmark comparing q's speed between python2, python3, and comparing both to textql and octosql. 

Your input about the validity of the benchmark and about the results would be greatly appreciated. More details are [here](test/BENCHMARK.md).

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Linkedin: [Harel Ben Attia](https://www.linkedin.com/in/harelba/)

Twitter [@harelba](https://twitter.com/harelba)

Email [harelba@gmail.com](mailto:harelba@gmail.com)

q on twitter: #qtextasdata

