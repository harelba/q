
# New beta version 3.1.0-beta is available
Installation instructions [at the end of this document](TBD)

Contains a lot of major changes, see sections below for details.

## Basic Example of using the caching
```
# Prepare some data
$ seq 1 1000000 > myfile.csv

# read from the resulting file (-c 1 just prevents the warning of having one column only)
$ time q -c 1 "select sum(c1),count(*) from myfile.csv"
500000500000 1000000
q -c 1 "select sum(c1),count(*) from myfile.csv"  4.02s user 0.06s system 99% cpu 4.108 total

# Running with `-C readwrite` auto-creates a cache file if there is none. The cache filename would be myfile.csv.qsql. The query runs as usual
$ time q -c 1 "select sum(c1),count(*) from myfile.csv" -C readwrite
time q -c 1 "select sum(c1),count(*) from myfile.csv" -C readwrite
500000500000 1000000
q -c 1 "select sum(c1),count(*) from myfile.csv" -C readwrite  3.96s user 0.08s system 99% cpu 4.057 total

# Now run with `-C read`. The query will run from the cache file and not the original. Change the query and run it several times, to notice the difference in speed.
$ time q -c 1 "select sum(c1),count(*) from myfile.csv" -C read
500000500000 1000000
q -c 1 "select sum(c1),count(*) from myfile.csv" -C read  0.17s user 0.05s system 94% cpu 0.229 total

# You can query the qsql file directly
$ time q -c 1 "select sum(c1),count(*) from myfile.csv.qsql"
500000500000 1000000
q -c 1 "select sum(c1),count(*) from myfile.csv.qsql"  0.17s user 0.05s system 95% cpu 0.226 total

# Now let's delete the original csv file
$ rm -vf myfile.csv

# Running another query on the qsql file just works
$ q -c 1 "select sum(c1),count(*) from myfile.csv.qsql"
500000500000 1000000
q -c 1 "select sum(c1),count(*) from myfile.csv.qsql"  0.17s user 0.04s system 94% cpu 0.226 total

# See the `.qrc` section below if you want to set the default `-C` (`--caching-mode`) to something other than `none` (the default)
```

The following sections provide the details of each of the new functionality in this major version.

## Automatic caching of data files
Speeding up subsequent reads from the same file by several orders of magnitude by automatically creating an immutable cache file for each tabular text file.  

For example, reading a 0.9GB file with 1M rows and 100 columns without caching takes ~50 seconds. When the cache exists, querying the same file will take less than 1 second. Obviously, the cache can be used in order to perform any query and not just the original query that was used for creating the cache.

When caching is enabled, the cache is created on the first read of a file, and used automatically when reading it in other queries. A separate cache is being created for each file that is being used, allowing reuse in multiple use-cases. For example, if two csv files each have their own cache file from previous queries, then running a query that JOINs these two files would use the caches as well (without loading the data into memory), speeding it up considerably.

The tradeoff for using cache files is disk space - A new file with the postfix `.qsql` is created and automatically detected and used in queries as needed. This file is essentially a standard sqlite file (with some additional metadata tables), and can be used directly by any standard sqlite tool later on.

For backward compatibility, the caching option is not turned on by default. You'd need to use the new `-C <mode>` to determine the caching mode. Available options are as follows:
* `none` - The default,  provides the original q's behaviour without caching
* `read` - Only reads cache files if they exists, but doesn't create any new ones
* `readwrite` - Uses cache files if they exists, or creates new ones if they don't. Writing new cache files doesn't interfere with the actual run of the query, so this option can be used in order to dynamically create the cache files if they don't exist

Content signatures are being stored in the caches, allowing to detect a state where the original file has been modified after the cache has been created. q will issue an error if this happens. For now, just delete the `.qsql` file in order to recreate the cache. In the future, another `-C` option would be added to automatically recreate the updated cache in such a case. Notice that the content signature contains various q flags which affect parsing, so make sure to use the same parameters to q when performing the queries, otherwise q will issue an error.

Notice that when running with `-A`, the cache is not written, even when `-C` is set to `readwrite`. This is due to the fact that `-A` does not really read the entire content of the files. For now, if you'd like to just prepare the cache without running the actual query, you can run it with a `select 1` query or something, although in terms of speed it will mostly not matter. If there's demand for adding an explicit `prepare caches only` option, I'll consider adding it.

## Revamped `.qrc` mechanism
Adding `-C <mode>` for each query can be cumbersome at some point, so the `.qrc` file has been revamped for easy addition of default parameters. 

For example, if you want the caching behaviour to be `read` all the time, then just add a `~/.qrc` file, and set the following in it:
```
[options]
caching_mode=read
```

All other flags and parameters to q can be controlled by the `.qrc` file. To see the proper names for each parameter, run `q --dump-defaults` and it will dump a default `.qrc` file that contains all parameters to `stdout`.

## Direct querying of standard sqlite databases
q now supports direct querying of standard sqlite databases. The syntax for accessing a table inside an sqlite database is `<sqlite-filename>:::<table_name>`. A query can contain any mix of sqlite files, qsql files or regular delimited files.

For example, this command joins two tables from two separate sqlite databases:
```
$ q "select count(*) from mydatabase1.sqlite:::mytable1 a left join mydatabase2.sqlite:::mytable2 b on (a.c1 = b.c1)"
```

Running queries on sqlite databases does not usually entail loading the data into memory. Databases are attached to a virtual database and queried directly from disk. This means that querying speed is practically identical to standard sqlite access. This is also true when multiple sqlite databases are used in a single query. The same mechanism is being used by q whenever it uses a qsql file (either directly or as a cache of a delimited fild). 

sqlite itself does have a pre-compiled limit of the number of databases that can be attached simultanously. If this limit is reached, then q will attach as many databases as possible, and then continue processing by loading additional tables into memory in order to execute the query. The standard limit in sqlite3 (unless compiled specifically with another limit) is 10 databases. This allows q to access as many as 8 user databases without having to load any data into memory (2 databases are always used for q's internal logic). Using more databases in a single query than this pre-compiled sqlite limit would slow things down, since some of the data would go into memory, but the query should still provide correct results.

Whenever the sqlite database file contains only one table, the table name part can be ommitted, and the user can specify only the sqlite-filename as the table name. For example, querying an sqlite database `mydatabase.sqlite` that only has one table `mytable` is possible with `q "SELECT ... FROM mydatabase.sqlite"`. There's no need to specify the table name in this case.

Since `.qsql` files are also standard sqlite files, they can be queried directly as well. This allows the user to actually delete the original CSV file and use the caches as if they were the original files. For example:

```
$ q "select count(*) from myfile.csv.qsql"
```

Notice that there's no need to write the `:::<table-name>` as part of the table name, since `qsql` files that are created as caches contain only one table (e.g. the table matching the original file).

Running a query that uses an sqlite/qsql database without specifying a table name will fail if there is more than one table in the database, showing the list of existing tables. This can be used in order to detect which tables exist in the database without resorting to other tools. For example:
```
$ q "select * from chinook.db:::blah"
Table blah could not be found in sqlite file chinook.db . Existing table names: albums,sqlite_sequence,artists,customers,employees,genres,invoices,invoice_items,media_types,playlists,playlist_track,tracks,sqlite_stat1
```

## Storing source data into a disk database
The `-S` option (`--save-db-to-disk`) has been modified to match the new capabilities. It works with all types of input tables/files, and writes the output database as a standard sqlite database. I've considered making the output a multi-table `qsql` file (e.g. with the additional metadata that q uses), but some things still need to be ironed out in order to make these qsql files work seamlessly with all other aspects of q. This will probably happen in the next version.  

This database can be accessed directly by q later on, by providing `<sqlite-database>:::<table-name>` as the table name in the query. The table names that are chosen match the original file names, but go through the following process:
* The names are normalised in order to by compatible with sqlite restrictions (e.g. `x.csv` is normalised to `x_dot_csv`)
* duplicate table names are de-deduped by adding `_<sequence-number>` to their names (e.g. two different csv files in separate folders which both have the name `companies` will be written to the file as `companies` and `companies_2`)

This table-name normalisation happens also inside `.qsql` cache files, but in most cases there won't be any need to know these table names, since q automatically detects table names for databases which have a single-table.

## File-concatenation and wildcard-matching features - Breaking change
File concatenation using '+' has been removed in this version, which is a breaking change.

This was a controversial feature anyway, and can be done using standard SQL relatively easily. It also complicated the caching implementation significantly, and it seemed that it was not worth it. If there's demand for bringing this feature back, please write to me and I'll consider re-adding it. 

If you have a case of using file concatenation, you can use the following SQL instead:
```
# Instead of writing
$ q "select * from myfile1+myfile2"
# Use the following:
$ q "select * from (select * from myfile1 UNION ALL select * from myfile2)"
```

This will provide the same results, but the error checking is a bit less robust, so be mindful on whether you're performing the right query on the right files.

Conceptually, this is similar to wildcard matching (e.g. `select * from myfolder/myfile*`), but I have decided to leave wildcard-matching intact, since it seems to be a more common use-case. Cache creation and use is limited for now when using wildcards. Use the same method as described above for file concatenation if you wanna make sure that caches are being used.

After this version is fully stabilised, I'll make more efforts to consolidate wildcard (and perhaps concatenation) to fully utilise caching seamlessly.

## Code runs only on python 3
Removed the dual py2/py3 support. Since q is packaged as a self-contained executable, along with python 3.8 itself, then this is not needed anymore.

Users which for some reason still use q's main source code file directly and use python 2 would need to stay with the latest 2.0.19 release. In some next version, q's code structure is going to change significantly anyway in order to become a standard python module, so using the main source code file directly would not be possible.

If you are such a user, and this decision hurts you considerably, please ping me.


# Installation of the new beta release
For now, only Linux RPM, DEB and Mac OSX are supported. Almost made the Windows version work, but there's some issue there, and the windows executable requires some external dependencies which I'm trying to eliminate.

The beta OSX version is not in `brew` yet, you'll need to take the `macos-q` executable, put it in your filesystem and `chmod +x` it.

DEB/RPM are working well, although for some reason showing the q manual (`man q`) does not work for Debian, even though it's packaged in the DEB file. I'll get around to fixing it later.

Download the relevant files directly from [The Beta Release Assets](https://github.com/harelba/q/releases/tag/v3.1.0-beta).

