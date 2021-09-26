
* Changed testing to pytest
* Move benchmark stuff around
* Modified benchmark to include cached file benchmarks as well
* Added logging through xprint()
* caching
  * Added mechanism for reading/writing a cache file for each source data file. Done as sqlite pages, fast
  * Added adhoc database, which is a virtual memory database that all data streams are loaded into
  * Added query-level-db which is a virtual memory database that attaches to multiple memory/disk databases
  * Each tablecreator now creates or loads a full db (either memory or disk based). Each DB contains only one table
  * data streams are written directly to adhoc database
  * Added disk-db content signatures
  * Fixed qtable_name from set to list, to support multiple tables with the same filename
* Fixed internal transaction handling of sqlite
* Fixed and completed .qrc file support
* Added --dump-defaults to show .qrc defaults
* Fixed column-name/count detection for empty files with a header
* Removed stdin special-handling, and added generic data-stram injection capability that is used for stdin
* Streamlined empty-data handling, so complete flow will work even with empty data (warning is still reported)
* Added .qrc tests and testing capability
* Fixed ensure_data_load* handling, so behaviour would be seamless
* -S - Saving to db materializes the query-level db and writes a multi-table qsql file instead
  * No support for table aliasing because of parsing limitations - the user cannot provide an alias in the query which will determine the actual table name
* Table names in qsql files take the base name of the original file
* Added ability to query directly from qsql files
  * autodetection of sqlite3 files
  * with one table, user can provide only the name of the qsql file
* -A shows data sources and types - file, disk-file, qsql
* Augmented e2e testing framework a bit
* Understood that -S output should be standard sqlite files and not qsql, this eliminated the need to propagate metaq data into the resulting stored database, and opened the way for a lot of progress
* added qsql/sqlite with multiple tables, the user can provide a table name with QSQLFILE:::TABLE_NAME, otherwise an error with the list of tables will appear
* made tablecreator an implementation only - MSs are each a full table. This caused globs to be pushed-down to the delimited file reader (using fileinput, hopefully it's fast enough), and also prevented the option of globbing with sqlite/qsql files, which is not the best, but it's a reasonable choice

TODO
* Add sqlitebck as dependency to brew installation
* -A should provide information about reading/writing to disk
* remove py2 support? py3 is encapsulated, so perhaps it's not a big issue anymore

Insights
* Was surprised at how imperative I was 8 years ago
* python refactoring is nice and helps readability, but can be misleading, as it propagates mutability down the stack
* no way this could have been done without e2e testing
* had to improve the reproducability of e2e test bugs in order to actually be effective
* The non sql-like '+' operator created a lot of issues - It forced skewing the internal data model, and caused a lot of issues. Eventually had to decide to quite supporting it, making a breaking change, and provide similar functionality using UNION ALL. This led to a bit weaker error checking for headers in multiple files. There was always and issue with it, since files with '+' in the filename were not supported anyway. This also led to the understanding that merging globs behavior (e.g. multiple data loads for the same table) and file-concatenation was not good. Very scary, breaking existing e2e tests
* Breaking something actually did a lot of good. Started to make progress again
* Turns out that moving globs to the MFS layer eliminated the usage of max-attached-sqlite-databases, even though the tests were still working. This means that the tests did not do a good job. Anyway, fixed the tests, and found out as well that auto-storing qsql files during read cannot work out of the box for above-the-limit csv files, as they are using the adhoc db. Writing them as cached files actually wrote the entire adhoc db as the cached file name, making it larger and not native to query (and less reusable for other queries). Theoretically, this could have been solved, but since this is an optimization, I won't bother with it now. It would just skip the cache-storing of the extra files, and will cache only the below-the-attached-limit files.
* Turns out that probably the UNION ALL capability of concatenating multiple files, which was complex to create, is not needed anymore, since the globs are handled within the TableCreator/DelimitedFileReader. Previously, the mess of exposing it up the stack, forced to create it. It allowed some progress, but the technical debt was probably worth fixing at that point.
* I was stuck at some point, because i didn't want to break anything from the user perspective, namely the concatenation syntax table1+table2. This was too strict, and until i allowed myself to break it, i couldn't move forward






Deprecated earlier - needs to be removed
* fluffy mode
