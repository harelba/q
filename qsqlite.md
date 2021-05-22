
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


TODO
* Add sqlitebck as dependency to brew installation
* Better naming in -A for the difference between a disk-file and a qsql file (perhaps consolidate?)
* qsql with multiple tables, the user needs to provide a table name with QSQLFILE:::TABLE_NAME

Insights
* Was surprised at how imperative I was 8 years ago
* python refactoring is nice and helps readability, but can be misleading, as it propagates mutability down the stack
* no way this could have been done without e2e testing
* had to improve the reproducability of e2e test bugs in order to actually be effective


Deprecated earlier - needs to be removed
* fluffy mode
