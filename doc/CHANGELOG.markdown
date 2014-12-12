# q - Treating Text as a Database


## Change log
**Fri Dec 12 2014 Harel Ben-Attia <harelba@gmail.com> 1.5.0-1**
- Full input/output support for double-quoting fields with delimiters
- Multiple query support in one command line, reusing previously loaded data in subsequent queries
- Support literal SELECT statements (e.g. SELECT 5+12.5/3)
- Full code restructuring (Internally working now using a full python API which will become public in the next version)
- Added sha1 function 
- Solved the following bugs/pull-requests:
  - [#10](../../../issues/10) - Reuse of previously loaded data when running multiple queries in one command line 
  - [#64](../../../issues/64) - Full support for literal SELECT statements without a table (e.g. SELECT 5+12.5) 
  - [#56](../../../issues/56),[#78](../../../issues/78) - Proper double quote handling, including multiline fields, for both input and output according to csv standards 
  - [#69](../../../issues/69) - Added warning suppression when the user provides a specific column count
  - [#40](../../../issues/40) - Code restructuring cleaning, creating a full python API
  - [#60](../../../issues/60) - Fixed RPM packaging
  - [#68](../../../issues/68) - UTF-8 with BOM files cause column naming issues
  - [#63](../../../issues/63) - Unicode string support in regexp function
**Sat Jun 14 2014 Harel Ben-Attia <harelba@gmail.com> 1.4.0-1**
- 2.5x Speed improvement due to better bulk loading
- Output header support
- Additional control over query and output encodings
- Solved the following bugs/pull-requests:
  - [#52](../../../issues/52) - Bulk insert for better performance
  - [#55](../../../issues/55) - Use UUID to ensure temporary table names don't clash
  - [#53](../../../issues/53) - Allow easier tab-delimited output
  - [#51](../../../issues/51) - Ensure that generated temp tables are uniquely named
  - [#50](../../../issues/50) - Copyright peculiarities
  - [#49](../../../issues/49) - Add option to output fieldnames as headers
  - [#48](../../../issues/48) - PEP 8
  - [#47](../../../issues/47) - Prevent regexp from failing when field value is null
  - [#41](../../../issues/41) - Fix project folder structure
  - [#32](../../../issues/32) - Remove duplicated definitions, and PEP8-ing (tabs to spaces, etc.)
  - [#29](../../../issues/29) - RPM: Source0 should be a URL
  - [#54](../../../issues/54) - Fix query encoding (data encoding works well)

**Thu Mar 03 2014 Harel Ben-Attia <harelba@gmail.com> 1.3.0-1**
- Added column name and type detection (Use -A to see name/type analysis for the specified input)
- Added support for multiple parsing modes - Relaxed, Strict and Fluffy (old, backward compatible behavior)
- Fixed tab delimition parameter problem
- More improvements to error reporting
- Added a test suite, in preparation for refactoring
- Solves the following bugs/pull-requests:
  - [#7](../../../issues/7)  - Dynamic column count support
  - [#8](../../../issues/8)  - Column name inference from input containing a header row
  - [#9](../../../issues/9)  - Automatic column type inference using sample data
  - [#30](../../../issues/30) - Header lines option does nothing
  - [#33](../../../issues/33) - Last column should allow for spaces?
  - [#35](../../../issues/35) - Add q.bat
  - [#38](../../../issues/38) - Problem with whitespace delimiter
  - [#43](../../../issues/43) - using the -t flag stopped the header flag from working
  - [#44](../../../issues/44) - Space in column name on TAB separated values break q
- Breaking changes:
  - Changed -H behavior so it's now a flag an not a parameter (1 line always)
  - Removed support for multi-char delimiters

**Sat Feb 22 2014 Harel Ben-Attia <harelba@gmail.com> 1.2.0-1**
- Support for quoted fields in CSVs
- Some bug fixes
- Improved error reporting

**Thu Feb 20 2014 Harel Ben-Attia <harelba@gmail.com> 1.1.7-1**
- Better error reporting
- Fixed python invocation for non stanard locations
- Added man page

**Tue Jan 7 2014 Harel Ben-Attia <harelba@gmail.com> 1.0.0-1**
- Initial version - tag was needed for homebrew formula

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

