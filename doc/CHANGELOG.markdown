# q - Treating Text as a Database

## Change log

**Thu Mar 03 2014 Harel Ben-Attia <harelba@gmail.com> 1.3.0-1**
- Added column name and type detection (Use -A to see name/type analysis for the specified input)
- Added support for multiple parsing modes - Relaxed, Strict and Fluffy (old, backward compatible behavior)
- Fixed tab delimition parameter problem
- More improvements to error reporting
- Added a test suite, in preparation for refactoring
- Solves the following bugs/pull-requests:
  - [#7](../../issues/7)  - Dynamic column count support
  - [#8](../../issues/8)  - Column name inference from input containing a header row
  - [#9](../../issues/9)  - Automatic column type inference using sample data
  - [#30](../../issues/30) - Header lines option does nothing
  - [#33](../../issues/33) - Last column should allow for spaces?
  - [#35](../../issues/35) - Add q.bat
  - [#38](../../issues/38) - Problem with whitespace delimiter
  - [#43](../../issues/43) - using the -t flag stopped the header flag from working
  - [#44](../../issues/44) - Space in column name on TAB separated values break q
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

