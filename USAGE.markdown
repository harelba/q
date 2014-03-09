# q - Treating Text as a Database 

## Usage
Basic usage format is `q <flags> <query>`. Simplest execution is `q "SELECT * FROM myfile"` which prints the entire file.

### Query
q gets one parameter - An SQL-like query. The following applies:

* The table name is the actual file name that you want to read from. Path names are allowed. Use "-" if you want to read from stdin (e.g. `q "SELECT * FROM -"`)
  * Actually multiple files can be provided by using one of both of the following ways:
     * By separating the filenames with a + sign: `SELECT * FROM datafile1+datafile2+datefile3`.
     * By using glob matching: `SELECT * FROM mydata*.dat`
  * Files with .gz extension are considered to be gzipped and decompressed on the fly.
* Use `-H` in order to specify that a header row exists. q will read the header row and set the column names accordingly. 
* If there is no header row, then the column names will be in the format cX where X is the column number starting from **1**. For example, to retrieve the second and fourth columns of the file, use `q "SELECT c2,c4 FROM myfile"`
* Any standard SQL expression, condition (both WHERE and HAVING), GROUP BY, ORDER BY etc. are allowed. NOTE: Full type detection is implemented, so there is no need for any casting or anything.
* For both consistency and for preventing shell expansion conflicts, q currently expects the entire query to be in a single command-line parameter. Here is an example standard usage: ```q "SELECT * FROM datafile"```. Notice that the entire SQL statement is enclosed in double quotes. Flags are obviously outside the quotes.

JOINs are supported and Subqueries are supported in the WHERE clause, but unfortunately not in the FROM clause for now. Use table alias when performing JOINs.

The SQL syntax itself is sqlite's syntax. For details look at http://www.sqlite.org/lang.html or search the net for examples.


### Runtime options and flags
q can also get some runtime flags (Linux style, before the parameter). The following parameters can be used, all optional:

* `-z` - Means that the file is gzipped. This is detected automatically if the file extension if .gz, but can be useful when reading gzipped data from stdin (since there is no content based detection for gzip).
* `-H <N>` - Tells q to skip N header lines in the beginning of the file - Used naturally for skipping a header line. This can possibly be detected automatically in the future.
* `-d` - Column/field delimiter. If it exists, then splitting lines will be done using this delimiter. If it doesn't, space will be used as the delimiter. If you need multi-character delimiters, run the tool with engine version 1 by adding `-E v1`. Using v1 will also revert to the old behavior where if no delimiter is provided, then any whitespace will be considered as a delimiter.
* `-D` - Column/field delimiter for output. If it exists, then the output will use this delimiter instead of the one used in input. Defaults to input delimiter if provided by `-d`, or space if not.
* `-b` - Beautify the output. If this flag exists, output will be aligned to the largest actual value of each column. **NOTE:** Use this only if needed, since it is slower and more CPU intensive.
* `-t` - Shorthand flag for a tab delimiter, one header line format (Same as `-d $'\t' -H 1` - The $ notation is required so Linux would escape the tab...)
* `-f <F>` - Output-formatting option. If you don't like the output formatting of a specific column, you can use python formatting in order to change the output format for that column. See below for details
* `-e <E>` - Specify the text encoding. Defaults to UTF-8. If you have ASCII only text and want a 33% speedup, use `-e none`. Unfortunately, proper encoding/decoding has its price.

* `-A` - Analyze sample input and provide an analysis of column names and their detected types. Does not run the query itself
* `-m` - Data parsing mode. `relaxed`, `strict`, or `fluffy`. In relaxed mode the -c column-count is optional. In strict mode, it must be provided. See separate section in the documentation about the various modes. Fluffy mode should only be used if backward compatibility (less well defined, but helpful...) to older versions of q is needed.
* `-c` - Specific column count. This parameter fixes the column count. In relaxed mode, this will cause missing columns to be null, and extra columns to be "merged" into the last column. In strict mode, any deviation from this column count will cause an error.
* `-k` - Keep leading whitespace. By default leading whitespace is removed from values in order to provide out-of-the-box usability. Using this flag instructs q to leave any leading whitespace in tact, making the output more strictly identical to the input.

### Output formatting option
The format of F is as a list of X=f separated by commas, where X is a column number and f is a python format:
* X - column number - This is the SELECTed column (or expression) number, not the one from the original table. E.g, 1 is the first SELECTed column, 3 is the third SELECTed column.
* f - A python formatting string - See http://docs.python.org/release/2.4.4/lib/typesseq-strings.html for details if needed.
  * Example: `-f 3=%-10s,5=%4.3f,1=%x`

### Parsing Modes
q supports multiple parsing modes:
* `relaxed` - This is the default mode. It tries to lean towards simplicity of use. When a row doesn't contains enough columns, they'll be filled with nulls, and when there are too many, the extra values will be merged to the last column. Defining the number of expected columns in this mode is done using the `-c` parameter.
* `strict` - Strict mode is for hardcode csv parsing. Whenever a row doesn't contain the proper number of columns, processing will stop. 
* `fluffy` - This mode should not be used, and is just some kind of "backward compatible" parsing mode which was used by q previously. It's left as a separate parsing mode on purpose, in order to accomodate existing users. If you are such a user, please open a bug for your use case, and I'll see how I can incorporate it into the other modes. It is reasonable to say that this mode will be removed in the future.

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

