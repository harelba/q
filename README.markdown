# Python api DEVELOPMENT branch

This is the python API development branch. Objectives are as follows:

* Python module providing all q's command line capabilities, including query execution and analysis
* Support different input parameters for each loaded file (e.g. different delimiter etc.)
* Data loading is optimized so that multiple uses of the same files do not require reloading the data
* The command line user can benefit from data-reuse as well by providing multiple queries on the same command line
* Provide as much metadata as possible as a response to the user
* The python module is exposed as a standard PyPI package, and installable using `pip install`
* As much as possible, the existing installation code for all platforms should remain the same
* Nothing that works now should break :)

Most of these are currently implemented and I will appreciate your review and comments. Making it a proper module including installation for the different platform seems to require changing all installation code, so if you have any elegant idea on how to make it happen without me needing to change all installations, please drop me a line.

To test the API, go to the bin/ folder and run ipython/python (or create a script there).

````
cd bin/

ipython
````

Here is a typical use case example:

````python
from qtextasdata import QTextAsData,QInputParams

# Create an instance of q. Default input parameters can be provided here if needed
q = QTextAsData()

# execute a query, using specific input parameters
r = q.execute('select * from /etc/passwd',QInputParams(delimiter=':'))

# Get the result status (ok/error). In case of error, r.error will contain a QError instance with the error information
r.status
'ok'

# Get the number of rows returned
len(r.data)
37

# Show an example row. Each row is a tuple of typed field values
r.data[0]
(u'root', u'x', 0, 0, u'root', u'/root', u'/bin/bash')

# Show warnings if any. Each warning will be of type QWarning
r.warnings
[]

# Explore the result metadata
r.metadata
QMetadata<table_count=1,output_column_name_list=[u'c1', u'c2', u'c3', u'c4', u'c5', u'c6', u'c7'],data_load_count=1

# Get the list of output columns
r.metadata.output_column_name_list
[u'c1', u'c2', u'c3', u'c4', u'c5', u'c6', u'c7']

# Get information about the data loadings that have taken place for this query to happen
r.metadata.data_loads
[DataLoad<'/etc/passwd' at 1416781622.56 (took 0.002 seconds)>]

# Get table structure information. You can see the column names of the table, and the column types, along with the original filename. Materialized filenames list can be accessed as well if needed
r.metadata.table_structures
[QTableStructure<filenames_str=/etc/passwd,materialized_file_count=1,column_names=['c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7'],column_types=['text', 'text', 'int', 'int', 'text', 'text', 'text']>]

#Note that one data loading has taken place. Now, if we still use the same `QTextAsData` instance and run queries on the same files, no additional data loading will take place:
r2 = q.execute('select c1 from /etc/passwd',QInputParams(delimiter=':'))

# Get the status again
r2.status
'ok'

# See the data returned (first row only)
r2.data[0]
(u'root',)

# See the data loading information. Note that no data loadings have been done for this query
r.metadata.data_loads
[]

# Any query running using the instance `q` and somehow using the `/etc/passwd` file will run immediately without requiring to load the data again. This is extremely useful for large files obviously, for cases where there are lots of queries that need to run against the same file, and for consistency of results. The command line interface of q has been extended to support this as well, by allowing multiple queries on the same command line - E.g. `q "select ..." "select ..." "select ..." ...`

# In addition, the API supports preloading of files before executing queries. Use the `load_data` or the `load_data_from_string` methods:

# Load the file using specific input parameters
q.load_data('my_file',QInputParams(delimiter='\t',skip_header=True,input_encoding='utf-16'))

# Execute a query using this file:
r3 = q.execute('select c5 from my_file where c1 > 1000')

# Note that the result indicates that no data loads have been performed
r3.metadata.data_loads
[]

# unload() can be used in order to erase the already-loaded files.
q.unload()

# Now execute another query with my_file
r4 = q.execute('select c5 from my_file where c1 > 2000')

# One data loading has been performed due to this query
r4.metadata.data_loads
[DataLoad<'my_file' at 1416781831.16 (took 0.001 seconds)>]

# Using a different instance of QTextAsData would also cause a separate data load. However, please note that in that case, both copies would reside in memory independently.
q2 = QTextAsData()
# q2.execute('select ... from my_file')

# Except for execute(), there is another method called analyze(), which will provide a response containing the metadata related to analyzing the query and the file they use.
r5 = q.analyze('select * from my_file')

# r5 is a standard response like above, except that it won't contain data (it will be None), so r5.status, r5.error, r5.metadata and r5.warnings will be filled with relevant data.

# In order to provide access to stdin, the execute command provides two parameters: `stdin_filename` and `stdin_file`. These two allow injecting a stream of data to queries.
r6 = q.execute('select * from my_stdin_file',stdin_filename='my_stdin_file',stdin_file=sys.stdin)

r7 = q.execute('select * from my_stdin_file',stdin_filename='my_stdin_file',stdin_file=file('mmmm','rb'))

# I'm looking at options of making the "stream" option more generic, allowing to inject multiple file objects as separate tables. Tell me what you think of such an option.
````

That's it for now. Any help with ideas regarding on how to morph this new python module to a full-fledged PyPI package without having to restructure the entire installation logic will be greatly appreciated.



