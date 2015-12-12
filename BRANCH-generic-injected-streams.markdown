
# Generic Injected Streams

This branch is not fully functional yet! 

The purpose of this change is to allow pluggable file object streams to be used with q by providing a function that opens and closed file objects based on the virtual "table name" provided in queries.

API is not final yet, this is just a playground for now. The rest of the API definition is in [Here](PYTHON-API.markdown)

No documentation is provided yet. However, suggestions on how to bundle q properly in order for it to become a full fledged python module, without losing the command line capability are most welcome.

open_file_func and close_file_func are expected in multiple function calls, such as QTextAsData.analyze() and QTextAsData.execute(). Please note that some of the standard q logic, such as concatenating files with +, is now part of the default open_file_func and can be overridden if you're using your own function.


definitions of open_file_func and close_file_func:

````
def open_file_func(table_name,default_open_file_func):
	return (X,Y)
	# Where:
	#   X - is a file object relevant for table_name
	#   Y - is a LIST of strings containing the actual file names. This is used for response metadata
	#
	# Call default_open_file_func to propagate logic to default behavior
	#
	# Raise FileNotFoundException is there is a problem with the table_name (The exception name will be changed soon)

def close_file_func(table_name,f,default_close_file_func):
	# Should make sure that f (the file object) is closed.
	# No return value expected
````	
Simplistic Example:
````
def open_file_func(table_name,default_open_file_func):
	if table_name == '-':
		return sys.stdin
	else:
		return default_open_file_func(table_name)

	# Notice - No need to provide the default_open_file_func parameter when delegating to the default_open_file_func inside the code.


def close_file_func(table_name,f,default_close_file_func):
	f.close()
````

