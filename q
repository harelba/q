#!/usr/bin/python

# Name      : q (With respect to The Q Continuum)
# Author    : Harel Ben Attia - harelba@gmail.com, harelba @ github, @harelba on twitter
# Requires  : python with sqlite3
# Version   : 0.1
#
#
# q allows performing SQL-like statements on tabular text data.
#
# Its purpose is to bring SQL expressive power to manipulating text data using the Linux command line.
#
# Full Documentation and details in https://github.com/harelba/q
#
# Run with --help for command line details
#

import os,sys
import random
import sqlite3
import gzip
import glob
from optparse import OptionParser
import traceback as tb
import codecs
import locale

# Encode stdout properly,
if sys.stdout.isatty():
	STDOUT = codecs.getwriter(sys.stdout.encoding)(sys.stdout)
else:
	STDOUT = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)

SHOW_SQL = False

parser = OptionParser()
parser.add_option("-b","--beautify",dest="beautify",default=False,action="store_true",
                help="Beautify output according to actual values. Might be slow...")
parser.add_option("-z","--gzipped",dest="gzipped",default=False,action="store_true",
                help="Data is gzipped. Useful for reading from stdin. For files, .gz means automatic gunzipping")
parser.add_option("-d","--delimiter",dest="delimiter",default=None,
                help="Field delimiter. If none specified, then standard whitespace is used as a delimiter")
parser.add_option("-t","--tab-delimited-with-header",dest="tab_delimited_with_header",default=False,action="store_true",
                help="Same as -d <tab> -H 1. Just a shorthand for handling standard tab delimited file with one header line at the beginning of the file")
parser.add_option("-H","--header-skip",dest="header_skip",default=0,
                help="Skip n lines at the beginning of the data (still takes those lines into account in terms of structure)")
parser.add_option("-f","--formatting",dest="formatting",default=None,
                help="Output-level formatting, in the format X=fmt,Y=fmt etc, where X,Y are output column numbers (e.g. 1 for first SELECT column etc.")
parser.add_option("-e","--encoding",dest="encoding",default='UTF-8',
                help="Input file encoding. Defaults to UTF-8")

class Sqlite3DB(object):
	def __init__(self,show_sql=SHOW_SQL):
		self.show_sql = show_sql
		self.conn = sqlite3.connect(':memory:')
		self.type_names = { str : 'TEXT' , int : 'INT' , float : 'FLOAT' }

	def execute_and_fetch(self,q):
		try:
			cursor = self.conn.cursor()
			if self.show_sql:
				print q
			cursor.execute(q)
			result = cursor.fetchall()
		finally:
			cursor.close()
		return result

	def generate_insert_row(self,table_name,column_names,col_vals):
		col_names_str = ",".join(["%s" % x for x in column_names])
		col_vals_str = ",".join(['"%s"' % x for x in col_vals])
		return 'INSERT INTO %s (%s) VALUES (%s)' % (table_name,col_names_str,col_vals_str)

	# Get a list of column names so order will be preserved (Could have used OrderedDict, but 
        # then we would need python 2.7)
	def generate_create_table(self,table_name,column_names,column_dict):
		# Convert dict from python types to db types
		column_name_to_db_type = dict((n,self.type_names[t]) for n,t in column_dict.iteritems())
		column_defs = ','.join(['%s %s' % (n,column_name_to_db_type[n]) for n in column_names])
		return 'CREATE TABLE %s (%s)' % (table_name,column_defs)

	def generate_temp_table_name(self):
		return "temp_table_%s" % random.randint(0,1000000000)

	def generate_drop_table(self,table_name):
		return "DROP TABLE %s" % table_name

	def drop_table(self,table_name):
		return self.execute_and_fetch(self.generate_drop_table(table_name))	

class Sql(object):
	def __init__(self,sql):
		# Currently supports only standard SELECT statements

		self.sql = sql
		self.sql_parts = sql.split()
		self.column_list = self.sql_parts[0]
		# Simplistic way to determine table name
		try:
			self.table_name_position = [i+1 for i in range(0,len(self.sql_parts)) if self.sql_parts[i].upper() == 'FROM'][0]
		except:
			raise Exception("Could not detect table name in query")
		self.table_name = self.sql_parts[self.table_name_position]

		self.actual_table_name = None

	def set_effective_table_name(self,actual):
		self.actual_table_name = actual

	def get_effective_sql(self):
		if self.actual_table_name is None:
			raise Exception('Actual table name has not been set')
		effective_sql = [x for x in self.sql_parts]
		effective_sql[self.table_name_position] = self.actual_table_name
		return " ".join(effective_sql)
		
	def execute_and_fetch(self,db):
		return db.execute_and_fetch(self.get_effective_sql())

class LineSplitter(object):
	def __init__(self,delimiter):
		self.delimiter = delimiter

	def split(self,line):
		if line and line[-1] == '\n':
			line = line[:-1]
		if self.delimiter:
			return line.split(self.delimiter)
		else:
			return line.split()

class TableColumnInferer(object):
	def __init__(self,line_splitter):
		self.inferred = False
		self.example_lines = []
		self.line_splitter = line_splitter

	def analyze(self,example_line):
		if self.inferred:
			raise Exception("Already inferred columns")

		# Save the line as an example
		self.example_lines.append(example_line)

		# Column count according to first line only for now
		self.column_count = len(self.line_splitter.split(self.example_lines[0]))

		# FIXME: Hack to provide for some small variation in the column count. Will be fixed as soon as we have better column inferring
		#self.column_count += max(6,int(self.column_count*0.2))
		self.column_count += 5

		if self.column_count == 0:
			raise Exception("Detected a column count of zero... Failing")

		# Only string type for now
		self.column_types = [str for i in range(self.column_count)]
		# Column names are cX starting from 1
		self.column_names = ['c%s' % (i+1) for i in range(self.column_count)]
		# For now, analysis always succeeds
		return True

	def get_column_dict(self):
		return dict(zip(self.column_names,self.column_types))

	def get_column_count(self):
		return self.column_count

	def get_column_names(self):
		return self.column_names

	def get_column_types(self):
		return self.column_types

class TableCreator(object):
	def __init__(self,db,filenames_str,line_splitter,header_skip=0,gzipped=False,encoding='UTF-8'):
		self.db = db
		self.filenames_str = filenames_str
		self.header_skip = header_skip
		self.gzipped = gzipped
		self.table_created = False
		self.line_splitter = line_splitter
		self.encoding = encoding
		self.column_inferer = TableColumnInferer(line_splitter)

		# Filled only after table population since we're inferring the table creation data
		self.table_name = None

	def get_table_name(self):
		return self.table_name

	def populate(self):
		# Get the list of filenames
		filenames = self.filenames_str.split("+")
		# for each filename (or pattern)
		for fileglob in filenames:
			# Allow either stdin or a glob match
			if fileglob == '-':
				files_to_go_over = ['-']
			else:
				files_to_go_over = glob.glob(fileglob)
			# For each match
			for filename in files_to_go_over:
				self.lines_read = 0

				# Check if it's standard input or a file 
				if filename == '-':
					f = sys.stdin
				else:
					f = file(filename,'rb')

				# Wrap it with gzip decompression if needed
				if self.gzipped or filename.endswith('.gz'):
					f = gzip.GzipFile(fileobj=f)

				# And wrap it in an decoder (e.g. ascii, UTF-8 etc)
				f = codecs.getreader(self.encoding)(f)

				# Read all the lines
				try:
					line = f.readline()
					while line:
						self._insert_row(line)
					        line = f.readline()
				finally:
					if f != sys.stdin:
						f.close()

	def _insert_row(self,line):
		# If table has not been created yet
		if not self.table_created:
			# Try to create it along with another "example" line of data
			self.try_to_create_table(line)	

		# If the table is still not created, then we don't have enough data, just return
		if not self.table_created:
			return
		# The table already exists, so we can just add a new row
		self._insert_row_i(line)

	def _insert_row_i(self,line):
		self.lines_read += 1
		if self.lines_read <= self.header_skip:
			return
	        col_vals = line_splitter.split(line)

		# If we have more columns than we inferred
		if len(col_vals) > len(self.column_inferer.column_names):
			raise Exception('Encountered a line in an invalid format - %s columns instead of %s. Did you make sure to set the correct delimiter?' % (len(col_vals),len(self.column_inferer.column_names)))

		effective_column_names = self.column_inferer.column_names[:len(col_vals)]

		insert_row_stmt = self.db.generate_insert_row(self.table_name,effective_column_names,col_vals)
		self.db.execute_and_fetch(insert_row_stmt)

	def try_to_create_table(self,line):
		if self.table_created:
			raise Exception('Table is already created')

		# Add that line to the column inferer
		result = self.column_inferer.analyze(line)
		# If inferer succeeded,
		if result:
			# Then generate a temp table name
			self.table_name = self.db.generate_temp_table_name()
			# Get the column definition dict from the inferer
			column_dict = self.column_inferer.get_column_dict()
			# Create the CREATE TABLE statement
			create_table_stmt = self.db.generate_create_table(self.table_name,self.column_inferer.get_column_names(),column_dict)
			# And create the table itself
			self.db.execute_and_fetch(create_table_stmt)
			# Mark the table as created
			self.table_created = True
		else:
			pass # We don't have enough information for creating the table yet

	def drop_table(self):
		if self.table_created:
			self.db.drop_table(self.table_name)

def determine_max_col_lengths(m):
	if len(m) == 0:
		return []
	max_lengths = [0 for x in xrange(0,len(m[0]))]
	for row_index in xrange(0,len(m)):
		for col_index in xrange(0,len(m[0])):
			new_len = len(str(m[row_index][col_index]))
			if new_len > max_lengths[col_index]:
				max_lengths[col_index] = new_len
	return max_lengths
		
(options,args) = parser.parse_args()
if len(args) != 1:
    parser.print_usage()
    sys.exit(1)
	
# Create DB object
db = Sqlite3DB()

# Create SQL statment (command line is 'select' for now, so we add it manually...)
sql_object = Sql('%s' % args[0])
# Get "table name" which is actually the file name
filename = sql_object.table_name

# If the user flagged for a tab-delimited file then set the delimiter to tab
if options.tab_delimited_with_header:
	options.delimiter = '\t'
	options.header_skip = "1"

# Create a line splitter
line_splitter = LineSplitter(options.delimiter)

# Create the matching database table and populate it
table_creator = TableCreator(db,filename,line_splitter,int(options.header_skip),options.gzipped,options.encoding)
table_creator.populate()

# Replace the logical table name with the real table name
sql_object.set_effective_table_name(table_creator.table_name)

# Execute the query and fetch the data
m = sql_object.execute_and_fetch(db)

# If the user requested beautifying the output
if options.beautify:
	max_lengths = determine_max_col_lengths(m)

if options.delimiter:
	output_delimiter = options.delimiter
else:
	output_delimiter = " "

if options.formatting:
	formatting_dict = dict([(x.split("=")[0],x.split("=")[1]) for x in options.formatting.split(",")])
else:
	formatting_dict = None

try:
	for rownum,row in enumerate(m):
		row_str = []
		for i,col in enumerate(row):
			if formatting_dict is not None and str(i+1) in formatting_dict.keys():
				fmt_str = formatting_dict[str(i+1)]
			else:
				if options.beautify:
					fmt_str = "%%-%ss" % max_lengths[i]
				else:
					fmt_str = "%s"

			if col is not None:
				row_str.append(fmt_str % col)
			else:
				row_str.append(fmt_str % "")

		STDOUT.write(output_delimiter.join(row_str)+"\n")
except KeyboardInterrupt:
	pass

table_creator.drop_table()
