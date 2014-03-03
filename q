#!/usr/bin/env python

#   Copyright (C) 1988, 1998, 2000, 2002, 2004-2005, 2007-2014 Free Software
#   Foundation, Inc.
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 3, or (at your option)
#   any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc.,
#   51 Franklin Street - Fifth Floor, Boston, MA  02110-1301, USA */
#
# 
# Name      : q (With respect to The Q Continuum)
# Author    : Harel Ben Attia - harelba@gmail.com, harelba @ github, @harelba on twitter
# Requires  : python with sqlite3 (standard in python>=2.6)
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
q_version = "1.3.0"

import os,sys
import random
import sqlite3
import gzip
import glob
from optparse import OptionParser
import traceback as tb
import codecs
import locale
import time
import re
from ConfigParser import ConfigParser
import traceback
import csv

DEBUG = False

# Encode stdout properly,
if sys.stdout.isatty():
	STDOUT = codecs.getwriter(sys.stdout.encoding)(sys.stdout)
else:
	STDOUT = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)

SHOW_SQL = False

p = ConfigParser()
p.read([os.path.expanduser('~/.qrc'),'.qrc'])

def get_option_with_default(p,option_type,option,default):
	if not p.has_option('options',option):
		return default
	if option_type == 'boolean':
		return p.getboolean('options',option)
	elif option_type == 'int':
		return p.getint('options',option)
	elif option_type == 'string' : 
		return p.get('options',option)
	elif option_type == 'escaped_string':
		return p.get('options',option).decode('string-escape')
	else:
		raise Exception("Unknown option type")

default_beautify = get_option_with_default(p,'boolean','beautify',False)
default_gzipped = get_option_with_default(p,'boolean','gzipped',False)
default_delimiter = get_option_with_default(p,'escaped_string','delimiter',None)
default_output_delimiter = get_option_with_default(p,'escaped_string','output_delimiter',None)
default_skip_header = get_option_with_default(p,'int','skip_header',0)
default_formatting = get_option_with_default(p,'string','formatting',None)
default_encoding = get_option_with_default(p,'string','encoding','UTF-8')

parser = OptionParser(usage="""
	q allows performing SQL-like statements on tabular text data.

	Its purpose is to bring SQL expressive power to manipulating text data using the Linux command line.

	Basic usage is q "<sql like query>" where table names are just regular file names (Use - to read from standard input)
        When the input contains a header row, use -H, and column names will be set according to the header row content. If there isn't a header row, then columns will automatically be named c1..cN.

	Column types are detected automatically. Use -A in order to see the column name/type analysis.

	Delimiter can be set using the -d (or -t) option. Output delimiter can be set using -D

	All sqlite3 SQL constructs are supported.

	Examples:

          Example 1: ls -ltrd * | q "select c1,count(1) from - group by c1"
	    This example would print a count of each unique permission string in the current folder.

	  Example 2: seq 1 1000 | q "select avg(c1),sum(c1) from -"
	    This example would provide the average and the sum of the numbers in the range 1 to 1000

	  Example 3: sudo find /tmp -ls | q "select c5,c6,sum(c7)/1024.0/1024 as total from - group by c5,c6 order by total desc"
	    This example will output the total size in MB per user+group in the /tmp subtree


        See the help or https://github.com/harelba/q for more details.
""")
parser.add_option("-b","--beautify",dest="beautify",default=default_beautify,action="store_true",
                help="Beautify output according to actual values. Might be slow...")
parser.add_option("-z","--gzipped",dest="gzipped",default=default_gzipped,action="store_true",
                help="Data is gzipped. Useful for reading from stdin. For files, .gz means automatic gunzipping")
parser.add_option("-d","--delimiter",dest="delimiter",default=default_delimiter,
                help="Field delimiter. If none specified, then space is used as the delimiter. If you need multi-character delimiters, run the tool with engine version 1 by adding '-E v1'. Using v1 will also revert to the old behavior where if no delimiter is provided, then any whitespace will be considered as a delimiter.")
parser.add_option("-D","--output-delimiter",dest="output_delimiter",default=default_output_delimiter,
                help="Field delimiter for output. If none specified, then the -d delimiter is used if present, or space if no delimiter is specified")
parser.add_option("-t","--tab-delimited",dest="tab_delimited",default=False,action="store_true",
                help="Same as -d <tab>. Just a shorthand for handling standard tab delimited file with one header line at the beginning of the file. You can use -d $'\t' if you want.")
parser.add_option("-H","--skip-header",dest="skip_header",default=default_skip_header,action="store_true",
                help="Skip header row. This has been changed from earlier version - Only one header row is supported, and the header row is used for column naming")
parser.add_option("-f","--formatting",dest="formatting",default=default_formatting,
                help="Output-level formatting, in the format X=fmt,Y=fmt etc, where X,Y are output column numbers (e.g. 1 for first SELECT column etc.")
parser.add_option("-e","--encoding",dest="encoding",default=default_encoding,
                help="Input file encoding. Defaults to UTF-8. set to none for not setting any encoding - faster, but at your own risk...")
parser.add_option("-v","--version",dest="version",default=False,action="store_true",
                help="Print version")
parser.add_option("-A","--analyze-only",dest="analyze_only",action='store_true',
                help="Analyze sample input and provide information about data types")
parser.add_option("-m","--mode",dest="mode",default="relaxed",
                help="Data parsing mode. fluffy, relaxed and strict. In relaxed and strict mode, the -c column-count parameter must be supplied as well")
parser.add_option("-c","--column-count",dest="column_count",default=None,
                help="Specific column count when using relaxed or strict mode")
parser.add_option("-k","--keep-leading-whitespace",dest="keep_leading_whitespace_in_values",default=False,action="store_true",
                help="Keep leading whitespace in values. Default behavior strips leading whitespace off values, in order to provide out-of-the-box usability for simple use cases. If you need to preserve whitespace, use this flag.")


def regexp(regular_expression, data):
    return re.search(regular_expression, data) is not None
    
class Sqlite3DB(object):
	def __init__(self,show_sql=SHOW_SQL):
		self.show_sql = show_sql
		self.conn = sqlite3.connect(':memory:')
		self.cursor = self.conn.cursor()
		self.type_names = { str : 'TEXT' , int : 'INT' , float : 'FLOAT' , None : 'TEXT' }
		self.add_user_functions()

	def add_user_functions(self):
		self.conn.create_function("regexp", 2, regexp)

	def execute_and_fetch(self,q):
		try:
			if self.show_sql:
				print q
			self.cursor.execute(q)
			result = self.cursor.fetchall()
		finally:
			pass#cursor.close()
		return result

	def _get_as_list_str(self,l):
		return ",".join(['"%s"' % x.replace('"','""') for x in l])

	def _get_col_values_as_list_str(self,col_vals,col_types):
		result = []
		for col_val,col_type in zip(col_vals,col_types):
			if col_val == '' and col_type is not str:
				col_val = "null"
			else:
				if col_val is not None:
					if "'" in col_val:
						col_val = col_val.replace("'","''")
					col_val = "'" + col_val + "'"
				else:
					col_val = "null"

			result.append(col_val)
		return ",".join(result)
			
	def generate_insert_row(self,table_name,column_names,column_types,col_vals):
		col_names_str = self._get_as_list_str(column_names)
		col_vals_str = self._get_col_values_as_list_str(col_vals,column_types)
		return 'INSERT INTO %s (%s) VALUES (%s)' % (table_name,col_names_str,col_vals_str)

	def generate_begin_transaction(self):
		return "BEGIN TRANSACTION"

	def generate_end_transaction(self):
		return "COMMIT"

	# Get a list of column names so order will be preserved (Could have used OrderedDict, but 
        # then we would need python 2.7)
	def generate_create_table(self,table_name,column_names,column_dict):
		# Convert dict from python types to db types
		column_name_to_db_type = dict((n,self.type_names[t]) for n,t in column_dict.iteritems())
		column_defs = ','.join(['"%s" %s' % (n.replace('"','""'),column_name_to_db_type[n]) for n in column_names])
		return 'CREATE TABLE %s (%s)' % (table_name,column_defs)

	def generate_temp_table_name(self):
		return "temp_table_%s" % random.randint(0,1000000000)

	def generate_drop_table(self,table_name):
		return "DROP TABLE %s" % table_name

	def drop_table(self,table_name):
		return self.execute_and_fetch(self.generate_drop_table(table_name))	

class BadHeaderException(Exception):
	def __init__(self,msg):
		self.msg = msg

	def __str(self):
		return repr(self.msg)

class EmptyDataException(Exception):
	def __init__(self):
		pass

class FileNotFoundException(Exception):
	def __init__(self,msg):
		self.msg = msg

	def __str(self):
		return repr(self.msg)


class ColumnCountMismatchException(Exception):
	def __init__(self,msg):
		self.msg = msg

	def __str(self):
		return repr(self.msg)

# Simplistic Sql "parsing" class... We'll eventually require a real SQL parser which will provide us with a parse tree
#
# A "qtable" is a filename which behaves like an SQL table...
class Sql(object):

	def __init__(self,sql):
		# Currently supports only standard SELECT statements

		# Holds original SQL
		self.sql = sql
		# Holds sql parts 
		self.sql_parts = sql.split()
	
		# Set of qtable names
		self.qtable_names = set()
		# Dict from qtable names to their positions in sql_parts. Value here is a *list* of positions,
		# since it is possible that the same qtable_name (file) is referenced in multiple positions
		# and we don't want the database table to be recreated for each reference
		self.qtable_name_positions = {}
		# Dict from qtable names to their effective (actual database) table names
		self.qtable_name_effective_table_names = {}

		# Go over all sql parts
		idx = 0
		while idx < len(self.sql_parts):
			# Get the part string
			part = self.sql_parts[idx]
			# If it's a FROM or a JOIN
			if part.upper() in ['FROM','JOIN']:
				# and there is nothing after it,
				if idx == len(self.sql_parts)-1:
					# Just fail
					raise Exception('FROM/JOIN is missing a table name after it')
				
				
				qtable_name = self.sql_parts[idx+1]
				# Otherwise, the next part contains the qtable name. In most cases the next part will be only the qtable name.
				# We handle one special case here, where this is a subquery as a column: "SELECT (SELECT ... FROM qtable),100 FROM ...". 
				# In that case, there will be an ending paranthesis as part of the name, and we want to handle this case gracefully.
				# This is obviously a hack of a hack :) Just until we have complete parsing capabilities
				if ')' in qtable_name:
					leftover = qtable_name[qtable_name.index(')'):]
					self.sql_parts.insert(idx+2,leftover)
					qtable_name = qtable_name[:qtable_name.index(')')]
					self.sql_parts[idx+1] = qtable_name
					

				self.qtable_names.add(qtable_name)

				if qtable_name not in self.qtable_name_positions.keys():
					self.qtable_name_positions[qtable_name] = []
				
				self.qtable_name_positions[qtable_name].append(idx+1)
				idx += 2
			else:
				idx += 1

	def set_effective_table_name(self,qtable_name,effective_table_name):
		if qtable_name not in self.qtable_names:
			raise Exception("Unknown qtable %s" % qtable_name)
		if qtable_name in self.qtable_name_effective_table_names.keys():
			raise Exception("Already set effective table name for qtable %s" % qtable_name)

		self.qtable_name_effective_table_names[qtable_name] = effective_table_name

	def get_effective_sql(self):
		if len(filter(lambda x: x is None,self.qtable_name_effective_table_names)) != 0:
			raise Exception('There are qtables without effective tables')
		
		effective_sql = [x for x in self.sql_parts]

		for qtable_name,positions in self.qtable_name_positions.iteritems():
			for pos in positions:
				effective_sql[pos] = self.qtable_name_effective_table_names[qtable_name]

		return " ".join(effective_sql)
		
	def execute_and_fetch(self,db):
		return db.execute_and_fetch(self.get_effective_sql())

class LineSplitter(object):
	def __init__(self,delimiter,expected_column_count):
		self.delimiter = delimiter
		self.expected_column_count = expected_column_count
		if delimiter is not None:
			escaped_delimiter = re.escape(delimiter)
			self.split_regexp = re.compile('(?:%s)+' % escaped_delimiter)
		else:
			self.split_regexp = re.compile(r'\s+')

	def split(self,line):
		if line and line[-1] == '\n':
			line = line[:-1]
		return self.split_regexp.split(line,max_split=self.expected_column_count)

class TableColumnInferer(object):
	def __init__(self,mode,expected_column_count,input_delimiter,skip_header=False):
		self.inferred = False
		self.mode = mode
		self.rows = []
		self.skip_header = skip_header
		self.header_row = None
		self.expected_column_count = expected_column_count
		self.input_delimiter = input_delimiter

	def analyze(self,col_vals):
		if self.inferred:
			raise Exception("Already inferred columns")

		if self.skip_header and self.header_row is None:
			self.header_row = col_vals
		else:
			self.rows.append(col_vals)

		if len(self.rows) < 100:
			return False

		self.do_analysis()
		return True

	def force_analysis(self):
		# This method is called whenever there is no more data, and an analysis needs
		# to be performed immediately, regardless of the amount of sample data that has
		# been collected
		self.do_analysis()

	def determine_type_of_value(self,value):
		if value is not None:
			value = value.strip()
		if value == '' or value is None:
			return None

		try:
			i = int(value)
			if type(i) == long:
				return long
			else:
				return int
		except:
			pass

		try:
			f = float(value)
			return float
		except:
			pass

		return str

	def determine_type_of_value_list(self,value_list):
		type_list = [self.determine_type_of_value(v) for v in value_list]
		all_types = set(type_list)
		if len(set(type_list)) == 1:
			# all the sample lines are of the same type
			return type_list[0]
		else:
			# check for the number of types without nulls,
			type_list_without_nulls = filter(lambda x: x is not None,type_list)
			# If all the sample lines are of the same type, 
			if len(set(type_list_without_nulls)) == 1:
				# return it
				return type_list_without_nulls[0]
			else:
				return str

	def do_analysis(self):
		if self.mode == 'strict':
			self._do_strict_analysis()
		elif self.mode in ['relaxed','fluffy']:
			self._do_relaxed_analysis()
		else:
			raise Exception('Unknown parsing mode %s' % self.mode)

		if self.column_count == 1:
			print >>sys.stderr,"Warning: column count is one - did you provide the correct delimiter?"
		if self.column_count == 0:
			raise Exception("Detected a column count of zero... Failing")

		self.infer_column_types()

		self.infer_column_names()

	def validate_column_names(self,value_list):
		column_name_errors = []
		for v in value_list:
			if v is None:
				# we allow column names to be None, in relaxed mode it'll be filled with default names.
				#RLRL
				continue
			if ',' in v:
				column_name_errors.append((v,"Column name cannot contain commas"))
				continue
			if self.input_delimiter in v:
				column_name_errors.append((v,"Column name cannot contain the input delimiter. Please make sure you've set the correct delimiter"))
				continue
			if '\n' in v:
				column_name_errors.append((v,"Column name cannot contain newline"))
				continue
			if v != v.strip():
				column_name_errors.append((v,"Column name contains leading/trailing spaces"))
				continue
			try:
				v.encode("utf-8","strict").decode("utf-8")
			except:
				column_name_errors.append((v,"Column name must be UTF-8 Compatible"))
				continue
			nul_index = v.find("\x00")
			if nul_index >= 0:
				column_name_errors.append((v,"Column name cannot contain NUL"))		
				continue
			t = self.determine_type_of_value(v)
			if t != str:
				column_name_errors.append((v,"Column name must be a string"))
		return column_name_errors
	
	def infer_column_names(self):
		if self.header_row is not None:
			column_name_errors = self.validate_column_names(self.header_row)
			if len(column_name_errors) > 0:
				raise BadHeaderException("Header must contain only strings and not numbers or empty strings: '%s'\n%s" % (",".join(self.header_row),"\n".join(["'%s': %s" % (x,y) for x,y in column_name_errors])))

			# use header row in order to name columns
			if len(self.header_row) < self.column_count:
				if self.mode == 'strict':
					raise ColumnCountMismatchException("Strict mode. Header row contains less columns than expected column count(%s vs %s)" % (len(self.header_row),self.column_count))
				elif self.mode in ['relaxed','fluffy']:
					# in relaxed mode, add columns to fill the missing ones
					self.header_row = self.header_row + ['c%s' % (x+len(self.header_row)+1) for x in xrange(self.column_count - len(self.header_row))]
			elif len(self.header_row) > self.column_count:
				if self.mode == 'strict':
					raise ColumnCountMismatchException("Strict mode. Header row contains more columns than expected column count (%s vs %s)" % (len(self.header_row),self.column_count))
				elif self.mode in ['relaxed','fluffy']:
					# In relaxed mode, just cut the extra column names
					self.header_row = self.header_row[:self.column_count]
			self.column_names = self.header_row
		else:
			# Column names are cX starting from 1
			self.column_names = ['c%s' % (i+1) for i in range(self.column_count)]

	def _do_relaxed_analysis(self):
		column_count_list = [len(col_vals) for col_vals in self.rows]

		if self.expected_column_count is not None:
			self.column_count = self.expected_column_count
		else:
			# If not specified, we'll take the largest row in the sample rows
			self.column_count = max(column_count_list)

	def get_column_count_summary(self,column_count_list):
		counts = {}
		for column_count in column_count_list:
			counts[column_count] = counts.get(column_count,0) + 1
		return ", ".join(["%s rows with %s columns" % (v,k) for k,v in counts.iteritems()])

	def _do_strict_analysis(self):
		column_count_list = [len(col_vals) for col_vals in self.rows]

		if len(set(column_count_list)) != 1:
			raise ColumnCountMismatchException('Strict mode. Column Count is expected to identical. Multiple column counts exist at the first part of the file. Try to check your delimiter, or change to relaxed mode. Details: %s' % (self.get_column_count_summary(column_count_list)))

		self.column_count = len(self.rows[0])

		if self.expected_column_count is not None and self.column_count != self.expected_column_count:
			raise ColumnCountMismatchException('Strict mode. Column count is expected to be %s but is %s' % (self.expected_column_count,self.column_count))

		self.infer_column_types()

	def infer_column_types(self):
		self.column_types = []
		self.column_types2 = []
		for column_number in xrange(self.column_count):
			column_value_list = [row[column_number] if column_number < len(row) else None for row in self.rows]
			column_type = self.determine_type_of_value_list(column_value_list)
			self.column_types.append(column_type)

			column_value_list2 = [row[column_number] if column_number < len(row) else None for row in self.rows[1:]]
			column_type2 = self.determine_type_of_value_list(column_value_list2)
			self.column_types2.append(column_type2)

		comparison = map(lambda x: x[0] == x[1],zip(self.column_types,self.column_types2))
		if False in comparison and not self.skip_header:
			number_of_column_types = len(set(self.column_types))
			if number_of_column_types == 1 and list(set(self.column_types))[0] == str:
				print >>sys.stderr,'Warning - There seems to be header line in the file, but -H has not been specified. All fields will be detected as text fields, and the header line will appear as part of the data'

	def get_column_dict(self):
		return dict(zip(self.column_names,self.column_types))

	def get_column_count(self):
		return self.column_count

	def get_column_names(self):
		return self.column_names

	def get_column_types(self):
		return self.column_types

def encoded_csv_reader(encoding,f,dialect,**kwargs):
	csv_reader = csv.reader(f,dialect,**kwargs)
	if encoding is not None and encoding != 'none':
		for row in csv_reader:
			yield [unicode(x,encoding) for x in row]
	else:
		for row in csv_reader:
			yield row

def normalized_filename(filename):
	if filename == '-':
		return 'stdin'
	else:
		return filename

class TableCreator(object):
	def __init__(self,db,filenames_str,line_splitter,skip_header=False,gzipped=False,encoding='UTF-8',mode='fluffy',expected_column_count=None,input_delimiter=None):
		self.db = db
		self.filenames_str = filenames_str
		self.skip_header = skip_header
		self.gzipped = gzipped
		self.table_created = False
		self.line_splitter = line_splitter
		self.encoding = encoding
		self.mode = mode
		self.expected_column_count = expected_column_count
		self.input_delimiter = input_delimiter
		self.column_inferer = TableColumnInferer(mode,expected_column_count,input_delimiter,skip_header)

		# Filled only after table population since we're inferring the table creation data
		self.table_name = None

		self.pre_creation_rows = []
		self.buffered_inserts = []

	def get_table_name(self):
		return self.table_name

	def populate(self,analyze_only=False):
		# Get the list of filenames
		filenames = self.filenames_str.split("+")
		# for each filename (or pattern)
		for fileglob in filenames:
			# Allow either stdin or a glob match
			if fileglob == '-':
				files_to_go_over = ['-']
			else:
				files_to_go_over = glob.glob(fileglob)

			# If there are no files to go over,
			if len(files_to_go_over) == 0:
				raise FileNotFoundException("File %s has not been found" % fileglob)

			# For each match
			for filename in files_to_go_over:
				self.current_filename = filename
				self.lines_read = 0

				# Check if it's standard input or a file 
				if filename == '-':
					f = sys.stdin
				else:
					f = file(filename,'rb')

				# Wrap it with gzip decompression if needed
				if self.gzipped or filename.endswith('.gz'):
					f = gzip.GzipFile(fileobj=f)

				self.read_file_using_csv(f,analyze_only)
				if not self.table_created:
					self.column_inferer.force_analysis()
					self._do_create_table()

	def _flush_pre_creation_rows(self):
		for i,col_vals in enumerate(self.pre_creation_rows):
			if self.skip_header and i == 0:
				# skip header line
				continue
			self._insert_row(col_vals)
		self._flush_inserts()
		self.pre_creation_rows = []

	def read_file_using_csv(self,f,analyze_only):
		csv_reader = encoded_csv_reader(self.encoding,f,dialect='q')
		try:
			for col_vals in csv_reader:
				self.lines_read += 1
				self._insert_row(col_vals)
				if analyze_only and self.column_inferer.inferred:
					return
			if self.lines_read == 0 or (self.lines_read == 1 and self.skip_header):
				raise EmptyDataException()
		finally:
			if f != sys.stdin:
				f.close()
			self._flush_inserts()

	def _insert_row(self,col_vals):
		# If table has not been created yet
		if not self.table_created:
			# Try to create it along with another "example" line of data
			self.try_to_create_table(col_vals)

		# If the table is still not created, then we don't have enough data, just
		# store the data and return
		if not self.table_created:
			self.pre_creation_rows.append(col_vals)
			return

		# The table already exists, so we can just add a new row
		self._insert_row_i(col_vals)

	def normalize_col_vals(self,col_vals):
		expected_col_count = self.column_inferer.get_column_count()
		actual_col_count = len(col_vals)
		if self.mode == 'strict':
			if actual_col_count != expected_col_count:
				raise ColumnCountMismatchException('Strict mode - Expected %s columns instead of %s columns in file %s row %s. Either use relaxed/fluffy modes or check your delimiter' % (expected_col_count,actual_col_count,normalized_filename(self.current_filename),self.lines_read))
			return col_vals

		# in all non strict mode, we add dummy data to missing columns

		if actual_col_count < expected_col_count:
			col_vals = col_vals + [None for x in xrange(expected_col_count - actual_col_count)]


		# in relaxed mode, we merge all extra columns to the last column value
		if self.mode == 'relaxed':
			if actual_col_count > expected_col_count:
				xxx = col_vals[:expected_col_count-1] + [self.input_delimiter.join(col_vals[expected_col_count-1:])]
				return xxx
			else:
				return col_vals

		if self.mode == 'fluffy':
			if actual_col_count > expected_col_count:
				raise ColumnCountMismatchException('Deprecated fluffy mode - Too many columns in file %s row %s (%s fields instead of %s fields). Consider moving to either relaxed or strict mode' % (normalized_filename(self.current_filename),self.lines_read,actual_col_count,expected_col_count))
			return col_vals

		raise Exception("Unidentified parsing mode %s" % self.mode)
		
	def _insert_row_i(self,col_vals):
		col_vals = self.normalize_col_vals(col_vals)
		effective_column_names = self.column_inferer.column_names[:len(col_vals)]

		if len(effective_column_names) > 0:
			self.buffered_inserts.append((effective_column_names,col_vals))
		else:
			self.buffered_inserts.append((["c1"],[""]))

		if len(self.buffered_inserts) < 1000:
			return
		self._flush_inserts()

	def _flush_inserts(self):
		#print self.db.execute_and_fetch(self.db.generate_begin_transaction())

		# If the table is still not created, then we don't have enough data
		if not self.table_created:
			return

		col_types = self.column_inferer.get_column_types()

		for col_names,col_vals in self.buffered_inserts:
			insert_row_stmt = self.db.generate_insert_row(self.table_name,col_names,col_types,col_vals)
			self.db.execute_and_fetch(insert_row_stmt)

		#print self.db.execute_and_fetch(self.db.generate_end_transaction())
		self.buffered_inserts = []


	def try_to_create_table(self,col_vals):
		if self.table_created:
			raise Exception('Table is already created')

		# Add that line to the column inferer
		result = self.column_inferer.analyze(col_vals)
		# If inferer succeeded,
		if result:
			self._do_create_table()
		else:
			pass # We don't have enough information for creating the table yet

	def _do_create_table(self):
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
		self._flush_pre_creation_rows()

	def drop_table(self):
		if self.table_created:
			self.db.drop_table(self.table_name)

def determine_max_col_lengths(m):
	if len(m) == 0:
		return []
	max_lengths = [0 for x in xrange(0,len(m[0]))]
	for row_index in xrange(0,len(m)):
		for col_index in xrange(0,len(m[0])):
			new_len = len(unicode(m[row_index][col_index]))
			if new_len > max_lengths[col_index]:
				max_lengths[col_index] = new_len
	return max_lengths
		
(options,args) = parser.parse_args()

if options.version:
	print "q version %s" % q_version
	sys.exit(0)

if len(args) != 1:
    parser.print_help()
    sys.exit(1)
	
if options.mode not in ['fluffy','relaxed','strict']:
	print >>sys.stderr,"Parsing mode can be one of fluffy, relaxed or strict"
	sys.exit(13)

# Create DB object
db = Sqlite3DB()

# Create SQL statment 
sql_object = Sql('%s' % args[0])

# If the user flagged for a tab-delimited file then set the delimiter to tab
if options.tab_delimited:
	options.delimiter = '\t'

if options.delimiter is None:
	options.delimiter = ' '
elif len(options.delimiter) != 1:
	print >>sys.stderr,"Delimiter must be one character only"
	sys.exit(5)

if options.keep_leading_whitespace_in_values:
	skip_initial_space = False
else:
	skip_initial_space = True

q_dialect = {'skipinitialspace': skip_initial_space, 'quoting': 0, 'delimiter': options.delimiter, 'quotechar': '"', 'doublequote': False}
csv.register_dialect('q',**q_dialect)
file_reading_method = 'csv'

if options.column_count is not None:
	expected_column_count = int(options.column_count)
else:
	# infer automatically
	expected_column_count = None

# Create a line splitter
line_splitter = LineSplitter(options.delimiter,expected_column_count)

if options.encoding != 'none':
	try:
		codecs.lookup(options.encoding)
	except LookupError:
		print >>sys.stderr,"Encoding %s could not be found" % options.encoding
		sys.exit(10)

try:
	table_creators = []
	# Get each "table name" which is actually the file name
	for filename in sql_object.qtable_names:
		# Create the matching database table and populate it
		table_creator = TableCreator(db,filename,line_splitter,options.skip_header,options.gzipped,options.encoding,mode=options.mode,expected_column_count=expected_column_count,input_delimiter=options.delimiter)
		start_time = time.time()
		table_creator.populate(options.analyze_only)
		table_creators.append(table_creator)
		if DEBUG:
			print >>sys.stderr,"TIMING - populate time is %4.3f" % (time.time() - start_time)

		# Replace the logical table name with the real table name
		sql_object.set_effective_table_name(filename,table_creator.table_name)

	if options.analyze_only:
		for table_creator in table_creators:
			column_names = table_creator.column_inferer.get_column_names()
			print "Table for file: %s" % normalized_filename(table_creator.filenames_str)
			for k in column_names:
				column_type = table_creator.column_inferer.get_column_dict()[k]
				print "  `%s` - %s" % (k,db.type_names[column_type].lower())
		sys.exit(0)

	# Execute the query and fetch the data
	m = sql_object.execute_and_fetch(db)
except EmptyDataException:
	print >>sys.stderr,"Warning - data is empty"
	sys.exit(0)
except FileNotFoundException,e:
	print >>sys.stderr,e.msg
	sys.exit(30)
except sqlite3.OperationalError,e:
	msg = str(e)
	print >>sys.stderr,"query error: %s" % msg
	if "no such column" in msg and options.skip_header:
		print >>sys.stderr,'Warning - There seems to be a "no such column" error, and -H (header line) exists. Please make sure that you are using the column names from the header line and not the default (cXX) column names'
	sys.exit(1)
except ColumnCountMismatchException,e:
	print >>sys.stderr,e.msg
	sys.exit(2)
except (UnicodeDecodeError,UnicodeError),e:
	print >>sys.stderr,"Cannot decode data. Try to change the encoding by setting it using the -e parameter. Error:%s" % e
	sys.exit(3)
except BadHeaderException,e:
	print >>sys.stderr,"Bad header row: %s" % e.msg
	sys.exit(35)
except KeyboardInterrupt:
	print >>sys.stderr,"Interrupted"
	sys.exit(0)


# If the user requested beautifying the output
if options.beautify:
	max_lengths = determine_max_col_lengths(m)

if options.output_delimiter:
	# If output delimiter is specified, then we use it
	output_delimiter = options.output_delimiter
else:
	# Otherwise, 
	if options.delimiter:
		# if an input delimiter is specified, then we use it as the output as well
		output_delimiter = options.delimiter
	else:
		# if no input delimiter is specified, then we use space as the default
		# (since no input delimiter means any whitespace)
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
except IOError,e:
	if e.errno == 32:
		# broken pipe, that's ok
		pass
	else:
		# dont miss other problem for now
		raise
except KeyboardInterrupt:
	pass

try:
	# Prevent python bug when order of pipe shutdowns is reversed
	sys.stdout.flush()
except IOError,e:
	pass


table_creator.drop_table()
