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
q_version = "1.2.0"

import os, sys
import random
import sqlite3
import gzip
import glob
from optparse import OptionParser
import codecs
import locale
import time
import re
from ConfigParser import ConfigParser
import csv

DEBUG = False

# Encode stdout properly,
if sys.stdout.isatty():
    STDOUT = codecs.getwriter(sys.stdout.encoding)(sys.stdout)
else:
    STDOUT = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)

SHOW_SQL = False

p = ConfigParser()
p.read([os.path.expanduser('~/.qrc'), '.qrc'])

def get_option_with_default(p, option_type, option, default):
    if not p.has_option('options', option):
        return default
    if option_type == 'boolean':
        return p.getboolean('options', option)
    elif option_type == 'int':
        return p.getint('options', option)
    elif option_type == 'string' : 
        return p.get('options', option)
    elif option_type == 'escaped_string':
        return p.get('options', option).decode('string-escape')
    else:
        raise Exception("Unknown option type")

default_beautify = get_option_with_default(p, 'boolean', 'beautify', False)
default_gzipped = get_option_with_default(p, 'boolean', 'gzipped', False)
default_delimiter = get_option_with_default(p, 'escaped_string', 'delimiter', None)
default_output_delimiter = get_option_with_default(p, 'escaped_string', 'output_delimiter', None)
default_header_skip = get_option_with_default(p, 'int', 'header_skip', 0)
default_formatting = get_option_with_default(p, 'string', 'formatting', None)
default_encoding = get_option_with_default(p, 'string', 'encoding', 'UTF-8')

parser = OptionParser(usage="""
    q allows performing SQL-like statements on tabular text data.

    Its purpose is to bring SQL expressive power to manipulating text data using the Linux command line.

    Basic usage is q "<sql like query>" where table names are just regular file names (Use - to read from standard input)
        Columns are named c1..cN and delimiter can be set using the -d (or -t) option.

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
parser.add_option("-b", "--beautify", dest="beautify", default=default_beautify, action="store_true",
                help="Beautify output according to actual values. Might be slow...")
parser.add_option("-z", "--gzipped", dest="gzipped", default=default_gzipped, action="store_true",
                help="Data is gzipped. Useful for reading from stdin. For files, .gz means automatic gunzipping")
parser.add_option("-d", "--delimiter", dest="delimiter", default=default_delimiter,
                help="Field delimiter. If none specified, then space is used as the delimiter. If you need multi-character delimiters, run the tool with engine version 1 by adding '-E v1'. Using v1 will also revert to the old behavior where if no delimiter is provided, then any whitespace will be considered as a delimiter.")
parser.add_option("-D", "--output-delimiter", dest="output_delimiter", default=default_output_delimiter,
                help="Field delimiter for output. If none specified, then the -d delimiter is used if present, or space if no delimiter is specified")
parser.add_option("-t", "--tab-delimited-with-header", dest="tab_delimited_with_header", default=False, action="store_true",
                help="Same as -d <tab> -H 1. Just a shorthand for handling standard tab delimited file with one header line at the beginning of the file")
parser.add_option("-H", "--header-skip", dest="header_skip", default=default_header_skip,
                help="Skip n lines at the beginning of the data (still takes those lines into account in terms of structure)")
parser.add_option("-f", "--formatting", dest="formatting", default=default_formatting,
                help="Output-level formatting, in the format X=fmt,Y=fmt etc, where X,Y are output column numbers (e.g. 1 for first SELECT column etc.")
parser.add_option("-e", "--encoding", dest="encoding", default=default_encoding,
                help="Input file encoding. Defaults to UTF-8. set to none for not setting any encoding - faster, but at your own risk...")
parser.add_option("-v", "--version", dest="version", default=False, action="store_true",
                help="Print version")
parser.add_option("-E", "--engine-version", dest="engine_version", default='v2',
                help="Engine version to use. v2 is the default, and supports quoted CSVs, but requires a specific delimiter (can't use multi-character delimiters). Use v1 if you need multi-char delimiters or if you encounter any problems (and please notify me if you do)")


def regexp(regular_expression, data):
    return re.search(regular_expression, data) is not None
    
class Sqlite3DB(object):
    def __init__(self, show_sql=SHOW_SQL):
        self.show_sql = show_sql
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()
        self.type_names = { str : 'TEXT' , int : 'INT' , float : 'FLOAT' }
        self.add_user_functions()

    def add_user_functions(self):
        self.conn.create_function("regexp", 2, regexp)

    def execute_and_fetch(self, q):
        try:
            if self.show_sql:
                print q
            self.cursor.execute(q)
            result = self.cursor.fetchall()
        finally:
            pass  # cursor.close()
        return result

    def _get_as_list_str(self, l, quote=False):
        if not quote:
            return ",".join(["%s" % x for x in l])
        else:
            # Quote the list items, and escape relevant strings
            # Unfortunately, using list comprehension and performing replace on all elements here slows things down, so this is an optimization
            def quote_and_escape(x):
                if "'" in x:
                    x = x.replace("'", "''")
                return "'" + x + "'"

            return ",".join(map(quote_and_escape, l))

    def generate_insert_row(self, table_name, column_names, col_vals):
        col_names_str = self._get_as_list_str(column_names)
        col_vals_str = self._get_as_list_str(col_vals, quote=True)
        return 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, col_names_str, col_vals_str)

    def generate_begin_transaction(self):
        return "BEGIN TRANSACTION"

    def generate_end_transaction(self):
        return "COMMIT"

    # Get a list of column names so order will be preserved (Could have used OrderedDict, but 
    # then we would need python 2.7)
    def generate_create_table(self, table_name, column_names, column_dict):
        # Convert dict from python types to db types
        column_name_to_db_type = dict((n, self.type_names[t]) for n, t in column_dict.iteritems())
        column_defs = ','.join(['%s %s' % (n, column_name_to_db_type[n]) for n in column_names])
        return 'CREATE TABLE %s (%s)' % (table_name, column_defs)

    def generate_temp_table_name(self):
        return "temp_table_%s" % random.randint(0, 1000000000)

    def generate_drop_table(self, table_name):
        return "DROP TABLE %s" % table_name

    def drop_table(self, table_name):
        return self.execute_and_fetch(self.generate_drop_table(table_name))    

class ColumnCountMismatchException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

# Simplistic Sql "parsing" class... We'll eventually require a real SQL parser which will provide us with a parse tree
#
# A "qtable" is a filename which behaves like an SQL table...
class Sql(object):

    def __init__(self, sql):
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
            if part.upper() in ['FROM', 'JOIN']:
                # and there is nothing after it,
                if idx == len(self.sql_parts) - 1:
                    # Just fail
                    raise Exception('FROM/JOIN is missing a table name after it')
                
                
                qtable_name = self.sql_parts[idx + 1]
                # Otherwise, the next part contains the qtable name. In most cases the next part will be only the qtable name.
                # We handle one special case here, where this is a subquery as a column: "SELECT (SELECT ... FROM qtable),100 FROM ...". 
                # In that case, there will be an ending paranthesis as part of the name, and we want to handle this case gracefully.
                # This is obviously a hack of a hack :) Just until we have complete parsing capabilities
                if ')' in qtable_name:
                    leftover = qtable_name[qtable_name.index(')'):]
                    self.sql_parts.insert(idx + 2, leftover)
                    qtable_name = qtable_name[:qtable_name.index(')')]
                    self.sql_parts[idx + 1] = qtable_name
                    

                self.qtable_names.add(qtable_name)

                if qtable_name not in self.qtable_name_positions.keys():
                    self.qtable_name_positions[qtable_name] = []
                
                self.qtable_name_positions[qtable_name].append(idx + 1)
                idx += 2
            else:
                idx += 1

    def set_effective_table_name(self, qtable_name, effective_table_name):
        if qtable_name not in self.qtable_names:
            raise Exception("Unknown qtable %s" % qtable_name)
        if qtable_name in self.qtable_name_effective_table_names.keys():
            raise Exception("Already set effective table name for qtable %s" % qtable_name)

        self.qtable_name_effective_table_names[qtable_name] = effective_table_name

    def get_effective_sql(self):
        if len(filter(lambda x: x is None, self.qtable_name_effective_table_names)) != 0:
            raise Exception('There are qtables without effective tables')
        
        effective_sql = [x for x in self.sql_parts]

        for qtable_name, positions in self.qtable_name_positions.iteritems():
            for pos in positions:
                effective_sql[pos] = self.qtable_name_effective_table_names[qtable_name]

        return " ".join(effective_sql)
        
    def execute_and_fetch(self, db):
        return db.execute_and_fetch(self.get_effective_sql())

class LineSplitter(object):
    def __init__(self, delimiter):
        self.delimiter = delimiter
        if options.delimiter is not None:
            escaped_delimiter = re.escape(delimiter)
            self.split_regexp = re.compile('(?:%s)+' % escaped_delimiter)
        else:
            self.split_regexp = re.compile(r'\s+')

    def split(self, line):
        if line and line[-1] == '\n':
            line = line[:-1]
        return self.split_regexp.split(line)

class TableColumnInferer(object):
    def __init__(self):
        self.inferred = False
        self.example_col_vals = []

    def analyze(self, example_col_vals):
        if self.inferred:
            raise Exception("Already inferred columns")

        # Save the line as an example
        self.example_col_vals.append(example_col_vals)

        # Column count is taken from the first row only, for now
        self.column_count = len(example_col_vals)

        if self.column_count == 1:
            print >> sys.stderr, "Warning: column count is one - did you provide the correct delimiter?"
        if self.column_count == 0:
            raise Exception("Detected a column count of zero... Failing")

        # FIXME: Hack to provide for some small variation in the column count. Will be fixed as soon as we have better column inferring
        # self.column_count += max(6,int(self.column_count*0.2))
        self.column_count += 7

        # Only string type for now
        self.column_types = [str for _ in range(self.column_count)]
        # Column names are cX starting from 1
        self.column_names = ['c%s' % (i + 1) for i in range(self.column_count)]
        # For now, analysis always succeeds
        return True

    def get_column_dict(self):
        return dict(zip(self.column_names, self.column_types))

    def get_column_count(self):
        return self.column_count

    def get_column_names(self):
        return self.column_names

    def get_column_types(self):
        return self.column_types

def encoded_csv_reader(encoding, f, dialect, **kwargs):
    csv_reader = csv.reader(f, dialect, **kwargs)
    if encoding is not None and encoding != 'none':
        for row in csv_reader:
            yield [unicode(x, encoding) for x in row]
    else:
        for row in csv_reader:
            yield row

class TableCreator(object):
    def __init__(self, db, filenames_str, line_splitter, header_skip=0, gzipped=False, encoding='UTF-8', file_reading_method='manual'):
        self.db = db
        self.filenames_str = filenames_str
        self.header_skip = header_skip
        self.gzipped = gzipped
        self.table_created = False
        self.line_splitter = line_splitter
        self.encoding = encoding
        self.column_inferer = TableColumnInferer()

        # Filled only after table population since we're inferring the table creation data
        self.table_name = None

        self.buffered_inserts = []

        self.file_reading_method = file_reading_method

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

            # If there are no files to go over,
            if len(files_to_go_over) == 0:
                raise Exception("File has not been found '%s'" % fileglob)

            if self.file_reading_method == 'csv':
                read_file_method = self.read_file_using_csv
            else:
                read_file_method = self.read_file_manually

            # For each match
            for filename in files_to_go_over:
                self.current_filename = filename
                self.lines_read = 0

                # Check if it's standard input or a file 
                if filename == '-':
                    f = sys.stdin
                else:
                    f = file(filename, 'rb')

                # Wrap it with gzip decompression if needed
                if self.gzipped or filename.endswith('.gz'):
                    f = gzip.GzipFile(fileobj=f)

                read_file_method(f)
                if not self.table_created:
                    raise Exception('Table should have already been created for file %s' % filename)

    def read_file_manually(self, f):
        # Wrap in encoder if needed
        if self.encoding != 'none' and self.encoding is not None:
            encoder = codecs.getreader(self.encoding)
            f = encoder(f)

        # Read all the lines
        try:
            line = f.readline()
            while line:
                col_vals = line_splitter.split(line)
                self._insert_row(col_vals)
                line = f.readline()
        finally:
            if f != sys.stdin:
                f.close()
            self._flush_inserts()

    def read_file_using_csv(self, f):
        csv_reader = encoded_csv_reader(self.encoding, f, dialect='q')
        try:
            for col_vals in csv_reader:
                self._insert_row(col_vals)
        finally:
            if f != sys.stdin:
                f.close()
            self._flush_inserts()

    def _insert_row(self, col_vals):
        # If table has not been created yet
        if not self.table_created:
            # Try to create it along with another "example" line of data
            self.try_to_create_table(col_vals)

        # If the table is still not created, then we don't have enough data, just return
        if not self.table_created:
            return
        # The table already exists, so we can just add a new row
        self._insert_row_i(col_vals)

    def _insert_row_i(self, col_vals):
        # If we have more columns than we inferred
        if len(col_vals) > len(self.column_inferer.column_names):
            msg = 'Encountered a line in an invalid format %s:%s - %s columns instead of %s. Did you make sure to set the correct delimiter?'
            raise ColumnCountMismatchException(msg % (self.current_filename, self.lines_read, len(col_vals), len(self.column_inferer.column_names)))

        effective_column_names = self.column_inferer.column_names[:len(col_vals)]

        if len(effective_column_names) > 0:
            self.buffered_inserts.append((effective_column_names, col_vals))
        else:
            self.buffered_inserts.append((["c1"], [""]))

        if len(self.buffered_inserts) < 1000:
            return
        self._flush_inserts()

    def _flush_inserts(self):
        # print self.db.execute_and_fetch(self.db.generate_begin_transaction())

        for col_names, col_vals in self.buffered_inserts:
            insert_row_stmt = self.db.generate_insert_row(self.table_name, col_names, col_vals)
            self.db.execute_and_fetch(insert_row_stmt)

        # print self.db.execute_and_fetch(self.db.generate_end_transaction())
        self.buffered_inserts = []


    def try_to_create_table(self, col_vals):
        if self.table_created:
            raise Exception('Table is already created')

        self.lines_read += 1
        if self.lines_read <= self.header_skip:
            return
    
        # Add that line to the column inferer
        result = self.column_inferer.analyze(col_vals)
        # If inferer succeeded,
        if result:
            # Then generate a temp table name
            self.table_name = self.db.generate_temp_table_name()
            # Get the column definition dict from the inferer
            column_dict = self.column_inferer.get_column_dict()
            # Create the CREATE TABLE statement
            create_table_stmt = self.db.generate_create_table(self.table_name, self.column_inferer.get_column_names(), column_dict)
            # And create the table itself
            self.db.execute_and_fetch(create_table_stmt)
            # Mark the table as created
            self.table_created = True
        else:
            pass  # We don't have enough information for creating the table yet

    def drop_table(self):
        if self.table_created:
            self.db.drop_table(self.table_name)

def determine_max_col_lengths(m):
    if len(m) == 0:
        return []
    max_lengths = [0 for _ in xrange(0, len(m[0]))]
    for row_index in xrange(0, len(m)):
        for col_index in xrange(0, len(m[0])):
            new_len = len(unicode(m[row_index][col_index]))
            if new_len > max_lengths[col_index]:
                max_lengths[col_index] = new_len
    return max_lengths
        
(options, args) = parser.parse_args()

if options.version:
    print "q version %s" % q_version
    sys.exit(0)

if len(args) != 1:
    parser.print_help()
    sys.exit(1)
    
if options.engine_version not in ['v1', 'v2']:
    print >> sys.stderr, "Engine version must be either v1 or v2"
    sys.exit(2)

# Create DB object
db = Sqlite3DB()

# Create SQL statement 
sql_object = Sql('%s' % args[0])

# If the user flagged for a tab-delimited file then set the delimiter to tab
if options.tab_delimited_with_header:
    options.delimiter = '\t'
    options.header_skip = "0"

if options.engine_version == 'v2':
    if options.delimiter is None:
        options.delimiter = ' '
    elif len(options.delimiter) != 1:
        print >> sys.stderr, "Delimiter must be one character only. Add '-E v1' to the command line if you need multi-character delimiters. This will revert to version 1 of the engine which supports that. Please note that v1 does not support quoted fields."
        sys.exit(5)
    q_dialect = {'skipinitialspace': True, 'quoting': 0, 'delimiter': options.delimiter, 'quotechar': '"', 'doublequote': False}
    csv.register_dialect('q', **q_dialect)
    file_reading_method = 'csv'
else:
    file_reading_method = 'manual'

# Create a line splitter
line_splitter = LineSplitter(options.delimiter)

if options.encoding != 'none':
    try:
        codecs.lookup(options.encoding)
    except LookupError:
        print >> sys.stderr, "Encoding %s could not be found" % options.encoding
        sys.exit(10)

try:
    # Get each "table name" which is actually the file name
    for filename in sql_object.qtable_names:
        # Create the matching database table and populate it
        table_creator = TableCreator(db, filename, line_splitter, int(options.header_skip), options.gzipped, options.encoding, file_reading_method)
        start_time = time.time()
        table_creator.populate()
        if DEBUG:
            print >> sys.stderr, "TIMING - populate time is %4.3f" % (time.time() - start_time)

        # Replace the logical table name with the real table name
        sql_object.set_effective_table_name(filename, table_creator.table_name)

    # Execute the query and fetch the data
    m = sql_object.execute_and_fetch(db)
except sqlite3.OperationalError, e:
    print >> sys.stderr, "query error: %s" % e
    sys.exit(1)
except ColumnCountMismatchException, e:
    print >> sys.stderr, e.msg
    sys.exit(2)
except (UnicodeDecodeError, UnicodeError), e:
    print >> sys.stderr, "Cannot decode data. Try to change the encoding by setting it using the -e parameter. Error:%s" % e
    sys.exit(3)
except KeyboardInterrupt:
    print >> sys.stderr, "Interrupted"
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
    formatting_dict = dict([(x.split("=")[0], x.split("=")[1]) for x in options.formatting.split(",")])
else:
    formatting_dict = None

try:
    for rownum, row in enumerate(m):
        row_str = []
        for i, col in enumerate(row):
            if formatting_dict is not None and str(i + 1) in formatting_dict.keys():
                fmt_str = formatting_dict[str(i + 1)]
            else:
                if options.beautify:
                    fmt_str = "%%-%ss" % max_lengths[i]
                else:
                    fmt_str = "%s"

            if col is not None:
                row_str.append(fmt_str % col)
            else:
                row_str.append(fmt_str % "")

        STDOUT.write(output_delimiter.join(row_str) + "\n")
except KeyboardInterrupt:
    pass

table_creator.drop_table()
