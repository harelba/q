#!/usr/bin/env python

#   Copyright (C) 2012-2014 Harel Ben-Attia
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 3, or (at your option)
#   any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details (doc/LICENSE contains
#   a copy of it)
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
# Full Documentation and details in http://harelba.github.io/q/ 
#
# Run with --help for command line details
#
q_version = "1.5.0" 

__all__ = [ 'QTextAsData' ]

import os
import sys
import sqlite3
import gzip
import glob
from optparse import OptionParser,OptionGroup
import traceback as tb
import codecs
import locale
import time
import re
from ConfigParser import ConfigParser
import traceback
import csv
import hashlib
import uuid
import cStringIO

DEBUG = False

def get_stdout_encoding(encoding_override=None):
    if encoding_override is not None and encoding_override != 'none':
       return encoding_override

    if sys.stdout.isatty():
        return sys.stdout.encoding
    else:
        return locale.getpreferredencoding()

SHOW_SQL = False

def sha1(data):
    if not isinstance(data,str) and not isinstance(data,unicode):
        return hashlib.sha1(str(data)).hexdigest()
    return hashlib.sha1(data).hexdigest()
    
def regexp(regular_expression, data):
    if data is not None:
        if not isinstance(data, str) and not isinstance(data, unicode):
            data = str(data)
        return re.search(regular_expression, data) is not None
    else:
        return False

class Sqlite3DBResults(object):
    def __init__(self,query_column_names,results):
        self.query_column_names = query_column_names
        self.results = results

class Sqlite3DB(object):

    def __init__(self, show_sql=SHOW_SQL):
        self.show_sql = show_sql
        self.conn = sqlite3.connect(':memory:')
        self.last_temp_table_id = 10000
        self.cursor = self.conn.cursor()
        self.type_names = {
            str: 'TEXT', int: 'INT', long : 'INT' , float: 'FLOAT', None: 'TEXT'}
        self.numeric_column_types = set([int, long, float])
        self.add_user_functions()

    def add_user_functions(self):
        self.conn.create_function("regexp", 2, regexp)
        self.conn.create_function("sha1", 1, sha1)

    def is_numeric_type(self, column_type):
        return column_type in self.numeric_column_types

    def update_many(self, sql, params):
        try:
            if self.show_sql:
                print sql, " params: " + str(params)
            self.cursor.executemany(sql, params)
        finally:
            pass  # cursor.close()

    def execute_and_fetch(self, q):
        try:
            if self.show_sql:
                print repr(q)
            self.cursor.execute(q)
            if self.cursor.description is not None:
                # we decode the column names, so they can be encoded to any output format later on
                query_column_names = [c[0].decode('utf-8') for c in self.cursor.description]
            else:
                query_column_names = None
            result = self.cursor.fetchall()
        finally:
            pass  # cursor.close()
        return Sqlite3DBResults(query_column_names,result)

    def _get_as_list_str(self, l):
        return ",".join(['"%s"' % x.replace('"', '""') for x in l])

    def _get_col_values_as_list_str(self, col_vals, col_types):
        result = []
        for col_val, col_type in zip(col_vals, col_types):
            if col_val == '' and col_type is not str:
                col_val = "null"
            else:
                if col_val is not None:
                    if "'" in col_val:
                        col_val = col_val.replace("'", "''")
                    col_val = "'" + col_val + "'"
                else:
                    col_val = "null"

            result.append(col_val)
        return ",".join(result)

    def generate_insert_row(self, table_name, column_names):
        col_names_str = self._get_as_list_str(column_names)
        question_marks = ", ".join(["?" for i in range(0, len(column_names))])
        return 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, col_names_str, question_marks)

    def generate_begin_transaction(self):
        return "BEGIN TRANSACTION"

    def generate_end_transaction(self):
        return "COMMIT"

    # Get a list of column names so order will be preserved (Could have used OrderedDict, but
    # then we would need python 2.7)
    def generate_create_table(self, table_name, column_names, column_dict):
        # Convert dict from python types to db types
        column_name_to_db_type = dict(
            (n, self.type_names[t]) for n, t in column_dict.iteritems())
        column_defs = ','.join(['"%s" %s' % (
            n.replace('"', '""'), column_name_to_db_type[n]) for n in column_names])
        return 'CREATE TABLE %s (%s)' % (table_name, column_defs)

    def generate_temp_table_name(self):
        self.last_temp_table_id += 1
        return "temp_table_%s" % self.last_temp_table_id

    def generate_drop_table(self, table_name):
        return "DROP TABLE %s" % table_name

    def drop_table(self, table_name):
        return self.execute_and_fetch(self.generate_drop_table(table_name))

class CouldNotConvertStringToNumericValueException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class CouldNotParseInputException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class BadHeaderException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class EncodedQueryException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)


class CannotUnzipStdInException(Exception):

    def __init__(self):
        pass

class UnprovidedStdInException(Exception):

    def __init__(self):
        pass

class EmptyDataException(Exception):

    def __init__(self):
        pass


class FileNotFoundException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)


class ColumnCountMismatchException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class StrictModeColumnCountMismatchException(Exception):

    def __init__(self,expected_col_count,actual_col_count):
        self.expected_col_count = expected_col_count
        self.actual_col_count = actual_col_count

class FluffyModeColumnCountMismatchException(Exception):

    def __init__(self,expected_col_count,actual_col_count):
        self.expected_col_count = expected_col_count
        self.actual_col_count = actual_col_count


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
        # and we don't want the database table to be recreated for each
        # reference
        self.qtable_name_positions = {}
        # Dict from qtable names to their effective (actual database) table
        # names
        self.qtable_name_effective_table_names = {}

        self.query_column_names = None

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
                    raise Exception(
                        'FROM/JOIN is missing a table name after it')

                qtable_name = self.sql_parts[idx + 1]
                # Otherwise, the next part contains the qtable name. In most cases the next part will be only the qtable name.
                # We handle one special case here, where this is a subquery as a column: "SELECT (SELECT ... FROM qtable),100 FROM ...".
                # In that case, there will be an ending paranthesis as part of the name, and we want to handle this case gracefully.
                # This is obviously a hack of a hack :) Just until we have
                # complete parsing capabilities
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
            raise Exception(
                "Already set effective table name for qtable %s" % qtable_name)

        self.qtable_name_effective_table_names[
            qtable_name] = effective_table_name

    def get_effective_sql(self):
        if len(filter(lambda x: x is None, self.qtable_name_effective_table_names)) != 0:
            raise Exception('There are qtables without effective tables')

        effective_sql = [x for x in self.sql_parts]

        for qtable_name, positions in self.qtable_name_positions.iteritems():
            for pos in positions:
                effective_sql[pos] = self.qtable_name_effective_table_names[
                    qtable_name]

        return " ".join(effective_sql)

    def execute_and_fetch(self, db):
        db_results_obj = db.execute_and_fetch(self.get_effective_sql())
        return db_results_obj


class LineSplitter(object):

    def __init__(self, delimiter, expected_column_count):
        self.delimiter = delimiter
        self.expected_column_count = expected_column_count
        if delimiter is not None:
            escaped_delimiter = re.escape(delimiter)
            self.split_regexp = re.compile('(?:%s)+' % escaped_delimiter)
        else:
            self.split_regexp = re.compile(r'\s+')

    def split(self, line):
        if line and line[-1] == '\n':
            line = line[:-1]
        return self.split_regexp.split(line, max_split=self.expected_column_count)


class TableColumnInferer(object):

    def __init__(self, mode, expected_column_count, input_delimiter, skip_header=False):
        self.inferred = False
        self.mode = mode
        self.rows = []
        self.skip_header = skip_header
        self.header_row = None
        self.expected_column_count = expected_column_count
        self.input_delimiter = input_delimiter

    def analyze(self, col_vals):
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

    def determine_type_of_value(self, value):
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

    def determine_type_of_value_list(self, value_list):
        type_list = [self.determine_type_of_value(v) for v in value_list]
        all_types = set(type_list)
        if len(set(type_list)) == 1:
            # all the sample lines are of the same type
            return type_list[0]
        else:
            # check for the number of types without nulls,
            type_list_without_nulls = filter(
                lambda x: x is not None, type_list)
            # If all the sample lines are of the same type,
            if len(set(type_list_without_nulls)) == 1:
                # return it
                return type_list_without_nulls[0]
            else:
                return str

    def do_analysis(self):
        if self.mode == 'strict':
            self._do_strict_analysis()
        elif self.mode in ['relaxed', 'fluffy']:
            self._do_relaxed_analysis()
        else:
            raise Exception('Unknown parsing mode %s' % self.mode)

        if self.column_count == 1 and self.expected_column_count != 1:
            print >>sys.stderr, "Warning: column count is one - did you provide the correct delimiter?"
        if self.column_count == 0:
            raise Exception("Detected a column count of zero... Failing")

        self.infer_column_types()

        self.infer_column_names()

    def validate_column_names(self, value_list):
        column_name_errors = []
        for v in value_list:
            if v is None:
                # we allow column names to be None, in relaxed mode it'll be filled with default names.
                # RLRL
                continue
            if ',' in v:
                column_name_errors.append(
                    (v, "Column name cannot contain commas"))
                continue
            if self.input_delimiter in v:
                column_name_errors.append(
                    (v, "Column name cannot contain the input delimiter. Please make sure you've set the correct delimiter"))
                continue
            if '\n' in v:
                column_name_errors.append(
                    (v, "Column name cannot contain newline"))
                continue
            if v != v.strip():
                column_name_errors.append(
                    (v, "Column name contains leading/trailing spaces"))
                continue
            try:
                v.encode("utf-8", "strict").decode("utf-8")
            except:
                column_name_errors.append(
                    (v, "Column name must be UTF-8 Compatible"))
                continue
            nul_index = v.find("\x00")
            if nul_index >= 0:
                column_name_errors.append(
                    (v, "Column name cannot contain NUL"))
                continue
            t = self.determine_type_of_value(v)
            if t != str:
                column_name_errors.append((v, "Column name must be a string"))
        return column_name_errors

    def infer_column_names(self):
        if self.header_row is not None:
            column_name_errors = self.validate_column_names(self.header_row)
            if len(column_name_errors) > 0:
                raise BadHeaderException("Header must contain only strings and not numbers or empty strings: '%s'\n%s" % (
                    ",".join(self.header_row), "\n".join(["'%s': %s" % (x, y) for x, y in column_name_errors])))

            # use header row in order to name columns
            if len(self.header_row) < self.column_count:
                if self.mode == 'strict':
                    raise ColumnCountMismatchException("Strict mode. Header row contains less columns than expected column count(%s vs %s)" % (
                        len(self.header_row), self.column_count))
                elif self.mode in ['relaxed', 'fluffy']:
                    # in relaxed mode, add columns to fill the missing ones
                    self.header_row = self.header_row + \
                        ['c%s' % (x + len(self.header_row) + 1)
                         for x in xrange(self.column_count - len(self.header_row))]
            elif len(self.header_row) > self.column_count:
                if self.mode == 'strict':
                    raise ColumnCountMismatchException("Strict mode. Header row contains more columns than expected column count (%s vs %s)" % (
                        len(self.header_row), self.column_count))
                elif self.mode in ['relaxed', 'fluffy']:
                    # In relaxed mode, just cut the extra column names
                    self.header_row = self.header_row[:self.column_count]
            self.column_names = self.header_row
        else:
            # Column names are cX starting from 1
            self.column_names = ['c%s' % (i + 1)
                                 for i in range(self.column_count)]

    def _do_relaxed_analysis(self):
        column_count_list = [len(col_vals) for col_vals in self.rows]

        if self.expected_column_count is not None:
            self.column_count = self.expected_column_count
        else:
            # If not specified, we'll take the largest row in the sample rows
            self.column_count = max(column_count_list)

    def get_column_count_summary(self, column_count_list):
        counts = {}
        for column_count in column_count_list:
            counts[column_count] = counts.get(column_count, 0) + 1
        return ", ".join(["%s rows with %s columns" % (v, k) for k, v in counts.iteritems()])

    def _do_strict_analysis(self):
        column_count_list = [len(col_vals) for col_vals in self.rows]

        if len(set(column_count_list)) != 1:
            raise ColumnCountMismatchException('Strict mode. Column Count is expected to identical. Multiple column counts exist at the first part of the file. Try to check your delimiter, or change to relaxed mode. Details: %s' % (
                self.get_column_count_summary(column_count_list)))

        self.column_count = len(self.rows[0])

        if self.expected_column_count is not None and self.column_count != self.expected_column_count:
            raise ColumnCountMismatchException('Strict mode. Column count is expected to be %s but is %s' % (
                self.expected_column_count, self.column_count))

        self.infer_column_types()

    def infer_column_types(self):
        self.column_types = []
        self.column_types2 = []
        for column_number in xrange(self.column_count):
            column_value_list = [
                row[column_number] if column_number < len(row) else None for row in self.rows]
            column_type = self.determine_type_of_value_list(column_value_list)
            self.column_types.append(column_type)

            column_value_list2 = [row[column_number] if column_number < len(
                row) else None for row in self.rows[1:]]
            column_type2 = self.determine_type_of_value_list(
                column_value_list2)
            self.column_types2.append(column_type2)

        comparison = map(
            lambda x: x[0] == x[1], zip(self.column_types, self.column_types2))
        if False in comparison and not self.skip_header:
            number_of_column_types = len(set(self.column_types))
            if number_of_column_types == 1 and list(set(self.column_types))[0] == str:
                print >>sys.stderr, 'Warning - There seems to be header line in the file, but -H has not been specified. All fields will be detected as text fields, and the header line will appear as part of the data'

    def get_column_dict(self):
        return dict(zip(self.column_names, self.column_types))

    def get_column_count(self):
        return self.column_count

    def get_column_names(self):
        return self.column_names

    def get_column_types(self):
        return self.column_types


def encoded_csv_reader(encoding, f, dialect, **kwargs):
    try:
        csv_reader = csv.reader(f, dialect, **kwargs)
        if encoding is not None and encoding != 'none':
            for row in csv_reader:
                yield [unicode(x, encoding) for x in row]
        else:
            for row in csv_reader:
                yield row
    except ValueError,e:
        if e.message is not None and e.message.startswith('could not convert string to'):
            raise CouldNotConvertStringToNumericValueException(e.message)
        else:
            raise CouldNotParseInputException(str(e))

def normalized_filename(filename):
    if filename == '-':
        return 'stdin'
    else:
        return filename

class TableCreatorState(object):
    NEW = 'NEW'
    INITIALIZED = 'INITIALIZED'
    ANALYZED = 'ANALYZED'
    FULLY_READ = 'FULLY_READ'

class MaterializedFileState(object):
    def __init__(self,filename,f,encoding,dialect,is_stdin):
        self.filename = filename
        self.lines_read = 0
        self.f = f
        self.encoding = encoding
        self.dialect = dialect
        self.is_stdin = is_stdin
        self.skipped_bom = False

    def read_file_using_csv(self):
        # This is a hack for utf-8 with BOM encoding in order to skip the BOM. python's csv module
        # has a bug which prevents fixing it using the proper encoding, and it has been encountered by 
        # multiple people.
        if self.encoding == 'utf-8-sig' and self.lines_read == 0 and not self.skipped_bom:
            try:
                BOM = self.f.read(3)
                if BOM != '\xef\xbb\xbf':
                    raise Exception('Value of BOM is not as expected - Value is "%s"' % str(BOM))
            except Exception,e:
                raise Exception('Tried to skip BOM for "utf-8-sig" encoding and failed. Error message is ' + str(e))
        csv_reader = encoded_csv_reader(self.encoding, self.f, dialect=self.dialect)
        for col_vals in csv_reader:
            self.lines_read += 1
            yield col_vals

    def close(self):
        if self.f != sys.stdin:
            self.f.close()

class TableCreator(object):

    def __init__(self, db, filenames_str, line_splitter, skip_header=False, gzipped=False, encoding='UTF-8', mode='fluffy', expected_column_count=None, input_delimiter=None,stdin_file=None,stdin_filename='-'):
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
        self.stdin_file = stdin_file
        self.stdin_filename = stdin_filename

        self.column_inferer = TableColumnInferer(
            mode, expected_column_count, input_delimiter, skip_header)

        # Filled only after table population since we're inferring the table
        # creation data
        self.table_name = None

        self.pre_creation_rows = []
        self.buffered_inserts = []

        # Column type indices for columns that contain numeric types. Lazily initialized
        # so column inferer can do its work before this information is needed
        self.numeric_column_indices = None

        self.materialized_file_list = self.materialize_file_list()
        self.materialized_file_dict = {}

        self.state = TableCreatorState.NEW

    def materialize_file_list(self):
        materialized_file_list = []

        # Get the list of filenames
        filenames = self.filenames_str.split("+")

        # for each filename (or pattern)
        for fileglob in filenames:
            # Allow either stdin or a glob match
            if fileglob == self.stdin_filename:
                materialized_file_list.append(self.stdin_filename)
            else:
                materialized_file_list += glob.glob(fileglob)

        # If there are no files to go over,
        if len(materialized_file_list) == 0:
            raise FileNotFoundException(
                "No files matching '%s' have been found" % self.filenames_str)

        return materialized_file_list

    def get_table_name(self):
        return self.table_name

    def open_file(self,filename):
        # Check if it's standard input or a file
        if filename == self.stdin_filename:
            if self.stdin_file is None:
                raise UnprovidedStdInException()
            f = self.stdin_file
            if self.gzipped:
                raise CannotUnzipStdInException()
        else:
            if self.gzipped or filename.endswith('.gz'):
                f = gzip.GzipFile(fileobj=file(filename,'rb'))    
            else:
                f = file(filename,'rb')
        return f

    def _pre_populate(self,dialect):
        # For each match
        for filename in self.materialized_file_list:
            if filename in self.materialized_file_dict.keys():
                continue

            f = self.open_file(filename)

            is_stdin = filename == self.stdin_filename

            mfs = MaterializedFileState(filename,f,self.encoding,dialect,is_stdin)
            self.materialized_file_dict[filename] = mfs

    def _populate(self,dialect,stop_after_analysis=False):
        # For each match
        for filename in self.materialized_file_list:
            mfs = self.materialized_file_dict[filename]

            try:
                try:
                    for col_vals in mfs.read_file_using_csv():
                        self._insert_row(col_vals)
                        if stop_after_analysis and self.column_inferer.inferred:
                            return
                    if mfs.lines_read == 0 or (mfs.lines_read == 1 and self.skip_header):
                        raise EmptyDataException()
                except StrictModeColumnCountMismatchException,e:
                    raise ColumnCountMismatchException(
                        'Strict mode - Expected %s columns instead of %s columns in file %s row %s. Either use relaxed/fluffy modes or check your delimiter' % (
                        e.expected_col_count, e.actual_col_count, normalized_filename(mfs.filename), mfs.lines_read))
                except FluffyModeColumnCountMismatchException,e:
                    raise ColumnCountMismatchException(
                        'Deprecated fluffy mode - Too many columns in file %s row %s (%s fields instead of %s fields). Consider moving to either relaxed or strict mode' % (
                        normalized_filename(mfs.filename), mfs.lines_read, e.actual_col_count, e.expected_col_count))
            finally:
                if not stop_after_analysis:
                    mfs.close()
                self._flush_inserts()

            if not self.table_created:
                self.column_inferer.force_analysis()
                self._do_create_table()

    def populate(self,dialect,stop_after_analysis=False):
        if self.state == TableCreatorState.NEW:
            self._pre_populate(dialect)    
            self.state = TableCreatorState.INITIALIZED

        if self.state == TableCreatorState.INITIALIZED:
            self._populate(dialect,stop_after_analysis=True)
            self.state = TableCreatorState.ANALYZED

            if stop_after_analysis:
                return

        if self.state == TableCreatorState.ANALYZED:
            self._populate(dialect,stop_after_analysis=False)
            self.state = TableCreatorState.FULLY_READ
            return

    def _flush_pre_creation_rows(self):
        for i, col_vals in enumerate(self.pre_creation_rows):
            if self.skip_header and i == 0:
                # skip header line
                continue
            self._insert_row(col_vals)
        self._flush_inserts()
        self.pre_creation_rows = []

    def _insert_row(self, col_vals):
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

    def initialize_numeric_column_indices_if_needed(self):
        # Lazy initialization of numeric column indices
        if self.numeric_column_indices is None:
            column_types = self.column_inferer.get_column_types()
            self.numeric_column_indices = [idx for idx, column_type in enumerate(
                column_types) if self.db.is_numeric_type(column_type)]

    def nullify_values_if_needed(self, col_vals):
        new_vals = col_vals[:]
        col_count = len(col_vals)
        for i in self.numeric_column_indices:
            if i >= col_count:
                continue
            v = col_vals[i]
            if v == '':
                new_vals[i] = None
        return new_vals

    def normalize_col_vals(self, col_vals):
        # Make sure that numeric column indices are initializd
        self.initialize_numeric_column_indices_if_needed()

        col_vals = self.nullify_values_if_needed(col_vals)

        expected_col_count = self.column_inferer.get_column_count()
        actual_col_count = len(col_vals)
        if self.mode == 'strict':
            if actual_col_count != expected_col_count:
                raise StrictModeColumnCountMismatchException(expected_col_count,actual_col_count)
            return col_vals

        # in all non strict mode, we add dummy data to missing columns

        if actual_col_count < expected_col_count:
            col_vals = col_vals + \
                [None for x in xrange(expected_col_count - actual_col_count)]

        # in relaxed mode, we merge all extra columns to the last column value
        if self.mode == 'relaxed':
            if actual_col_count > expected_col_count:
                xxx = col_vals[:expected_col_count - 1] + \
                    [self.input_delimiter.join(
                        col_vals[expected_col_count - 1:])]
                return xxx
            else:
                return col_vals

        if self.mode == 'fluffy':
            if actual_col_count > expected_col_count:
                raise FluffyModeColumnCountMismatchException(expected_col_count,actual_col_count)
            return col_vals

        raise Exception("Unidentified parsing mode %s" % self.mode)

    def _insert_row_i(self, col_vals):
        col_vals = self.normalize_col_vals(col_vals)
        effective_column_names = self.column_inferer.column_names[
            :len(col_vals)]

        if len(effective_column_names) > 0:
            self.buffered_inserts.append((effective_column_names, col_vals))
        else:
            self.buffered_inserts.append((["c1"], [""]))

        if len(self.buffered_inserts) < 5000:
            return
        self._flush_inserts()

    def _flush_inserts(self):
        # print self.db.execute_and_fetch(self.db.generate_begin_transaction())

        # If the table is still not created, then we don't have enough data
        if not self.table_created:
            return

        if len(self.buffered_inserts) > 0:
            insert_row_stmt = self.db.generate_insert_row(
                self.table_name, self.buffered_inserts[0][0])
            params = [col_vals for col_names, col_vals in self.buffered_inserts]

            self.db.update_many(insert_row_stmt, params)
        # print self.db.execute_and_fetch(self.db.generate_end_transaction())
        self.buffered_inserts = []

    def try_to_create_table(self, col_vals):
        if self.table_created:
            raise Exception('Table is already created')

        # Add that line to the column inferer
        result = self.column_inferer.analyze(col_vals)
        # If inferer succeeded,
        if result:
            self._do_create_table()
        else:
            pass  # We don't have enough information for creating the table yet

    def _do_create_table(self):
        # Then generate a temp table name
        self.table_name = self.db.generate_temp_table_name()
        # Get the column definition dict from the inferer
        column_dict = self.column_inferer.get_column_dict()
        # Create the CREATE TABLE statement
        create_table_stmt = self.db.generate_create_table(
            self.table_name, self.column_inferer.get_column_names(), column_dict)
        # And create the table itself
        self.db.execute_and_fetch(create_table_stmt)
        # Mark the table as created
        self.table_created = True
        self._flush_pre_creation_rows()

    def drop_table(self):
        if self.table_created:
            self.db.drop_table(self.table_name)


def determine_max_col_lengths(m,output_field_quoting_func,output_delimiter):
    if len(m) == 0:
        return []
    max_lengths = [0 for x in xrange(0, len(m[0]))]
    for row_index in xrange(0, len(m)):
        for col_index in xrange(0, len(m[0])):
            new_len = len(unicode(output_field_quoting_func(output_delimiter,m[row_index][col_index])))
            if new_len > max_lengths[col_index]:
                max_lengths[col_index] = new_len
    return max_lengths

def print_credentials():
    print >>sys.stderr,"q version %s" % q_version
    print >>sys.stderr,"Copyright (C) 2012-2014 Harel Ben-Attia (harelba@gmail.com, @harelba on twitter)"
    print >>sys.stderr,"http://harelba.github.io/q/"
    print >>sys.stderr

class QWarning(object):
    def __init__(self,exception,msg):
        self.exception = exception
        self.msg = msg

class QError(object):
    def __init__(self,exception,msg,errorcode):
        self.exception = exception
        self.msg = msg
        self.errorcode = errorcode
        self.traceback = traceback.format_exc()

class QDataLoad(object):
    def __init__(self,filename,start_time,end_time):
        self.filename = filename
        self.start_time = start_time
        self.end_time = end_time

    def duration(self):
        return self.end_time - self.start_time

    def __str__(self):
        return "DataLoad<'%s' at %s (took %4.3f seconds)>" % (self.filename,self.start_time,self.duration())
    __repr__ = __str__

class QMaterializedFile(object):
    def __init__(self,filename,is_stdin):
        self.filename = filename
        self.is_stdin = is_stdin

    def __str__(self):
        return "QMaterializedFile<filename=%s,is_stdin=%s>" % (self.filename,self.is_stdin)
    __repr__ = __str__

class QTableStructure(object):
    def __init__(self,filenames_str,materialized_files,column_names,column_types):
        self.filenames_str = filenames_str
        self.materialized_files = materialized_files
        self.column_names = column_names
        self.column_types = column_types

    def __str__(self):
        return "QTableStructure<filenames_str=%s,materialized_file_count=%s,column_names=%s,column_types=%s>" % (
            self.filenames_str,len(self.materialized_files.keys()),self.column_names,self.column_types)
    __repr__ = __str__

class QMetadata(object):
    def __init__(self,table_structures=[],output_column_name_list=None,data_loads=[]):
        self.table_structures = table_structures
        self.output_column_name_list = output_column_name_list
        self.data_loads = data_loads

    def __str__(self):
        return "QMetadata<table_count=%s,output_column_name_list=%s,data_load_count=%s" % (
            len(self.table_structures),self.output_column_name_list,len(self.data_loads))
    __repr__ = __str__

class QOutput(object):
    def __init__(self,data=None,metadata=None,warnings=[],error=None):
        self.data = data
        self.metadata = metadata

        self.warnings = warnings
        self.error = error
        if error is None:
            self.status = 'ok'
        else:
            self.status = 'error'

    def __str__(self):
        s = []
        s.append('status=%s' % self.status)
        if self.error is not None:
            s.append("error=%s" % self.error.msg)
        if len(self.warnings) > 0:
            s.append("warning_count=%s" % len(self.warnings))
        if self.data is not None:
            s.append("row_count=%s" % len(self.data))
        else:
            s.append("row_count=None")
        if self.metadata is not None:
            s.append("metadata=<%s>" % self.metadata)
        else:
            s.append("metadata=None")
        return "QOutput<%s>" % ",".join(s)
    __repr__ = __str__

class QInputParams(object):
    def __init__(self,skip_header=False,
            delimiter=' ',input_encoding='UTF-8',gzipped_input=False,parsing_mode='relaxed',
            expected_column_count=None,keep_leading_whitespace_in_values=False,
            disable_double_double_quoting=False,disable_escaped_double_quoting=False,
            input_quoting_mode='minimal',stdin_file=None,stdin_filename='-'):
        self.skip_header = skip_header
        self.delimiter = delimiter
        self.input_encoding = input_encoding
        self.gzipped_input = gzipped_input
        self.parsing_mode = parsing_mode
        self.expected_column_count = expected_column_count
        self.keep_leading_whitespace_in_values = keep_leading_whitespace_in_values
        self.disable_double_double_quoting = disable_double_double_quoting
        self.disable_escaped_double_quoting = disable_escaped_double_quoting
        self.input_quoting_mode = input_quoting_mode

    def merged_with(self,input_params):
        params = QInputParams(**self.__dict__)
        if input_params is not None:
            params.__dict__.update(**input_params.__dict__)
        return params

    def __str__(self):
        return "QInputParams<%s>" % str(self.__dict__)

    def __repr__(self):
        return "QInputParams(...)"

class QTextAsData(object):
    def __init__(self,default_input_params=QInputParams()):
        self.default_input_params = default_input_params

        self.table_creators = {}

        # Create DB object
        self.db = Sqlite3DB()


    input_quoting_modes = {   'minimal' : csv.QUOTE_MINIMAL,
                        'all' : csv.QUOTE_ALL,
                        # nonnumeric is not supported for input quoting modes, since we determine the data types 
                        # ourselves instead of letting the csv module try to identify the types
                        'none' : csv.QUOTE_NONE }

    def determine_proper_dialect(self,input_params):

        input_quoting_mode_csv_numeral = QTextAsData.input_quoting_modes[input_params.input_quoting_mode]

        if input_params.keep_leading_whitespace_in_values:
            skip_initial_space = False
        else:
            skip_initial_space = True

        dialect = {'skipinitialspace': skip_initial_space,
                    'delimiter': input_params.delimiter, 'quotechar': '"' }
        dialect['quoting'] = input_quoting_mode_csv_numeral
        dialect['doublequote'] = input_params.disable_double_double_quoting

        if input_params.disable_escaped_double_quoting:
            dialect['escapechar'] = '\\'

        return dialect

    def get_dialect_id(self,filename):
        return 'q_dialect_%s' % filename

    def _load_data(self,filename,input_params=QInputParams(),stdin_file=None,stdin_filename='-',stop_after_analysis=False):
        start_time = time.time()

        q_dialect = self.determine_proper_dialect(input_params)
        dialect_id = self.get_dialect_id(filename)
        csv.register_dialect(dialect_id, **q_dialect)

        # Create a line splitter
        line_splitter = LineSplitter(input_params.delimiter, input_params.expected_column_count)

        # reuse already loaded data, except for stdin file data (stdin file data will always
        # be reloaded and overwritten)
        if filename in self.table_creators.keys() and filename != stdin_filename:
            return None

        # Create the matching database table and populate it
        table_creator = TableCreator(
            self.db, filename, line_splitter, input_params.skip_header, input_params.gzipped_input, input_params.input_encoding,
            mode=input_params.parsing_mode, expected_column_count=input_params.expected_column_count, 
            input_delimiter=input_params.delimiter,stdin_file = stdin_file,stdin_filename = stdin_filename)

        table_creator.populate(dialect_id,stop_after_analysis)

        self.table_creators[filename] = table_creator

        return QDataLoad(filename,start_time,time.time())

    def load_data(self,filename,input_params=QInputParams(),stop_after_analysis=False):
        self._load_data(filename,input_params,stop_after_analysis=stop_after_analysis)

    def load_data_from_string(self,filename,str_data,input_params=QInputParams(),stop_after_analysis=False):
        sf = cStringIO.StringIO(str_data)
        try:
            self._load_data(filename,input_params,stdin_file=sf,stdin_filename=filename,stop_after_analysis=stop_after_analysis)
        finally:
            if sf is not None:
                sf.close()

    def _ensure_data_is_loaded(self,sql_object,input_params,stdin_file,stdin_filename='-',stop_after_analysis=False):
        data_loads = []

        # Get each "table name" which is actually the file name
        for filename in sql_object.qtable_names:
            data_load = self._load_data(filename,input_params,stdin_file=stdin_file,stdin_filename=stdin_filename,stop_after_analysis=stop_after_analysis)
            if data_load is not None:
                data_loads.append(data_load)

        return data_loads

    def materialize_sql_object(self,sql_object):
        for filename in sql_object.qtable_names:
            sql_object.set_effective_table_name(filename,self.table_creators[filename].table_name)

    def _execute(self,query_str,input_params=None,stdin_file=None,stdin_filename='-',stop_after_analysis=False):
        warnings = []
        error = None
        data_loads = []
        table_structures = []

        db_results_obj = None

        effective_input_params = self.default_input_params.merged_with(input_params)

        if type(query_str) != unicode:
            try:
                # Hueristic attempt to auto convert the query to unicode before failing
                query_str = query_str.decode('utf-8')
            except:
                error = QError(EncodedQueryException(),"Query should be in unicode. Please make sure to provide a unicode literal string or decode it using proper the character encoding.",91)
                return QOutput(error = error)

        # Create SQL statment
        sql_object = Sql('%s' % query_str)

        try:
            data_loads += self._ensure_data_is_loaded(sql_object,effective_input_params,stdin_file=stdin_file,stdin_filename=stdin_filename,stop_after_analysis=stop_after_analysis)

            table_structures = self._create_table_structures_list()

            self.materialize_sql_object(sql_object)

            # Execute the query and fetch the data
            db_results_obj = sql_object.execute_and_fetch(self.db)

            return QOutput(
                data = db_results_obj.results, 
                metadata = QMetadata(
                    table_structures=table_structures,
                    output_column_name_list=db_results_obj.query_column_names,
                    data_loads=data_loads),
                warnings = warnings,
                error = error)

        except EmptyDataException,e:
            warnings.append(QWarning(e,"Warning - data is empty"))
        except FileNotFoundException, e:
            error = QError(e,e.msg,30)
        except sqlite3.OperationalError, e:
            msg = str(e)
            error = QError(e,"query error: %s" % msg,1)
            if "no such column" in msg and effective_input_params.skip_header:
                warnings.append(QWarning(e,'Warning - There seems to be a "no such column" error, and -H (header line) exists. Please make sure that you are using the column names from the header line and not the default (cXX) column names'))
        except ColumnCountMismatchException, e:
            error = QError(e,e.msg,2)
        except (UnicodeDecodeError, UnicodeError), e:
            error = QError(e,"Cannot decode data. Try to change the encoding by setting it using the -e parameter. Error:%s" % e,3)
        except BadHeaderException, e:
            error = QError(e,"Bad header row: %s" % e.msg,35)
        except CannotUnzipStdInException,e:
            error = QError(e,"Cannot decompress standard input. Pipe the input through zcat in order to decompress.",36)
        except UnprovidedStdInException,e:
            error = QError(e,"Standard Input must be provided in order to use it as a table",61)
        except CouldNotConvertStringToNumericValueException,e:
            error = QError(e,"Could not convert string to a numeric value. Did you use `-w nonnumeric` with unquoted string values? Error: %s" % e.msg,58)
        except CouldNotParseInputException,e:
            error = QError(e,"Could not parse the input. Please make sure to set the proper -w input-wrapping parameter for your input, and that you use the proper input encoding (-e). Error: %s" % e.msg,59)
        except KeyboardInterrupt,e:
            warnings.append(QWarning(e,"Interrupted"))
        except Exception, e:
            error = QError(e,repr(e),199)

        return QOutput(warnings = warnings,error = error , metadata=QMetadata(table_structures=table_structures,data_loads = data_loads))

    def execute(self,query_str,input_params=None,stdin_file=None,stdin_filename='-'):
        return self._execute(query_str,input_params,stdin_file,stdin_filename,stop_after_analysis=False)

    def unload(self):

        for filename,table_creator in self.table_creators.iteritems():
            try:
                table_creator.drop_table()
            except:
                # Support no-table select queries
                pass
        self.table_creators = {}

    def _create_materialized_files(self,table_creator):
        d = table_creator.materialized_file_dict
        m = {}
        for filename,mfs in d.iteritems():
            m[filename] = QMaterializedFile(filename,mfs.is_stdin)
        return m

    def _create_table_structures_list(self):
        table_structures = []
        for filename,table_creator in self.table_creators.iteritems():
            column_names = table_creator.column_inferer.get_column_names()
            column_types = [self.db.type_names[table_creator.column_inferer.get_column_dict()[k]].lower() for k in column_names]
            materialized_files = self._create_materialized_files(table_creator)
            table_structure = QTableStructure(table_creator.filenames_str,materialized_files,column_names,column_types)
            table_structures.append(table_structure)
        return table_structures

    def analyze(self,query_str,input_params=None,stdin_file=None,stdin_filename='-'):
        q_output = self._execute(query_str,input_params,stdin_file,stdin_filename,stop_after_analysis=True)

        return q_output

def quote_none_func(output_delimiter,v):
    return v

def quote_minimal_func(output_delimiter,v):
    if v is None:
        return v
    t = type(v)
    if t == str or t == unicode and output_delimiter in v:
        return '"%s"' % (v)
    return v;

def quote_nonnumeric_func(output_delimiter,v):
    if v is None:
        return v
    if type(v) == str or type(v) == unicode:
        return '"%s"' % (v)
    return v;

def quote_all_func(output_delimiter,v):
    return '"%s"' % (v)

class QOutputParams(object):
    def __init__(self,
            delimiter=' ',
            beautify=False,
            output_quoting_mode='minimal',
            formatting=None,
            output_header=False):
        self.delimiter = delimiter
        self.beautify = beautify
        self.output_quoting_mode = output_quoting_mode
        self.formatting = formatting
        self.output_header = output_header

    def __str__(self):
        return "QOutputParams<%s>" % str(self.__dict__)

    def __repr__(self):
        return "QOutputParams(...)"

class QOutputPrinter(object):
    output_quoting_modes = {   'minimal' : quote_minimal_func,
                        'all' : quote_all_func,
                        'nonnumeric' : quote_nonnumeric_func,
                        'none' : quote_none_func }

    def __init__(self,output_params):
        self.output_params = output_params

        self.output_field_quoting_func = QOutputPrinter.output_quoting_modes[output_params.output_quoting_mode]

    def print_errors_and_warnings(self,f,results):
        if results.status == 'error':
            error = results.error
            print >>f,error.msg

        for warning in results.warnings:
            print >>f,"%s" % warning.msg

    def print_analysis(self,f_out,f_err,results):
        self.print_errors_and_warnings(f_err,results)

        if results.metadata is None:
            return

        if results.metadata.table_structures is None:
            return

        for table_structure in results.metadata.table_structures:
            print >>f_out,"Table for file: %s" % normalized_filename(table_structure.filenames_str)
            for n,t in zip(table_structure.column_names,table_structure.column_types):
                print >>f_out,"  `%s` - %s" % (n,t)

    def print_output(self,f_out,f_err,results):
        try:
            self._print_output(f_out,f_err,results)
        except (UnicodeEncodeError, UnicodeError), e:
            print >>f_err, "Cannot encode data. Error:%s" % e
            sys.exit(3)
        except IOError, e:
            if e.errno == 32:
                # broken pipe, that's ok
                pass
            else:
                # dont miss other problems for now
                raise
        except KeyboardInterrupt:
            pass

    def _print_output(self,f_out,f_err,results):
        self.print_errors_and_warnings(f_err,results)

        data = results.data

        if data is None:
            return

        # If the user requested beautifying the output
        if self.output_params.beautify:
            max_lengths = determine_max_col_lengths(data,self.output_field_quoting_func,self.output_params.delimiter)

        if self.output_params.formatting:
            formatting_dict = dict(
                [(x.split("=")[0], x.split("=")[1]) for x in self.output_params.formatting.split(",")])
        else:
            formatting_dict = None

        try:
            if self.output_params.output_header and results.metadata.output_column_name_list is not None:
                data.insert(0,results.metadata.output_column_name_list)
            for rownum, row in enumerate(data):
                row_str = []
                for i, col in enumerate(row):
                    if formatting_dict is not None and str(i + 1) in formatting_dict.keys():
                        fmt_str = formatting_dict[str(i + 1)]
                    else:
                        if self.output_params.beautify:
                            fmt_str = "%%-%ss" % max_lengths[i]
                        else:
                            fmt_str = "%s"

                    if col is not None:
                        row_str.append(fmt_str % self.output_field_quoting_func(self.output_params.delimiter,col))
                    else:
                        row_str.append(fmt_str % "")

                f_out.write(self.output_params.delimiter.join(row_str) + "\n")
        except (UnicodeEncodeError, UnicodeError), e:
            print >>sys.stderr, "Cannot encode data. Error:%s" % e
            sys.exit(3)
        except IOError, e:
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
            f_out.flush()
        except IOError, e:
            pass

def run_standalone():
    p = ConfigParser()
    p.read([os.path.expanduser('~/.qrc'), '.qrc'])

    def get_option_with_default(p, option_type, option, default):
        if not p.has_option('options', option):
            return default
        if option_type == 'boolean':
            return p.getboolean('options', option)
        elif option_type == 'int':
            return p.getint('options', option)
        elif option_type == 'string':
            return p.get('options', option)
        elif option_type == 'escaped_string':
            return p.get('options', option).decode('string-escape')
        else:
            raise Exception("Unknown option type")

    default_beautify = get_option_with_default(p, 'boolean', 'beautify', False)
    default_gzipped = get_option_with_default(p, 'boolean', 'gzipped', False)
    default_delimiter = get_option_with_default(
        p, 'escaped_string', 'delimiter', None)
    default_output_delimiter = get_option_with_default(
        p, 'escaped_string', 'output_delimiter', None)
    default_skip_header = get_option_with_default(p, 'int', 'skip_header', 0)
    default_formatting = get_option_with_default(p, 'string', 'formatting', None)
    default_encoding = get_option_with_default(p, 'string', 'encoding', 'UTF-8')
    default_output_encoding = get_option_with_default(p, 'string', 'encoding', None)
    default_query_encoding = get_option_with_default(p, 'string', 'query_encoding', locale.getpreferredencoding())
    default_output_header = get_option_with_default(p, 'string', 'output_header', False)

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


            See the help or https://github.com/harelba/q/ for more details.
    """)

    #-----------------------------------------------
    parser.add_option("-v", "--version", dest="version", default=False, action="store_true",
                      help="Print version")
    #-----------------------------------------------
    input_data_option_group = OptionGroup(parser,"Input Data Options")
    input_data_option_group.add_option("-H", "--skip-header", dest="skip_header", default=default_skip_header, action="store_true",
                      help="Skip header row. This has been changed from earlier version - Only one header row is supported, and the header row is used for column naming")
    input_data_option_group.add_option("-d", "--delimiter", dest="delimiter", default=default_delimiter,
                      help="Field delimiter. If none specified, then space is used as the delimiter.")
    input_data_option_group.add_option("-t", "--tab-delimited", dest="tab_delimited", default=False, action="store_true",
                      help="Same as -d <tab>. Just a shorthand for handling standard tab delimited file You can use $'\\t' if you want (this is how Linux expects to provide tabs in the command line")
    input_data_option_group.add_option("-e", "--encoding", dest="encoding", default=default_encoding,
                      help="Input file encoding. Defaults to UTF-8. set to none for not setting any encoding - faster, but at your own risk...")
    input_data_option_group.add_option("-z", "--gzipped", dest="gzipped", default=default_gzipped, action="store_true",
                      help="Data is gzipped. Useful for reading from stdin. For files, .gz means automatic gunzipping")
    input_data_option_group.add_option("-A", "--analyze-only", dest="analyze_only", action='store_true',
                      help="Analyze sample input and provide information about data types")
    input_data_option_group.add_option("-m", "--mode", dest="mode", default="relaxed",
                      help="Data parsing mode. fluffy, relaxed and strict. In strict mode, the -c column-count parameter must be supplied as well")
    input_data_option_group.add_option("-c", "--column-count", dest="column_count", default=None,
                      help="Specific column count when using relaxed or strict mode")
    input_data_option_group.add_option("-k", "--keep-leading-whitespace", dest="keep_leading_whitespace_in_values", default=False, action="store_true",
                      help="Keep leading whitespace in values. Default behavior strips leading whitespace off values, in order to provide out-of-the-box usability for simple use cases. If you need to preserve whitespace, use this flag.")
    input_data_option_group.add_option("--disable-double-double-quoting", dest="disable_double_double_quoting", default=True, action="store_false",
                      help="Disable support for double double-quoting for escaping the double quote character. By default, you can use \"\" inside double quoted fields to escape double quotes. Mainly for backward compatibility.")
    input_data_option_group.add_option("--disable-escaped-double-quoting", dest="disable_escaped_double_quoting", default=True, action="store_false",
                      help="Disable support for escaped double-quoting for escaping the double quote character. By default, you can use \\\" inside double quoted fields to escape double quotes. Mainly for backward compatibility.")
    input_data_option_group.add_option("-w","--input-quoting-mode",dest="input_quoting_mode",default="minimal",
                      help="Input quoting mode. Possible values are all, minimal and none. Note the slightly misleading parameter name, and see the matching -W parameter for output quoting.")
    parser.add_option_group(input_data_option_group)
    #-----------------------------------------------
    output_data_option_group = OptionGroup(parser,"Output Options") 
    output_data_option_group.add_option("-D", "--output-delimiter", dest="output_delimiter", default=default_output_delimiter,
                      help="Field delimiter for output. If none specified, then the -d delimiter is used if present, or space if no delimiter is specified")
    output_data_option_group.add_option("-T", "--tab-delimited-output", dest="tab_delimited_output", default=False, action="store_true",
                      help="Same as -D <tab>. Just a shorthand for outputing tab delimited output. You can use -D $'\\t' if you want.")
    output_data_option_group.add_option("-O", "--output-header", dest="output_header", default=default_output_header, action="store_true",help="Output header line. Output column-names are determined from the query itself. Use column aliases in order to set your column names in the query. For example, 'select name FirstName,value1/value2 MyCalculation from ...'. This can be used even if there was no header in the input.")
    output_data_option_group.add_option("-b", "--beautify", dest="beautify", default=default_beautify, action="store_true",
                      help="Beautify output according to actual values. Might be slow...")
    output_data_option_group.add_option("-f", "--formatting", dest="formatting", default=default_formatting,
                      help="Output-level formatting, in the format X=fmt,Y=fmt etc, where X,Y are output column numbers (e.g. 1 for first SELECT column etc.")
    output_data_option_group.add_option("-E", "--output-encoding", dest="output_encoding", default=default_output_encoding,
                      help="Output encoding. Defaults to 'none', leading to selecting the system/terminal encoding")
    output_data_option_group.add_option("-W","--output-quoting-mode",dest="output_quoting_mode",default="minimal",
                      help="Output quoting mode. Possible values are all, minimal, nonnumeric and none. Note the slightly misleading parameter name, and see the matching -w parameter for input quoting.")
    parser.add_option_group(output_data_option_group)
    #-----------------------------------------------
    query_option_group = OptionGroup(parser,"Query Related Options") 
    query_option_group.add_option("-q", "--query-filename", dest="query_filename", default=None,
                      help="Read query from the provided filename instead of the command line, possibly using the provided query encoding (using -Q).")
    query_option_group.add_option("-Q", "--query-encoding", dest="query_encoding", default=default_query_encoding,
                      help="query text encoding. Experimental. Please send your feedback on this")
    parser.add_option_group(query_option_group)
    #-----------------------------------------------

    (options, args) = parser.parse_args()

    if options.version:
        print_credentials()
        sys.exit(0)

    if len(args) == 0 and options.query_filename is None:
        print_credentials()
        print >>sys.stderr,"Must provide at least one query in the command line, or through a file with the -f parameter"
        sys.exit(1)

    if options.query_filename is not None:
        if len(args) != 0:
            print >>sys.stderr,"Can't provide both a query file and a query on the command line"
            sys.exit(1)
        try:
            f = file(options.query_filename)
            query_strs = [f.read()]
            f.close()
        except:
            print >>sys.stderr,"Could not read query from file %s" % options.query_filename
            sys.exit(1)
    else:
        query_strs = args

    if options.query_encoding is not None and options.query_encoding != 'none':
        try:
            for idx in range(len(query_strs)):
                query_strs[idx] = query_strs[idx].decode(options.query_encoding).strip()

                if len(query_strs[idx]) == 0:
                    print >>sys.stderr,"Query cannot be empty (query number %s)" % (idx+1)
                    sys.exit(1)

        except Exception,e:
            print >>sys.stderr,"Could not decode query number %s using the provided query encoding (%s)" % (idx+1,options.query_encoding)
            sys.exit(3)

    if options.mode not in ['fluffy', 'relaxed', 'strict']:
        print >>sys.stderr, "Parsing mode can be one of fluffy, relaxed or strict"
        sys.exit(13)

    output_encoding = get_stdout_encoding(options.output_encoding)
    try:
        STDOUT = codecs.getwriter(output_encoding)(sys.stdout)
    except:
        print >>sys.stderr,"Could not create output stream using output encoding %s" % (output_encoding)
        sys.exit(200)

    # If the user flagged for a tab-delimited file then set the delimiter to tab
    if options.tab_delimited:
        options.delimiter = '\t'

    if options.tab_delimited_output:
        options.output_delimiter = '\t'

    if options.delimiter is None:
        options.delimiter = ' '
    elif len(options.delimiter) != 1:
        print >>sys.stderr, "Delimiter must be one character only"
        sys.exit(5)

    if options.input_quoting_mode not in QTextAsData.input_quoting_modes.keys():
        print >>sys.stderr,"Input quoting mode can only be one of %s. It cannot be set to '%s'" % (",".join(QTextAsData.input_quoting_modes.keys()),options.input_quoting_mode)
        sys.exit(55)

    if options.output_quoting_mode not in QOutputPrinter.output_quoting_modes.keys():
        print >>sys.stderr,"Output quoting mode can only be one of %s. It cannot be set to '%s'" % (",".join(QOutputPrinter.output_quoting_modes.keys()),options.input_quoting_mode)
        sys.exit(56)

    if options.column_count is not None:
        expected_column_count = int(options.column_count)
    else:
        # infer automatically
        expected_column_count = None

    if options.encoding != 'none':
        try:
            codecs.lookup(options.encoding)
        except LookupError:
            print >>sys.stderr, "Encoding %s could not be found" % options.encoding
            sys.exit(10)

    if options.output_delimiter:
        # If output delimiter is specified, then we use it
        output_delimiter = options.output_delimiter
    else:
        # Otherwise,
        if options.delimiter:
            # if an input delimiter is specified, then we use it as the output as
            # well
            options.output_delimiter = options.delimiter
        else:
            # if no input delimiter is specified, then we use space as the default
            # (since no input delimiter means any whitespace)
            options.output_delimiter = " "

    default_input_params = QInputParams(skip_header=options.skip_header,
        delimiter=options.delimiter,
        input_encoding=options.encoding,
        gzipped_input=options.gzipped,
        parsing_mode=options.mode,
        expected_column_count=expected_column_count,
        keep_leading_whitespace_in_values=options.keep_leading_whitespace_in_values,
        disable_double_double_quoting=options.disable_double_double_quoting,
        disable_escaped_double_quoting=options.disable_escaped_double_quoting,
        input_quoting_mode=options.input_quoting_mode)
    q_engine = QTextAsData(default_input_params=default_input_params)

    output_params = QOutputParams(
        delimiter=options.output_delimiter,
        beautify=options.beautify,
        output_quoting_mode=options.output_quoting_mode,
        formatting=options.formatting,
        output_header=options.output_header)
    q_output_printer = QOutputPrinter(output_params)

    for query_str in query_strs:
        if options.analyze_only:
            q_output = q_engine.analyze(query_str,stdin_file=sys.stdin)
            q_output_printer.print_analysis(STDOUT,sys.stderr,q_output)
        else:
            q_output = q_engine.execute(query_str,stdin_file=sys.stdin)
            q_output_printer.print_output(STDOUT,sys.stderr,q_output)

        if q_output.status == 'error':
            sys.exit(q_output.error.errorcode)

    q_engine.unload()

    sys.exit(0)


if __name__ == '__main__':
    run_standalone()
