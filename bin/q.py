#!/usr/bin/env python

#   Copyright (C) 2012-2020 Harel Ben-Attia
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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict
from sqlite3.dbapi2 import OperationalError

q_version = '2.0.19'

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
from six.moves import configparser, range, filter
import traceback
import csv
import hashlib
import uuid
import math
import six
import io
import json
import inspect

if six.PY3:
    long = int
    unicode = six.text_type

DEBUG = '-V' in sys.argv

def xprint(*args,**kwargs):
    global DEBUG
    if DEBUG:
        print(*args,**kwargs)

def get_stdout_encoding(encoding_override=None):
    if encoding_override is not None and encoding_override != 'none':
       return encoding_override

    if sys.stdout.isatty():
        return sys.stdout.encoding
    else:
        return locale.getpreferredencoding()

SHOW_SQL = False

sha_algorithms = {
    1 : hashlib.sha1,
    224: hashlib.sha224,
    256: hashlib.sha256,
    386: hashlib.sha384,
    512: hashlib.sha512
}

def sha(data,algorithm,encoding):
    try:
        f = sha_algorithms[algorithm]
        return f(six.text_type(data).encode(encoding)).hexdigest()
    except Exception as e:
        print(e)

# For backward compatibility only (doesn't handle encoding well enough)
def sha1(data):
    return hashlib.sha1(six.text_type(data).encode('utf-8')).hexdigest()

# TODO Add caching of compiled regexps - Will be added after benchmarking capability is baked in
def regexp(regular_expression, data):
    if data is not None:
        if not isinstance(data, str) and not isinstance(data, unicode):
            data = str(data)
        return re.search(regular_expression, data) is not None
    else:
        return False

def md5(data,encoding):
    m = hashlib.md5()
    m.update(six.text_type(data).encode(encoding))
    return m.hexdigest()

def sqrt(data):
    return math.sqrt(data)

def power(data,p):
    return data**p

def percentile(l, p):
    # TODO Alpha implementation, need to provide multiple interpolation methods, and add tests
    if not l:
        return None
    k = p*(len(l) - 1)
    f = math.floor(k)
    c = math.ceil(k)
    if c == f:
        return l[int(k)]
    return (c-k) * l[int(f)] + (k-f) * l[int(c)]

# TODO Streaming Percentile to prevent memory consumption blowup for large datasets
class StrictPercentile(object):
    def __init__(self):
        self.values = []
        self.p = None
    def step(self,value,p):
        if self.p is None:
          self.p = p
        self.values.append(value)

    def finalize(self):
        if len(self.values) == 0 or (self.p < 0 or self.p > 1):
            return None
        else:
            return percentile(sorted(self.values),self.p)

class StdevPopulation(object):
    def __init__(self):
        self.M = 0.0
        self.S = 0.0
        self.k = 0

    def step(self, value):
        try:
            # Ignore nulls
            if value is None:
                return
            val = float(value) # if fails, skips this iteration, which also ignores nulls
            tM = self.M
            self.k += 1
            self.M += ((val - tM) / self.k)
            self.S += ((val - tM) * (val - self.M))
        except ValueError:
            # TODO propagate udf errors to console
            raise Exception("Data is not numeric when calculating stddev (%s)" % value)

    def finalize(self):
        if self.k <= 1: # avoid division by zero
            return None
        else:
            return math.sqrt(self.S / (self.k))

class StdevSample(object):
    def __init__(self):
        self.M = 0.0
        self.S = 0.0
        self.k = 0

    def step(self, value):
        try:
            # Ignore nulls
            if value is None:
                return
            val = float(value) # if fails, skips this iteration, which also ignores nulls
            tM = self.M
            self.k += 1
            self.M += ((val - tM) / self.k)
            self.S += ((val - tM) * (val - self.M))
        except ValueError:
            # TODO propagate udf errors to console
            raise Exception("Data is not numeric when calculating stddev (%s)" % value)

    def finalize(self):
        if self.k <= 1: # avoid division by zero
            return None
        else:
            return math.sqrt(self.S / (self.k-1))

class FunctionType(object):
    REGULAR = 1
    AGG = 2

class UserFunctionDef(object):
    def __init__(self,func_type,name,usage,description,func_or_obj,param_count):
        self.func_type = func_type
        self.name = name
        self.usage = usage
        self.description = description
        self.func_or_obj = func_or_obj
        self.param_count = param_count

user_functions = [
    UserFunctionDef(FunctionType.REGULAR,
                    "regexp","regexp(<regular_expression>,<expr>) = <1|0>",
                    "Find regexp in string expression. Returns 1 if found or 0 if not",
                    regexp,
                    2),
    UserFunctionDef(FunctionType.REGULAR,
                    "sha","sha(<expr>,<encoding>,<algorithm>) = <hex-string-of-sha>",
                    "Calculate sha of some expression. Algorithm can be one of 1,224,256,384,512. For now encoding must be manually provided. Will use the input encoding automatically in the future.",
                    sha,
                    3),
    UserFunctionDef(FunctionType.REGULAR,
                    "sha1","sha1(<expr>) = <hex-string-of-sha>",
                    "Exists for backward compatibility only, since it doesn't handle encoding properly. Calculates sha1 of some expression",
                    sha1,
                    1),
    UserFunctionDef(FunctionType.REGULAR,
                    "md5","md5(<expr>,<encoding>) = <hex-string-of-md5>",
                    "Calculate md5 of expression. Returns a hex-string of the result. Currently requires to manually provide the encoding of the data. Will be taken automatically from the input encoding in the future.",
                    md5,
                    2),
    UserFunctionDef(FunctionType.REGULAR,
                    "sqrt","sqrt(<expr>) = <square-root>",
                    "Calculate the square root of the expression",
                    sqrt,
                    1),
    UserFunctionDef(FunctionType.REGULAR,
                    "power","power(<expr1>,<expr2>) = <expr1-to-the-power-of-expr2>",
                    "Raise expr1 to the power of expr2",
                    power,
                    2),
    UserFunctionDef(FunctionType.AGG,
                    "percentile","percentile(<expr>,<percentile-in-the-range-0-to-1>) = <percentile-value>",
                    "Calculate the strict percentile of a set of a values.",
                    StrictPercentile,
                    2),
    UserFunctionDef(FunctionType.AGG,
                    "stddev_pop","stddev_pop(<expr>) = <stddev-value>",
                    "Calculate the population standard deviation of a set of values",
                    StdevPopulation,
                    1),
    UserFunctionDef(FunctionType.AGG,
                    "stddev_sample","stddev_sample(<expr>) = <stddev-value>",
                    "Calculate the sample standard deviation of a set of values",
                    StdevSample,
                    1)
]

def print_user_functions():
    for udf in user_functions:
        print("Function: %s" % udf.name)
        print("     Usage: %s" % udf.usage)
        print("     Description: %s" % udf.description)

class Sqlite3DBResults(object):
    def __init__(self,query_column_names,results):
        self.query_column_names = query_column_names
        self.results = results

class Sqlite3DB(object):

    def __init__(self, sqlite_db_url, create_metaq, show_sql=SHOW_SQL):
        self.show_sql = show_sql
        self.create_metaq = create_metaq

        self.sqlite_db_url = sqlite_db_url
        if six.PY2:
            self.conn = sqlite3.connect(self.sqlite_db_url)
        else:
            self.conn = sqlite3.connect(self.sqlite_db_url, uri=True)
        self.last_temp_table_id = 10000
        self.cursor = self.conn.cursor()
        self.type_names = {
            str: 'TEXT', int: 'INT', long : 'INT' , float: 'FLOAT', None: 'TEXT'}
        self.numeric_column_types = set([int, long, float])
        self.add_user_functions()

        if create_metaq:
            self.create_metaq_table()

    def create_metaq_table(self):
        with self.conn as cursor:
            r = cursor.execute('CREATE TABLE if not exists metaq (filenames_str text, temp_table_name, content_signature text, creation_time text)')
            _ = r.fetchall()

    def add_to_metaq_table(self, filenames_str, temp_table_name, content_signature, creation_time):
        xprint("adding to metaq table")
        xprint("Adding to metaq table: %s %s" % (filenames_str,temp_table_name))
        import json
        with self.conn as cursor:
            r = cursor.execute('INSERT INTO metaq (filenames_str,temp_table_name,content_signature,creation_time) VALUES (?,?,?,?)',
                               (filenames_str,temp_table_name,json.dumps(content_signature),creation_time))
            _ = r.fetchall()

    def get_from_metaq(self,filenames_str):
        xprint("getting from metaq %s" % filenames_str)
        with self.conn as cursor:
            q = 'SELECT filenames_str,temp_table_name,content_signature,creation_time FROM metaq where filenames_str = ?'
            xprint("Query from metaq %s" % q)
            r = cursor.execute(q,(filenames_str,))


            results = r.fetchall()
            if results is None:
                raise InvalidQSqliteFileException("Invalid qsqlite file - Cannot find table %s" % (filenames_str))

            if len(results) > 1:
                raise Exception("Bug - Exactly one result should have been provided: %s" % str(results))

            d = dict(zip(["filenames_str","temp_table_name","content_signature","creation_time"],results[0]))
            return d

    def done(self):
        self.conn.commit()

    # TODO RLRL - Remove standard method, we can now release with dependencies
    def store_db_to_disk_standard(self,sqlite_db_filename,table_names_mapping):
        new_db = sqlite3.connect(sqlite_db_filename,isolation_level=None)
        c = new_db.cursor()
        for s in self.conn.iterdump():
            c.execute(s)
            _ = c.fetchall()
        for source_filename_str,tn in six.iteritems(table_names_mapping):
            c.execute('alter table `%s` rename to `%s`' % (tn, source_filename_str))
        new_db.close()

    def store_db_to_disk_fast(self,sqlite_db_filename,table_names_mapping):
        try:
            import sqlitebck
        except ImportError as e:
            msg = "sqlitebck python module cannot be found - fast store to disk cannot be performed. Note that for now, sqlitebck is not packaged as part of q. In order to use the fast method, you need to manually `pip install sqlitebck` into your python environment. We obviously consider this as a bug and it will be fixed once proper packaging will be done, making the fast method the standard one."
            raise MissingSqliteBckModuleException(msg)

        new_db = sqlite3.connect(sqlite_db_filename)
        sqlitebck.copy(self.conn,new_db)
        c = new_db.cursor()
        for source_filename_str,tn in six.iteritems(table_names_mapping):
            c.execute('alter table `%s` rename to `%s`' % (tn, source_filename_str))
        new_db.close()

    def store_db_to_disk(self,sqlite_db_filename,table_names_mapping,method='standard'):
        if method == 'standard':
            self.store_db_to_disk_standard(sqlite_db_filename,table_names_mapping)
        elif method == 'fast':
            self.store_db_to_disk_fast(sqlite_db_filename,table_names_mapping)
        else:
            raise ValueError('Unknown store-db-to-disk method %s' % method)

    def add_user_functions(self):
        for udf in user_functions:
            if type(udf.func_or_obj) == type(object):
                self.conn.create_aggregate(udf.name,udf.param_count,udf.func_or_obj)
            elif type(udf.func_or_obj) == type(md5):
                self.conn.create_function(udf.name,udf.param_count,udf.func_or_obj)
            else:
                raise Exception("Invalid user function definition %s" % str(udf))

    def is_numeric_type(self, column_type):
        return column_type in self.numeric_column_types

    def update_many(self, sql, params):
        try:
            if self.show_sql:
                print(sql, " params: " + str(params))
            self.cursor.executemany(sql, params)
            _ = self.cursor.fetchall()
            # TODO RLRL transaction commits
        finally:
            pass  # cursor.close()

    def execute_and_fetch(self, q):
        try:
            if self.show_sql:
                print(repr(q))
            self.cursor.execute(q)
            if self.cursor.description is not None:
                # we decode the column names, so they can be encoded to any output format later on
                if six.PY2:
                    query_column_names = [unicode(c[0],'utf-8') for c in self.cursor.description]
                else:
                    query_column_names = [c[0] for c in self.cursor.description]
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
            (n, self.type_names[t]) for n, t in six.iteritems(column_dict))
        column_defs = ','.join(['"%s" %s' % (
            n.replace('"', '""'), column_name_to_db_type[n]) for n in column_names])
        return 'CREATE TABLE %s (%s)' % (table_name, column_defs)

    def generate_temp_table_name(self):
        # WTF - From my own past mutable-self
        self.last_temp_table_id += 1
        tn = "temp_table_%s" % self.last_temp_table_id
        return tn

    def generate_drop_table(self, table_name):
        return "DROP TABLE %s" % table_name

    def drop_table(self, table_name):
        return self.execute_and_fetch(self.generate_drop_table(table_name))

class CouldNotConvertStringToNumericValueException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class ColumnMaxLengthLimitExceededException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class MissingSqliteBckModuleException(Exception):

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


class CannotUnzipDataStreamException(Exception):

    def __init__(self):
        pass

class UniversalNewlinesExistException(Exception):

    def __init__(self):
        pass

class UnprovidedStdInException(Exception):

    def __init__(self):
        pass

class EmptyDataException(Exception):

    def __init__(self):
        pass

class MissingHeaderException(Exception):

    def __init__(self,msg):
        self.msg = msg


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

class ContentSignatureDiffersException(Exception):

    def __init__(self,filenames_str,key,source_value,signature_value):
        self.filenames_str = filenames_str
        self.key = key
        self.source_value = source_value
        self.signature_value = signature_value


class ContentSignatureDataDiffersException(Exception):

    def __init__(self,msg):
        self.msg = msg


class InvalidQSqliteFileException(Exception):

    def __init__(self,msg):
        self.msg = msg

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
        self.qtable_names = []
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

                self.qtable_names += [qtable_name]

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
            if self.qtable_name_effective_table_names[qtable_name] != effective_table_name:
                raise Exception(
                    "Already set effective table name for qtable %s. Trying to change the effective table name from %s to %s" %
                    (qtable_name,self.qtable_name_effective_table_names[qtable_name],effective_table_name))

        self.qtable_name_effective_table_names[
            qtable_name] = effective_table_name

    def get_effective_sql(self,original_names=False):
        if len(list(filter(lambda x: x is None, self.qtable_name_effective_table_names))) != 0:
            raise Exception('There are qtables without effective tables')

        effective_sql = [x for x in self.sql_parts]

        for qtable_name, positions in six.iteritems(self.qtable_name_positions):
            for pos in positions:
                if not original_names:
                    effective_sql[pos] = self.qtable_name_effective_table_names[
                        qtable_name]
                else:
                    effective_sql[pos] = "`%s`" % qtable_name

        return " ".join(effective_sql)

    def get_qtable_name_effective_table_names(self):
        return self.qtable_name_effective_table_names

    def execute_and_fetch(self, db):
        x = self.get_effective_sql()
        xprint("Final query: %s" % x)
        db_results_obj = db.execute_and_fetch(x)
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

    def generate_content_signature(self):
        return OrderedDict({
            "delimiter": self.delimiter,
            "expected_column_count": self.expected_column_count
        })


class TableColumnInferer(object):

    def __init__(self, mode, expected_column_count, input_delimiter, skip_header=False,disable_column_type_detection=False):
        self.inferred = False
        self.mode = mode
        self.rows = []
        self.skip_header = skip_header
        self.header_row = None
        self.header_row_filename = None
        self.expected_column_count = expected_column_count
        self.input_delimiter = input_delimiter
        self.disable_column_type_detection = disable_column_type_detection

    def generate_content_signature(self):
        return OrderedDict({
            "inferred": self.inferred,
            "mode": self.mode,
            "rows": "\n".join([",".join(x) for x in self.rows]),
            "skip_header": self.skip_header,
            "header_row": self.header_row,
            "expected_column_count": self.expected_column_count,
            "input_delimiter": self.input_delimiter,
            "disable_column_type_detection": self.disable_column_type_detection
        })

    def analyze(self, filename, col_vals):
        if self.inferred:
            raise Exception("Already inferred columns")

        if self.skip_header and self.header_row is None:
            self.header_row = col_vals
            self.header_row_filename = filename
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
        if self.disable_column_type_detection:
            return str

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
            type_list_without_nulls = list(filter(
                lambda x: x is not None, type_list))
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
            print("Warning: column count is one - did you provide the correct delimiter?", file=sys.stderr)

        self.infer_column_types()
        self.infer_column_names()
        self.inferred = True

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
            # We're checking for column duplication for each field in order to be able to still provide it along with other errors
            if len(list(filter(lambda x: x == v,value_list))) > 1:
                entry = (v, "Column name is duplicated")
                # Don't duplicate the error report itself
                if entry not in column_name_errors:
                    column_name_errors.append(entry)
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
                         for x in range(self.column_count - len(self.header_row))]
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

        if len(self.rows) == 0:
            self.column_count = 0
        else:
            if self.expected_column_count is not None:
                self.column_count = self.expected_column_count
            else:
                # If not specified, we'll take the largest row in the sample rows
                self.column_count = max(column_count_list)

    def get_column_count_summary(self, column_count_list):
        counts = {}
        for column_count in column_count_list:
            counts[column_count] = counts.get(column_count, 0) + 1
        return six.u(", ").join([six.u("{} rows with {} columns".format(v, k)) for k, v in six.iteritems(counts)])

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
        for column_number in range(self.column_count):
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
                print('Warning - There seems to be header line in the file, but -H has not been specified. All fields will be detected as text fields, and the header line will appear as part of the data', file=sys.stderr)

    def get_column_dict(self):
        return dict(zip(self.column_names, self.column_types))

    def get_column_count(self):
        return self.column_count

    def get_column_names(self):
        return self.column_names

    def get_column_types(self):
        return self.column_types


def py3_encoded_csv_reader(encoding, f, dialect, **kwargs):
    try:
        csv_reader = csv.reader(f, dialect, **kwargs)

        for row in csv_reader:
            yield row
    except ValueError as e:
        if e.message is not None and e.message.startswith('could not convert string to'):
            raise CouldNotConvertStringToNumericValueException(e.message)
        else:
            raise CouldNotParseInputException(str(e))
    except Exception as e:
        if str(e).startswith("field larger than field limit"):
            raise ColumnMaxLengthLimitExceededException(str(e))
        elif 'universal-newline' in str(e):
            raise UniversalNewlinesExistException()
        else:
            raise


def py2_encoded_csv_reader(encoding, f, dialect, **kwargs):
    try:
        csv_reader = csv.reader(f, dialect, **kwargs)
        if encoding is not None and encoding != 'none':
            for row in csv_reader:
                yield [unicode(x, encoding) for x in row]
        else:
            for row in csv_reader:
                yield row
    except ValueError as e:
        if e.message is not None and e.message.startswith('could not convert string to'):
            raise CouldNotConvertStringToNumericValueException(e.message)
        else:
            raise CouldNotParseInputException(str(e))
    except Exception as e:
        if str(e).startswith("field larger than field limit"):
            raise ColumnMaxLengthLimitExceededException(str(e))
        elif 'universal-newline' in str(e):
            raise UniversalNewlinesExistException()
        else:
            raise

if six.PY2:
    encoded_csv_reader = py2_encoded_csv_reader
else:
    encoded_csv_reader = py3_encoded_csv_reader

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
    def __init__(self,filename,f,encoding,dialect,data_stream=None):
        self.filename = filename
        self.lines_read = 0
        self.f = f
        self.encoding = encoding
        self.dialect = dialect
        self.data_stream = data_stream
        self.skipped_bom = False

    def read_file_using_csv(self):
        # This is a hack for utf-8 with BOM encoding in order to skip the BOM. python's csv module
        # has a bug which prevents fixing it using the proper encoding, and it has been encountered by 
        # multiple people.
        if self.encoding == 'utf-8-sig' and self.lines_read == 0 and not self.skipped_bom:
            try:
                if six.PY2:
                    BOM = self.f.read(3)
                else:
                    BOM = self.f.buffer.read(3)

                if BOM != six.b('\xef\xbb\xbf'):
                    raise Exception('Value of BOM is not as expected - Value is "%s"' % str(BOM))
            except Exception as e:
                raise Exception('Tried to skip BOM for "utf-8-sig" encoding and failed. Error message is ' + str(e))
        csv_reader = encoded_csv_reader(self.encoding, self.f, dialect=self.dialect)
        try:
            for col_vals in csv_reader:
                self.lines_read += 1
                yield col_vals
        except ColumnMaxLengthLimitExceededException as e:
            msg = "Column length is larger than the maximum. Offending file is '%s' - Line is %s, counting from 1 (encoding %s). The line number is the raw line number of the file, ignoring whether there's a header or not" % (self.filename,self.lines_read + 1,self.encoding)
            raise ColumnMaxLengthLimitExceededException(msg)
        except UniversalNewlinesExistException as e2:
            # No need to translate the exception, but we want it to be explicitly defined here for clarity
            raise UniversalNewlinesExistException()

    def close(self):
        if self.f != sys.stdin:
            self.f.close()

import hashlib

class TableCreator(object):

    def __init__(self, filenames_str, line_splitter, skip_header=False, gzipped=False, with_universal_newlines=False,
                 encoding='UTF-8', mode='fluffy', expected_column_count=None, input_delimiter=None,
                 disable_column_type_detection=False,data_stream=None,
                 read_caching=False,write_caching=False,adhoc_db_to_use=None):
        xprint("in table creator init ",inspect.getouterframes(inspect.currentframe(),2)[1])
        self.filenames_str = filenames_str

        # TODO RLRL - "disk_db should actually become the "db", as we're splitting everything to run through attached dbs
        #             whether in memory or disk based
        if adhoc_db_to_use is not None:
            self.db = adhoc_db_to_use
        else:
            self.db = Sqlite3DB('file:mem-%s?mode=memory&cache=shared' % self._generate_disk_db_name(),create_metaq=True)
        self.adhoc_db_to_use = adhoc_db_to_use

        self.skip_header = skip_header
        self.gzipped = gzipped
        self.table_created = False
        self.line_splitter = line_splitter
        self.encoding = encoding
        self.mode = mode
        self.expected_column_count = expected_column_count
        self.input_delimiter = input_delimiter
        self.data_stream = data_stream
        self.with_universal_newlines = with_universal_newlines

        self.column_inferer = TableColumnInferer(
            mode, expected_column_count, input_delimiter, skip_header,disable_column_type_detection)

        # Filled only after table population since we're inferring the table
        # creation data
        self.table_name = None

        self.pre_creation_rows = []
        self.buffered_inserts = []
        self.effective_column_names = None

        # Column type indices for columns that contain numeric types. Lazily initialized
        # so column inferer can do its work before this information is needed
        self.numeric_column_indices = None

        self.materialized_file_list = self.materialize_file_list()
        self.materialized_file_dict = {}

        self.state = TableCreatorState.NEW

        self.read_caching = read_caching
        self.write_caching = write_caching
        self.disk_db = None
        self.disk_db_filename = self._generate_disk_db_filename()
        self.disk_db_name = self._generate_disk_db_name()
        self.disk_db_file_exists = os.path.exists(self.disk_db_filename)
        self.disk_db_content_signature = None

    def attach_to(self,query_level_db,disk_url=None):
        if disk_url is None:
            effective_url = self.db.sqlite_db_url
        else:
            effective_url = disk_url

        q = "attach '%s' as %s" % (effective_url,self._generate_disk_db_name())
        xprint("Attach query: %s" % q)
        c = query_level_db.execute(q)
        c.fetchall()

    def generate_content_signature(self):
        # TODO RLRL - Push metaq access to the upper layer instead of inside TableCreator
        if self.data_stream is None:
            parts = self.filenames_str.split("+")
            fns = os.path.abspath(parts[0])
            size = [os.stat(x).st_size for x in parts]
        else:
            fns = self.filenames_str
            size = 0

        m = OrderedDict({
            "_signature_version": "v1",
            "filenames_str": fns,
            "line_splitter": self.line_splitter.generate_content_signature(),
            "skip_header": self.skip_header,
            "gzipped": self.gzipped,
            "with_universal_newlines": self.with_universal_newlines,
            "encoding": self.encoding,
            "mode": self.mode,
            "expected_column_count": self.expected_column_count,
            "input_delimiter": self.input_delimiter,
            "inferer": self.column_inferer.generate_content_signature(),
            "original_file_size": size
        })

        # TODO RLRL - allow changing default caching through a side-file
        # TODO RLRL - Solve multi table caching
        return m

    def _generate_disk_db_filename(self):
        fn = '%s.qsqlite' % (os.path.abspath(self.filenames_str).replace("+","__"))
        return fn

    def _generate_disk_db_name(self):
        return 'ddb_' + hashlib.sha1(six.b(self.filenames_str)).hexdigest()


    def materialize_file_list(self):
        materialized_file_list = []

        # Get the list of filenames
        filenames = self.filenames_str.split("+")

        # for each filename (or pattern)
        for fileglob in filenames:
            # Allow either a stream or a glob match
            if self.data_stream is not None:
                materialized_file_list.append(self.data_stream.filename)
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
        # TODO Support universal newlines for gzipped and stdin data as well

        # Check if it's a data stream
        if self.data_stream is not None:
            f = self.data_stream.stream
            if self.gzipped:
                raise CannotUnzipDataStreamException()
        else:
            if self.gzipped or filename.endswith('.gz'):
                f = codecs.iterdecode(gzip.GzipFile(fileobj=io.open(filename,'rb')),encoding=self.encoding)
            else:
                if six.PY3:
                    if self.with_universal_newlines:
                        f = io.open(filename, 'rU',newline=None,encoding=self.encoding)
                    else:
                        f = io.open(filename, 'r', newline=None, encoding=self.encoding)
                else:
                    if self.with_universal_newlines:
                        file_opening_mode = 'rbU'
                    else:
                        file_opening_mode = 'rb'
                    f = open(filename, file_opening_mode)
        return f

    def _pre_populate(self,dialect):
        # For each match
        for filename in self.materialized_file_list:
            if filename in self.materialized_file_dict.keys():
                continue

            f = self.open_file(filename)

            mfs = MaterializedFileState(filename,f,self.encoding,dialect,self.data_stream)
            self.materialized_file_dict[filename] = mfs

    def _should_skip_extra_headers(self, filenumber, filename, mfs, col_vals):
        if not self.skip_header:
            return False

        if filenumber == 0:
            return False

        header_already_exists = self.column_inferer.header_row is not None

        is_extra_header = self.skip_header and mfs.lines_read == 1 and header_already_exists

        if is_extra_header:
            if tuple(self.column_inferer.header_row) != tuple(col_vals):
                raise BadHeaderException("Extra header {} in file {} mismatches original header {} from file {}. Table name is {}".format(",".join(col_vals),mfs.filename,",".join(self.column_inferer.header_row),self.column_inferer.header_row_filename,self.filenames_str))

        return is_extra_header

    def _populate(self,dialect,stop_after_analysis=False):
        total_data_lines_read = 0

        # For each match
        for filenumber,filename in enumerate(self.materialized_file_list):
            mfs = self.materialized_file_dict[filename]

            try:
                try:
                    for col_vals in mfs.read_file_using_csv():
                        if self._should_skip_extra_headers(filenumber,filename,mfs,col_vals):
                            continue
                        self._insert_row(filename, col_vals)
                        if self.column_inferer.inferred and self.disk_db_file_exists and self.read_caching:
                            return
                        if stop_after_analysis and self.column_inferer.inferred:
                            return
                    if mfs.lines_read == 0 and self.skip_header:
                        raise MissingHeaderException("Header line is expected but missing in file %s" % filename)

                    total_data_lines_read += mfs.lines_read - (1 if self.skip_header else 0)
                except StrictModeColumnCountMismatchException as e:
                    raise ColumnCountMismatchException(
                        'Strict mode - Expected %s columns instead of %s columns in file %s row %s. Either use relaxed/fluffy modes or check your delimiter' % (
                        e.expected_col_count, e.actual_col_count, normalized_filename(mfs.filename), mfs.lines_read))
                except FluffyModeColumnCountMismatchException as e:
                    raise ColumnCountMismatchException(
                        'Deprecated fluffy mode - Too many columns in file %s row %s (%s fields instead of %s fields). Consider moving to either relaxed or strict mode' % (
                        normalized_filename(mfs.filename), mfs.lines_read, e.actual_col_count, e.expected_col_count))
            finally:
                if not stop_after_analysis:
                    mfs.close()
                self._flush_inserts()

            if not self.table_created:
                self.column_inferer.force_analysis()
                self._do_create_table(filename)
            #self.db.conn.execute('COMMIT').fetchall()


        if total_data_lines_read == 0:
            raise EmptyDataException()

    def populate(self,dialect,stop_after_analysis=False):
        xprint("in populate ",self.filenames_str)
        if self.state == TableCreatorState.NEW:
            self._pre_populate(dialect)
            self.state = TableCreatorState.INITIALIZED

        if self.state == TableCreatorState.INITIALIZED:
            self._populate(dialect,stop_after_analysis=True)
            self.state = TableCreatorState.ANALYZED

            import datetime
            now = datetime.datetime.utcnow().isoformat()
            # RLRLRL
            # TODO RLRL - Pass metaq only the first file when using data1+data2, so the location of the qsqlite file will be near it
            # TODO RLRL - Move abspath to a separate member
            self.db.add_to_metaq_table(os.path.abspath(self.filenames_str), self.table_name,
                                       self.generate_content_signature(), now)

            if stop_after_analysis:
                return

        if self.state == TableCreatorState.ANALYZED:
            if self.disk_db_file_exists and self.read_caching:
                tmp_c = self.db.conn.execute('COMMIT')
                _ = tmp_c.fetchall()
                self.load_data_from_disk_db()
                self.state = TableCreatorState.FULLY_READ
            else:
                self._populate(dialect,stop_after_analysis=False)
                self.state = TableCreatorState.FULLY_READ
                if self.write_caching:
                    self.store_data_as_disk_db()

            return

    def get_table_name_for_querying(self):
        xprint("Getting table name for querying %s" % self.disk_db_name)
        xprint("getting table name for querying: ",self.db.conn.execute("select * from metaq").fetchall())
        # TODO RLRL - adhoc db is ok, but the filenames_str needs more work in order to allow stdin injection isolation
        # TODO and is incorrect in its nature
        d = self.db.get_from_metaq(os.path.abspath(self.filenames_str))
        table_name_in_disk_db = d['temp_table_name']
        return table_name_in_disk_db

    def _flush_pre_creation_rows(self, filename):
        for i, col_vals in enumerate(self.pre_creation_rows):
            if self.skip_header and i == 0:
                # skip header line
                continue
            self._insert_row(filename, col_vals)
        self._flush_inserts()
        self.pre_creation_rows = []

    def _insert_row(self, filename, col_vals):
        # If table has not been created yet
        if not self.table_created:
            # Try to create it along with another "example" line of data
            self.try_to_create_table(filename, col_vals)

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
                [None for x in range(expected_col_count - actual_col_count)]

        # in relaxed mode, we merge all extra columns to the last column value
        if self.mode == 'relaxed':
            if actual_col_count > expected_col_count:
                xxx = col_vals[:expected_col_count - 1] + \
                    [self.input_delimiter.join([v if v  is not None else '' for v in
                        col_vals[expected_col_count - 1:]])]
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

        if self.effective_column_names is None:
            self.effective_column_names = self.column_inferer.column_names[:len(col_vals)]

        if len(self.effective_column_names) > 0:
            self.buffered_inserts.append(col_vals)
        else:
            self.buffered_inserts.append([""])

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
                self.table_name, self.effective_column_names)

            self.db.update_many(insert_row_stmt, self.buffered_inserts)
        # print self.db.execute_and_fetch(self.db.generate_end_transaction())
        self.buffered_inserts = []

    def try_to_create_table(self, filename, col_vals):
        if self.table_created:
            raise Exception('Table is already created')

        # Add that line to the column inferer
        result = self.column_inferer.analyze(filename, col_vals)
        # If inferer succeeded,
        if result:
            self._do_create_table(filename)
        else:
            pass  # We don't have enough information for creating the table yet

    def _do_create_table(self,filename):
        # Then generate a temp table name
        tbl_name = self.db.generate_temp_table_name()
        self.table_name = tbl_name
        # Get the column definition dict from the inferer
        column_dict = self.column_inferer.get_column_dict()

        # Guard against empty tables (instead of preventing the creation, just create with a dummy column)
        if len(column_dict) == 0:
            column_dict = { 'dummy_column_for_empty_tables' : str }
            ordered_column_names = [ 'dummy_column_for_empty_tables' ]
        else:
            ordered_column_names = self.column_inferer.get_column_names()

        # Create the CREATE TABLE statement
        create_table_stmt = self.db.generate_create_table(
            self.table_name, ordered_column_names, column_dict)
        # And create the table itself
        self.db.execute_and_fetch(create_table_stmt)
        # Mark the table as created
        self.table_created = True
        self._flush_pre_creation_rows(filename)

    def drop_table(self):
        if self.table_created:
            self.db.drop_table(self.table_name)

    def store_data_as_disk_db(self):
        import sqlitebck
        if self.state != TableCreatorState.FULLY_READ:
            raise Exception("Bug - storing data to a disk db is supposed to be happen right after table is fully read")
        disk_db_conn = sqlite3.connect(self.disk_db_filename)
        sqlitebck.copy(self.db.conn,disk_db_conn)
        print("--- Written db to disk: disk db filename %s metaq: %s" % (self.disk_db_filename,disk_db_conn.execute('select filenames_str,temp_table_name from metaq').fetchall()))
        print(disk_db_conn.execute("select 'x',count(*) from temp_table_10001").fetchall())
        disk_db_conn.close()

    def load_data_from_disk_db(self):
        start = time.time()
        if self.state != TableCreatorState.ANALYZED:
            raise Exception("Bug - loading data from disk db is supposed to happen right after analysis")

        self.db.done()
        self.db.conn.close()

        x = 'file:%s?immutable=1' % self.disk_db_filename
        self.db = Sqlite3DB(x,create_metaq=False)

        xprint("Getting content signature for %s" % x)
        r = self.db.get_from_metaq(os.path.abspath(self.filenames_str))
        self.validate_content_signature(self.generate_content_signature(),json.loads(r['content_signature']))

        xprint("--- db has been from disk: disk db name: %s disk db filename %s metaq: %s" % (self.disk_db_name,
                                                                                     self.disk_db_filename,
                                                                                     self.db.conn.execute('select filenames_str,temp_table_name from metaq').fetchall()))

    def validate_content_signature(self,source_signature,content_signature,scope=None):
        if scope is None:
            scope = []
        for k in source_signature:
            if type(source_signature[k]) == OrderedDict:
                r = self.validate_content_signature(source_signature[k],content_signature[k],scope + [k])
                if r:
                    return True
            else:
                if k not in content_signature:
                    raise ContentSignatureDataDiffersException("Content Signatures differ. %s is missing from content signature" % k)
                if source_signature[k] != content_signature[k]:
                    if k == 'rows':
                        raise ContentSignatureDataDiffersException("Content Signatures differ at %s.%s (actual analysis data differs)" % (".".join(scope),k))
                    else:
                        raise ContentSignatureDiffersException(self.filenames_str,".".join(scope + [k]),source_signature[k],content_signature[k])



def determine_max_col_lengths(m,output_field_quoting_func,output_delimiter):
    if len(m) == 0:
        return []
    max_lengths = [0 for x in range(0, len(m[0]))]
    for row_index in range(0, len(m)):
        for col_index in range(0, len(m[0])):
            # TODO Optimize this and make sure that py2 hack of float precision is applied here as well
            new_len = len("{}".format(output_field_quoting_func(output_delimiter,m[row_index][col_index])))
            if new_len > max_lengths[col_index]:
                max_lengths[col_index] = new_len
    return max_lengths

def print_credentials():
    print("q version %s" % q_version, file=sys.stderr)
    print("Python: %s" % " // ".join([str(x).strip() for x in sys.version.split("\n")]), file=sys.stderr)
    print("Copyright (C) 2012-2020 Harel Ben-Attia (harelba@gmail.com, @harelba on twitter)", file=sys.stderr)
    print("http://harelba.github.io/q/", file=sys.stderr)
    print(file=sys.stderr)

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
    def __init__(self,filename,start_time,end_time,data_stream=None):
        self.filename = filename
        self.start_time = start_time
        self.end_time = end_time
        self.data_stream = data_stream

    def duration(self):
        return self.end_time - self.start_time

    def __str__(self):
        return "DataLoad<'%s' at %s (took %4.3f seconds),data_stream=%s>" % (self.filename,self.start_time,self.duration(),self.data_stream)
    __repr__ = __str__

class QMaterializedFile(object):
    def __init__(self,filename,data_stream):
        self.filename = filename
        self.data_stream = data_stream

    def __str__(self):
        return "QMaterializedFile<filename=%s,data_stream=%s>" % (self.filename,self.data_stream)
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
            delimiter=' ',input_encoding='UTF-8',gzipped_input=False,with_universal_newlines=False,parsing_mode='relaxed',
            expected_column_count=None,keep_leading_whitespace_in_values=False,
            disable_double_double_quoting=False,disable_escaped_double_quoting=False,
            disable_column_type_detection=False,
            input_quoting_mode='minimal',stdin_file=None,stdin_filename='-',
            max_column_length_limit=131072,
            read_caching=False,
            write_caching=False):
        self.skip_header = skip_header
        self.delimiter = delimiter
        self.input_encoding = input_encoding
        self.gzipped_input = gzipped_input
        self.with_universal_newlines = with_universal_newlines
        self.parsing_mode = parsing_mode
        self.expected_column_count = expected_column_count
        self.keep_leading_whitespace_in_values = keep_leading_whitespace_in_values
        self.disable_double_double_quoting = disable_double_double_quoting
        self.disable_escaped_double_quoting = disable_escaped_double_quoting
        self.input_quoting_mode = input_quoting_mode
        self.disable_column_type_detection = disable_column_type_detection
        self.max_column_length_limit = max_column_length_limit
        self.read_caching = read_caching
        self.write_caching = write_caching

    def merged_with(self,input_params):
        params = QInputParams(**self.__dict__)
        if input_params is not None:
            params.__dict__.update(**input_params.__dict__)
        return params

    def __str__(self):
        return "QInputParams<%s>" % str(self.__dict__)

    def __repr__(self):
        return "QInputParams(...)"

class DataStream(object):
    # TODO RLRL - Is there a need for stream id?
    def __init__(self,stream_id,filename,stream):
        self.stream_id = stream_id
        self.filename = filename
        self.stream = stream

class DataStreams(object):
    def __init__(self, data_streams_dict):
        if data_streams_dict is not None:
            self.validate(data_streams_dict)
            self.data_streams_dict = data_streams_dict
        else:
            self.data_streams_dict = {}

    def validate(self,d):
        for k in d:
            v = d[k]
            if type(k) != str or type(v) != DataStream:
                raise Exception('Bug - Invalid dict: %s' % str(d))

    def get_for_filename(self, filename):
        x = self.data_streams_dict.get(filename)
        return x

class QTextAsData(object):
    def __init__(self,default_input_params=QInputParams(),data_streams_dict=None):
        self.default_input_params = default_input_params

        self.table_creators = {}

        if data_streams_dict is not None:
            self.data_streams = DataStreams(data_streams_dict)
        else:
            self.data_streams = DataStreams({})

        # Create DB object
        self.query_level_db = Sqlite3DB('file:query-level-db?mode=memory&cache=shared',create_metaq=True)
        self.adhoc_db_name = 'file:adhoc-db?mode=memory&cache=shared'
        self.adhoc_db = Sqlite3DB(self.adhoc_db_name,create_metaq=True)
        self.query_level_db.conn.execute("attach '%s' as adhoc_db" % self.adhoc_db_name)


    input_quoting_modes = {   'minimal' : csv.QUOTE_MINIMAL,
                        'all' : csv.QUOTE_ALL,
                        # nonnumeric is not supported for input quoting modes, since we determine the data types
                        # ourselves instead of letting the csv module try to identify the types
                        'none' : csv.QUOTE_NONE }

    def close_all(self):
        for tc in self.table_creators:
            xprint("Closing %s" % tc)
            self.table_creators[tc].db.conn.close()
            #XXX
        self.query_level_db.conn.close()

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

    def _load_data(self,filename,input_params=QInputParams(),stop_after_analysis=False):
        xprint("loading data",inspect.getouterframes(inspect.currentframe(),2)[1])

        start_time = time.time()

        q_dialect = self.determine_proper_dialect(input_params)
        dialect_id = self.get_dialect_id(filename)
        csv.register_dialect(dialect_id, **q_dialect)

        csv.field_size_limit(input_params.max_column_length_limit)

        # Create a line splitter
        line_splitter = LineSplitter(input_params.delimiter, input_params.expected_column_count)

        # reuse already loaded data, except for data streams
        xprint("checking",self.table_creators.keys())
        if filename in self.table_creators.keys():
            return None

        # Skip caching for streams input
        if self.data_streams.get_for_filename(filename):
            effective_read_caching = False
            effective_write_caching = False
            adhoc_db_to_use = self.adhoc_db
            data_stream = self.data_streams.get_for_filename(filename)
        else:
            effective_read_caching = input_params.read_caching
            effective_write_caching = input_params.write_caching
            adhoc_db_to_use = None
            data_stream = None

        # Create the matching database table and populate it
        table_creator = TableCreator(
            filename, line_splitter, input_params.skip_header, input_params.gzipped_input, input_params.with_universal_newlines,input_params.input_encoding,
            mode=input_params.parsing_mode, expected_column_count=input_params.expected_column_count,
            input_delimiter=input_params.delimiter,disable_column_type_detection=input_params.disable_column_type_detection,
            data_stream=data_stream,
            read_caching=effective_read_caching,write_caching=effective_write_caching,adhoc_db_to_use=adhoc_db_to_use)

        table_creator.populate(dialect_id,stop_after_analysis)

        if adhoc_db_to_use is None:
            table_creator.attach_to(self.query_level_db.conn)

        self.table_creators[filename] = table_creator

        return QDataLoad(filename,start_time,time.time(),data_stream=data_stream)

    def load_data(self,filename,input_params=QInputParams(),stop_after_analysis=False):
        return self._load_data(filename,input_params,stop_after_analysis=stop_after_analysis)

    def _ensure_data_is_loaded(self,sql_object,input_params,data_streams=None,stop_after_analysis=False):
        xprint("Data load")
        data_loads = []

        # Get each "table name" which is actually the file name
        for filename in sql_object.qtable_names:
            xprint("XXX",filename)
            data_load = self._load_data(filename,input_params,stop_after_analysis=stop_after_analysis)
            if data_load is not None:
                data_loads.append(data_load)

        return data_loads

    def materialize_sql_object(self,sql_object):
        for filename in sql_object.qtable_names:
            tc = self.table_creators[filename]
            table_name_in_disk_db = tc.get_table_name_for_querying()
            if tc.adhoc_db_to_use is not None:
                effective_table_name = 'adhoc_db.%s' % (table_name_in_disk_db)
            else:
                effective_table_name = '%s.%s' % (tc.disk_db_name,table_name_in_disk_db)
            sql_object.set_effective_table_name(filename,effective_table_name)

    def _execute(self,query_str,input_params=None,data_streams=None,stop_after_analysis=False,save_db_to_disk_filename=None,save_db_to_disk_method=None):
        warnings = []
        error = None
        data_loads = []
        table_structures = []

        db_results_obj = None

        effective_input_params = self.default_input_params.merged_with(input_params)

        if type(query_str) != unicode:
            try:
                # Heuristic attempt to auto convert the query to unicode before failing
                query_str = query_str.decode('utf-8')
            except:
                error = QError(EncodedQueryException(''),"Query should be in unicode. Please make sure to provide a unicode literal string or decode it using proper the character encoding.",91)
                return QOutput(error = error)

        # Create SQL statement
        sql_object = Sql('%s' % query_str)

        try:
            load_start_time = time.time()
            xprint("going to ensure data is loaded")
            data_loads += self._ensure_data_is_loaded(sql_object,effective_input_params,data_streams,stop_after_analysis=stop_after_analysis)

            table_structures = self._create_table_structures_list()

            self.materialize_sql_object(sql_object)

            # TODO RLRL - Breaking change - save to db needs another approach?
            if save_db_to_disk_filename is not None:
                self.query_level_db.done()
                dump_start_time = time.time()
                print("Data has been loaded in %4.3f seconds" % (dump_start_time - load_start_time), file=sys.stderr)
                print("Saving data to db file %s" % save_db_to_disk_filename, file=sys.stderr)
                self.query_level_db.store_db_to_disk(save_db_to_disk_filename,sql_object.get_qtable_name_effective_table_names(),save_db_to_disk_method)
                print("Data has been saved into %s . Saving has taken %4.3f seconds" % (save_db_to_disk_filename,time.time()-dump_start_time), file=sys.stderr)
                print("Query to run on the database: %s;" % sql_object.get_effective_sql(True), file=sys.stderr)
                # TODO Propagate dump results using a different output class instead of an empty one

                return QOutput()

            xprint("--- query level db: databases %s" % self.query_level_db.conn.execute('pragma database_list').fetchall())
            # Execute the query and fetch the data
            db_results_obj = sql_object.execute_and_fetch(self.query_level_db)

            # TODO RLRL - consolidate save_db_to_disk feature into qsqlite

            return QOutput(
                data = db_results_obj.results,
                metadata = QMetadata(
                    table_structures=table_structures,
                    output_column_name_list=db_results_obj.query_column_names,
                    data_loads=data_loads),
                warnings = warnings,
                error = error)

        except EmptyDataException as e:
            warnings.append(QWarning(e,"Warning - data is empty"))
        except MissingHeaderException as e:
            error = QError(e,e.msg,117)
        except FileNotFoundException as e:
            error = QError(e,e.msg,30)
        except sqlite3.OperationalError as e:
            msg = str(e)
            error = QError(e,"query error: %s" % msg,1)
            if "no such column" in msg and effective_input_params.skip_header:
                warnings.append(QWarning(e,'Warning - There seems to be a "no such column" error, and -H (header line) exists. Please make sure that you are using the column names from the header line and not the default (cXX) column names. Another issue might be that the file contains a BOM. Files that are encoded with UTF8 and contain a BOM can be read by specifying `-e utf-9-sig` in the command line. Support for non-UTF8 encoding will be provided in the future.'))
        except ColumnCountMismatchException as e:
            error = QError(e,e.msg,2)
        except (UnicodeDecodeError, UnicodeError) as e:
            error = QError(e,"Cannot decode data. Try to change the encoding by setting it using the -e parameter. Error:%s" % e,3)
        except BadHeaderException as e:
            error = QError(e,"Bad header row: %s" % e.msg,35)
        except CannotUnzipDataStreamException as e:
            error = QError(e,"Cannot decompress standard input. Pipe the input through zcat in order to decompress.",36)
        except UniversalNewlinesExistException as e:
            error = QError(e,"Data contains universal newlines. Run q with -U to use universal newlines. Please note that q still doesn't support universal newlines for .gz files or for stdin. Route the data through a regular file to use -U.",103)
        except UnprovidedStdInException as e:
            error = QError(e,"Standard Input must be provided in order to use it as a table",61)
        except CouldNotConvertStringToNumericValueException as e:
            error = QError(e,"Could not convert string to a numeric value. Did you use `-w nonnumeric` with unquoted string values? Error: %s" % e.msg,58)
        except CouldNotParseInputException as e:
            error = QError(e,"Could not parse the input. Please make sure to set the proper -w input-wrapping parameter for your input, and that you use the proper input encoding (-e). Error: %s" % e.msg,59)
        except ColumnMaxLengthLimitExceededException as e:
            error = QError(e,e.msg,31)
        except MissingSqliteBckModuleException as e:
            error = QError(e,e.msg,79)
        except ContentSignatureDiffersException as e:
            error = QError(e,"Content Signatures for table %s differ at %s (source value '%s' disk signature value '%s')" %
                           (e.filenames_str,e.key,e.source_value,e.signature_value),80)
        except ContentSignatureDataDiffersException as e:
            error = QError(e,e.msg,81)
        except KeyboardInterrupt as e:
            warnings.append(QWarning(e,"Interrupted"))
        except Exception as e:
            global DEBUG
            if DEBUG:
                print(traceback.format_exc())
            error = QError(e,repr(e),199)

        return QOutput(warnings = warnings,error = error , metadata=QMetadata(table_structures=table_structures,data_loads = data_loads))

    def execute(self,query_str,input_params=None,save_db_to_disk_filename=None,save_db_to_disk_method=None):
        r = self._execute(query_str,input_params,stop_after_analysis=False,save_db_to_disk_filename=save_db_to_disk_filename,save_db_to_disk_method=save_db_to_disk_method)
        return r

    def unload(self):
        for filename,table_creator in six.iteritems(self.table_creators):
            try:
                table_creator.drop_table()
            except:
                # Support no-table select queries
                pass
        self.table_creators = {}



    def _create_materialized_files(self,table_creator):
        d = table_creator.materialized_file_dict
        m = {}
        for filename,mfs in six.iteritems(d):
            m[filename] = QMaterializedFile(filename,mfs.data_stream)
        return m

    def _create_table_structures_list(self):
        table_structures = []
        for filename,table_creator in six.iteritems(self.table_creators):
            column_names = table_creator.column_inferer.get_column_names()
            column_types = [self.query_level_db.type_names[table_creator.column_inferer.get_column_dict()[k]].lower() for k in column_names]
            materialized_files = self._create_materialized_files(table_creator)
            table_structure = QTableStructure(table_creator.filenames_str,materialized_files,column_names,column_types)
            table_structures.append(table_structure)
        return table_structures

    def analyze(self,query_str,input_params=None,data_streams=None):
        q_output = self._execute(query_str,input_params,data_streams=data_streams,stop_after_analysis=True)

        return q_output

def escape_double_quotes_if_needed(v):
    x = v.replace(six.u('"'), six.u('""'))
    return x

def quote_none_func(output_delimiter,v):
    return v

def quote_minimal_func(output_delimiter,v):
    if v is None:
        return v
    t = type(v)
    if (t == str or t == unicode) and ((output_delimiter in v) or ('\n' in v) or ('"' in v)):
        return six.u('"{}"').format(escape_double_quotes_if_needed(v))
    return v

def quote_nonnumeric_func(output_delimiter,v):
    if v is None:
        return v
    if type(v) == str or type(v) == unicode:
        return six.u('"{}"').format(escape_double_quotes_if_needed(v))
    return v

def quote_all_func(output_delimiter,v):
    if type(v) == str or type(v) == unicode:
        return six.u('"{}"').format(escape_double_quotes_if_needed(v))
    else:
        return six.u('"{}"').format(v)

class QOutputParams(object):
    def __init__(self,
            delimiter=' ',
            beautify=False,
            output_quoting_mode='minimal',
            formatting=None,
            output_header=False,
                 encoding=None):
        self.delimiter = delimiter
        self.beautify = beautify
        self.output_quoting_mode = output_quoting_mode
        self.formatting = formatting
        self.output_header = output_header
        self.encoding = encoding

    def __str__(self):
        return "QOutputParams<%s>" % str(self.__dict__)

    def __repr__(self):
        return "QOutputParams(...)"

class QOutputPrinter(object):
    output_quoting_modes = {   'minimal' : quote_minimal_func,
                        'all' : quote_all_func,
                        'nonnumeric' : quote_nonnumeric_func,
                        'none' : quote_none_func }

    def __init__(self,output_params,show_tracebacks=False):
        self.output_params = output_params
        self.show_tracebacks = show_tracebacks

        self.output_field_quoting_func = QOutputPrinter.output_quoting_modes[output_params.output_quoting_mode]

    def print_errors_and_warnings(self,f,results):
        if results.status == 'error':
            error = results.error
            print(error.msg, file=f)
            if self.show_tracebacks:
                print(error.traceback, file=f)

        for warning in results.warnings:
            print("%s" % warning.msg, file=f)

    def print_analysis(self,f_out,f_err,results):
        self.print_errors_and_warnings(f_err,results)

        if results.metadata is None:
            return

        if results.metadata.table_structures is None:
            return

        for table_structure in results.metadata.table_structures:
            print("Table for file: %s" % normalized_filename(table_structure.filenames_str), file=f_out)
            for n,t in zip(table_structure.column_names,table_structure.column_types):
                print("  `%s` - %s" % (n,t), file=f_out)

    def print_output(self,f_out,f_err,results):
        try:
            self._print_output(f_out,f_err,results)
        except (UnicodeEncodeError, UnicodeError) as e:
            print("Cannot encode data. Error:%s" % e, file=f_err)
            sys.exit(3)
        except IOError as e:
            if e.errno == 32:
                # broken pipe, that's ok
                pass
            else:
                # don't miss other problems for now
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
            if self.output_params.output_header:
                data_with_possible_headers = data + [tuple(results.metadata.output_column_name_list)]
            else:
                data_with_possible_headers = data
            max_lengths = determine_max_col_lengths(data_with_possible_headers,self.output_field_quoting_func,self.output_params.delimiter)

        if self.output_params.formatting:
            formatting_dict = dict(
                [(x.split("=")[0], x.split("=")[1]) for x in self.output_params.formatting.split(",")])
        else:
            formatting_dict = {}

        try:
            if self.output_params.output_header and results.metadata.output_column_name_list is not None:
                data.insert(0,results.metadata.output_column_name_list)
            for rownum, row in enumerate(data):
                row_str = []
                skip_formatting = rownum == 0 and self.output_params.output_header
                for i, col in enumerate(row):
                    if str(i + 1) in formatting_dict.keys() and not skip_formatting:
                        fmt_str = formatting_dict[str(i + 1)]
                    else:
                        if self.output_params.beautify:
                            fmt_str = six.u("{{0:<{}}}").format(max_lengths[i])
                        else:
                            fmt_str = six.u("{}")

                    if col is not None:
                        # Hack for python2 - The defaulting rendering of a float to string is losing precision. This hack works around it by using repr()
                        if six.PY2 and isinstance(col, float) and str(i+1) not in formatting_dict:
                            col = repr(col)
                        xx = self.output_field_quoting_func(self.output_params.delimiter,col)
                        row_str.append(fmt_str.format(xx))
                    else:
                        row_str.append(fmt_str.format(""))


                xxxx = six.u(self.output_params.delimiter).join(row_str) + six.u("\n")
                f_out.write(xxxx)
        except (UnicodeEncodeError, UnicodeError) as e:
            print("Cannot encode data. Error:%s" % e, file=sys.stderr)
            sys.exit(3)
        except TypeError as e:
            print(traceback.format_exc())
            print("Error while formatting output: %s" % e, file=sys.stderr)
            sys.exit(4)
        except IOError as e:
            if e.errno == 32:
                # broken pipe, that's ok
                pass
            else:
                # don't miss other problem for now
                raise
        except KeyboardInterrupt:
            pass

        try:
            # Prevent python bug when order of pipe shutdowns is reversed
            f_out.flush()
        except IOError as e:
            pass

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

def run_standalone():
    p = configparser.ConfigParser()
    p.read([os.path.expanduser('~/.qrc'), '.qrc'])

    default_verbose = get_option_with_default(p, 'boolean', 'verbose', False)
    default_save_db_to_disk = get_option_with_default(p, 'string', 'save_db_to_disk', None)
    default_save_db_to_disk_method = get_option_with_default(p, 'string', 'save_db_to_disk_method', 'fast')
    default_caching_mode = get_option_with_default(p, 'string', 'caching_mode', 'none')

    default_skip_header = get_option_with_default(p, 'boolean', 'skip_header', False)
    default_delimiter = get_option_with_default(p, 'escaped_string', 'delimiter', None)
    default_pipe_delimited = get_option_with_default(p, 'boolean', 'pipe_delimited', False)
    default_tab_delimited = get_option_with_default(p, 'boolean', 'tab_delimited', False)
    default_encoding = get_option_with_default(p, 'string', 'encoding', 'UTF-8')
    default_gzipped = get_option_with_default(p, 'boolean', 'gzipped', False)
    default_analyze_only = get_option_with_default(p, 'boolean', 'analyze_only', False)
    default_mode = get_option_with_default(p, 'string', 'mode', "relaxed")
    default_column_count = get_option_with_default(p, 'string', 'column_count', None)
    default_keep_leading_whitespace_in_values = get_option_with_default(p, 'boolean',
                                                                        'keep_leading_whitespace_in_values', False)
    default_disable_double_double_quoting = get_option_with_default(p, 'boolean', 'disable_double_double_quoting', True)
    default_disable_escaped_double_quoting = get_option_with_default(p, 'boolean', 'disable_escaped_double_quoting',
                                                                     True)
    default_disable_column_type_detection = get_option_with_default(p, 'boolean', 'disable_column_type_detection',
                                                                    False)
    default_input_quoting_mode = get_option_with_default(p, 'string', 'input_quoting_mode', 'minimal')
    default_max_column_length_limit = get_option_with_default(p, 'int', 'max_column_length_limit', 131072)
    default_with_universal_newlines = get_option_with_default(p, 'boolean', 'with_universal_newlines', False)

    default_output_delimiter = get_option_with_default(p, 'escaped_string', 'output_delimiter', None)
    default_pipe_delimited_output = get_option_with_default(p, 'boolean', 'pipe_delimited_output', False)
    default_tab_delimited_output = get_option_with_default(p, 'boolean', 'tab_delimited_output', False)
    default_output_header = get_option_with_default(p, 'string', 'output_header', False)
    default_beautify = get_option_with_default(p, 'boolean', 'beautify', False)
    default_formatting = get_option_with_default(p, 'string', 'formatting', None)
    default_output_encoding = get_option_with_default(p, 'string', 'encoding', 'none')
    default_output_quoting_mode = get_option_with_default(p, 'string', 'output_quoting_mode', 'minimal')
    default_list_user_functions = get_option_with_default(p, 'boolean', 'list_user_functions', False)

    default_query_filename = get_option_with_default(p, 'string', 'query_filename', None)
    default_query_encoding = get_option_with_default(p, 'string', 'query_encoding', locale.getpreferredencoding())

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
    parser.add_option("-V", "--verbose", dest="verbose", default=default_verbose, action="store_true",
                      help="Print debug info in case of problems")
    parser.add_option("-S", "--save-db-to-disk", dest="save_db_to_disk_filename", default=default_save_db_to_disk,
                      help="Save database to an sqlite database file")
    parser.add_option("", "--save-db-to-disk-method", dest="save_db_to_disk_method", default=default_save_db_to_disk_method,
                      help="Method to use to save db to disk. 'standard' does not require any deps, 'fast' currenty requires manually running `pip install sqlitebck` on your python installation. Once packing issues are solved, the fast method will be the default.")
    parser.add_option("-C", "--caching-mode", dest="caching_mode", default=default_caching_mode,
                      help="Choose the autocaching mode (none/read/readwrite). Autocaches files to disk db so further queries will be faster. Caching is done to a side-file with the same name of the table, but with an added extension .qsqlite")
    #-----------------------------------------------
    input_data_option_group = OptionGroup(parser,"Input Data Options")
    input_data_option_group.add_option("-H", "--skip-header", dest="skip_header", default=default_skip_header, action="store_true",
                      help="Skip header row. This has been changed from earlier version - Only one header row is supported, and the header row is used for column naming")
    input_data_option_group.add_option("-d", "--delimiter", dest="delimiter", default=default_delimiter,
                      help="Field delimiter. If none specified, then space is used as the delimiter.")
    input_data_option_group.add_option("-p", "--pipe-delimited", dest="pipe_delimited", default=default_pipe_delimited, action="store_true",
                      help="Same as -d '|'. Added for convenience and readability")
    input_data_option_group.add_option("-t", "--tab-delimited", dest="tab_delimited", default=default_tab_delimited, action="store_true",
                      help="Same as -d <tab>. Just a shorthand for handling standard tab delimited file You can use $'\\t' if you want (this is how Linux expects to provide tabs in the command line")
    input_data_option_group.add_option("-e", "--encoding", dest="encoding", default=default_encoding,
                      help="Input file encoding. Defaults to UTF-8. set to none for not setting any encoding - faster, but at your own risk...")
    input_data_option_group.add_option("-z", "--gzipped", dest="gzipped", default=default_gzipped, action="store_true",
                      help="Data is gzipped. Useful for reading from stdin. For files, .gz means automatic gunzipping")
    input_data_option_group.add_option("-A", "--analyze-only", dest="analyze_only", default=default_analyze_only, action='store_true',
                      help="Analyze sample input and provide information about data types")
    input_data_option_group.add_option("-m", "--mode", dest="mode", default=default_mode,
                      help="Data parsing mode. fluffy, relaxed and strict. In strict mode, the -c column-count parameter must be supplied as well")
    input_data_option_group.add_option("-c", "--column-count", dest="column_count", default=default_column_count,
                      help="Specific column count when using relaxed or strict mode")
    input_data_option_group.add_option("-k", "--keep-leading-whitespace", dest="keep_leading_whitespace_in_values", default=default_keep_leading_whitespace_in_values, action="store_true",
                      help="Keep leading whitespace in values. Default behavior strips leading whitespace off values, in order to provide out-of-the-box usability for simple use cases. If you need to preserve whitespace, use this flag.")
    input_data_option_group.add_option("--disable-double-double-quoting", dest="disable_double_double_quoting", default=default_disable_double_double_quoting, action="store_false",
                      help="Disable support for double double-quoting for escaping the double quote character. By default, you can use \"\" inside double quoted fields to escape double quotes. Mainly for backward compatibility.")
    input_data_option_group.add_option("--disable-escaped-double-quoting", dest="disable_escaped_double_quoting", default=default_disable_escaped_double_quoting, action="store_false",
                      help="Disable support for escaped double-quoting for escaping the double quote character. By default, you can use \\\" inside double quoted fields to escape double quotes. Mainly for backward compatibility.")
    input_data_option_group.add_option("--as-text", dest="disable_column_type_detection", default=default_disable_column_type_detection, action="store_true",
                      help="Don't detect column types - All columns will be treated as text columns")
    input_data_option_group.add_option("-w","--input-quoting-mode",dest="input_quoting_mode",default=default_input_quoting_mode,
                      help="Input quoting mode. Possible values are all, minimal and none. Note the slightly misleading parameter name, and see the matching -W parameter for output quoting.")
    input_data_option_group.add_option("-M","--max-column-length-limit",dest="max_column_length_limit",default=default_max_column_length_limit,
                      help="Sets the maximum column length.")
    input_data_option_group.add_option("-U","--with-universal-newlines",dest="with_universal_newlines",default=default_with_universal_newlines,action="store_true",
                      help="Expect universal newlines in the data. Limitation: -U works only with regular files for now, stdin or .gz files are not supported yet.")
    parser.add_option_group(input_data_option_group)
    #-----------------------------------------------
    output_data_option_group = OptionGroup(parser,"Output Options")
    output_data_option_group.add_option("-D", "--output-delimiter", dest="output_delimiter", default=default_output_delimiter,
                      help="Field delimiter for output. If none specified, then the -d delimiter is used if present, or space if no delimiter is specified")
    output_data_option_group.add_option("-P", "--pipe-delimited-output", dest="pipe_delimited_output", default=default_pipe_delimited_output, action="store_true",
                      help="Same as -D '|'. Added for convenience and readability.")
    output_data_option_group.add_option("-T", "--tab-delimited-output", dest="tab_delimited_output", default=default_tab_delimited_output, action="store_true",
                      help="Same as -D <tab>. Just a shorthand for outputting tab delimited output. You can use -D $'\\t' if you want.")
    output_data_option_group.add_option("-O", "--output-header", dest="output_header", default=default_output_header, action="store_true",help="Output header line. Output column-names are determined from the query itself. Use column aliases in order to set your column names in the query. For example, 'select name FirstName,value1/value2 MyCalculation from ...'. This can be used even if there was no header in the input.")
    output_data_option_group.add_option("-b", "--beautify", dest="beautify", default=default_beautify, action="store_true",
                      help="Beautify output according to actual values. Might be slow...")
    output_data_option_group.add_option("-f", "--formatting", dest="formatting", default=default_formatting,
                      help="Output-level formatting, in the format X=fmt,Y=fmt etc, where X,Y are output column numbers (e.g. 1 for first SELECT column etc.")
    output_data_option_group.add_option("-E", "--output-encoding", dest="output_encoding", default=default_output_encoding,
                      help="Output encoding. Defaults to 'none', leading to selecting the system/terminal encoding")
    output_data_option_group.add_option("-W","--output-quoting-mode",dest="output_quoting_mode",default=default_output_quoting_mode,
                      help="Output quoting mode. Possible values are all, minimal, nonnumeric and none. Note the slightly misleading parameter name, and see the matching -w parameter for input quoting.")
    output_data_option_group.add_option("-L","--list-user-functions",dest="list_user_functions",default=default_list_user_functions,action="store_true",
                      help="List all user functions")
    parser.add_option_group(output_data_option_group)
    #-----------------------------------------------
    query_option_group = OptionGroup(parser,"Query Related Options")
    query_option_group.add_option("-q", "--query-filename", dest="query_filename", default=default_query_filename,
                      help="Read query from the provided filename instead of the command line, possibly using the provided query encoding (using -Q).")
    query_option_group.add_option("-Q", "--query-encoding", dest="query_encoding", default=default_query_encoding,
                      help="query text encoding. Experimental. Please send your feedback on this")
    parser.add_option_group(query_option_group)
    #-----------------------------------------------

    (options, args) = parser.parse_args()

    if options.version:
        print_credentials()
        sys.exit(0)

###

    if options.list_user_functions:
        print_user_functions()
        sys.exit(0)

    if len(args) == 0 and options.query_filename is None:
        print_credentials()
        print("Must provide at least one query in the command line, or through a file with the -q parameter", file=sys.stderr)
        sys.exit(1)

    if options.query_filename is not None:
        if len(args) != 0:
            print("Can't provide both a query file and a query on the command line", file=sys.stderr)
            sys.exit(1)
        try:
            f = open(options.query_filename,'rb')
            query_strs = [f.read()]
            f.close()
        except:
            print("Could not read query from file %s" % options.query_filename, file=sys.stderr)
            sys.exit(1)
    else:
        if sys.stdin.encoding is not None:
            query_strs = [x.encode(sys.stdin.encoding) for x in args]
        else:
            query_strs = args

    if options.query_encoding is not None and options.query_encoding != 'none':
        try:
            for idx in range(len(query_strs)):
                query_strs[idx] = query_strs[idx].decode(options.query_encoding).strip()

                if len(query_strs[idx]) == 0:
                    print("Query cannot be empty (query number %s)" % (idx+1), file=sys.stderr)
                    sys.exit(1)

        except Exception as e:
            print("Could not decode query number %s using the provided query encoding (%s)" % (idx+1,options.query_encoding), file=sys.stderr)
            sys.exit(3)
###

    if options.mode not in ['fluffy', 'relaxed', 'strict']:
        print("Parsing mode can be one of fluffy, relaxed or strict", file=sys.stderr)
        sys.exit(13)

    output_encoding = get_stdout_encoding(options.output_encoding)
    try:
        if six.PY3:
            STDOUT = codecs.getwriter(output_encoding)(sys.stdout.buffer)
        else:
            STDOUT = codecs.getwriter(output_encoding)(sys.stdout)
    except:
        print("Could not create output stream using output encoding %s" % (output_encoding), file=sys.stderr)
        sys.exit(200)

    # If the user flagged for a tab-delimited file then set the delimiter to tab
    if options.tab_delimited:
        if options.delimiter is not None and options.delimiter != '\t':
            print("Warning: -t parameter overrides -d parameter (%s)" % options.delimiter,file=sys.stderr)
        options.delimiter = '\t'

    # If the user flagged for a pipe-delimited file then set the delimiter to pipe
    if options.pipe_delimited:
        if options.delimiter is not None and options.delimiter != '|':
            print("Warning: -p parameter overrides -d parameter (%s)" % options.delimiter,file=sys.stderr)
        options.delimiter = '|'

    if options.delimiter is None:
        options.delimiter = ' '
    elif len(options.delimiter) != 1:
        print("Delimiter must be one character only", file=sys.stderr)
        sys.exit(5)

    if options.tab_delimited_output:
        if options.output_delimiter is not None and options.output_delimiter != '\t':
            print("Warning: -T parameter overrides -D parameter (%s)" % options.output_delimiter,file=sys.stderr)
        options.output_delimiter = '\t'

    if options.pipe_delimited_output:
        if options.output_delimiter is not None and options.output_delimiter != '|':
            print("Warning: -P parameter overrides -D parameter (%s)" % options.output_delimiter,file=sys.stderr)
        options.output_delimiter = '|'

    if options.output_delimiter:
        # If output delimiter is specified, then we use it
        options.output_delimiter = options.output_delimiter
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

    try:
        max_column_length_limit = int(options.max_column_length_limit)
        if max_column_length_limit < 1:
            raise Exception()
    except:
        print("Max column length limit must be a positive integer (%s)" % max_column_length_limit, file=sys.stderr)
        sys.exit(31)


    if options.input_quoting_mode not in list(QTextAsData.input_quoting_modes.keys()):
        print("Input quoting mode can only be one of %s. It cannot be set to '%s'" % (",".join(sorted(QTextAsData.input_quoting_modes.keys())),options.input_quoting_mode), file=sys.stderr)
        sys.exit(55)

    if options.output_quoting_mode not in list(QOutputPrinter.output_quoting_modes.keys()):
        print("Output quoting mode can only be one of %s. It cannot be set to '%s'" % (",".join(QOutputPrinter.output_quoting_modes.keys()),options.input_quoting_mode), file=sys.stderr)
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
            print("Encoding %s could not be found" % options.encoding, file=sys.stderr)
            sys.exit(10)

    if options.save_db_to_disk_filename is not None:
        if options.analyze_only:
            print("Cannot save database to disk when running with -A (analyze-only) option.", file=sys.stderr)
            sys.exit(119)

        print("Going to save data into a disk database: %s" % options.save_db_to_disk_filename, file=sys.stderr)
        if os.path.exists(options.save_db_to_disk_filename):
            print("Disk database file %s already exists." % options.save_db_to_disk_filename, file=sys.stderr)
            sys.exit(77)

    if options.save_db_to_disk_method is not None:
        if options.save_db_to_disk_method not in ['standard','fast']:
            print("save-db-to-disk method should be either standard or fast (%s)" % options.save_db_to_disk_method, file=sys.stderr)
            sys.exit(78)

    if options.caching_mode not in ['none','read','readwrite']:
        print("caching mode must be none,read or readwrite")
        sys.exit(85)

    read_caching = options.caching_mode in ['read','readwrite']
    write_caching = options.caching_mode in ['readwrite']

    default_input_params = QInputParams(skip_header=options.skip_header,
        delimiter=options.delimiter,
        input_encoding=options.encoding,
        gzipped_input=options.gzipped,
        with_universal_newlines=options.with_universal_newlines,
        parsing_mode=options.mode,
        expected_column_count=expected_column_count,
        keep_leading_whitespace_in_values=options.keep_leading_whitespace_in_values,
        disable_double_double_quoting=options.disable_double_double_quoting,
        disable_escaped_double_quoting=options.disable_escaped_double_quoting,
        input_quoting_mode=options.input_quoting_mode,
        disable_column_type_detection=options.disable_column_type_detection,
        max_column_length_limit=max_column_length_limit,
        read_caching=read_caching,
        write_caching=write_caching)

    data_streams_dict = {
        '-': DataStream('stdin','-',sys.stdin)
    }

    q_engine = QTextAsData(default_input_params=default_input_params,data_streams_dict=data_streams_dict)

    output_params = QOutputParams(
        delimiter=options.output_delimiter,
        beautify=options.beautify,
        output_quoting_mode=options.output_quoting_mode,
        formatting=options.formatting,
        output_header=options.output_header,
        encoding=output_encoding)
    q_output_printer = QOutputPrinter(output_params,show_tracebacks=options.verbose)

    for query_str in query_strs:
        if options.analyze_only:
            q_output = q_engine.analyze(query_str)
            q_output_printer.print_analysis(STDOUT,sys.stderr,q_output)
        else:
            q_output = q_engine.execute(query_str,save_db_to_disk_filename=options.save_db_to_disk_filename,save_db_to_disk_method=options.save_db_to_disk_method)
            q_output_printer.print_output(STDOUT,sys.stderr,q_output)

        if q_output.status == 'error':
            sys.exit(q_output.error.errorcode)

    q_engine.unload()
    q_engine.close_all()

    sys.exit(0)


if __name__ == '__main__':
    run_standalone()
