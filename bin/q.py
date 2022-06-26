#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#   Copyright (C) 2012-2021 Harel Ben-Attia
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
# Author    : Harel Ben-Attia - harelba@gmail.com, harelba @ github, @harelba on twitter
#
#
# q allows performing SQL-like statements on tabular text data.
#
# Its purpose is to bring SQL expressive power to manipulating text data using the Linux command line.
#
# Full Documentation and details in https://harelba.github.io/q/
#
# Run with --help for command line details
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict
from sqlite3.dbapi2 import OperationalError
from uuid import uuid4

q_version = '3.1.6'

#__all__ = [ 'QTextAsData' ]

import os
import sys
import sqlite3
import glob
from argparse import ArgumentParser
import codecs
import locale
import time
import re
from six.moves import configparser, range, filter
import traceback
import csv
import uuid
import math
import six
import io
import json
import datetime
import hashlib

if six.PY2:
    assert False, 'Python 2 is not longer supported by q'

long = int
unicode = six.text_type

DEBUG = bool(os.environ.get('Q_DEBUG', None)) or '-V' in sys.argv
SQL_DEBUG = False

if DEBUG:
    def xprint(*args,**kwargs):
        print(datetime.datetime.utcnow().isoformat()," DEBUG ",*args,file=sys.stderr,**kwargs)

    def iprint(*args,**kwargs):
        print(datetime.datetime.utcnow().isoformat()," INFO ",*args,file=sys.stderr,**kwargs)

    def sqlprint(*args,**kwargs):
        pass
else:
    def xprint(*args,**kwargs): pass
    def iprint(*args,**kwargs): pass
    def sqlprint(*args,**kwargs): pass

if SQL_DEBUG:
    def sqlprint(*args,**kwargs):
        print(datetime.datetime.utcnow().isoformat(), " SQL ", *args, file=sys.stderr, **kwargs)


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

def regexp_extract(regular_expression, data,group_number):
    if data is not None:
        if not isinstance(data, str) and not isinstance(data, unicode):
            data = str(data)
        m = re.search(regular_expression, data)
        if m is not None:
            return m.groups()[group_number]
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

def file_ext(data):
    if data is None:
        return None

    return os.path.splitext(data)[1]

def file_folder(data):
    if data is None:
        return None
    return os.path.split(data)[0]

def file_basename(data):
    if data is None:
        return None
    return os.path.split(data)[1]
    
def file_basename_no_ext(data):
    if data is None:
        return None

    return os.path.split(os.path.splitext(data)[0])[-1]

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
                    "regexp_extract","regexp_extract(<regular_expression>,<expr>,group_number) = <substring|null>",
                    "Get regexp capture group content",
                    regexp_extract,
                    3),
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
    UserFunctionDef(FunctionType.REGULAR,
                    "file_ext","file_ext(<expr>) = <filename-extension-or-empty-string>",
                    "Get the extension of a filename",
                    file_ext,
                    1),
    UserFunctionDef(FunctionType.REGULAR,
                    "file_folder","file_folder(<expr>) = <folder-name-of-filename>",
                    "Get the folder part of a filename",
                    file_folder,
                    1),
    UserFunctionDef(FunctionType.REGULAR,
                    "file_basename","file_basename(<expr>) = <basename-of-filename-including-extension>",
                    "Get the basename of a filename, including extension if any",
                    file_basename,
                    1),
    UserFunctionDef(FunctionType.REGULAR,
                    "file_basename_no_ext","file_basename_no_ext(<expr>) = <basename-of-filename-without-extension>",
                    "Get the basename of a filename, without the extension if there is one",
                    file_basename_no_ext,
                    1),
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

    def __str__(self):
        return "Sqlite3DBResults<result_count=%d,query_column_names=%s>" % (len(self.results),str(self.query_column_names))
    __repr__ = __str__

def get_sqlite_type_affinity(sqlite_type):
    sqlite_type = sqlite_type.upper()
    if 'INT' in sqlite_type:
        return 'INTEGER'
    elif 'CHAR' in sqlite_type or 'TEXT' in sqlite_type or 'CLOB' in sqlite_type:
        return 'TEXT'
    elif 'BLOB' in sqlite_type:
        return 'BLOB'
    elif 'REAL' in sqlite_type or 'FLOA' in sqlite_type or 'DOUB' in sqlite_type:
        return 'REAL'
    else:
        return 'NUMERIC'

def sqlite_type_to_python_type(sqlite_type):
    SQLITE_AFFINITY_TO_PYTHON_TYPE_NAMES = {
        'INTEGER': long,
        'TEXT': unicode,
        'BLOB': bytes,
        'REAL': float,
        'NUMERIC': float
    }
    return SQLITE_AFFINITY_TO_PYTHON_TYPE_NAMES[get_sqlite_type_affinity(sqlite_type)]


class Sqlite3DB(object):
    # TODO Add metadata table with qsql file version

    QCATALOG_TABLE_NAME = '_qcatalog'
    NUMERIC_COLUMN_TYPES =  {int, long, float}
    PYTHON_TO_SQLITE_TYPE_NAMES = { str: 'TEXT', int: 'INT', long : 'INT' , float: 'REAL', None: 'TEXT' }


    def __str__(self):
        return "Sqlite3DB<url=%s>" % self.sqlite_db_url
    __repr__ = __str__

    def __init__(self, db_id, sqlite_db_url, sqlite_db_filename, create_qcatalog, show_sql=SHOW_SQL):
        self.show_sql = show_sql
        self.create_qcatalog = create_qcatalog

        self.db_id = db_id
        # TODO Is this needed anymore?
        self.sqlite_db_filename = sqlite_db_filename
        self.sqlite_db_url = sqlite_db_url
        self.conn = sqlite3.connect(self.sqlite_db_url, uri=True)
        self.last_temp_table_id = 10000
        self.cursor = self.conn.cursor()
        self.add_user_functions()

        if create_qcatalog:
            self.create_qcatalog_table()
        else:
            xprint('Not creating qcatalog for db_id %s' % db_id)

    def retrieve_all_table_names(self):
        return [x[0] for x in self.execute_and_fetch("select tbl_name from sqlite_master where type='table'").results]

    def get_sqlite_table_info(self,table_name):
        return self.execute_and_fetch('PRAGMA table_info(%s)' % table_name).results

    def get_sqlite_database_list(self):
        return self.execute_and_fetch('pragma database_list').results

    def find_new_table_name(self,planned_table_name):
        existing_table_names = self.retrieve_all_table_names()

        possible_indices = range(1,1000)

        for index in possible_indices:
            if index == 1:
                suffix = ''
            else:
                suffix = '_%s' % index

            table_name_attempt = '%s%s' % (planned_table_name,suffix)

            if table_name_attempt not in existing_table_names:
                xprint("Found free table name %s in db %s for planned table name %s" % (table_name_attempt,self.db_id,planned_table_name))
                return table_name_attempt

        # TODO Add test for this
        raise Exception('Cannot find free table name in db %s for planned table name %s' % (self.db_id,planned_table_name))

    def create_qcatalog_table(self):
        if not self.qcatalog_table_exists():
            xprint("qcatalog table does not exist. Creating it")
            r = self.conn.execute("""CREATE TABLE %s ( 
                               qcatalog_entry_id text not null primary key,
                               content_signature_key text,
                               temp_table_name text,
                               content_signature text,
                               creation_time text,
                               source_type text,
                               source text)""" % self.QCATALOG_TABLE_NAME).fetchall()
        else:
            xprint("qcatalog table already exists. No need to create it")

    def qcatalog_table_exists(self):
        return sqlite_table_exists(self.conn,self.QCATALOG_TABLE_NAME)

    def calculate_content_signature_key(self,content_signature):
        assert type(content_signature) == OrderedDict
        pp = json.dumps(content_signature,sort_keys=True)
        xprint("Calculating content signature for:",pp,six.b(pp))
        return hashlib.sha1(six.b(pp)).hexdigest()

    def add_to_qcatalog_table(self, temp_table_name, content_signature, creation_time,source_type, source):
        assert source is not None
        assert source_type is not None
        content_signature_key = self.calculate_content_signature_key(content_signature)
        xprint("db_id: %s Adding to qcatalog table: %s. Calculated signature key %s" % (self.db_id, temp_table_name,content_signature_key))
        r = self.execute_and_fetch(
            'INSERT INTO %s (qcatalog_entry_id,content_signature_key, temp_table_name,content_signature,creation_time,source_type,source) VALUES (?,?,?,?,?,?,?)' % self.QCATALOG_TABLE_NAME,
                              (str(uuid4()),content_signature_key,temp_table_name,json.dumps(content_signature),creation_time,source_type,source))
        # Ensure transaction is completed
        self.conn.commit()

    def get_from_qcatalog(self, content_signature):
        content_signature_key = self.calculate_content_signature_key(content_signature)
        xprint("Finding table in db_id %s that matches content signature key %s" % (self.db_id,content_signature_key))

        field_names = ["content_signature_key", "temp_table_name", "content_signature", "creation_time","source_type","source","qcatalog_entry_id"]

        q = "SELECT %s FROM %s where content_signature_key = ?" % (",".join(field_names),self.QCATALOG_TABLE_NAME)
        r = self.execute_and_fetch(q,(content_signature_key,))

        if r is None:
            return None

        if len(r.results) == 0:
            return None

        if len(r.results) > 1:
            raise Exception("Bug - Exactly one result should have been provided: %s" % str(r.results))

        d = dict(zip(field_names,r.results[0]))
        return d

    def get_from_qcatalog_using_table_name(self, temp_table_name):
        xprint("getting from qcatalog using table name")

        field_names = ["content_signature", "temp_table_name","creation_time","source_type","source","content_signature_key","qcatalog_entry_id"]

        q = "SELECT %s FROM %s where temp_table_name = ?" % (",".join(field_names),self.QCATALOG_TABLE_NAME)
        xprint("Query from qcatalog %s params %s" % (q,str(temp_table_name,)))
        r = self.execute_and_fetch(q,(temp_table_name,))
        xprint("results: ",r.results)

        if r is None:
            return None

        if len(r.results) == 0:
            return None

        if len(r.results) > 1:
            raise Exception("Bug - Exactly one result should have been provided: %s" % str(r.results))

        d = dict(zip(field_names,r.results[0]))
        # content_signature should be the first in the list of field_names
        cs = OrderedDict(json.loads(r.results[0][0]))
        if self.calculate_content_signature_key(cs) != d['content_signature_key']:
            raise Exception('Table contains an invalid entry - content signature key is not matching the actual content signature')
        return d

    def get_all_from_qcatalog(self):
        xprint("getting from qcatalog using table name")

        field_names = ["temp_table_name", "content_signature", "creation_time","source_type","source","qcatalog_entry_id"]

        q = "SELECT %s FROM %s" % (",".join(field_names),self.QCATALOG_TABLE_NAME)
        xprint("Query from qcatalog %s" % q)
        r = self.execute_and_fetch(q)

        if r is None:
            return None

        def convert(res):
            d = dict(zip(field_names, res))
            cs = OrderedDict(json.loads(res[1]))
            d['content_signature_key'] = self.calculate_content_signature_key(cs)
            return d

        rr = [convert(r) for r in r.results]

        return rr

    def done(self):
        xprint("Closing database %s" % self.db_id)
        try:
            self.conn.commit()
            self.conn.close()
            xprint("Database %s closed" % self.db_id)
        except Exception as e:
            xprint("Could not close database %s" % self.db_id)
            raise

    def add_user_functions(self):
        for udf in user_functions:
            if type(udf.func_or_obj) == type(object):
                self.conn.create_aggregate(udf.name,udf.param_count,udf.func_or_obj)
            elif type(udf.func_or_obj) == type(md5):
                self.conn.create_function(udf.name,udf.param_count,udf.func_or_obj)
            else:
                raise Exception("Invalid user function definition %s" % str(udf))

    def is_numeric_type(self, column_type):
        return column_type in Sqlite3DB.NUMERIC_COLUMN_TYPES

    def update_many(self, sql, params):
        try:
            sqlprint(sql, " params: " + str(params))
            self.cursor.executemany(sql, params)
            _ = self.cursor.fetchall()
        finally:
            pass  # cursor.close()

    def execute_and_fetch(self, q,params = None):
        try:
            try:
                if self.show_sql:
                    print(repr(q))
                if params is None:
                    r = self.cursor.execute(q)
                else:
                    r = self.cursor.execute(q,params)
                if self.cursor.description is not None:
                    # we decode the column names, so they can be encoded to any output format later on
                    query_column_names = [c[0] for c in self.cursor.description]
                else:
                    query_column_names = None
                result = self.cursor.fetchall()
            finally:
                pass  # cursor.close()
        except OperationalError as e:
            raise SqliteOperationalErrorException("Failed executing sqlite query %s with params %s . error: %s" % (q,params,str(e)),e)
        return Sqlite3DBResults(query_column_names,result)

    def _get_as_list_str(self, l):
        return ",".join(['"%s"' % x.replace('"', '""') for x in l])

    def generate_insert_row(self, table_name, column_names):
        col_names_str = self._get_as_list_str(column_names)
        question_marks = ", ".join(["?" for i in range(0, len(column_names))])
        return 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, col_names_str, question_marks)

    # Get a list of column names so order will be preserved (Could have used OrderedDict, but
    # then we would need python 2.7)
    def generate_create_table(self, table_name, column_names, column_dict):
        # Convert dict from python types to db types
        column_name_to_db_type = dict(
            (n, Sqlite3DB.PYTHON_TO_SQLITE_TYPE_NAMES[t]) for n, t in six.iteritems(column_dict))
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

    def attach_and_copy_table(self, from_db, relevant_table,stop_after_analysis):
        xprint("Attaching %s into db %s and copying table %s into it" % (from_db,self,relevant_table))
        temp_db_id = 'temp_db_id'
        q = "attach '%s' as %s" % (from_db.sqlite_db_url,temp_db_id)
        xprint("Attach query: %s" % q)
        c = self.execute_and_fetch(q)

        new_temp_table_name = 'temp_table_%s' % (self.last_temp_table_id + 1)
        fully_qualified_table_name = '%s.%s' % (temp_db_id,relevant_table)

        if stop_after_analysis:
            limit = ' limit 100'
        else:
            limit = ''

        copy_query = 'create table %s as select * from %s %s' % (new_temp_table_name,fully_qualified_table_name,limit)
        copy_results = self.execute_and_fetch(copy_query)
        xprint("Copied %s.%s into %s in db_id %s. Results %s" % (temp_db_id,relevant_table,new_temp_table_name,self.db_id,copy_results))
        self.last_temp_table_id += 1

        xprint("Copied table into %s. Detaching db that was attached temporarily" % self.db_id)

        q = "detach database %s" % temp_db_id
        xprint("detach query: %s" % q)
        c = self.execute_and_fetch(q)
        xprint(c)
        return new_temp_table_name


class CouldNotConvertStringToNumericValueException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class SqliteOperationalErrorException(Exception):

    def __init__(self, msg,original_error):
        self.msg = msg
        self.original_error = original_error

    def __str(self):
        return repr(self.msg) + "//" + repr(self.original_error)

class IncorrectDefaultValueException(Exception):

    def __init__(self, option_type,option,actual_value):
        self.option_type = option_type
        self.option = option
        self.actual_value = actual_value

    def __str__(self):
        return repr(self)

class NonExistentTableNameInQsql(Exception):

    def __init__(self, qsql_filename,table_name,existing_table_names):
        self.qsql_filename = qsql_filename
        self.table_name = table_name
        self.existing_table_names = existing_table_names

class NonExistentTableNameInSqlite(Exception):

    def __init__(self, qsql_filename,table_name,existing_table_names):
        self.qsql_filename = qsql_filename
        self.table_name = table_name
        self.existing_table_names = existing_table_names

class TooManyTablesInQsqlException(Exception):

    def __init__(self, qsql_filename,existing_table_names):
        self.qsql_filename = qsql_filename
        self.existing_table_names = existing_table_names

class NoTableInQsqlExcption(Exception):

    def __init__(self, qsql_filename):
        self.qsql_filename = qsql_filename

class TooManyTablesInSqliteException(Exception):

    def __init__(self, qsql_filename,existing_table_names):
        self.qsql_filename = qsql_filename
        self.existing_table_names = existing_table_names

class NoTablesInSqliteException(Exception):

    def __init__(self, sqlite_filename):
        self.sqlite_filename = sqlite_filename

class ColumnMaxLengthLimitExceededException(Exception):

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

class EmptyDataException(Exception):

    def __init__(self):
        pass

class MissingHeaderException(Exception):

    def __init__(self,msg):
        self.msg = msg

class InvalidQueryException(Exception):

    def __init__(self,msg):
        self.msg = msg

class TooManyAttachedDatabasesException(Exception):

    def __init__(self,msg):
        self.msg = msg

class FileNotFoundException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class UnknownFileTypeException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)


class ColumnCountMismatchException(Exception):

    def __init__(self, msg):
        self.msg = msg

class ContentSignatureNotFoundException(Exception):

    def __init__(self, msg):
        self.msg = msg

class StrictModeColumnCountMismatchException(Exception):

    def __init__(self,atomic_fn, expected_col_count,actual_col_count,lines_read):
        self.atomic_fn = atomic_fn
        self.expected_col_count = expected_col_count
        self.actual_col_count = actual_col_count
        self.lines_read = lines_read

class FluffyModeColumnCountMismatchException(Exception):

    def __init__(self,atomic_fn, expected_col_count,actual_col_count,lines_read):
        self.atomic_fn = atomic_fn
        self.expected_col_count = expected_col_count
        self.actual_col_count = actual_col_count
        self.lines_read = lines_read

class ContentSignatureDiffersException(Exception):

    def __init__(self,original_filename, other_filename, filenames_str,key,source_value,signature_value):
        self.original_filename = original_filename
        self.other_filename = other_filename
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


class MaximumSourceFilesExceededException(Exception):

    def __init__(self,msg):
        self.msg = msg



# Simplistic Sql "parsing" class... We'll eventually require a real SQL parser which will provide us with a parse tree
#
# A "qtable" is a filename which behaves like an SQL table...
class Sql(object):

    def __init__(self, sql, data_streams):
        # Currently supports only standard SELECT statements

        # Holds original SQL
        self.sql = sql
        # Holds sql parts
        self.sql_parts = sql.split()
        self.data_streams = data_streams

        self.qtable_metadata_dict = OrderedDict()

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
                    raise InvalidQueryException(
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

                if qtable_name[0] != '(':
                    normalized_qtable_name = self.normalize_qtable_name(qtable_name)
                    xprint("Normalized qtable name for %s is %s" % (qtable_name,normalized_qtable_name))
                    self.qtable_names += [normalized_qtable_name]

                    if normalized_qtable_name not in self.qtable_name_positions.keys():
                        self.qtable_name_positions[normalized_qtable_name] = []

                    self.qtable_name_positions[normalized_qtable_name].append(idx + 1)
                    self.sql_parts[idx + 1] = normalized_qtable_name
                    idx += 2
                else:
                    idx += 1
            else:
                idx += 1
        xprint("Final sql parts: %s" % self.sql_parts)

    def normalize_qtable_name(self,qtable_name):
        if self.data_streams.is_data_stream(qtable_name):
            return qtable_name

        if ':::' in qtable_name:
            qsql_filename, table_name = qtable_name.split(":::", 1)
            return '%s:::%s' % (os.path.realpath(os.path.abspath(qsql_filename)),table_name)
        else:
            return os.path.realpath(os.path.abspath(qtable_name))

    def set_effective_table_name(self, qtable_name, effective_table_name):
        if qtable_name in self.qtable_name_effective_table_names.keys():
            if self.qtable_name_effective_table_names[qtable_name] != effective_table_name:
                raise Exception(
                    "Already set effective table name for qtable %s. Trying to change the effective table name from %s to %s" %
                    (qtable_name,self.qtable_name_effective_table_names[qtable_name],effective_table_name))

        xprint("Setting effective table name for %s - effective table name is set to %s" % (qtable_name,effective_table_name))
        self.qtable_name_effective_table_names[
            qtable_name] = effective_table_name

    def get_effective_sql(self,table_name_mapping=None):
        if len(list(filter(lambda x: x is None, self.qtable_name_effective_table_names))) != 0:
            assert False, 'There are qtables without effective tables'

        effective_sql = [x for x in self.sql_parts]

        xprint("Effective table names",self.qtable_name_effective_table_names)
        for qtable_name, positions in six.iteritems(self.qtable_name_positions):
            xprint("Positions for qtable name %s are %s" % (qtable_name,positions))
            for pos in positions:
                if table_name_mapping is not None:
                    x = self.qtable_name_effective_table_names[qtable_name]
                    effective_sql[pos] = table_name_mapping[x]
                else:
                    effective_sql[pos] = self.qtable_name_effective_table_names[qtable_name]

        return " ".join(effective_sql)

    def get_qtable_name_effective_table_names(self):
        return self.qtable_name_effective_table_names

    def execute_and_fetch(self, db):
        x = self.get_effective_sql()
        xprint("Final query: %s" % x)
        db_results_obj = db.execute_and_fetch(x)
        return db_results_obj

    def materialize_using(self,loaded_table_structures_dict):
        xprint("Materializing sql object: %s" % str(self.qtable_names))
        xprint("loaded table structures dict %s" % loaded_table_structures_dict)
        for qtable_name in self.qtable_names:
            table_structure = loaded_table_structures_dict[qtable_name]

            table_name_in_disk_db = table_structure.get_table_name_for_querying()

            effective_table_name = '%s.%s' % (table_structure.db_id, table_name_in_disk_db)

            # for a single file - no need to create a union, just use the table name
            self.set_effective_table_name(qtable_name, effective_table_name)
            xprint("Materialized filename %s to effective table name %s" % (qtable_name,effective_table_name))


class TableColumnInferer(object):

    def __init__(self, input_params):
        self.inferred = False
        self.mode = input_params.parsing_mode
        self.rows = []
        self.skip_header = input_params.skip_header
        self.header_row = None
        self.header_row_filename = None
        self.expected_column_count = input_params.expected_column_count
        self.input_delimiter = input_params.delimiter
        self.disable_column_type_detection = input_params.disable_column_type_detection

    def _generate_content_signature(self):
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
            assert False, "Already inferred columns"

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
                # If there are only two types, one float an one int, then choose a float type
                if len(set(type_list_without_nulls)) == 2 and float in type_list_without_nulls and int in type_list_without_nulls:
                    return float
                return str

    def do_analysis(self):
        if self.mode == 'strict':
            self._do_strict_analysis()
        elif self.mode in ['relaxed']:
            self._do_relaxed_analysis()
        else:
            raise Exception('Unknown parsing mode %s' % self.mode)

        if self.column_count == 1 and self.expected_column_count != 1 and self.expected_column_count is not None:
            print(f"Warning: column count is one (expected column count is {self.expected_column_count} - did you provide the correct delimiter?", file=sys.stderr)

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
                elif self.mode in ['relaxed']:
                    # in relaxed mode, add columns to fill the missing ones
                    self.header_row = self.header_row + \
                        ['c%s' % (x + len(self.header_row) + 1)
                         for x in range(self.column_count - len(self.header_row))]
            elif len(self.header_row) > self.column_count:
                if self.mode == 'strict':
                    raise ColumnCountMismatchException("Strict mode. Header row contains more columns than expected column count (%s vs %s)" % (
                        len(self.header_row), self.column_count))
                elif self.mode in ['relaxed']:
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
            if self.header_row is None:
                self.column_count = 0
            else:
                self.column_count = len(self.header_row)
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
        assert self.column_count > -1
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
        return OrderedDict(zip(self.column_names, self.column_types))

    def get_column_count(self):
        return self.column_count

    def get_column_names(self):
        return self.column_names

    def get_column_types(self):
        return self.column_types


def py3_encoded_csv_reader(encoding, f, dialect,row_data_only=False,**kwargs):
    try:
        xprint("f is %s" % str(f))
        xprint("dialect is %s" % dialect)
        csv_reader = csv.reader(f, dialect, **kwargs)

        if row_data_only:
            for row in csv_reader:
                yield row
        else:
            for row in csv_reader:
                yield (f.filename(),f.isfirstline(),row)

    except UnicodeDecodeError as e1:
        raise CouldNotParseInputException(e1)
    except ValueError as e:
        # TODO Add test for this
        if str(e) is not None and str(e).startswith('could not convert string to'):
            raise CouldNotConvertStringToNumericValueException(str(e))
        else:
            raise CouldNotParseInputException(str(e))
    except Exception as e:
        if str(e).startswith("field larger than field limit"):
            raise ColumnMaxLengthLimitExceededException(str(e))
        elif 'universal-newline' in str(e):
            raise UniversalNewlinesExistException()
        else:
            raise

encoded_csv_reader = py3_encoded_csv_reader

def normalized_filename(filename):
    return filename

class TableCreatorState(object):
    INITIALIZED = 'INITIALIZED'
    ANALYZED = 'ANALYZED'
    FULLY_READ = 'FULLY_READ'

class MaterializedStateType(object):
    UNKNOWN = 'unknown'
    DELIMITED_FILE = 'delimited-file'
    QSQL_FILE = 'qsql-file'
    SQLITE_FILE = 'sqlite-file'
    DATA_STREAM = 'data-stream'

class TableSourceType(object):
    DELIMITED_FILE = 'file'
    DELIMITED_FILE_WITH_UNUSED_QSQL = 'file-with-unused-qsql'
    QSQL_FILE = 'qsql-file'
    QSQL_FILE_WITH_ORIGINAL = 'qsql-file-with-original'
    SQLITE_FILE = 'sqlite-file'
    DATA_STREAM = 'data-stream'

def skip_BOM(f):
    try:
        BOM = f.buffer.read(3)

        if BOM != six.b('\xef\xbb\xbf'):
            # TODO Add test for this (propagates to try:except)
            raise Exception('Value of BOM is not as expected - Value is "%s"' % str(BOM))
    except Exception as e:
        # TODO Add a test for this
        raise Exception('Tried to skip BOM for "utf-8-sig" encoding and failed. Error message is ' + str(e))

def detect_qtable_name_source_info(qtable_name,data_streams,read_caching_enabled):
    data_stream = data_streams.get_for_filename(qtable_name)
    xprint("Found data stream %s" % data_stream)

    if data_stream is not None:
        return MaterializedStateType.DATA_STREAM, TableSourceType.DATA_STREAM,(data_stream,)

    if ':::' in qtable_name:
        qsql_filename, table_name = qtable_name.split(":::", 1)
        if not os.path.exists(qsql_filename):
            raise FileNotFoundException("Could not find file %s" % qsql_filename)

        if is_qsql_file(qsql_filename):
            return MaterializedStateType.QSQL_FILE, TableSourceType.QSQL_FILE, (qsql_filename, table_name,)
        if is_sqlite_file(qsql_filename):
            return MaterializedStateType.SQLITE_FILE, TableSourceType.SQLITE_FILE, (qsql_filename, table_name,)
        raise UnknownFileTypeException("Cannot detect the type of table %s" % qtable_name)
    else:
        if is_qsql_file(qtable_name):
            return MaterializedStateType.QSQL_FILE, TableSourceType.QSQL_FILE, (qtable_name, None)
        if is_sqlite_file(qtable_name):
            return MaterializedStateType.SQLITE_FILE, TableSourceType.SQLITE_FILE, (qtable_name, None)
        matching_qsql_file_candidate = qtable_name + '.qsql'

        table_source_type = TableSourceType.DELIMITED_FILE
        if is_qsql_file(matching_qsql_file_candidate):
            if read_caching_enabled:
                xprint("Found matching qsql file for original file %s (matching file %s) and read caching is enabled. Using it" % (qtable_name,matching_qsql_file_candidate))
                return MaterializedStateType.QSQL_FILE, TableSourceType.QSQL_FILE_WITH_ORIGINAL, (matching_qsql_file_candidate, None)
            else:
                xprint("Found matching qsql file for original file %s (matching file %s), but read caching is disabled. Not using it" % (qtable_name,matching_qsql_file_candidate))
                table_source_type = TableSourceType.DELIMITED_FILE_WITH_UNUSED_QSQL


        return MaterializedStateType.DELIMITED_FILE,table_source_type ,(qtable_name, None)


def is_sqlite_file(filename):
    if not os.path.exists(filename):
        return False

    f = open(filename,'rb')
    magic = f.read(16)
    f.close()
    return magic == six.b("SQLite format 3\x00")

def sqlite_table_exists(cursor,table_name):
    results = cursor.execute("select count(*) from sqlite_master where type='table' and tbl_name == '%s'" % table_name).fetchall()
    return results[0][0] == 1

def is_qsql_file(filename):
    if not is_sqlite_file(filename):
        return False

    db = Sqlite3DB('check_qsql_db',filename,filename,create_qcatalog=False)
    qcatalog_exists = db.qcatalog_table_exists()
    db.done()
    return qcatalog_exists

def normalize_filename_to_table_name(filename):
    xprint("Normalizing filename %s" % filename)
    if filename[0].isdigit():
        xprint("Filename starts with a digit, adding prefix")
        filename = 't_%s' % filename
    if filename.lower().endswith(".qsql"):
        filename = filename[:-5]
    elif filename.lower().endswith('.sqlite'):
        filename = filename[:-7]
    elif filename.lower().endswith('.sqlite3'):
        filename = filename[:-8]
    return filename.replace("-","_dash_").replace(".","_dot_").replace('?','_qm_').replace("/","_slash_").replace("\\","_backslash_").replace(":","_colon_").replace(" ","_space_").replace("+","_plus_")

def validate_content_signature(original_filename, source_signature,other_filename, content_signature,scope=None,dump=False):
    if dump:
        xprint("Comparing: source value: %s target value: %s" % (source_signature,content_signature))

    s = "%s vs %s:" % (original_filename,other_filename)
    if scope is None:
        scope = []
    for k in source_signature:
        if type(source_signature[k]) == OrderedDict:
            validate_content_signature(original_filename, source_signature[k],other_filename, content_signature[k],scope + [k])
        else:
            if k not in content_signature:
                raise ContentSignatureDataDiffersException("%s Content Signatures differ. %s is missing from content signature" % (s,k))
            if source_signature[k] != content_signature[k]:
                if k == 'rows':
                    raise ContentSignatureDataDiffersException("%s Content Signatures differ at %s.%s (actual analysis data differs)" % (s,".".join(scope),k))
                else:
                    raise ContentSignatureDiffersException(original_filename, other_filename, original_filename,".".join(scope + [k]),source_signature[k],content_signature[k])

class DelimitedFileReader(object):
    def __init__(self,atomic_fns, input_params, dialect, f = None,external_f_name = None):
        if f is not None:
            assert len(atomic_fns) == 0

        self.atomic_fns = atomic_fns
        self.input_params = input_params
        self.dialect = dialect

        self.f = f
        self.lines_read = 0
        self.file_number = -1

        self.skipped_bom = False

        self.is_open = f is not None

        self.external_f = f is not None
        self.external_f_name = external_f_name

    def get_lines_read(self):
        return self.lines_read

    def get_size_hash(self):
        if self.atomic_fns is None or len(self.atomic_fns) == 0:
            return "data-stream-size"
        else:
            return ",".join(map(str,[os.stat(atomic_fn).st_size for atomic_fn in self.atomic_fns]))

    def get_last_modification_time_hash(self):
        if self.atomic_fns is None or len(self.atomic_fns) == 0:
            return "data stream-lmt"
        else:
            x = ",".join(map(lambda x: ':%s:' % x,[os.stat(x).st_mtime_ns for x in self.atomic_fns]))
            res = hashlib.sha1(six.b(x)).hexdigest() + '///' + x
            xprint("Hash of last modification time is %s" % res)
            return res

    def open_file(self):
        if self.external_f:
            xprint("External f has been provided. No need to open the file")
            return

        # TODO Support universal newlines for gzipped and stdin data as well

        xprint("XX Opening file %s" % ",".join(self.atomic_fns))
        import fileinput

        def q_openhook(filename, mode):
            if self.input_params.gzipped_input or filename.endswith('.gz'):
                import gzip
                f = gzip.open(filename,mode='rt',encoding=self.input_params.input_encoding)
            else:
                if six.PY3:
                    if self.input_params.with_universal_newlines:
                        f = io.open(filename, 'rU', newline=None, encoding=self.input_params.input_encoding)
                    else:
                        f = io.open(filename, 'r', newline=None, encoding=self.input_params.input_encoding)
                else:
                    if self.input_params.with_universal_newlines:
                        file_opening_mode = 'rbU'
                    else:
                        file_opening_mode = 'rb'
                    f = open(filename, file_opening_mode)

            if self.input_params.input_encoding == 'utf-8-sig' and not self.skipped_bom:
                skip_BOM(f)

            return f

        f = fileinput.input(self.atomic_fns,mode='rb',openhook=q_openhook)

        self.f = f
        self.is_open = True
        xprint("Actually opened file %s" % self.f)
        return f

    def close_file(self):
        if not self.is_open:
            # TODO Convert to assertion
            raise Exception("Bug - file should already be open: %s" % ",".join(self.atomic_fns))

        self.f.close()
        xprint("XX Closed file %s" % ",".join(self.atomic_fns))

    def generate_rows(self):
        csv_reader = encoded_csv_reader(self.input_params.input_encoding, self.f, dialect=self.dialect,row_data_only=self.external_f)
        try:
            # TODO Some order with regard to separating data-streams for actual files
            if self.external_f:
                for col_vals in csv_reader:
                    self.lines_read += 1
                    yield self.external_f_name,0, self.lines_read == 0, col_vals
            else:
                for file_name,is_first_line,col_vals in csv_reader:
                    if is_first_line:
                        self.file_number = self.file_number + 1
                    self.lines_read += 1
                    yield file_name,self.file_number,is_first_line,col_vals
        except ColumnMaxLengthLimitExceededException as e:
            msg = "Column length is larger than the maximum. Offending file is '%s' - Line is %s, counting from 1 (encoding %s). The line number is the raw line number of the file, ignoring whether there's a header or not" % (",".join(self.atomic_fns),self.lines_read + 1,self.input_params.input_encoding)
            raise ColumnMaxLengthLimitExceededException(msg)
        except UniversalNewlinesExistException as e2:
            # No need to translate the exception, but we want it to be explicitly defined here for clarity
            raise UniversalNewlinesExistException()

class MaterializedState(object):
    def __init__(self, table_source_type,qtable_name, engine_id):
        xprint("Creating new MS: %s %s" % (id(self), qtable_name))

        self.table_source_type = table_source_type

        self.qtable_name = qtable_name
        self.engine_id = engine_id

        self.db_to_use = None
        self.db_id = None

        self.source_type = None
        self.source = None

        self.mfs_structure = None

        self.start_time = None
        self.end_time = None
        self.duration = None

        self.effective_table_name = None


    def get_materialized_state_type(self):
        return MaterializedStateType.UNKNOWN

    def get_planned_table_name(self):
        assert False, 'not implemented'

    def autodetect_table_name(self):
        xprint("Autodetecting table name. db_to_use=%s" % self.db_to_use)
        existing_table_names = self.db_to_use.retrieve_all_table_names()
        xprint("Existing table names: %s" % existing_table_names)

        possible_indices = range(1,1000)

        for index in possible_indices:
            if index == 1:
                suffix = ''
            else:
                suffix = '_%s' % index

            table_name_attempt = '%s%s' % (self.get_planned_table_name(),suffix)
            xprint("Table name attempt: index=%s name=%s" % (index,table_name_attempt))

            if table_name_attempt not in existing_table_names:
                xprint("Found free table name %s for source type %s source %s" % (table_name_attempt,self.source_type,self.source))
                return table_name_attempt

        raise Exception('Cannot find free table name for source type %s source %s' % (self.source_type,self.source))

    def initialize(self):
        self.start_time = time.time()

    def finalize(self):
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time

    def choose_db_to_use(self,forced_db_to_use=None,stop_after_analysis=False):
        assert False, 'not implemented'

    def make_data_available(self,stop_after_analysis):
        assert False, 'not implemented'

class MaterializedDelimitedFileState(MaterializedState):
    def __init__(self, table_source_type,qtable_name, input_params, dialect_id,engine_id,target_table_name=None):
        super().__init__(table_source_type,qtable_name,engine_id)

        self.input_params = input_params
        self.dialect_id = dialect_id
        self.target_table_name = target_table_name

        self.content_signature = None

        self.atomic_fns = None

        self.can_store_as_cached = None

    def get_materialized_state_type(self):
        return MaterializedStateType.DELIMITED_FILE

    def initialize(self):
        super(MaterializedDelimitedFileState, self).initialize()

        self.atomic_fns = self.materialize_file_list(self.qtable_name)
        self.delimited_file_reader = DelimitedFileReader(self.atomic_fns,self.input_params,self.dialect_id)

        self.source_type = self.table_source_type
        self.source = ",".join(self.atomic_fns)

        return

    def materialize_file_list(self,qtable_name):
        materialized_file_list = []

        unfound_files = []
        # First check if the file exists without globbing. This will ensure that we don't support non-existent files
        if os.path.exists(qtable_name):
            # If it exists, then just use it
            found_files = [qtable_name]
        else:
            # If not, then try with globs (and sort for predictability)
            found_files = list(sorted(glob.glob(qtable_name)))
            # If no files
            if len(found_files) == 0:
                unfound_files += [qtable_name]
        materialized_file_list += found_files

        # If there are no files to go over,
        if len(unfound_files) == 1:
            raise FileNotFoundException(
                "No files matching '%s' have been found" % unfound_files[0])
        elif len(unfound_files) > 1:
            # TODO Add test for this
            raise FileNotFoundException(
                "The following files have not been found for table %s: %s" % (qtable_name,",".join(unfound_files)))

        # deduplicate with matching qsql files
        filtered_file_list = list(filter(lambda x: not x.endswith('.qsql'),materialized_file_list))
        xprint("Filtered qsql files from glob search. Original file count: %s new file count: %s" % (len(materialized_file_list),len(filtered_file_list)))

        l = len(filtered_file_list)
        # If this proves to be a problem for users in terms of usability, then we'll just materialize the files
        # into the adhoc db, as with the db attach limit of sqlite
        if l > 500:
            msg = "Maximum source files for table must be 500. Table is name is %s Number of actual files is %s" % (qtable_name,l)
            raise MaximumSourceFilesExceededException(msg)

        absolute_path_list = [os.path.abspath(x) for x in filtered_file_list]
        return absolute_path_list

    def choose_db_to_use(self,forced_db_to_use=None,stop_after_analysis=False):
        if forced_db_to_use is not None:
            self.db_id = forced_db_to_use.db_id
            self.db_to_use = forced_db_to_use
            self.can_store_as_cached = False
            assert self.target_table_name is None
            self.target_table_name = self.autodetect_table_name()
            return

        self.can_store_as_cached = True

        self.db_id = '%s' % self._generate_db_name(self.atomic_fns[0])
        xprint("Database id is %s" % self.db_id)
        self.db_to_use = Sqlite3DB(self.db_id, 'file:%s?mode=memory&cache=shared' % self.db_id, 'memory<%s>' % self.db_id,create_qcatalog=True)

        if self.target_table_name is None:
            self.target_table_name = self.autodetect_table_name()


    def __analyze_delimited_file(self,database_info):
        xprint("Analyzing delimited file")
        if self.target_table_name is not None:
            target_sqlite_table_name = self.target_table_name
        else:
            assert False

        xprint("Target sqlite table name is %s" % target_sqlite_table_name)
        # Create the matching database table and populate it
        table_creator = TableCreator(self.qtable_name, self.delimited_file_reader,self.input_params, sqlite_db=database_info.sqlite_db,
                                     target_sqlite_table_name=target_sqlite_table_name)
        table_creator.perform_analyze(self.dialect_id)
        xprint("after perform_analyze")
        self.content_signature = table_creator._generate_content_signature()

        now = datetime.datetime.utcnow().isoformat()

        database_info.sqlite_db.add_to_qcatalog_table(target_sqlite_table_name,
                                          self.content_signature,
                                          now,
                                          self.source_type,
                                          self.source)
        return table_creator

    def _generate_disk_db_filename(self, filenames_str):
        fn = '%s.qsql' % (os.path.abspath(filenames_str).replace("+","__"))
        return fn


    def _get_should_read_from_cache(self, disk_db_filename):
        disk_db_file_exists = os.path.exists(disk_db_filename)

        should_read_from_cache = self.input_params.read_caching and disk_db_file_exists

        return should_read_from_cache

    def calculate_should_read_from_cache(self):
        # TODO cache filename is chosen according to first filename only, which makes multi-file (glob) caching difficult
        #  cache writing is blocked for now in these cases. Will be added in the future (see save_cache_to_disk_if_needed)
        disk_db_filename = self._generate_disk_db_filename(self.atomic_fns[0])
        should_read_from_cache = self._get_should_read_from_cache(disk_db_filename)
        xprint("should read from cache %s" % should_read_from_cache)
        return disk_db_filename,should_read_from_cache

    def get_planned_table_name(self):
        return normalize_filename_to_table_name(os.path.basename(self.atomic_fns[0]))

    def make_data_available(self,stop_after_analysis):
        xprint("In make_data_available. db_id %s db_to_use %s" % (self.db_id,self.db_to_use))
        assert self.db_id is not None

        disk_db_filename, should_read_from_cache = self.calculate_should_read_from_cache()
        xprint("disk_db_filename=%s should_read_from_cache=%s" % (disk_db_filename,should_read_from_cache))

        database_info = DatabaseInfo(self.db_id,self.db_to_use, needs_closing=True)
        xprint("db %s (%s) has been added to the database list" % (self.db_id, self.db_to_use))

        self.delimited_file_reader.open_file()

        table_creator = self.__analyze_delimited_file(database_info)

        self.mfs_structure = MaterializedStateTableStructure(self.qtable_name, self.atomic_fns, self.db_id,
                                                             table_creator.column_inferer.get_column_names(),
                                                             table_creator.column_inferer.get_column_types(),
                                                             None,
                                                             self.target_table_name,
                                                             self.source_type,
                                                             self.source,
                                                             self.get_planned_table_name())

        content_signature = table_creator.content_signature
        content_signature_key = self.db_to_use.calculate_content_signature_key(content_signature)
        xprint("table creator signature key: %s" % content_signature_key)

        relevant_table = self.db_to_use.get_from_qcatalog(content_signature)['temp_table_name']

        if not stop_after_analysis:
            table_creator.perform_read_fully(self.dialect_id)

            self.save_cache_to_disk_if_needed(disk_db_filename, table_creator)


        self.delimited_file_reader.close_file()

        return database_info, relevant_table

    def save_cache_to_disk_if_needed(self, disk_db_filename, table_creator):
        if len(self.atomic_fns) > 1:
            xprint("Cannot save cache for multi-files for now, deciding auto-naming for cache is challenging. Will be added in the future.")
            return

        effective_write_caching = self.input_params.write_caching
        if effective_write_caching:
            if self.can_store_as_cached:
                assert self.table_source_type != TableSourceType.DELIMITED_FILE_WITH_UNUSED_QSQL
                xprint("Going to write file cache for %s. Disk filename is %s" % (",".join(self.atomic_fns), disk_db_filename))
                self._store_qsql(table_creator.sqlite_db, disk_db_filename)
            else:
                xprint("Database has been provided externally. Skipping storing a cached version of the data")

    def _store_qsql(self, source_sqlite_db, disk_db_filename):
        xprint("Storing data as disk db")
        disk_db_conn = sqlite3.connect(disk_db_filename)
        with disk_db_conn:
            source_sqlite_db.conn.backup(disk_db_conn)
        xprint("Written db to disk: disk db filename %s" % (disk_db_filename))
        disk_db_conn.close()

    def _generate_db_name(self, qtable_name):
        return 'e_%s_fn_%s' % (self.engine_id,normalize_filename_to_table_name(qtable_name))


class MaterialiedDataStreamState(MaterializedDelimitedFileState):
    def __init__(self, table_source_type, qtable_name, input_params, dialect_id, engine_id, data_stream, stream_target_db): ## should pass adhoc_db
        assert data_stream is not None

        super().__init__(table_source_type, qtable_name, input_params, dialect_id, engine_id,target_table_name=None)

        self.data_stream = data_stream

        self.stream_target_db = stream_target_db

        self.target_table_name = None

    def get_planned_table_name(self):
        return 'data_stream_%s' % (normalize_filename_to_table_name(self.source))

    def get_materialized_state_type(self):
        return MaterializedStateType.DATA_STREAM

    def initialize(self):
        self.start_time = time.time()
        if self.input_params.gzipped_input:
            raise CannotUnzipDataStreamException()

        self.source_type = self.table_source_type
        self.source = self.data_stream.stream_id

        self.delimited_file_reader = DelimitedFileReader([], self.input_params, self.dialect_id, f=self.data_stream.stream,external_f_name=self.source)

    def choose_db_to_use(self,forced_db_to_use=None,stop_after_analysis=False):
        assert forced_db_to_use is None

        self.db_id = self.stream_target_db.db_id
        self.db_to_use = self.stream_target_db

        self.target_table_name = self.autodetect_table_name()

        return

    def calculate_should_read_from_cache(self):
        # No disk_db_filename, and no reading from cache when reading a datastream
        return None, False

    def finalize(self):
        super(MaterialiedDataStreamState, self).finalize()

    def save_cache_to_disk_if_needed(self, disk_db_filename, table_creator):
        xprint("Saving to cache is disabled for data streams")
        return


class MaterializedSqliteState(MaterializedState):
    def __init__(self,table_source_type,qtable_name,sqlite_filename,table_name, engine_id):
        super(MaterializedSqliteState, self).__init__(table_source_type,qtable_name,engine_id)
        self.sqlite_filename = sqlite_filename
        self.table_name = table_name

        self.table_name_autodetected = None

    def initialize(self):
        super(MaterializedSqliteState, self).initialize()

        self.table_name_autodetected = False
        if self.table_name is None:
            self.table_name = self.autodetect_table_name()
            self.table_name_autodetected = True
            return

        self.validate_table_name()

    def get_planned_table_name(self):
        if self.table_name_autodetected:
            return normalize_filename_to_table_name(os.path.basename(self.qtable_name))
        else:
            return self.table_name


    def autodetect_table_name(self):
        db = Sqlite3DB('temp_db','file:%s?immutable=1' % self.sqlite_filename,self.sqlite_filename,create_qcatalog=False)
        try:
            table_names = list(sorted(db.retrieve_all_table_names()))
            if len(table_names) == 1:
                return table_names[0]
            elif len(table_names) == 0:
                raise NoTablesInSqliteException(self.sqlite_filename)
            else:
                raise TooManyTablesInSqliteException(self.sqlite_filename,table_names)
        finally:
            db.done()

    def validate_table_name(self):
        db = Sqlite3DB('temp_db', 'file:%s?immutable=1' % self.sqlite_filename, self.sqlite_filename,
                       create_qcatalog=False)
        try:
            table_names = list(db.retrieve_all_table_names())
            if self.table_name.lower() not in map(lambda x:x.lower(),table_names):
                raise NonExistentTableNameInSqlite(self.sqlite_filename, self.table_name, table_names)
        finally:
            db.done()

    def finalize(self):
        super(MaterializedSqliteState, self).finalize()

    def get_materialized_state_type(self):
        return MaterializedStateType.SQLITE_FILE

    def _generate_qsql_only_db_name__temp(self, filenames_str):
        return 'e_%s_fn_%s' % (self.engine_id,hashlib.sha1(six.b(filenames_str)).hexdigest())

    def choose_db_to_use(self,forced_db_to_use=None,stop_after_analysis=False):
        self.source = self.sqlite_filename
        self.source_type = self.table_source_type

        self.db_id = '%s' % self._generate_qsql_only_db_name__temp(self.qtable_name)

        x = 'file:%s?immutable=1' % self.sqlite_filename
        self.db_to_use = Sqlite3DB(self.db_id, x, self.sqlite_filename,create_qcatalog=False)

        if forced_db_to_use:
            xprint("Forced sqlite db_to_use %s" % forced_db_to_use)
            new_table_name = forced_db_to_use.attach_and_copy_table(self.db_to_use,self.table_name,stop_after_analysis)
            self.table_name = new_table_name
            self.db_id = forced_db_to_use.db_id
            self.db_to_use = forced_db_to_use

        return

    def make_data_available(self,stop_after_analysis):
        xprint("db %s (%s) has been added to the database list" % (self.db_id, self.db_to_use))

        database_info,relevant_table = DatabaseInfo(self.db_id,self.db_to_use, needs_closing=True), self.table_name

        column_names, column_types, sqlite_column_types = self._extract_information()

        self.mfs_structure = MaterializedStateTableStructure(self.qtable_name, [self.qtable_name], self.db_id,
                                                             column_names, column_types, sqlite_column_types,
                                                             self.table_name,
                                                             self.source_type,self.source,
                                                             self.get_planned_table_name())
        return database_info, relevant_table

    def _extract_information(self):
        table_list = self.db_to_use.retrieve_all_table_names()
        if len(table_list) == 1:
            table_name = table_list[0][0]
            xprint("Only one table in sqlite database, choosing it: %s" % table_name)
        else:
            # self.table_name has either beein autodetected, or validated as an existing table up the stack
            table_name = self.table_name
            xprint("Multiple tables in sqlite file. Using provided table name %s" % self.table_name)

        table_info = self.db_to_use.get_sqlite_table_info(table_name)
        xprint('Table info is %s' % table_info)
        column_names = list(map(lambda x: x[1], table_info))
        sqlite_column_types = list(map(lambda x: x[2].lower(),table_info))
        column_types = list(map(lambda x: sqlite_type_to_python_type(x[2]), table_info))
        xprint("Column names and types for table %s: %s" % (table_name, list(zip(column_names, zip(sqlite_column_types,column_types)))))
        self.content_signature = OrderedDict()

        return column_names, column_types, sqlite_column_types


class MaterializedQsqlState(MaterializedState):
    def __init__(self,table_source_type,qtable_name,qsql_filename,table_name, engine_id,input_params,dialect_id):
        super(MaterializedQsqlState, self).__init__(table_source_type,qtable_name,engine_id)
        self.qsql_filename = qsql_filename
        self.table_name = table_name

        # These are for cases where the qsql file is just a cache and the original is still there, used for content
        # validation
        self.input_params = input_params
        self.dialect_id = dialect_id

        self.table_name_autodetected = None

    def initialize(self):
        super(MaterializedQsqlState, self).initialize()

        self.table_name_autodetected = False
        if self.table_name is None:
            self.table_name = self.autodetect_table_name()
            self.table_name_autodetected = True
            return

        self.validate_table_name()

    def get_planned_table_name(self):
        if self.table_name_autodetected:
            return normalize_filename_to_table_name(os.path.basename(self.qtable_name))
        else:
            return self.table_name


    def autodetect_table_name(self):
        db = Sqlite3DB('temp_db','file:%s?immutable=1' % self.qsql_filename,self.qsql_filename,create_qcatalog=False)
        assert db.qcatalog_table_exists()
        try:
            qcatalog_entries = db.get_all_from_qcatalog()
            if len(qcatalog_entries) == 0:
                raise NoTableInQsqlExcption(self.qsql_filename)
            elif len(qcatalog_entries) == 1:
                return qcatalog_entries[0]['temp_table_name']
            else:
                # TODO Add a test for this
                table_names = list(sorted([x['temp_table_name'] for x in qcatalog_entries]))
                raise TooManyTablesInQsqlException(self.qsql_filename,table_names)
        finally:
            db.done()

    def validate_table_name(self):
        db = Sqlite3DB('temp_db', 'file:%s?immutable=1' % self.qsql_filename, self.qsql_filename,
                       create_qcatalog=False)
        assert db.qcatalog_table_exists()
        try:
            entry = db.get_from_qcatalog_using_table_name(self.table_name)
            if entry is None:
                qcatalog_entries = db.get_all_from_qcatalog()
                table_names = list(sorted([x['temp_table_name'] for x in qcatalog_entries]))
                raise NonExistentTableNameInQsql(self.qsql_filename,self.table_name,table_names)
        finally:
            db.done()

    def finalize(self):
        super(MaterializedQsqlState, self).finalize()

    def get_materialized_state_type(self):
        return MaterializedStateType.QSQL_FILE

    def _generate_qsql_only_db_name__temp(self, filenames_str):
        return 'e_%s_fn_%s' % (self.engine_id,hashlib.sha1(six.b(filenames_str)).hexdigest())

    def choose_db_to_use(self,forced_db_to_use=None,stop_after_analysis=False):
        self.source = self.qsql_filename
        self.source_type = self.table_source_type

        self.db_id = '%s' % self._generate_qsql_only_db_name__temp(self.qtable_name)

        x = 'file:%s?immutable=1' % self.qsql_filename
        self.db_to_use = Sqlite3DB(self.db_id, x, self.qsql_filename,create_qcatalog=False)

        if forced_db_to_use:
            xprint("Forced qsql to use forced_db: %s" % forced_db_to_use)

            # TODO RLRL Move query to Sqlite3DB
            all_table_names = [(x[0],x[1]) for x in self.db_to_use.execute_and_fetch("select content_signature_key,temp_table_name from %s" % self.db_to_use.QCATALOG_TABLE_NAME).results]
            csk,t = list(filter(lambda x: x[1] == self.table_name,all_table_names))[0]
            xprint("Copying table %s from db_id %s" % (t,self.db_id))
            d = self.db_to_use.get_from_qcatalog_using_table_name(t)

            new_table_name = forced_db_to_use.attach_and_copy_table(self.db_to_use,self.table_name,stop_after_analysis)

            xprint("CS",d['content_signature'])
            cs = OrderedDict(json.loads(d['content_signature']))
            forced_db_to_use.add_to_qcatalog_table(new_table_name, cs, d['creation_time'],
                                    d['source_type'], d['source'])

            self.table_name = new_table_name
            self.db_id = forced_db_to_use.db_id
            self.db_to_use = forced_db_to_use

        return

    def make_data_available(self,stop_after_analysis):
        xprint("db %s (%s) has been added to the database list" % (self.db_id, self.db_to_use))

        database_info,relevant_table = self._read_table_from_cache(stop_after_analysis)

        column_names, column_types, sqlite_column_types = self._extract_information()

        self.mfs_structure = MaterializedStateTableStructure(self.qtable_name, [self.qtable_name], self.db_id,
                                                             column_names, column_types, sqlite_column_types,
                                                             self.table_name,
                                                             self.source_type,self.source,
                                                             self.get_planned_table_name())
        return database_info, relevant_table

    def _extract_information(self):
        assert self.db_to_use.qcatalog_table_exists()
        table_info = self.db_to_use.get_sqlite_table_info(self.table_name)
        xprint('table_name=%s Table info is %s' % (self.table_name,table_info))

        x = self.db_to_use.get_from_qcatalog_using_table_name(self.table_name)

        column_names = list(map(lambda x: x[1], table_info))
        sqlite_column_types = list(map(lambda x: x[2].lower(),table_info))
        column_types = list(map(lambda x: sqlite_type_to_python_type(x[2]), table_info))
        self.content_signature = OrderedDict(
            **json.loads(x['content_signature']))
        xprint('Inferred column names and types from qsql: %s' % list(zip(column_names, zip(sqlite_column_types,column_types))))

        return column_names, column_types, sqlite_column_types

    def _backing_original_file_exists(self):
        return '%s.qsql' % self.qtable_name == self.qsql_filename

    def _read_table_from_cache(self, stop_after_analysis):
        if self._backing_original_file_exists():
            xprint("Found a matching source file for qsql file with qtable name %s. Checking content signature by creating a temp MFDS + analysis" % self.qtable_name)
            mdfs = MaterializedDelimitedFileState(TableSourceType.DELIMITED_FILE,self.qtable_name,self.input_params,self.dialect_id,self.engine_id,target_table_name=None)
            mdfs.initialize()
            mdfs.choose_db_to_use(forced_db_to_use=None,stop_after_analysis=stop_after_analysis)
            _,_ = mdfs.make_data_available(stop_after_analysis=True)

            original_file_content_signature = mdfs.content_signature
            original_file_content_signature_key = self.db_to_use.calculate_content_signature_key(original_file_content_signature)

            qcatalog_entry = self.db_to_use.get_from_qcatalog_using_table_name(self.table_name)

            if qcatalog_entry is None:
                raise Exception('missing content signature!')

            xprint("Actual Signature Key: %s Expected Signature Key: %s" % (qcatalog_entry['content_signature_key'],original_file_content_signature_key))
            actual_content_signature = json.loads(qcatalog_entry['content_signature'])

            xprint("Validating content signatures: original %s vs qsql %s" % (original_file_content_signature,actual_content_signature))
            validate_content_signature(self.qtable_name, original_file_content_signature, self.qsql_filename, actual_content_signature,dump=True)
            mdfs.finalize()
        return DatabaseInfo(self.db_id,self.db_to_use, needs_closing=True), self.table_name


class MaterializedStateTableStructure(object):
    def __init__(self,qtable_name, atomic_fns, db_id, column_names, python_column_types, sqlite_column_types, table_name_for_querying,source_type,source,planned_table_name):
        self.qtable_name = qtable_name
        self.atomic_fns = atomic_fns
        self.db_id = db_id
        self.column_names = column_names
        self.python_column_types = python_column_types
        self.table_name_for_querying = table_name_for_querying
        self.source_type = source_type
        self.source = source
        self.planned_table_name = planned_table_name

        if sqlite_column_types is not None:
            self.sqlite_column_types = sqlite_column_types
        else:
            self.sqlite_column_types = [Sqlite3DB.PYTHON_TO_SQLITE_TYPE_NAMES[t].lower() for t in python_column_types]

    def get_table_name_for_querying(self):
        return self.table_name_for_querying

    def __str__(self):
        return "MaterializedStateTableStructure<%s>" % self.__dict__
    __repr__ = __str__

class TableCreator(object):
    def __str__(self):
        return "TableCreator<%s>" % str(self)
    __repr__ = __str__

    def __init__(self, qtable_name, delimited_file_reader,input_params,sqlite_db=None,target_sqlite_table_name=None):

        self.qtable_name = qtable_name
        self.delimited_file_reader = delimited_file_reader

        self.db_id = sqlite_db.db_id

        self.sqlite_db = sqlite_db
        self.target_sqlite_table_name = target_sqlite_table_name

        self.skip_header = input_params.skip_header
        self.gzipped = input_params.gzipped_input
        self.table_created = False

        self.encoding = input_params.input_encoding
        self.mode = input_params.parsing_mode
        self.expected_column_count = input_params.expected_column_count
        self.input_delimiter = input_params.delimiter
        self.with_universal_newlines = input_params.with_universal_newlines

        self.column_inferer = TableColumnInferer(input_params)

        self.pre_creation_rows = []
        self.buffered_inserts = []
        self.effective_column_names = None

        # Column type indices for columns that contain numeric types. Lazily initialized
        # so column inferer can do its work before this information is needed
        self.numeric_column_indices = None

        self.state = TableCreatorState.INITIALIZED

        self.content_signature = None

    def _generate_content_signature(self):
        if self.state != TableCreatorState.ANALYZED:
            # TODO Change to assertion
            raise Exception('Bug - Wrong state %s. Table needs to be analyzed before a content signature can be calculated' % self.state)

        size = self.delimited_file_reader.get_size_hash()
        last_modification_time = self.delimited_file_reader.get_last_modification_time_hash()

        m = OrderedDict({
            "_signature_version": "v1",
            "skip_header": self.skip_header,
            "gzipped": self.gzipped,
            "with_universal_newlines": self.with_universal_newlines,
            "encoding": self.encoding,
            "mode": self.mode,
            "expected_column_count": self.expected_column_count,
            "input_delimiter": self.input_delimiter,
            "inferer": self.column_inferer._generate_content_signature(),
            "original_file_size": size,
            "last_modification_time": last_modification_time
        })

        return m

    def validate_extra_header_if_needed(self, file_number, filename,col_vals):
        xprint("HHX validate",file_number,filename,col_vals)
        if not self.skip_header:
            xprint("No need to validate header")
            return False

        if file_number == 0:
            xprint("First file, no need to validate extra header")
            return False

        header_already_exists = self.column_inferer.header_row is not None

        if header_already_exists:
            xprint("Validating extra header")
            if tuple(self.column_inferer.header_row) != tuple(col_vals):
                raise BadHeaderException("Extra header '{}' in file '{}' mismatches original header '{}' from file '{}'. Table name is '{}'".format(
                    ",".join(col_vals),filename,
                    ",".join(self.column_inferer.header_row),
                    self.column_inferer.header_row_filename,
                    self.qtable_name))
            xprint("header already exists: %s" % self.column_inferer.header_row)
        else:
            xprint("Header doesn't already exist")

        return header_already_exists

    def _populate(self,dialect,stop_after_analysis=False):
        total_data_lines_read = 0
        try:
            try:
                for file_name,file_number,is_first_line,col_vals in self.delimited_file_reader.generate_rows():
                    if is_first_line:
                        if self.validate_extra_header_if_needed(file_number,file_name,col_vals):
                            continue
                    self._insert_row(file_name, col_vals)
                    if stop_after_analysis:
                        if self.column_inferer.inferred:
                            xprint("Stopping after analysis")
                            return
                if self.delimited_file_reader.get_lines_read() == 0 and self.skip_header:
                    raise MissingHeaderException("Header line is expected but missing in file %s" % ",".join(self.delimited_file_reader.atomic_fns))

                total_data_lines_read += self.delimited_file_reader.lines_read - (1 if self.skip_header else 0)
                xprint("Total Data lines read %s" % total_data_lines_read)
            except StrictModeColumnCountMismatchException as e:
                raise ColumnCountMismatchException(
                    'Strict mode - Expected %s columns instead of %s columns in file %s row %s. Either use relaxed modes or check your delimiter' % (
                    e.expected_col_count, e.actual_col_count, normalized_filename(e.atomic_fn), e.lines_read))
            except FluffyModeColumnCountMismatchException as e:
                raise ColumnCountMismatchException(
                    'Deprecated fluffy mode - Too many columns in file %s row %s (%s fields instead of %s fields). Consider moving to either relaxed or strict mode' % (
                    normalized_filename(e.atomic_fn), e.lines_read, e.actual_col_count, e.expected_col_count))
        finally:
            self._flush_inserts()

        if not self.table_created:
            self.column_inferer.force_analysis()
            self._do_create_table(self.qtable_name)

        self.sqlite_db.conn.commit()

    def perform_analyze(self, dialect):
        xprint("Analyzing... %s" % dialect)
        if self.state == TableCreatorState.INITIALIZED:
            self._populate(dialect,stop_after_analysis=True)
            self.state = TableCreatorState.ANALYZED

            self.content_signature = self._generate_content_signature()
            content_signature_key = self.sqlite_db.calculate_content_signature_key(self.content_signature)
            xprint("Setting content signature after analysis: %s" % content_signature_key)
        else:
            # TODO Convert to assertion
            raise Exception('Bug - Wrong state %s' % self.state)

    def perform_read_fully(self, dialect):
        if self.state == TableCreatorState.ANALYZED:
            self._populate(dialect,stop_after_analysis=False)
            self.state = TableCreatorState.FULLY_READ
        else:
            # TODO Convert to assertion
            raise Exception('Bug - Wrong state %s' % self.state)

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
                column_types) if self.sqlite_db.is_numeric_type(column_type)]

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
                raise StrictModeColumnCountMismatchException(",".join(self.delimited_file_reader.atomic_fns), expected_col_count,actual_col_count,self.delimited_file_reader.get_lines_read())
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

        assert False, "Unidentified parsing mode %s" % self.mode

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
        # If the table is still not created, then we don't have enough data
        if not self.table_created:
            return

        if len(self.buffered_inserts) > 0:
            insert_row_stmt = self.sqlite_db.generate_insert_row(
                self.target_sqlite_table_name, self.effective_column_names)

            self.sqlite_db.update_many(insert_row_stmt, self.buffered_inserts)
        self.buffered_inserts = []

    def try_to_create_table(self, filename, col_vals):
        if self.table_created:
            # TODO Convert to assertion
            raise Exception('Table is already created')

        # Add that line to the column inferer
        result = self.column_inferer.analyze(filename, col_vals)
        # If inferer succeeded,
        if result:
            self._do_create_table(filename)
        else:
            pass  # We don't have enough information for creating the table yet

    def _do_create_table(self,filename):
        # Get the column definition dict from the inferer
        column_dict = self.column_inferer.get_column_dict()

        # Guard against empty tables (instead of preventing the creation, just create with a dummy column)
        if len(column_dict) == 0:
            column_dict = { 'dummy_column_for_empty_tables' : str }
            ordered_column_names = [ 'dummy_column_for_empty_tables' ]
        else:
            ordered_column_names = self.column_inferer.get_column_names()

        # Create the CREATE TABLE statement
        create_table_stmt = self.sqlite_db.generate_create_table(
            self.target_sqlite_table_name, ordered_column_names, column_dict)
        # And create the table itself
        self.sqlite_db.execute_and_fetch(create_table_stmt)
        # Mark the table as created
        self.table_created = True
        self._flush_pre_creation_rows(filename)


def determine_max_col_lengths(m,output_field_quoting_func,output_delimiter):
    if len(m) == 0:
        return []
    max_lengths = [0 for x in range(0, len(m[0]))]
    for row_index in range(0, len(m)):
        for col_index in range(0, len(m[0])):
            # TODO Optimize this
            new_len = len("{}".format(output_field_quoting_func(output_delimiter,m[row_index][col_index])))
            if new_len > max_lengths[col_index]:
                max_lengths[col_index] = new_len
    return max_lengths

def print_credentials():
    print("q version %s" % q_version, file=sys.stderr)
    print("Python: %s" % " // ".join([str(x).strip() for x in sys.version.split("\n")]), file=sys.stderr)
    print("Copyright (C) 2012-2021 Harel Ben-Attia (harelba@gmail.com, @harelba on twitter)", file=sys.stderr)
    print("https://harelba.github.io/q/", file=sys.stderr)
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

    def __str__(self):
        return "QError<errorcode=%s,msg=%s,exception=%s,traceback=%s>" % (self.errorcode,self.msg,self.exception,str(self.traceback))
    __repr__ = __str__

class QMetadata(object):
    def __init__(self,table_structures={},new_table_structures={},output_column_name_list=None):
        self.table_structures = table_structures
        self.new_table_structures = new_table_structures
        self.output_column_name_list = output_column_name_list

    def __str__(self):
        return "QMetadata<%s" % (self.__dict__)
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
            write_caching=False,
            max_attached_sqlite_databases = 10):
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
        self.max_attached_sqlite_databases = max_attached_sqlite_databases

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
    # TODO Can stream-id be removed?
    def __init__(self,stream_id,filename,stream):
        self.stream_id = stream_id
        self.filename = filename
        self.stream = stream

    def __str__(self):
        return "QDataStream<stream_id=%s,filename=%s,stream=%s>" % (self.stream_id,self.filename,self.stream)
    __repr__ = __str__


class DataStreams(object):
    def __init__(self, data_streams_dict):
        assert type(data_streams_dict) == dict
        self.validate(data_streams_dict)
        self.data_streams_dict = data_streams_dict

    def validate(self,d):
        for k in d:
            v = d[k]
            if type(k) != str or type(v) != DataStream:
                raise Exception('Bug - Invalid dict: %s' % str(d))

    def get_for_filename(self, filename):
        xprint("Data streams dict is %s. Trying to find %s" % (self.data_streams_dict,filename))
        x = self.data_streams_dict.get(filename)
        return x

    def is_data_stream(self,filename):
        return filename in self.data_streams_dict

class DatabaseInfo(object):
    def __init__(self,db_id,sqlite_db,needs_closing):
        self.db_id = db_id
        self.sqlite_db = sqlite_db
        self.needs_closing = needs_closing

    def __str__(self):
        return "DatabaseInfo<sqlite_db=%s,needs_closing=%s>" % (self.sqlite_db,self.needs_closing)
    __repr__ = __str__

class QTextAsData(object):
    def __init__(self,default_input_params=QInputParams(),data_streams_dict=None):
        self.engine_id = str(uuid.uuid4()).replace("-","_")

        self.default_input_params = default_input_params
        xprint("Default input params: %s" % self.default_input_params)

        self.loaded_table_structures_dict = OrderedDict()
        self.databases = OrderedDict()

        if data_streams_dict is not None:
            self.data_streams = DataStreams(data_streams_dict)
        else:
            self.data_streams = DataStreams({})

        # Create DB object
        self.query_level_db_id = 'query_e_%s' % self.engine_id
        self.query_level_db = Sqlite3DB(self.query_level_db_id,
                                        'file:%s?mode=memory&cache=shared' % self.query_level_db_id,'<query-level-db>',create_qcatalog=True)
        self.adhoc_db_id = 'adhoc_e_%s' % self.engine_id
        self.adhoc_db_name = 'file:%s?mode=memory&cache=shared' % self.adhoc_db_id
        self.adhoc_db = Sqlite3DB(self.adhoc_db_id,self.adhoc_db_name,'<adhoc-db>',create_qcatalog=True)
        self.query_level_db.conn.execute("attach '%s' as %s" % (self.adhoc_db_name,self.adhoc_db_id))

        self.add_db_to_database_list(DatabaseInfo(self.query_level_db_id,self.query_level_db,needs_closing=True))
        self.add_db_to_database_list(DatabaseInfo(self.adhoc_db_id,self.adhoc_db,needs_closing=True))

    def done(self):
        xprint("Inside done: Database list is %s" % self.databases)
        for db_id in reversed(self.databases.keys()):
            database_info = self.databases[db_id]
            if database_info.needs_closing:
                xprint("Gonna close database %s - %s" % (db_id,self.databases[db_id]))
                self.databases[db_id].sqlite_db.done()
                xprint("Database %s has been closed" % db_id)
            else:
                xprint("No need to close database %s" % db_id)
        xprint("Closed all databases")

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

    def _open_files_and_get_mfss(self,qtable_name,input_params,dialect):
        materialized_file_dict = OrderedDict()

        materialized_state_type,table_source_type,source_info = detect_qtable_name_source_info(qtable_name,self.data_streams,read_caching_enabled=input_params.read_caching)
        xprint("Detected source type %s source info %s" % (materialized_state_type,source_info))

        if materialized_state_type == MaterializedStateType.DATA_STREAM:
            (data_stream,) = source_info
            ms = MaterialiedDataStreamState(table_source_type,qtable_name,input_params,dialect,self.engine_id,data_stream,stream_target_db=self.adhoc_db)
            effective_qtable_name = data_stream.stream_id
        elif materialized_state_type == MaterializedStateType.QSQL_FILE:
            (qsql_filename,table_name) = source_info
            ms = MaterializedQsqlState(table_source_type,qtable_name, qsql_filename=qsql_filename, table_name=table_name,
                                       engine_id=self.engine_id, input_params=input_params, dialect_id=dialect)
            effective_qtable_name = '%s:::%s' % (qsql_filename, table_name)
        elif materialized_state_type == MaterializedStateType.SQLITE_FILE:
            (sqlite_filename,table_name) = source_info
            ms = MaterializedSqliteState(table_source_type,qtable_name, sqlite_filename=sqlite_filename, table_name=table_name,
                                       engine_id=self.engine_id)
            effective_qtable_name = '%s:::%s' % (sqlite_filename, table_name)
        elif materialized_state_type == MaterializedStateType.DELIMITED_FILE:
            (source_qtable_name,_) = source_info
            ms = MaterializedDelimitedFileState(table_source_type,source_qtable_name, input_params, dialect, self.engine_id)
            effective_qtable_name = source_qtable_name
        else:
            assert False, "Unknown file type for qtable %s should have exited with an exception" % (qtable_name)

        assert effective_qtable_name not in materialized_file_dict
        materialized_file_dict[effective_qtable_name] = ms

        xprint("MS dict: %s" % str(materialized_file_dict))

        return list([item for item in materialized_file_dict.values()])

    def _load_mfs(self,mfs,input_params,dialect_id,stop_after_analysis):
        xprint("Loading MFS:", mfs)

        materialized_state_type = mfs.get_materialized_state_type()
        xprint("Detected materialized state type for %s: %s" % (mfs.qtable_name,materialized_state_type))

        mfs.initialize()

        if not materialized_state_type in [MaterializedStateType.DATA_STREAM]:
            if stop_after_analysis or self.should_copy_instead_of_attach(input_params):
                xprint("Should copy instead of attaching. Forcing db to use to adhoc db")
                forced_db_to_use = self.adhoc_db
            else:
                forced_db_to_use = None
        else:
            forced_db_to_use = None

        mfs.choose_db_to_use(forced_db_to_use,stop_after_analysis)
        xprint("Chosen db to use: source %s source_type %s db_id %s db_to_use %s" % (mfs.source,mfs.source_type,mfs.db_id,mfs.db_to_use))

        database_info,relevant_table = mfs.make_data_available(stop_after_analysis)

        if not self.is_adhoc_db(mfs.db_to_use) and not self.should_copy_instead_of_attach(input_params):
            if not self.already_attached_to_query_level_db(mfs.db_to_use):
                self.attach_to_db(mfs.db_to_use, self.query_level_db)
                self.add_db_to_database_list(database_info)
            else:
                xprint("DB %s is already attached to query level db. No need to attach it again.")

        mfs.finalize()

        xprint("MFS Loaded")

        return mfs.source,mfs.source_type

    def add_db_to_database_list(self,database_info):
        db_id = database_info.db_id
        assert db_id is not None
        assert database_info.sqlite_db is not None
        if db_id in self.databases:
            # TODO Convert to assertion
            if id(database_info.sqlite_db) != id(self.databases[db_id].sqlite_db):
                raise Exception('Bug - database already in database list: db_id %s: old %s new %s' % (db_id,self.databases[db_id],database_info))
            else:
                return
        self.databases[db_id] = database_info

    def is_adhoc_db(self,db_to_use):
        return db_to_use.db_id == self.adhoc_db_id

    def should_copy_instead_of_attach(self,input_params):
        attached_database_count = len(self.query_level_db.get_sqlite_database_list())
        x = attached_database_count >= input_params.max_attached_sqlite_databases
        xprint("should_copy_instead_of_attach: attached_database_count=%s should_copy=%s" % (attached_database_count,x))
        return x

    def _load_data(self,qtable_name,input_params=QInputParams(),stop_after_analysis=False):
        xprint("Attempting to load data for materialized file names %s" % qtable_name)

        q_dialect = self.determine_proper_dialect(input_params)
        xprint("Dialect is %s" % q_dialect)
        dialect_id = self.get_dialect_id(qtable_name)
        csv.register_dialect(dialect_id, **q_dialect)

        xprint("qtable metadata for loading is %s" % qtable_name)
        mfss = self._open_files_and_get_mfss(qtable_name,
                                             input_params,
                                             dialect_id)
        assert len(mfss) == 1, "one MS now encapsulated an entire table"
        mfs = mfss[0]

        xprint("MFS to load: %s" % mfs)

        if qtable_name in self.loaded_table_structures_dict.keys():
            xprint("Atomic filename %s found. no need to load" % qtable_name)
            return None

        xprint("qtable %s not found - loading" % qtable_name)


        self._load_mfs(mfs, input_params, dialect_id, stop_after_analysis)
        xprint("Loaded: source-type %s source %s mfs_structure %s" % (mfs.source_type, mfs.source, mfs.mfs_structure))

        assert qtable_name not in self.loaded_table_structures_dict, "loaded_table_structures_dict has been changed to have a non-list value"
        self.loaded_table_structures_dict[qtable_name] = mfs.mfs_structure

        return mfs.mfs_structure

    def already_attached_to_query_level_db(self,db_to_attach):
        attached_dbs = list(map(lambda x:x[1],self.query_level_db.get_sqlite_database_list()))
        return db_to_attach.db_id in attached_dbs

    def attach_to_db(self, target_db, source_db):
        q = "attach '%s' as %s" % (target_db.sqlite_db_url,target_db.db_id)
        xprint("Attach query: %s" % q)
        try:
            c = source_db.execute_and_fetch(q)
        except SqliteOperationalErrorException as e:
            if 'too many attached databases' in str(e):
                raise TooManyAttachedDatabasesException('There are too many attached databases. Use a proper --max-attached-sqlite-databases parameter which is below the maximum. Original error: %s' % str(e))
        except Exception as e1:
            raise

    def detach_from_db(self, target_db, source_db):
        q = "detach %s" % (target_db.db_id)
        xprint("Detach query: %s" % q)
        try:
            c = source_db.execute_and_fetch(q)
        except Exception as e1:
            raise

    def load_data(self,filename,input_params=QInputParams(),stop_after_analysis=False):
        return self._load_data(filename,input_params,stop_after_analysis=stop_after_analysis)

    def _ensure_data_is_loaded_for_sql(self,sql_object,input_params,data_streams=None,stop_after_analysis=False):
        xprint("Ensuring Data load")
        new_table_structures = OrderedDict()

        # For each "table name"
        for qtable_name in sql_object.qtable_names:
            tss = self._load_data(qtable_name,input_params,stop_after_analysis=stop_after_analysis)
            if tss is not None:
                xprint("New Table Structures:",new_table_structures)
                assert qtable_name not in new_table_structures, "new_table_structures was changed not to contain a list as a value"
                new_table_structures[qtable_name] = tss

        return new_table_structures

    def materialize_query_level_db(self,save_db_to_disk_filename,sql_object):
        # TODO More robust creation - Create the file in a separate folder and move it to the target location only after success

        materialized_db = Sqlite3DB("materialized","file:%s" % save_db_to_disk_filename,save_db_to_disk_filename,create_qcatalog=False)
        table_name_mapping = OrderedDict()

        # For each table in the query
        effective_table_names = sql_object.get_qtable_name_effective_table_names()

        for i, qtable_name in enumerate(effective_table_names):
            # table name, in the format db_id.table_name
            effective_table_name_for_qtable_name = effective_table_names[qtable_name]

            source_db_id, actual_table_name_in_db = effective_table_name_for_qtable_name.split(".", 1)
            # The DatabaseInfo instance for this db
            source_database = self.databases[source_db_id]
            if source_db_id != self.query_level_db_id:
                self.attach_to_db(source_database.sqlite_db,materialized_db)

            ts = self.loaded_table_structures_dict[qtable_name]
            proposed_new_table_name = ts.planned_table_name
            xprint("Proposed table name is %s" % proposed_new_table_name)

            new_table_name = materialized_db.find_new_table_name(proposed_new_table_name)

            xprint("Materializing",source_db_id,actual_table_name_in_db,"as",new_table_name)
            # Copy the table into the materialized database
            xx = materialized_db.execute_and_fetch('CREATE TABLE %s AS SELECT * FROM %s' % (new_table_name,effective_table_name_for_qtable_name))

            table_name_mapping[effective_table_name_for_qtable_name] = new_table_name

            # TODO RLRL Preparation for writing materialized database as a qsql file
            # if source_database.sqlite_db.qcatalog_table_exists():
            #     qcatalog_entry = source_database.sqlite_db.get_from_qcatalog_using_table_name(actual_table_name_in_db)
            #     # TODO RLRL Encapsulate dictionary transform inside qcatalog access methods
            #     materialized_db.add_to_qcatalog_table(new_table_name,OrderedDict(json.loads(qcatalog_entry['content_signature'])),
            #                                           qcatalog_entry['creation_time'],
            #                                           qcatalog_entry['source_type'],
            #                                           qcatalog_entry['source_type'])
            #     xprint("PQX Added to qcatalog",source_db_id,actual_table_name_in_db,'as',new_table_name)
            # else:
            #     xprint("PQX Skipped adding to qcatalog",source_db_id,actual_table_name_in_db)

            if source_db_id != self.query_level_db:
                self.detach_from_db(source_database.sqlite_db,materialized_db)

        return table_name_mapping

    def validate_query(self,sql_object,table_structures):

        for qtable_name in sql_object.qtable_names:
            relevant_table_structures = [table_structures[qtable_name]]

            column_names = None
            column_types = None
            for ts in relevant_table_structures:
                names = ts.column_names
                types = ts.python_column_types
                xprint("Comparing column names: %s with %s" % (column_names,names))
                if column_names is None:
                    column_names = names
                else:
                    if column_names != names:
                        raise BadHeaderException("Column names differ for table %s: %s vs %s" % (
                            qtable_name, ",".join(column_names), ",".join(names)))

                xprint("Comparing column types: %s with %s" % (column_types,types))
                if column_types is None:
                    column_types = types
                else:
                    if column_types != types:
                        raise BadHeaderException("Column types differ for table %s: %s vs %s" % (
                        qtable_name, ",".join(column_types), ",".join(types)))

                xprint("All column names match for qtable name %s: column names: %s column types: %s" % (ts.qtable_name,column_names,column_types))

        xprint("Query validated")

    def _execute(self,query_str,input_params=None,data_streams=None,stop_after_analysis=False,save_db_to_disk_filename=None):
        warnings = []
        error = None
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


        try:
            # Create SQL statement
            sql_object = Sql('%s' % query_str, self.data_streams)

            load_start_time = time.time()
            iprint("Going to ensure data is loaded. Currently loaded tables: %s" % str(self.loaded_table_structures_dict))
            new_table_structures = self._ensure_data_is_loaded_for_sql(sql_object,effective_input_params,data_streams,stop_after_analysis=stop_after_analysis)
            iprint("Ensured data is loaded. loaded tables: %s" % self.loaded_table_structures_dict)

            self.validate_query(sql_object,self.loaded_table_structures_dict)

            iprint("Query validated")

            sql_object.materialize_using(self.loaded_table_structures_dict)

            iprint("Materialized sql object")

            if save_db_to_disk_filename is not None:
                xprint("Saving query data to disk")
                dump_start_time = time.time()
                table_name_mapping = self.materialize_query_level_db(save_db_to_disk_filename,sql_object)
                print("Data has been saved into %s . Saving has taken %4.3f seconds" % (save_db_to_disk_filename,time.time()-dump_start_time), file=sys.stderr)
                effective_sql = sql_object.get_effective_sql(table_name_mapping)
                print("Query to run on the database: %s;" % effective_sql, file=sys.stderr)
                command_line = 'echo "%s" | sqlite3 %s' % (effective_sql,save_db_to_disk_filename)
                print("You can run the query directly from the command line using the following command: %s" % command_line, file=sys.stderr)

                # TODO Propagate dump results using a different output class instead of an empty one
                return QOutput()

            # Ensure that adhoc db is not in the middle of a transaction
            self.adhoc_db.conn.commit()

            all_databases = self.query_level_db.get_sqlite_database_list()
            xprint("Query level db: databases %s" % all_databases)

            # Execute the query and fetch the data
            db_results_obj = sql_object.execute_and_fetch(self.query_level_db)
            iprint("Query executed")

            if len(db_results_obj.results) == 0:
                warnings.append(QWarning(None, "Warning - data is empty"))

            return QOutput(
                data = db_results_obj.results,
                metadata = QMetadata(
                    table_structures=self.loaded_table_structures_dict,
                    new_table_structures=new_table_structures,
                    output_column_name_list=db_results_obj.query_column_names),
                warnings = warnings,
                error = error)
        except InvalidQueryException as e:
            error = QError(e,str(e),118)
        except MissingHeaderException as e:
            error = QError(e,e.msg,117)
        except FileNotFoundException as e:
            error = QError(e,e.msg,30)
        except SqliteOperationalErrorException as e:
            xprint("Sqlite Operational error: %s" % e)
            msg = str(e.original_error)
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
        # deprecated, but shouldn't be used:  error = QError(e,"Standard Input must be provided in order to use it as a table",61)
        except CouldNotConvertStringToNumericValueException as e:
            error = QError(e,"Could not convert string to a numeric value. Did you use `-w nonnumeric` with unquoted string values? Error: %s" % e.msg,58)
        except CouldNotParseInputException as e:
            error = QError(e,"Could not parse the input. Please make sure to set the proper -w input-wrapping parameter for your input, and that you use the proper input encoding (-e). Error: %s" % e.msg,59)
        except ColumnMaxLengthLimitExceededException as e:
            error = QError(e,e.msg,31)
        # deprecated, but shouldn't be used: error = QError(e,e.msg,79)
        except ContentSignatureDiffersException as e:
            error = QError(e,"%s vs %s: Content Signatures for table %s differ at %s (source value '%s' disk signature value '%s')" %
                           (e.original_filename,e.other_filename,e.filenames_str,e.key,e.source_value,e.signature_value),80)
        except ContentSignatureDataDiffersException as e:
            error = QError(e,e.msg,81)
        except MaximumSourceFilesExceededException as e:
            error = QError(e,e.msg,82)
        except ContentSignatureNotFoundException as e:
            error = QError(e,e.msg,83)
        except NonExistentTableNameInQsql as e:
            msg = "Table %s could not be found in qsql file %s . Existing table names: %s" % (e.table_name,e.qsql_filename,",".join(e.existing_table_names))
            error = QError(e,msg,84)
        except NonExistentTableNameInSqlite as e:
            msg = "Table %s could not be found in sqlite file %s . Existing table names: %s" % (e.table_name,e.qsql_filename,",".join(e.existing_table_names))
            error = QError(e,msg,85)
        except TooManyTablesInQsqlException as e:
            msg = "Could not autodetect table name in qsql file. Existing Tables %s" % ",".join(e.existing_table_names)
            error = QError(e,msg,86)
        except NoTableInQsqlExcption as e:
            msg = "Could not autodetect table name in qsql file. File contains no record of a table"
            error = QError(e,msg,97)
        except TooManyTablesInSqliteException as e:
            msg = "Could not autodetect table name in sqlite file %s . Existing tables: %s" % (e.qsql_filename,",".join(e.existing_table_names))
            error = QError(e,msg,87)
        except NoTablesInSqliteException as e:
            msg = "sqlite file %s has no tables" % e.sqlite_filename
            error = QError(e,msg,88)
        except TooManyAttachedDatabasesException as e:
            msg = str(e)
            error = QError(e,msg,89)
        except UnknownFileTypeException as e:
            msg = str(e)
            error = QError(e,msg,95)
        except KeyboardInterrupt as e:
            warnings.append(QWarning(e,"Interrupted"))
        except Exception as e:
            global DEBUG
            if DEBUG:
                xprint(traceback.format_exc())
            error = QError(e,repr(e),199)

        return QOutput(data=None,warnings = warnings,error = error , metadata=QMetadata(table_structures=self.loaded_table_structures_dict,new_table_structures=self.loaded_table_structures_dict,output_column_name_list=[]))

    def execute(self,query_str,input_params=None,save_db_to_disk_filename=None):
        r = self._execute(query_str,input_params,stop_after_analysis=False,save_db_to_disk_filename=save_db_to_disk_filename)
        return r

    def unload(self):
        # TODO This would fail, since table structures are just value objects now. Will be fixed as part of making q a full python module
        for qtable_name,table_creator in six.iteritems(self.loaded_table_structures_dict):
            try:
                table_creator.drop_table()
            except:
                # Support no-table select queries
                pass
        self.loaded_table_structures_dict = OrderedDict()

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

        for qtable_name in results.metadata.table_structures:
            table_structures = results.metadata.table_structures[qtable_name]
            print("Table: %s" % qtable_name,file=f_out)
            print("  Sources:",file=f_out)
            dl = results.metadata.new_table_structures[qtable_name]
            print("    source_type: %s source: %s" % (dl.source_type,dl.source),file=f_out)
            print("  Fields:",file=f_out)
            for n,t in zip(table_structures.column_names,table_structures.sqlite_column_types):
                print("    `%s` - %s" % (n,t), file=f_out)

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
    try:
        if not p.has_option('options', option):
            return default
        if p.get('options',option) == 'None':
            return None
        if option_type == 'boolean':
            r = p.getboolean('options', option)
            return r
        elif option_type == 'int':
            r = p.getint('options', option)
            return r
        elif option_type == 'string':
            r = p.get('options', option)
            return r
        else:
            raise Exception("Unknown option type %s " % option_type)
    except ValueError as e:
        raise IncorrectDefaultValueException(option_type,option,p.get("options",option))

QRC_FILENAME_ENVVAR = 'QRC_FILENAME'

def dump_default_values_as_qrc(parser,exclusions):
    m = parser.parse_args([]).__dict__
    m.pop('leftover')
    print("[options]",file=sys.stdout)
    for k in sorted(m.keys()):
        if k not in exclusions:
            print("%s=%s" % (k,m[k]),file=sys.stdout)

USAGE_TEXT = """
	q <flags> <query>

	Example Execution for a delimited file:

		q "select * from myfile.csv"

	Example Execution for an sqlite3 database:

		q "select * from mydatabase.sqlite:::my_table_name"

            or

		q "select * from mydatabase.sqlite"

            if the database file contains only one table

	Auto-caching of delimited files can be activated through `-C readwrite` (writes new caches if needed)  or `-C read` (only reads existing cache files)

	Setting the default caching mode (`-C`) can be done by writing a `~/.qrc` file. See docs for more info.
	
q's purpose is to bring SQL expressive power to the Linux command line and to provide easy access to text as actual data.

q allows the following:

* Performing SQL-like statements directly on tabular text data, auto-caching the data in order to accelerate additional querying on the same file
* Performing SQL statements directly on multi-file sqlite3 databases, without having to merge them or load them into memory

Changing the default values for parameters can be done by creating a `~/.qrc` file. Run q with `--dump-defaults` in order to dump a default `.qrc` file into stdout.

See https://github.com/harelba/q for more details.

"""

def run_standalone():
    sqlite3.enable_callback_tracebacks(True)

    p, qrc_filename = parse_qrc_file()

    args, options, parser = initialize_command_line_parser(p, qrc_filename)

    dump_defaults_and_stop__if_needed(options, parser)

    dump_version_and_stop__if_needed(options)

    STDOUT, default_input_params, q_output_printer, query_strs = parse_options(args, options)

    data_streams_dict = initialize_default_data_streams()

    q_engine = QTextAsData(default_input_params=default_input_params,data_streams_dict=data_streams_dict)

    execute_queries(STDOUT, options, q_engine, q_output_printer, query_strs)

    q_engine.done()

    sys.exit(0)


def dump_version_and_stop__if_needed(options):
    if options.version:
        print_credentials()
        sys.exit(0)


def dump_defaults_and_stop__if_needed(options, parser):
    if options.dump_defaults:
        dump_default_values_as_qrc(parser, ['dump-defaults', 'version'])
        sys.exit(0)


def execute_queries(STDOUT, options, q_engine, q_output_printer, query_strs):
    for query_str in query_strs:
        if options.analyze_only:
            q_output = q_engine.analyze(query_str)
            q_output_printer.print_analysis(STDOUT, sys.stderr, q_output)
        else:
            q_output = q_engine.execute(query_str, save_db_to_disk_filename=options.save_db_to_disk_filename)
            q_output_printer.print_output(STDOUT, sys.stderr, q_output)

        if q_output.status == 'error':
            sys.exit(q_output.error.errorcode)


def initialize_command_line_parser(p, qrc_filename):
    try:
        default_verbose = get_option_with_default(p, 'boolean', 'verbose', False)
        default_save_db_to_disk = get_option_with_default(p, 'string', 'save_db_to_disk_filename', None)
        default_caching_mode = get_option_with_default(p, 'string', 'caching_mode', 'none')

        default_skip_header = get_option_with_default(p, 'boolean', 'skip_header', False)
        default_delimiter = get_option_with_default(p, 'string', 'delimiter', None)
        default_pipe_delimited = get_option_with_default(p, 'boolean', 'pipe_delimited', False)
        default_tab_delimited = get_option_with_default(p, 'boolean', 'tab_delimited', False)
        default_encoding = get_option_with_default(p, 'string', 'encoding', 'UTF-8')
        default_gzipped = get_option_with_default(p, 'boolean', 'gzipped', False)
        default_analyze_only = get_option_with_default(p, 'boolean', 'analyze_only', False)
        default_mode = get_option_with_default(p, 'string', 'mode', "relaxed")
        default_column_count = get_option_with_default(p, 'string', 'column_count', None)
        default_keep_leading_whitespace_in_values = get_option_with_default(p, 'boolean',
                                                                            'keep_leading_whitespace_in_values', False)
        default_disable_double_double_quoting = get_option_with_default(p, 'boolean', 'disable_double_double_quoting',
                                                                        True)
        default_disable_escaped_double_quoting = get_option_with_default(p, 'boolean', 'disable_escaped_double_quoting',
                                                                         True)
        default_disable_column_type_detection = get_option_with_default(p, 'boolean', 'disable_column_type_detection',
                                                                        False)
        default_input_quoting_mode = get_option_with_default(p, 'string', 'input_quoting_mode', 'minimal')
        default_max_column_length_limit = get_option_with_default(p, 'int', 'max_column_length_limit', 131072)
        default_with_universal_newlines = get_option_with_default(p, 'boolean', 'with_universal_newlines', False)

        default_output_delimiter = get_option_with_default(p, 'string', 'output_delimiter', None)
        default_pipe_delimited_output = get_option_with_default(p, 'boolean', 'pipe_delimited_output', False)
        default_tab_delimited_output = get_option_with_default(p, 'boolean', 'tab_delimited_output', False)
        default_output_header = get_option_with_default(p, 'boolean', 'output_header', False)
        default_beautify = get_option_with_default(p, 'boolean', 'beautify', False)
        default_formatting = get_option_with_default(p, 'string', 'formatting', None)
        default_output_encoding = get_option_with_default(p, 'string', 'output_encoding', 'none')
        default_output_quoting_mode = get_option_with_default(p, 'string', 'output_quoting_mode', 'minimal')
        default_list_user_functions = get_option_with_default(p, 'boolean', 'list_user_functions', False)
        default_overwrite_qsql = get_option_with_default(p, 'boolean', 'overwrite_qsql', False)

        default_query_filename = get_option_with_default(p, 'string', 'query_filename', None)
        default_query_encoding = get_option_with_default(p, 'string', 'query_encoding', locale.getpreferredencoding())
        default_max_attached_sqlite_databases = get_option_with_default(p,'int','max_attached_sqlite_databases', 10)
    except IncorrectDefaultValueException as e:
        print("Incorrect value '%s' for option %s in .qrc file %s (option type is %s)" % (
        e.actual_value, e.option, qrc_filename, e.option_type))
        sys.exit(199)
    parser = ArgumentParser(prog="q",usage=USAGE_TEXT)
    parser.add_argument("-v", "--version", action="store_true", help="Print version")
    parser.add_argument("-V", "--verbose", default=default_verbose, action="store_true",
                      help="Print debug info in case of problems")
    parser.add_argument("-S", "--save-db-to-disk", dest="save_db_to_disk_filename", default=default_save_db_to_disk,
                      help="Save database to an sqlite database file")
    parser.add_argument("-C", "--caching-mode", default=default_caching_mode,
                      help="Choose the autocaching mode (none/read/readwrite). Autocaches files to disk db so further queries will be faster. Caching is done to a side-file with the same name of the table, but with an added extension .qsql")
    parser.add_argument("--dump-defaults", action="store_true",
                      help="Dump all default values for parameters and exit. Can be used in order to make sure .qrc file content is being read properly.")
    parser.add_argument("--max-attached-sqlite-databases", default=default_max_attached_sqlite_databases,type=int,
                      help="Set the maximum number of concurrently-attached sqlite dbs. This is a compile time definition of sqlite. q's performance will slow down once this limit is reached for a query, since it will perform table copies in order to avoid that limit.")
    # -----------------------------------------------
    input_data_option_group = parser.add_argument_group("Input Data Options")
    input_data_option_group.add_argument("-H", "--skip-header", default=default_skip_header,
                                       action="store_true",
                                       help="Skip header row. This has been changed from earlier version - Only one header row is supported, and the header row is used for column naming")
    input_data_option_group.add_argument("-d", "--delimiter", default=default_delimiter,
                                       help="Field delimiter. If none specified, then space is used as the delimiter.")
    input_data_option_group.add_argument("-p", "--pipe-delimited", default=default_pipe_delimited,
                                       action="store_true",
                                       help="Same as -d '|'. Added for convenience and readability")
    input_data_option_group.add_argument("-t", "--tab-delimited", default=default_tab_delimited,
                                       action="store_true",
                                       help="Same as -d <tab>. Just a shorthand for handling standard tab delimited file You can use $'\\t' if you want (this is how Linux expects to provide tabs in the command line")
    input_data_option_group.add_argument("-e", "--encoding", default=default_encoding,
                                       help="Input file encoding. Defaults to UTF-8. set to none for not setting any encoding - faster, but at your own risk...")
    input_data_option_group.add_argument("-z", "--gzipped", default=default_gzipped, action="store_true",
                                       help="Data is gzipped. Useful for reading from stdin. For files, .gz means automatic gunzipping")
    input_data_option_group.add_argument("-A", "--analyze-only", default=default_analyze_only,
                                       action='store_true',
                                       help="Analyze sample input and provide information about data types")
    input_data_option_group.add_argument("-m", "--mode", default=default_mode,
                                       help="Data parsing mode. fluffy, relaxed and strict. In strict mode, the -c column-count parameter must be supplied as well")
    input_data_option_group.add_argument("-c", "--column-count", default=default_column_count,
                                       help="Specific column count when using relaxed or strict mode")
    input_data_option_group.add_argument("-k", "--keep-leading-whitespace", dest="keep_leading_whitespace_in_values",
                                       default=default_keep_leading_whitespace_in_values, action="store_true",
                                       help="Keep leading whitespace in values. Default behavior strips leading whitespace off values, in order to provide out-of-the-box usability for simple use cases. If you need to preserve whitespace, use this flag.")
    input_data_option_group.add_argument("--disable-double-double-quoting", 
                                       default=default_disable_double_double_quoting, action="store_false",
                                       help="Disable support for double double-quoting for escaping the double quote character. By default, you can use \"\" inside double quoted fields to escape double quotes. Mainly for backward compatibility.")
    input_data_option_group.add_argument("--disable-escaped-double-quoting", 
                                       default=default_disable_escaped_double_quoting, action="store_false",
                                       help="Disable support for escaped double-quoting for escaping the double quote character. By default, you can use \\\" inside double quoted fields to escape double quotes. Mainly for backward compatibility.")
    input_data_option_group.add_argument("--as-text", dest="disable_column_type_detection",
                                       default=default_disable_column_type_detection, action="store_true",
                                       help="Don't detect column types - All columns will be treated as text columns")
    input_data_option_group.add_argument("-w", "--input-quoting-mode", 
                                       default=default_input_quoting_mode,
                                       help="Input quoting mode. Possible values are all, minimal and none. Note the slightly misleading parameter name, and see the matching -W parameter for output quoting.")
    input_data_option_group.add_argument("-M", "--max-column-length-limit", 
                                       default=default_max_column_length_limit,
                                       help="Sets the maximum column length.")
    input_data_option_group.add_argument("-U", "--with-universal-newlines", 
                                       default=default_with_universal_newlines, action="store_true",
                                       help="Expect universal newlines in the data. Limitation: -U works only with regular files for now, stdin or .gz files are not supported yet.")
    # -----------------------------------------------
    output_data_option_group = parser.add_argument_group("Output Options")
    output_data_option_group.add_argument("-D", "--output-delimiter", 
                                        default=default_output_delimiter,
                                        help="Field delimiter for output. If none specified, then the -d delimiter is used if present, or space if no delimiter is specified")
    output_data_option_group.add_argument("-P", "--pipe-delimited-output", 
                                        default=default_pipe_delimited_output, action="store_true",
                                        help="Same as -D '|'. Added for convenience and readability.")
    output_data_option_group.add_argument("-T", "--tab-delimited-output", 
                                        default=default_tab_delimited_output, action="store_true",
                                        help="Same as -D <tab>. Just a shorthand for outputting tab delimited output. You can use -D $'\\t' if you want.")
    output_data_option_group.add_argument("-O", "--output-header", default=default_output_header,
                                        action="store_true",
                                        help="Output header line. Output column-names are determined from the query itself. Use column aliases in order to set your column names in the query. For example, 'select name FirstName,value1/value2 MyCalculation from ...'. This can be used even if there was no header in the input.")
    output_data_option_group.add_argument("-b", "--beautify", default=default_beautify,
                                        action="store_true",
                                        help="Beautify output according to actual values. Might be slow...")
    output_data_option_group.add_argument("-f", "--formatting", default=default_formatting,
                                        help="Output-level formatting, in the format X=fmt,Y=fmt etc, where X,Y are output column numbers (e.g. 1 for first SELECT column etc.")
    output_data_option_group.add_argument("-E", "--output-encoding", 
                                        default=default_output_encoding,
                                        help="Output encoding. Defaults to 'none', leading to selecting the system/terminal encoding")
    output_data_option_group.add_argument("-W", "--output-quoting-mode", 
                                        default=default_output_quoting_mode,
                                        help="Output quoting mode. Possible values are all, minimal, nonnumeric and none. Note the slightly misleading parameter name, and see the matching -w parameter for input quoting.")
    output_data_option_group.add_argument("-L", "--list-user-functions", 
                                        default=default_list_user_functions, action="store_true",
                                        help="List all user functions")
    parser.add_argument("--overwrite-qsql", default=default_overwrite_qsql,
                      help="When used, qsql files (both caches and store-to-db) will be overwritten if they already exist. Use with care.")
    # -----------------------------------------------
    query_option_group = parser.add_argument_group("Query Related Options")
    query_option_group.add_argument("-q", "--query-filename", default=default_query_filename,
                                  help="Read query from the provided filename instead of the command line, possibly using the provided query encoding (using -Q).")
    query_option_group.add_argument("-Q", "--query-encoding", default=default_query_encoding,
                                  help="query text encoding. Experimental. Please send your feedback on this")
    # -----------------------------------------------
    parser.add_argument('leftover', nargs='*')
    args = parser.parse_args()
    return args.leftover, args, parser


def parse_qrc_file():
    p = configparser.ConfigParser()
    if QRC_FILENAME_ENVVAR in os.environ:
        qrc_filename = os.environ[QRC_FILENAME_ENVVAR]
        if qrc_filename != 'None':
            xprint("qrc filename is %s" % qrc_filename)
            if os.path.exists(qrc_filename):
                p.read([os.environ[QRC_FILENAME_ENVVAR]])
            else:
                print('QRC_FILENAME env var exists, but cannot find qrc file at %s' % qrc_filename, file=sys.stderr)
                sys.exit(244)
        else:
            pass  # special handling of 'None' env var value for QRC_FILENAME. Allows to eliminate the default ~/.qrc reading
    else:
        qrc_filename = os.path.expanduser('~/.qrc')
        p.read([qrc_filename, '.qrc'])
    return p, qrc_filename


def initialize_default_data_streams():
    data_streams_dict = {
        '-': DataStream('stdin', '-', sys.stdin)
    }
    return data_streams_dict


def parse_options(args, options):
    if options.list_user_functions:
        print_user_functions()
        sys.exit(0)
    if len(args) == 0 and options.query_filename is None:
        print_credentials()
        print("Must provide at least one query in the command line, or through a file with the -q parameter",
              file=sys.stderr)
        sys.exit(1)
    if options.query_filename is not None:
        if len(args) != 0:
            print("Can't provide both a query file and a query on the command line", file=sys.stderr)
            sys.exit(1)
        try:
            f = open(options.query_filename, 'rb')
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
                    print("Query cannot be empty (query number %s)" % (idx + 1), file=sys.stderr)
                    sys.exit(1)

        except Exception as e:
            print("Could not decode query number %s using the provided query encoding (%s)" % (
            idx + 1, options.query_encoding), file=sys.stderr)
            sys.exit(3)
    ###
    if options.mode not in ['relaxed', 'strict']:
        print("Parsing mode can either be relaxed or strict", file=sys.stderr)
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
            print("Warning: -t parameter overrides -d parameter (%s)" % options.delimiter, file=sys.stderr)
        options.delimiter = '\t'
    # If the user flagged for a pipe-delimited file then set the delimiter to pipe
    if options.pipe_delimited:
        if options.delimiter is not None and options.delimiter != '|':
            print("Warning: -p parameter overrides -d parameter (%s)" % options.delimiter, file=sys.stderr)
        options.delimiter = '|'
    if options.delimiter is None:
        options.delimiter = ' '
    elif len(options.delimiter) != 1:
        print("Delimiter must be one character only", file=sys.stderr)
        sys.exit(5)
    if options.tab_delimited_output:
        if options.output_delimiter is not None and options.output_delimiter != '\t':
            print("Warning: -T parameter overrides -D parameter (%s)" % options.output_delimiter, file=sys.stderr)
        options.output_delimiter = '\t'
    if options.pipe_delimited_output:
        if options.output_delimiter is not None and options.output_delimiter != '|':
            print("Warning: -P parameter overrides -D parameter (%s)" % options.output_delimiter, file=sys.stderr)
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
    except:
        print("Max column length limit must be an integer larger than 2 (%s)" % options.max_column_length_limit,
              file=sys.stderr)
        sys.exit(31)
    if max_column_length_limit < 3:
        print("Maximum column length must be larger than 2",file=sys.stderr)
        sys.exit(31)

    csv.field_size_limit(max_column_length_limit)
    xprint("Max column length limit is %s" % options.max_column_length_limit)

    if options.input_quoting_mode not in list(QTextAsData.input_quoting_modes.keys()):
        print("Input quoting mode can only be one of %s. It cannot be set to '%s'" % (
        ",".join(sorted(QTextAsData.input_quoting_modes.keys())), options.input_quoting_mode), file=sys.stderr)
        sys.exit(55)
    if options.output_quoting_mode not in list(QOutputPrinter.output_quoting_modes.keys()):
        print("Output quoting mode can only be one of %s. It cannot be set to '%s'" % (
        ",".join(QOutputPrinter.output_quoting_modes.keys()), options.input_quoting_mode), file=sys.stderr)
        sys.exit(56)
    if options.column_count is not None:
        expected_column_count = int(options.column_count)
        if expected_column_count < 1 or expected_column_count > int(options.max_column_length_limit):
            print("Column count must be between 1 and %s" % int(options.max_column_length_limit),file=sys.stderr)
            sys.exit(90)
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
    # sys.exit(78) Deprecated, but shouldn't be reused
    if options.caching_mode not in ['none', 'read', 'readwrite']:
        print("caching mode must be none,read or readwrite",file=sys.stderr)
        sys.exit(85)
    read_caching = options.caching_mode in ['read', 'readwrite']
    write_caching = options.caching_mode in ['readwrite']

    if options.max_attached_sqlite_databases <= 3:
        print("Max attached sqlite databases must be larger than 3")
        sys.exit(99)

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
                                        write_caching=write_caching,
                                        max_attached_sqlite_databases=options.max_attached_sqlite_databases)

    output_params = QOutputParams(
        delimiter=options.output_delimiter,
        beautify=options.beautify,
        output_quoting_mode=options.output_quoting_mode,
        formatting=options.formatting,
        output_header=options.output_header,
        encoding=output_encoding)
    q_output_printer = QOutputPrinter(output_params, show_tracebacks=DEBUG)

    return STDOUT, default_input_params, q_output_printer, query_strs


if __name__ == '__main__':
    run_standalone()
