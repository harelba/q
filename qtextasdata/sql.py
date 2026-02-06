#!/usr/bin/env python

from collections import OrderedDict
import datetime
import glob
import sqlite3
from qtextasdata.csv_reader import encoded_csv_reader
from qtextasdata.exceptions import BadHeaderException, CannotUnzipDataStreamException, ColumnCountMismatchException, ColumnMaxLengthLimitExceededException, ContentSignatureDataDiffersException, ContentSignatureDiffersException, FileNotFoundException, FluffyModeColumnCountMismatchException, InvalidQueryException, MaximumSourceFilesExceededException, MissingHeaderException, NoTableInQsqlExcption, NoTablesInSqliteException, NonExistentTableNameInQsql, NonExistentTableNameInSqlite, StrictModeColumnCountMismatchException, TooManyTablesInQsqlException, TooManyTablesInSqliteException, UniversalNewlinesExistException, UnknownFileTypeException
from qtextasdata.utilities import ( sqlite_type_to_python_type, user_functions, md5 )
from qtextasdata.logging import xprint,sqlprint
import json
import hashlib
from uuid import uuid4
import io
import time
from qtextasdata.exceptions import ( 
    SqliteOperationalErrorException
)
import sys
import os

SHOW_SQL = False

class DatabaseInfo(object):
    def __init__(self,db_id,sqlite_db,needs_closing):
        self.db_id = db_id
        self.sqlite_db = sqlite_db
        self.needs_closing = needs_closing

    def __str__(self):
        return "DatabaseInfo<sqlite_db=%s,needs_closing=%s>" % (self.sqlite_db,self.needs_closing)
    __repr__ = __str__

class Sqlite3DBResults(object):
    def __init__(self,query_column_names,results):
        self.query_column_names = query_column_names
        self.results = results

    def __str__(self):
        return "Sqlite3DBResults<result_count=%d,query_column_names=%s>" % (len(self.results),str(self.query_column_names))
    __repr__ = __str__

class Sqlite3DB(object):
    # TODO Add metadata table with qsql file version

    QCATALOG_TABLE_NAME = '_qcatalog'
    NUMERIC_COLUMN_TYPES =  {int, float}
    PYTHON_TO_SQLITE_TYPE_NAMES = { str: 'TEXT', int: 'INT', float: 'REAL', None: 'TEXT' }


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
        xprint("Calculating content signature for:",pp)
        return hashlib.sha1(pp.encode('utf-8')).hexdigest()

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
        except sqlite3.OperationalError as e:
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
            (n, Sqlite3DB.PYTHON_TO_SQLITE_TYPE_NAMES[t]) for n, t in column_dict.items())
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
        for qtable_name, positions in self.qtable_name_positions.items():
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
            if type(i) == int:
                return int
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
        return ", ".join(["{} rows with {} columns".format(v, k) for k, v in counts.items()])

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


def is_sqlite_file(filename):
    # SQLite database file header is 100 bytes
    if os.path.isfile(filename) and os.access(filename, os.R_OK):
        with open(filename, 'rb') as fd:
            header = fd.read(100)
            return header[:16] == b"SQLite format 3\x00"
    else:
        return False


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


def skip_BOM(f):
    # BOM is a byte order mark that some editors add as the first
    # three bytes in a file to mark it as a UTF8 file.
    pos = f.tell()
    # Not all file objects support peek([size]) and thus since we dont really know
    # if peek is available, we'll handle it in a try/except clause and seek back the original position if things fail
    try:
        BOM = f.peek(3)
        if BOM.startswith(b'\xef\xbb\xbf'):
            f.read(3)
    except:
        f.seek(pos)


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
           res = hashlib.sha1(x.encode('utf-8')).hexdigest() + '///' + x
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
                if self.input_params.with_universal_newlines:
                    f = io.open(filename, 'r', newline=None, encoding=self.input_params.input_encoding)
                else:
                    f = io.open(filename, 'r', newline=None, encoding=self.input_params.input_encoding)

            if self.input_params.input_encoding == 'utf-8-sig' and not self.skipped_bom:
                skip_BOM(f)

            return f

        f = fileinput.input(self.atomic_fns,mode='r',openhook=q_openhook)

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

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

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
        return 'e_%s_fn_%s' % (self.engine_id,hashlib.sha1(filenames_str.encode('utf-8')).hexdigest())

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
        return 'e_%s_fn_%s' % (self.engine_id,hashlib.sha1(filenames_str.encode('utf-8')).hexdigest())

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

