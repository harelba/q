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

import os
import sys
import sqlite3
import codecs
import locale
import re
import time
import traceback
import csv
import uuid
import math
import datetime
from argparse import ArgumentParser
from configparser import ConfigParser
from collections import OrderedDict
from sqlite3.dbapi2 import OperationalError
from uuid import uuid4

from qtextasdata import q_version
from qtextasdata.sql import MaterializedStateType
from qtextasdata.sql import MaterializedDelimitedFileState
from qtextasdata.sql import detect_qtable_name_source_info
from qtextasdata.sql import MaterialiedDataStreamState
from qtextasdata.sql import MaterializedSqliteState
from qtextasdata.sql import MaterializedQsqlState
from qtextasdata.utilities import (
    get_stdout_encoding, sha_algorithms, sha, sha1, regexp, regexp_extract, md5, sqrt, power,
    file_ext, file_folder, file_basename, file_basename_no_ext, percentile, StrictPercentile,
    StdevPopulation, StdevSample, FunctionType, UserFunctionDef, user_functions, print_user_functions,
    get_sqlite_type_affinity
)
from qtextasdata.logging import xprint,DEBUG,iprint,sqlprint
from qtextasdata.sql import Sqlite3DBResults, Sqlite3DB,DatabaseInfo

from qtextasdata.exceptions import (
    CouldNotConvertStringToNumericValueException,
    SqliteOperationalErrorException,
    IncorrectDefaultValueException,
    ColumnMaxLengthLimitExceededException,
    CouldNotParseInputException,
    MissingHeaderException,
    FileNotFoundException,
    BadHeaderException,
    EncodedQueryException,
    UniversalNewlinesExistException,
    EmptyDataException,
    InvalidQueryException,
    TooManyAttachedDatabasesException,
    ContentSignatureNotFoundException,
    InvalidQSqliteFileException,
    ColumnCountMismatchException,
    FluffyModeColumnCountMismatchException,
    MaximumSourceFilesExceededException,
    NoTableInQsqlExcption,
    NoTablesInSqliteException,
    NonExistentTableNameInQsql,
    NonExistentTableNameInSqlite,
    CannotUnzipDataStreamException,
    ContentSignatureDiffersException,
    ContentSignatureDataDiffersException,
    StrictModeColumnCountMismatchException,
    TooManyTablesInQsqlException,
    TooManyTablesInSqliteException,
    UnknownFileTypeException
)

from qtextasdata.sql import ( Sqlite3DBResults, Sqlite3DB, Sql, TableCreator, MaterializedState, MaterializedStateTableStructure, TableCreatorState, MaterializedStateType, MaterializedDelimitedFileState, MaterialiedDataStreamState, MaterializedSqliteState, MaterializedQsqlState )


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

        if type(query_str) != str:
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
        for qtable_name,table_creator in self.loaded_table_structures_dict.items():
            table_creator.close_file()
        self.loaded_table_structures_dict = OrderedDict()

    def analyze(self,query_str,input_params=None,data_streams=None):
        q_output = self._execute(query_str,input_params,data_streams=data_streams,stop_after_analysis=True)

        return q_output

def escape_double_quotes_if_needed(v):
    x = v.replace('"', '""')
    return x

def quote_none_func(output_delimiter,v):
    return v

def quote_minimal_func(output_delimiter,v):
    if v is None:
        return v
    if isinstance(v, str) and (output_delimiter in v or '\n' in v or '\r' in v):
        return '"{}"'.format(escape_double_quotes_if_needed(v))
    return v

def quote_nonnumeric_func(output_delimiter,v):
    if v is None:
        return v
    if isinstance(v, str):
        return '"{}"'.format(escape_double_quotes_if_needed(v))
    return v

def quote_all_func(output_delimiter,v):
    if isinstance(v, str):
        return '"{}"'.format(escape_double_quotes_if_needed(v))
    else:
        return str('"{}"').format(v)

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
                            fmt_str = str("{{0:<{}}}").format(max_lengths[i])
                        else:
                            fmt_str = str("{}")

                    if col is not None:
                        xx = self.output_field_quoting_func(self.output_params.delimiter,col)
                        row_str.append(fmt_str.format(xx))
                    else:
                        row_str.append(fmt_str.format(""))


                xxxx = str(self.output_params.delimiter).join(row_str) + str("\n")
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
    p = ConfigParser()
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
        STDOUT = codecs.getwriter(output_encoding)(sys.stdout.buffer)
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

