#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#   Copyright (C) 2012-2025 Harel Ben-Attia
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

from argparse import ArgumentParser
import codecs
from configparser import ConfigParser
import csv
import locale
import os
import sqlite3
import sys
from qtextasdata import q_version
from qtextasdata.core import DataStream, QInputParams, QOutputParams, QOutputPrinter, QTextAsData
from qtextasdata.exceptions import IncorrectDefaultValueException
from qtextasdata.logging import DEBUG, xprint
from qtextasdata.utilities import get_stdout_encoding, print_user_functions

def print_credentials():
    print("q version %s" % q_version, file=sys.stderr)
    print("Python: %s" % " // ".join([str(x).strip() for x in sys.version.split("\n")]), file=sys.stderr)
    print("Copyright (C) 2012-2021 Harel Ben-Attia (harelba@gmail.com, @harelba on twitter)", file=sys.stderr)
    print("https://harelba.github.io/q/", file=sys.stderr)
    print(file=sys.stderr)

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

