#!/usr/bin/env python3

#
# test suite for q.
# 
# Prefer end-to-end tests, running the actual q command and testing stdout/stderr, and the return code.
# Some utilities are provided for making that easy, see other tests for examples.
#
# Q_EXECUTABLE env var can be used to inject the path of q. This allows full e2e testing of the resulting executable
# instead of just testing the python code.
#
# Tests are compatible with Linux and OSX (path separators, tmp folder, etc.).

from __future__ import print_function

import collections
import functools
import tempfile
import unittest
import random
import json
import uuid
from collections import OrderedDict
from json import JSONEncoder
from subprocess import PIPE, Popen, STDOUT
import sys
import os
import time
from tempfile import NamedTemporaryFile
import locale
import pprint
import six
from six.moves import range
import codecs
import itertools
from gzip import GzipFile
import pytest
import uuid
import sqlite3
import re
import collections

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])),'..','bin'))
from bin.q import QTextAsData, QOutput, QOutputPrinter, QInputParams, DataStream, Sqlite3DB

# q uses this encoding as the default output encoding. Some of the tests use it in order to 
# make sure that the output is correctly encoded
SYSTEM_ENCODING = locale.getpreferredencoding()

EXAMPLES = os.path.abspath(os.path.join(os.getcwd(), 'examples'))

Q_EXECUTABLE = os.getenv('Q_EXECUTABLE', os.path.abspath('./bin/q.py'))
Q_SKIP_EXECUTABLE_VALIDATION = os.getenv('Q_SKIP_EXECUTABLE_VALIDATION','false')

if not Q_SKIP_EXECUTABLE_VALIDATION == 'true':
    Q_EXECUTABLE = os.path.abspath(Q_EXECUTABLE)
    if not os.path.exists(Q_EXECUTABLE):
        raise Exception("q executable must reside in {}".format(Q_EXECUTABLE))
else:
    Q_EXECUTABLE = os.getenv('Q_EXECUTABLE')
    # Skip checking of executable (useful for testing that q is in the path)
    pass

DEBUG = '-v' in sys.argv
if os.environ.get('Q_DEBUG'):
    DEBUG = True

def batch(iterable, n=1):
    r = []
    l = len(iterable)
    for ndx in range(0, l, n):
        r += [iterable[ndx:min(ndx + n, l)]]
    return r

def partition(pred, iterable):
    t1, t2 = itertools.tee(iterable)
    return list(itertools.filterfalse(pred, t1)), list(filter(pred, t2))

def run_command(cmd_to_run,env_to_inject=None):
    global DEBUG
    if DEBUG:
        print("CMD: {}".format(cmd_to_run))

    if env_to_inject is None:
        env_to_inject = os.environ

    env = env_to_inject

    p = Popen(cmd_to_run, stdout=PIPE, stderr=PIPE, shell=True,env=env)
    o, e = p.communicate()
    # remove last newline
    o = o.rstrip()
    e = e.strip()
    # split rows
    if o != six.b(''):
        o = o.split(six.b(os.linesep))
    else:
        o = []
    if e != six.b(''):
        e = e.split(six.b(os.linesep))
    else:
        e = []

    res = (p.returncode, o, e)
    if DEBUG:
        print("RESULT:{}".format(res))
    return res


uneven_ls_output = six.b("""drwxr-xr-x   2 root     root      4096 Jun 11  2012 /selinux
drwxr-xr-x   2 root     root      4096 Apr 19  2013 /mnt
drwxr-xr-x   2 root     root      4096 Apr 24  2013 /srv
drwx------   2 root     root     16384 Jun 21  2013 /lost+found
lrwxrwxrwx   1 root     root        33 Jun 21  2013 /initrd.img.old -> /boot/initrd.img-3.8.0-19-generic
drwxr-xr-x   2 root     root      4096 Jun 21  2013 /cdrom
drwxr-xr-x   3 root     root      4096 Jun 21  2013 /home
lrwxrwxrwx   1 root     root        29 Jun 21  2013 /vmlinuz -> boot/vmlinuz-3.8.0-19-generic
lrwxrwxrwx   1 root     root        32 Jun 21  2013 /initrd.img -> boot/initrd.img-3.8.0-19-generic
""")


find_output = six.b("""8257537   32 drwxrwxrwt 218 root     root        28672 Mar  1 11:00 /tmp
8299123    4 drwxrwxr-x   2 harel    harel        4096 Feb 27 10:06 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/stormdist/testTopology3fad644a-54c0-4def-b19e-77ca97941595-1-1393513576
8263229  964 -rw-rw-r--   1 mapred   mapred      984569 Feb 27 10:06 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/stormdist/testTopology3fad644a-54c0-4def-b19e-77ca97941595-1-1393513576/stormcode.ser
8263230    4 -rw-rw-r--   1 harel    harel        1223 Feb 27 10:06 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/stormdist/testTopology3fad644a-54c0-4def-b19e-77ca97941595-1-1393513576/stormconf.ser
8299113    4 drwxrwxr-x   2 harel    harel        4096 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate
8263406    4 -rw-rw-r--   1 harel    harel        2002 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate/1393514168746
8263476    0 -rw-rw-r--   1 harel    harel           0 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate/1393514168746.version
8263607    0 -rw-rw-r--   1 harel    harel           0 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate/1393514169735.version
8263533    0 -rw-rw-r--   1 harel    harel           0 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate/1393514172733.version
8263604    0 -rw-rw-r--   1 harel    harel           0 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate/1393514175754.version
""")


header_row = six.b('name,value1,value2')
sample_data_rows = [six.b('a,1,0'), six.b('b,2,0'), six.b('c,,0')]
sample_data_rows_with_empty_string = [six.b('a,aaa,0'), six.b('b,bbb,0'), six.b('c,,0')]
sample_data_no_header = six.b("\n").join(sample_data_rows) + six.b("\n")
sample_data_with_empty_string_no_header = six.b("\n").join(
    sample_data_rows_with_empty_string) + six.b("\n")
sample_data_with_header = header_row + six.b("\n") + sample_data_no_header
sample_data_with_missing_header_names = six.b("name,value1\n") + sample_data_no_header

def generate_sample_data_with_header(header):
    return header + six.b("\n") + sample_data_no_header

sample_quoted_data = six.b('''non_quoted regular_double_quoted double_double_quoted escaped_double_quoted multiline_double_double_quoted multiline_escaped_double_quoted
control-value-1 "control-value-2" control-value-3 "control-value-4" control-value-5 "control-value-6"
non-quoted-value "this is a quoted value" "this is a ""double double"" quoted value" "this is an escaped \\"quoted value\\"" "this is a double double quoted ""multiline
  value""." "this is an escaped \\"multiline
  value\\"."
control-value-1 "control-value-2" control-value-3 "control-value-4" control-value-5 "control-value-6"
''')

double_double_quoted_data = six.b('''regular_double_quoted double_double_quoted
"this is a quoted value" "this is a quoted value with ""double double quotes"""
''')

escaped_double_quoted_data = six.b('''regular_double_quoted escaped_double_quoted
"this is a quoted value" "this is a quoted value with \\"escaped double quotes\\""
''')

combined_quoted_data = six.b('''regular_double_quoted double_double_quoted escaped_double_quoted
"this is a quoted value" "this is a quoted value with ""double double quotes""" "this is a quoted value with \\"escaped double quotes\\""
''')

sample_quoted_data2 = six.b('"quoted data" 23\nunquoted-data 54')

sample_quoted_data2_with_newline = six.b('"quoted data with\na new line inside it":23\nunquoted-data:54')

one_column_data = six.b('''data without commas 1
data without commas 2
''')

# Values with leading whitespace
sample_data_rows_with_spaces = [six.b('a,1,0'), six.b('   b,   2,0'), six.b('c,,0')]
sample_data_with_spaces_no_header = six.b("\n").join(
    sample_data_rows_with_spaces) + six.b("\n")

header_row_with_spaces = six.b('name,value 1,value2')
sample_data_with_spaces_with_header = header_row_with_spaces + \
    six.b("\n") + sample_data_with_spaces_no_header

long_value1 = "23683289372328372328373"
int_value = "2328372328373"
sample_data_with_long_values = "%s\n%s\n%s" % (long_value1,int_value,int_value)


def one_column_warning(e):
    return e[0].startswith(six.b('Warning: column count is one'))

def sqlite_dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class AbstractQTestCase(unittest.TestCase):

    def create_file_with_data(self, data, encoding=None,prefix=None,suffix=None,use_real_path=True):
        if encoding is not None:
            raise Exception('Deprecated: Encoding must be none')
        tmpfile = NamedTemporaryFile(delete=False,prefix=prefix,suffix=suffix)
        tmpfile.write(data)
        tmpfile.close()
        if use_real_path:
            tmpfile.name = os.path.realpath(tmpfile.name)
        return tmpfile

    def generate_tmpfile_name(self,prefix=None,suffix=None):
        tmpfile = NamedTemporaryFile(delete=False,prefix=prefix,suffix=suffix)
        os.remove(tmpfile.name)
        return os.path.realpath(tmpfile.name)

    def arrays_to_csv_file_content(self,delimiter,header_row_list,cell_list):
        all_rows = [delimiter.join(row) for row in [header_row_list] + cell_list]
        return six.b("\n").join(all_rows)

    def create_qsql_file_with_content_and_return_filename(self, header_row,cell_list):
        csv_content = self.arrays_to_csv_file_content(six.b(','),header_row,cell_list)
        tmpfile = self.create_file_with_data(csv_content)

        cmd = '%s -d , -H "select count(*) from %s" -C readwrite' % (Q_EXECUTABLE,tmpfile.name)
        r, o, e = run_command(cmd)
        self.assertEqual(r, 0)

        created_qsql_filename = '%s.qsql' % tmpfile.name
        self.assertTrue(os.path.exists(created_qsql_filename))

        return created_qsql_filename

    def arrays_to_qsql_file_content(self, header_row,cell_list):
        csv_content = self.arrays_to_csv_file_content(six.b(','),header_row,cell_list)
        tmpfile = self.create_file_with_data(csv_content)

        cmd = '%s -d , -H "select count(*) from %s" -C readwrite' % (Q_EXECUTABLE,tmpfile.name)
        r, o, e = run_command(cmd)
        self.assertEqual(r, 0)

        matching_qsql_filename = '%s.qsql' % tmpfile.name
        f = open(matching_qsql_filename,'rb')
        qsql_file_bytes = f.read()
        f.close()

        self.assertEqual(matching_qsql_filename,'%s.qsql' % tmpfile.name)

        return qsql_file_bytes

    def write_file(self,filename,data):
        f = open(filename,'wb')
        f.write(data)
        f.close()

    def create_folder_with_files(self,filename_to_content_dict,prefix, suffix):
        name = self.random_tmp_filename(prefix,suffix)
        os.makedirs(name)
        for filename,content in six.iteritems(filename_to_content_dict):
            if os.path.sep in filename:
                os.makedirs('%s/%s' % (name,os.path.split(filename)[0]))
            f = open(os.path.join(name,filename),'wb')
            f.write(content)
            f.close()
        return name

    def cleanup_folder(self,tmpfolder):
        if not tmpfolder.startswith(os.path.realpath('/var/tmp')):
            raise Exception('Guard against accidental folder deletions: %s' % tmpfolder)
        global DEBUG
        if not DEBUG:
            print("should have removed tmpfolder %s. Not doing it for the sake of safety. # TODO re-add" % tmpfolder)
            pass # os.remove(tmpfolder)

    def cleanup(self, tmpfile):
        global DEBUG
        if not DEBUG:
            os.remove(tmpfile.name)

    def random_tmp_filename(self,prefix,postfix):
        # TODO Use more robust method for this
        path = '/var/tmp'
        return os.path.realpath('%s/%s-%s.%s' % (path,prefix,random.randint(0,1000000000),postfix))



def get_sqlite_table_list(c,exclude_qcatalog=True):
    if exclude_qcatalog:
        r = c.execute("select tbl_name from sqlite_master where type='table' and tbl_name != '_qcatalog'").fetchall()
    else:
        r = c.execute("select tbl_name from sqlite_master where type='table'").fetchall()

    return r

class SaveToSqliteTests(AbstractQTestCase):

    # Returns a folder with files and a header in each, one column named 'a'
    def generate_files_in_folder(self,batch_size, file_count):
        numbers = list(range(1, 1 + batch_size * file_count))
        numbers_as_text = batch([str(x) for x in numbers], n=batch_size)

        content_list = list(map(six.b, ['a\n' + "\n".join(x) + '\n' for x in numbers_as_text]))

        filename_list = list(map(lambda x: 'file-%s' % x, range(file_count)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d, 'split-files', 'sqlite-stuff')
        return (tmpfolder,filename_list)

    # 11074  3.8.2021 10:53  bin/q.py "select count(*) from xxxx/file-95 left join xxxx/file-96 left join xxxx/file-97 left join xxxx/file-97 left join xxxx/file-98 left join xxxx/*" -c 1 -C readwrite -A
    # # fails because it takes qsql files as well

    def test_save_glob_files_to_sqlite(self):
        BATCH_SIZE = 50
        FILE_COUNT = 5

        tmpfolder,filename_list = self.generate_files_in_folder(BATCH_SIZE,FILE_COUNT)

        output_sqlite_file = self.random_tmp_filename("x","sqlite")

        cmd = '%s -H "select count(*) from %s/*" -c 1 -S %s' % (Q_EXECUTABLE,tmpfolder,output_sqlite_file)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)

        c = sqlite3.connect(output_sqlite_file)
        results = c.execute('select a from file_dash_0').fetchall()
        self.assertEqual(len(results),BATCH_SIZE*FILE_COUNT)
        self.assertEqual(sum(map(lambda x:x[0],results)),sum(range(1,BATCH_SIZE*FILE_COUNT+1)))
        tables = get_sqlite_table_list(c)
        self.assertEqual(len(tables),1)

        c.close()

        self.cleanup_folder(tmpfolder)

    def test_save_multiple_files_to_sqlite(self):
        BATCH_SIZE = 50
        FILE_COUNT = 5

        tmpfolder,filename_list = self.generate_files_in_folder(BATCH_SIZE,FILE_COUNT)

        output_sqlite_file = self.random_tmp_filename("x","sqlite")

        tables_as_str = " left join ".join(["%s/%s" % (tmpfolder,x) for x in filename_list])
        cmd = '%s -H "select count(*) from %s" -c 1 -S %s' % (Q_EXECUTABLE,tables_as_str,output_sqlite_file)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)

        c = sqlite3.connect(output_sqlite_file)

        tables = get_sqlite_table_list(c)
        self.assertEqual(len(tables), FILE_COUNT)

        for i in range(FILE_COUNT):
            results = c.execute('select a from file_dash_%s' % i).fetchall()
            self.assertEqual(len(results),BATCH_SIZE)
            self.assertEqual(sum(map(lambda x:x[0],results)),sum(range(1+i*BATCH_SIZE,1+(i+1)*BATCH_SIZE)))

        c.close()

        self.cleanup_folder(tmpfolder)

    def test_save_multiple_files_to_sqlite_without_duplicates(self):
        BATCH_SIZE = 50
        FILE_COUNT = 5

        tmpfolder,filename_list = self.generate_files_in_folder(BATCH_SIZE,FILE_COUNT)

        output_sqlite_file = self.random_tmp_filename("x","sqlite")

        tables_as_str = " left join ".join(["%s/%s" % (tmpfolder,x) for x in filename_list])

        # duplicate the left-joins for all the files, so the query will contain each filename twice
        tables_as_str = "%s left join %s" % (tables_as_str,tables_as_str)

        cmd = '%s -H "select count(*) from %s" -c 1 -S %s' % (Q_EXECUTABLE,tables_as_str,output_sqlite_file)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)

        c = sqlite3.connect(output_sqlite_file)

        tables = get_sqlite_table_list(c)
        # total table count should still be FILE_COUNT, even with the duplications
        self.assertEqual(len(tables), FILE_COUNT)

        for i in range(FILE_COUNT):
            results = c.execute('select a from file_dash_%s' % i).fetchall()
            self.assertEqual(len(results),BATCH_SIZE)
            self.assertEqual(sum(map(lambda x:x[0],results)),sum(range(1+i*BATCH_SIZE,1+(i+1)*BATCH_SIZE)))

        c.close()

        self.cleanup_folder(tmpfolder)

    def test_sqlite_file_is_not_created_if_some_table_does_not_exist(self):
        BATCH_SIZE = 50
        FILE_COUNT = 5

        tmpfolder,filename_list = self.generate_files_in_folder(BATCH_SIZE,FILE_COUNT)

        output_sqlite_file = self.random_tmp_filename("x","sqlite")

        tables_as_str = " left join ".join(["%s/%s" % (tmpfolder,x) for x in filename_list])

        tables_as_str = tables_as_str + ' left join %s/non_existent_table' % (tmpfolder)

        cmd = '%s -H "select count(*) from %s" -c 1 -S %s' % (Q_EXECUTABLE,tables_as_str,output_sqlite_file)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 30)
        self.assertEqual(len(e), 2)
        self.assertEqual(e[0],six.b("Going to save data into a disk database: %s" % output_sqlite_file))
        self.assertEqual(e[1],six.b("No files matching '%s/non_existent_table' have been found" % tmpfolder))

        self.assertTrue(not os.path.exists(output_sqlite_file))

        self.cleanup_folder(tmpfolder)

    def test_recurring_glob_and_separate_files_in_same_query_when_writing_to_sqlite(self):
        BATCH_SIZE = 50
        FILE_COUNT = 5

        tmpfolder,filename_list = self.generate_files_in_folder(BATCH_SIZE,FILE_COUNT)

        output_sqlite_file = self.random_tmp_filename("x","sqlite")

        tables_as_str = " left join ".join(["%s/%s" % (tmpfolder,x) for x in filename_list])
        # The same files are left-joined in the query as an additional "left join <folder>/*". This should create an additional table
        # in the sqlite file, with all the data in it
        cmd = '%s -H "select count(*) from %s left join %s/*" -c 1 -S %s' % (Q_EXECUTABLE,tables_as_str,tmpfolder,output_sqlite_file)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)

        c = sqlite3.connect(output_sqlite_file)

        tables = get_sqlite_table_list(c)
        # plus the additional table from the glob
        self.assertEqual(len(tables), FILE_COUNT+1)

        # check all the per-file tables
        for i in range(FILE_COUNT):
            results = c.execute('select a from file_dash_%s' % i).fetchall()
            self.assertEqual(len(results),BATCH_SIZE)
            self.assertEqual(sum(map(lambda x:x[0],results)),sum(range(1+i*BATCH_SIZE,1+(i+1)*BATCH_SIZE)))

        # ensure the glob-based table exists, with an _2 added to the name, as the original "file_dash_0" already exists in the sqlite db
        results = c.execute('select a from file_dash_0_2').fetchall()
        self.assertEqual(len(results),FILE_COUNT*BATCH_SIZE)
        self.assertEqual(sum(map(lambda x:x[0],results)),sum(range(1,1+FILE_COUNT*BATCH_SIZE)))
        c.close()

        self.cleanup_folder(tmpfolder)

    def test_empty_sqlite_handling(self):
        fn = self.generate_tmpfile_name("empty",".sqlite")

        c = sqlite3.connect(fn)
        c.execute('create table x (a int)').fetchall()
        c.execute('drop table x').fetchall()
        c.close()

        cmd = '%s "select * from %s"' % (Q_EXECUTABLE,fn)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,88)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertEqual(e[0],six.b('sqlite file %s has no tables' % fn))

    def test_storing_to_disk_too_many_qsql_files(self):
        BATCH_SIZE = 10
        MAX_ATTACHED_DBS = 5
        FILE_COUNT = MAX_ATTACHED_DBS + 4

        numbers_as_text = batch([str(x) for x in range(1, 1 + BATCH_SIZE * FILE_COUNT)], n=BATCH_SIZE)

        content_list = map(six.b, ["\n".join(x) for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x, range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d, 'split-files', 'attach-limit')

        for fn in filename_list:
            cmd = '%s -c 1 "select count(*) from %s/%s" -C readwrite' % (Q_EXECUTABLE,tmpfolder, fn)
            retcode, o, e = run_command(cmd)

            self.assertEqual(retcode, 0)

        output_sqlite_file = self.generate_tmpfile_name("many-sqlites",".sqlite")

        table_refs = list(['select * from %s/%s.qsql' % (tmpfolder,x) for x in filename_list])
        table_refs_str = " UNION ALL ".join(table_refs)
        # Limit max attached dbs according to the parameter (must be below the hardcoded sqlite limit, which is 10 when having a standard version compiled)
        cmd = '%s "select * from (%s)" -S %s --max-attached-sqlite-databases=%s' % (Q_EXECUTABLE,table_refs_str,output_sqlite_file,MAX_ATTACHED_DBS)
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode,0)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),4)

        c = sqlite3.connect(output_sqlite_file)
        tables_results = c.execute("select tbl_name from sqlite_master where type='table'").fetchall()
        table_names = list(sorted([x[0] for x in tables_results]))
        self.assertEqual(len(table_names),FILE_COUNT)

        for i,tn in enumerate(table_names):
            self.assertEqual(tn,'file_dash_%s' % i)

            table_content = c.execute('select * from %s' % tn).fetchall()
            self.assertEqual(len(table_content),BATCH_SIZE)

            cmd = '%s "select * from %s:::%s"' % (Q_EXECUTABLE,output_sqlite_file,tn)
            retcode, o, e = run_command(cmd)
            self.assertEqual(retcode, 0)
            self.assertEqual(len(e),0)
            self.assertEqual(len(o),BATCH_SIZE)
            self.assertEqual(o,list([six.b(str(x)) for x in range(1 + i*BATCH_SIZE,1+(i+1)*BATCH_SIZE)]))

        self.cleanup_folder(tmpfolder)

    def test_storing_to_disk_too_many_sqlite_files(self):
        # a variation of test_storing_to_disk_too_many_qsql_files, which deletes the qcatalog file from the caches,
        # so they'll be just regular sqlite files

        BATCH_SIZE = 10
        MAX_ATTACHED_DBS = 5
        FILE_COUNT = MAX_ATTACHED_DBS + 4

        numbers_as_text = batch([str(x) for x in range(1, 1 + BATCH_SIZE * FILE_COUNT)], n=BATCH_SIZE)

        content_list = map(six.b, ["\n".join(x) for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x, range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d, 'split-files', 'attach-limit')

        for fn in filename_list:
            cmd = '%s -c 1 "select count(*) from %s/%s" -C readwrite' % (Q_EXECUTABLE,tmpfolder, fn)
            retcode, o, e = run_command(cmd)

            self.assertEqual(retcode, 0)

            c = sqlite3.connect('%s/%s.qsql' % (tmpfolder,fn))
            c.execute('drop table _qcatalog').fetchall()
            c.close()
            os.rename('%s/%s.qsql' % (tmpfolder,fn),'%s/%s.sqlite' % (tmpfolder,fn))

        output_sqlite_file = self.generate_tmpfile_name("many-sqlites",".sqlite")

        table_refs = list(['select * from %s/%s.sqlite' % (tmpfolder,x) for x in filename_list])
        table_refs_str = " UNION ALL ".join(table_refs)
        # Limit max attached dbs according to the parameter (must be below the hardcoded sqlite limit, which is 10 when having a standard version compiled)
        cmd = '%s "select * from (%s)" -S %s --max-attached-sqlite-databases=%s' % (Q_EXECUTABLE,table_refs_str,output_sqlite_file,MAX_ATTACHED_DBS)
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode,0)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),4)

        c = sqlite3.connect(output_sqlite_file)
        tables_results = c.execute("select tbl_name from sqlite_master where type='table'").fetchall()
        table_names = list(sorted([x[0] for x in tables_results]))
        self.assertEqual(len(table_names),FILE_COUNT)

        for i,tn in enumerate(table_names):
            self.assertEqual(tn,'file_dash_%s' % i)

            table_content = c.execute('select * from %s' % tn).fetchall()
            self.assertEqual(len(table_content),BATCH_SIZE)

            cmd = '%s "select * from %s:::%s"' % (Q_EXECUTABLE,output_sqlite_file,tn)
            retcode, o, e = run_command(cmd)
            self.assertEqual(retcode, 0)
            self.assertEqual(len(e),0)
            self.assertEqual(len(o),BATCH_SIZE)
            self.assertEqual(o,list([six.b(str(x)) for x in range(1 + i*BATCH_SIZE,1+(i+1)*BATCH_SIZE)]))

        self.cleanup_folder(tmpfolder)

    def test_storing_to_disk_too_many_sqlite_files__over_the_sqlite_limit(self):
        # a variation of test_storing_to_disk_too_many_sqlite_files, but with a limit above the sqlite hardcoded limit
        MAX_ATTACHED_DBS = 20 # standard sqlite limit is 10, so q should throw an error

        BATCH_SIZE = 10
        FILE_COUNT = MAX_ATTACHED_DBS + 4

        numbers_as_text = batch([str(x) for x in range(1, 1 + BATCH_SIZE * FILE_COUNT)], n=BATCH_SIZE)

        content_list = map(six.b, ["\n".join(x) for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x, range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d, 'split-files', 'attach-limit')

        for fn in filename_list:
            cmd = '%s -c 1 "select count(*) from %s/%s" -C readwrite' % (Q_EXECUTABLE,tmpfolder, fn)
            retcode, o, e = run_command(cmd)

            self.assertEqual(retcode, 0)

            c = sqlite3.connect('%s/%s.qsql' % (tmpfolder,fn))
            c.execute('drop table _qcatalog').fetchall()
            c.close()
            os.rename('%s/%s.qsql' % (tmpfolder,fn),'%s/%s.sqlite' % (tmpfolder,fn))

        output_sqlite_file = self.generate_tmpfile_name("many-sqlites",".sqlite")

        table_refs = list(['select * from %s/%s.sqlite' % (tmpfolder,x) for x in filename_list])
        table_refs_str = " UNION ALL ".join(table_refs)
        # Limit max attached dbs according to the parameter (must be below the hardcoded sqlite limit, which is 10 when having a standard version compiled)
        cmd = '%s "select * from (%s)" -S %s --max-attached-sqlite-databases=%s' % (Q_EXECUTABLE,table_refs_str,output_sqlite_file,MAX_ATTACHED_DBS)
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode,89)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),2)
        self.assertTrue(e[0].startswith(six.b('Going to save data into')))
        self.assertTrue(e[1].startswith(six.b('There are too many attached databases. Use a proper --max-attached-sqlite-databases parameter which is below the maximum')))

        self.cleanup_folder(tmpfolder)

    def test_qtable_name_normalization__starting_with_a_digit(self):
        numbers = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 101)]

        header = [six.b('aa'), six.b('bb'), six.b('cc')]

        base_filename_with_digits = '010'

        new_tmp_folder = self.create_folder_with_files({
            base_filename_with_digits : self.arrays_to_csv_file_content(six.b(','),header,numbers)
        },prefix='xx',suffix='digits')

        effective_filename = '%s/010' % new_tmp_folder

        output_sqlite_filename = self.generate_tmpfile_name("starting-with-digit",".sqlite")
        cmd = '%s -d , -H "select count(aa),count(bb),count(cc) from %s" -S %s' % (Q_EXECUTABLE,effective_filename,output_sqlite_filename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),4)

        c = sqlite3.connect(output_sqlite_filename)
        results = c.execute('select aa,bb,cc from t_%s' % base_filename_with_digits).fetchall()
        self.assertEqual(results,list([(x,x,x) for x in range(1,101)]))
        c.close()

        self.cleanup_folder(new_tmp_folder)

    def test_qtable_name_normalization(self):
        x = [six.b(a) for a in map(str, range(1, 101))]
        large_file_data = six.b("val\n") + six.b("\n").join(x)
        tmpfile = self.create_file_with_data(large_file_data)

        tmpfile_folder = os.path.dirname(tmpfile.name)
        tmpfile_basename = os.path.basename(tmpfile.name)

        cmd = 'cd %s && %s -c 1 -H -D , -O "select a.val,b.val from %s a cross join ./%s b on (a.val = b.val * 2)"' % (tmpfile_folder,Q_EXECUTABLE,tmpfile_basename,tmpfile_basename)
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 51)

        evens = list(filter(lambda x: x%2 == 0,range(1,101)))
        expected_result_rows = [six.b('val,val')] + [six.b('%d,%d' % (x,x / 2)) for x in evens]
        self.assertEqual(o,expected_result_rows)

    def test_qtable_name_normalization2(self):
        cmd = '%s "select * from"' % Q_EXECUTABLE

        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 118)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0],six.b('FROM/JOIN is missing a table name after it'))

    def test_qtable_name_normalization3(self):
        # with a space after the from
        cmd = '%s "select * from "' % Q_EXECUTABLE

        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 118)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0],six.b('FROM/JOIN is missing a table name after it'))

    def test_save_multiple_files_to_sqlite_while_caching_them(self):
        BATCH_SIZE = 50
        FILE_COUNT = 5

        tmpfolder,filename_list = self.generate_files_in_folder(BATCH_SIZE,FILE_COUNT)

        output_sqlite_file = self.random_tmp_filename("x","sqlite")

        tables_as_str = " left join ".join(["%s/%s" % (tmpfolder,x) for x in filename_list])
        cmd = '%s -H "select count(*) from %s" -c 1 -S %s -C readwrite' % (Q_EXECUTABLE,tables_as_str,output_sqlite_file)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)

        c = sqlite3.connect(output_sqlite_file)

        tables = get_sqlite_table_list(c)
        self.assertEqual(len(tables), FILE_COUNT)

        for i,filename in enumerate(filename_list):
            matching_table_name = 'file_dash_%s' % i

            results = c.execute('select a from %s' % matching_table_name).fetchall()
            self.assertEqual(len(results),BATCH_SIZE)
            self.assertEqual(sum(map(lambda x:x[0],results)),sum(range(1+i*BATCH_SIZE,1+(i+1)*BATCH_SIZE)))

            # check actual resulting qsql file for the file
            cmd = '%s -c 1 -H "select a from %s/%s"' % (Q_EXECUTABLE,tmpfolder,filename)
            retcode, o, e = run_command(cmd)

            self.assertEqual(retcode, 0)
            self.assertEqual(len(o), BATCH_SIZE)
            self.assertEqual(sum(map(int,o)),sum(range(1+i*BATCH_SIZE,1+(i+1)*BATCH_SIZE)))
            self.assertEqual(len(e), 0)

            # check analysis returns proper file-with-unused-qsql for each file, since by default `-C none` which means don't read the cache
            # even if it exists
            cmd = '%s -c 1 -H "select a from %s/%s" -A' % (Q_EXECUTABLE,tmpfolder,filename)
            retcode, o, e = run_command(cmd)

            self.assertEqual(retcode, 0)
            self.assertEqual(len(o), 5)
            self.assertEqual(o,[
                six.b('Table: %s/file-%s' % (tmpfolder,i)),
                six.b('  Sources:'),
                six.b('    source_type: file-with-unused-qsql source: %s/file-%s' % (tmpfolder,i)),
                six.b('  Fields:'),
                six.b('    `a` - int')
            ])

            cmd = '%s -c 1 -H "select a from %s/%s" -A -C read' % (Q_EXECUTABLE,tmpfolder,filename)
            retcode, o, e = run_command(cmd)

            self.assertEqual(retcode, 0)
            self.assertEqual(len(o), 5)
            self.assertEqual(o,[
                six.b('Table: %s/file-%s' % (tmpfolder,i)),
                six.b('  Sources:'),
                six.b('    source_type: qsql-file-with-original source: %s/file-%s.qsql' % (tmpfolder,i)),
                six.b('  Fields:'),
                six.b('    `a` - int')
            ])

            # check qsql file is readable directly through q
            cmd = '%s -c 1 -H "select a from %s/%s.qsql"' % (Q_EXECUTABLE,tmpfolder,filename)
            retcode, o, e = run_command(cmd)

            self.assertEqual(retcode, 0)
            self.assertEqual(len(o), BATCH_SIZE)
            self.assertEqual(sum(map(int,o)),sum(range(1+i*BATCH_SIZE,1+(i+1)*BATCH_SIZE)))
            self.assertEqual(len(e), 0)

            # check analysis returns proper qsql-with-original for each file when running directly against the qsql file
            cmd = '%s -c 1 -H "select a from %s/%s.qsql" -A' % (Q_EXECUTABLE,tmpfolder,filename)
            retcode, o, e = run_command(cmd)

            self.assertEqual(retcode, 0)
            self.assertEqual(len(o), 5)
            self.assertEqual(o,[
                six.b('Table: %s/file-%s.qsql' % (tmpfolder,i)),
                six.b('  Sources:'),
                six.b('    source_type: qsql-file source: %s/file-%s.qsql' % (tmpfolder,i)),
                six.b('  Fields:'),
                six.b('    `a` - int')
            ])
        c.close()

        import glob
        filename_list_with_qsql = list(map(lambda x: x+'.qsql',filename_list))

        files_in_folder = glob.glob('%s/*' % tmpfolder)
        regular_files,qsql_files = partition(lambda x: x.endswith('.qsql'),files_in_folder)

        self.assertEqual(len(files_in_folder),2*FILE_COUNT)
        self.assertEqual(sorted(list(map(os.path.basename,regular_files))),sorted(list(map(os.path.basename,filename_list))))
        self.assertEqual(sorted(list(map(os.path.basename,qsql_files))),sorted(list(map(os.path.basename,filename_list_with_qsql))))

        self.cleanup_folder(tmpfolder)

    def test_globs_ignore_matching_qsql_files(self):
        BATCH_SIZE = 10
        FILE_COUNT = 5

        tmpfolder,filename_list = self.generate_files_in_folder(BATCH_SIZE,FILE_COUNT)

        tables_as_str = " left join ".join(["%s/%s" % (tmpfolder,x) for x in filename_list])
        cmd = '%s -H "select count(*) from %s" -c 1 -C readwrite' % (Q_EXECUTABLE,tables_as_str)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b(str(pow(BATCH_SIZE,FILE_COUNT))))

        cmd = '%s -H "select a from %s/*" -c 1 -C read' % (Q_EXECUTABLE,tmpfolder)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), BATCH_SIZE*FILE_COUNT)
        self.assertEqual(len(e), 0)
        self.assertEqual(sum(map(int,o)),sum(range(1,1+BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_error_on_reading_from_multi_table_sqlite_without_explicit_table_name(self):
        BATCH_SIZE = 50
        FILE_COUNT = 5

        tmpfolder,filename_list = self.generate_files_in_folder(BATCH_SIZE,FILE_COUNT)

        output_sqlite_file = self.random_tmp_filename("x","sqlite")

        tables_as_str = " left join ".join(["%s/%s" % (tmpfolder,x) for x in filename_list])
        cmd = '%s -H "select count(*) from %s" -c 1 -S %s' % (Q_EXECUTABLE,tables_as_str,output_sqlite_file)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)


        cmd = '%s -H "select count(*) from %s"' % (Q_EXECUTABLE,output_sqlite_file)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 87)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0],six.b("Could not autodetect table name in sqlite file %s . Existing tables: file_dash_0,file_dash_1,file_dash_2,file_dash_3,file_dash_4" % output_sqlite_file))

        self.cleanup_folder(tmpfolder)

    def test_error_on_trying_to_specify_an_explicit_non_existent_qsql_file(self):
        cmd = '%s -H "select count(*) from /non-existent-folder/non-existent.qsql:::mytable"' % (Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 30)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0],six.b("Could not find file /non-existent-folder/non-existent.qsql"))

    def test_error_on_providing_a_non_qsql_file_when_specifying_an_explicit_table(self):
        data = six.b("\x1f\x8b\x08\x00\tZ\x0ea\x00\x03\xed\x93\xdd\n\xc20\x0cF\xf3(}\x01ij\x93\xf6y:\xd9P\x10)\xb3\xbe\xbf\x9d\x1d\xbbQ\xc6\x06F\x10rn\xbe\x9b\xd0\xfc\x1c\x9a-\x88\x83\x88\x91\xd9\xbc2\xb4\xc4#\xb5\x9c1\x8e\x1czb\x8a\xd1\x19t\xdeS\x00\xc3\xf2\xa3\x01<\xee%\x8du\x94s\x1a\xfbk\xd7\xdf\x0e\xa9\x94Kz\xaf\xabe\xc3\xb0\xf2\xce\xbc\xc7\x92\x7fB\xb6\x1fv\xfd2\xf5\x1e\x81h\xa3\xff\x10'\xff\x8c\x04\x06\xc5'\x03\xf5oO\xe2=v\xf9o\xff\x9f\xd1\xa9\xff_\x90m'\xdec\x9f\x7f\x9c\xfc\xd7T\xff\x8a\xa2(\x92<\x01WY\x0c\x06\x00\x0c\x00\x00")
        tmpfilename = self.random_tmp_filename('xx','yy')
        f = open(tmpfilename,'wb')
        f.write(data)
        f.close()

        cmd = '%s -H "select count(*) from %s:::mytable1"' % (Q_EXECUTABLE,tmpfilename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 95)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0],six.b("Cannot detect the type of table %s:::mytable1" % tmpfilename))

    def test_error_on_providing_a_non_qsql_file_when_not_specifying_an_explicit_table(self):
        data = six.b("\x1f\x8b\x08\x00\tZ\x0ea\x00\x03\xed\x93\xdd\n\xc20\x0cF\xf3(}\x01ij\x93\xf6y:\xd9P\x10)\xb3\xbe\xbf\x9d\x1d\xbbQ\xc6\x06F\x10rn\xbe\x9b\xd0\xfc\x1c\x9a-\x88\x83\x88\x91\xd9\xbc2\xb4\xc4#\xb5\x9c1\x8e\x1czb\x8a\xd1\x19t\xdeS\x00\xc3\xf2\xa3\x01<\xee%\x8du\x94s\x1a\xfbk\xd7\xdf\x0e\xa9\x94Kz\xaf\xabe\xc3\xb0\xf2\xce\xbc\xc7\x92\x7fB\xb6\x1fv\xfd2\xf5\x1e\x81h\xa3\xff\x10'\xff\x8c\x04\x06\xc5'\x03\xf5oO\xe2=v\xf9o\xff\x9f\xd1\xa9\xff_\x90m'\xdec\x9f\x7f\x9c\xfc\xd7T\xff\x8a\xa2(\x92<\x01WY\x0c\x06\x00\x0c\x00\x00")
        tmpfilename = self.random_tmp_filename('xx','yy')
        f = open(tmpfilename,'wb')
        f.write(data)
        f.close()

        cmd = '%s -H "select count(*) from %s"' % (Q_EXECUTABLE,tmpfilename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 59)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertTrue(e[0].startswith(six.b("Could not parse the input. Please make sure to set the proper -w input-wrapping parameter for your input, and that you use the proper input encoding (-e). Error:")))

class OldSaveDbToDiskTests(AbstractQTestCase):

    def test_join_with_stdin_and_save(self):
        x = [six.b(a) for a in map(str,range(1,101))]
        large_file_data = six.b("val\n") + six.b("\n").join(x)
        tmpfile = self.create_file_with_data(large_file_data)
        tmpfile_expected_table_name = os.path.basename(tmpfile.name)

        disk_db_filename = self.random_tmp_filename('save-to-db','sqlite')

        cmd = '(echo id ; seq 1 2 10) | ' + Q_EXECUTABLE + ' -c 1 -H -O "select stdin.*,f.* from - stdin left join %s f on (stdin.id * 10 = f.val)" -S %s' % \
            (tmpfile.name,disk_db_filename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)

        self.assertEqual(e[0],six.b('Going to save data into a disk database: %s' % disk_db_filename))
        self.assertTrue(e[1].startswith(six.b('Data has been saved into %s . Saving has taken ' % disk_db_filename)))
        self.assertEqual(e[2],six.b('Query to run on the database: select stdin.*,f.* from data_stream_stdin stdin left join %s f on (stdin.id * 10 = f.val);' % \
                         tmpfile_expected_table_name))
        self.assertEqual(e[3],six.b('You can run the query directly from the command line using the following command: echo "select stdin.*,f.* from data_stream_stdin stdin left join %s f on (stdin.id * 10 = f.val)" | sqlite3 %s' %
                                    (tmpfile_expected_table_name,disk_db_filename)))

        P = re.compile(six.b("^Query to run on the database: (?P<query_to_run_on_db>.*)$"))
        m = P.search(e[2])
        query_to_run_on_db = m.groupdict()['query_to_run_on_db']

        self.assertTrue(os.path.exists(disk_db_filename))

        # validate disk db content natively
        c = sqlite3.connect(disk_db_filename)
        c.row_factory = sqlite_dict_factory
        t0_results = c.execute('select * from data_stream_stdin').fetchall()
        self.assertEqual(len(t0_results),5)
        self.assertEqual(sorted(list(t0_results[0].keys())), ['id'])
        self.assertEqual(list(map(lambda x:x['id'],t0_results)),[1,3,5,7,9])
        t1_results = c.execute('select * from %s' % tmpfile_expected_table_name).fetchall()
        self.assertEqual(len(t1_results),100)
        self.assertEqual(sorted(list(t1_results[0].keys())), ['val'])
        self.assertEqual("\n".join(list(map(lambda x:str(x['val']),t1_results))),"\n".join(map(str,range(1,101))))

        query_results = c.execute(query_to_run_on_db.decode('utf-8')).fetchall()

        self.assertEqual(query_results[0],{ 'id': 1 , 'val': 10})
        self.assertEqual(query_results[1],{ 'id': 3 , 'val': 30})
        self.assertEqual(query_results[2],{ 'id': 5 , 'val': 50})
        self.assertEqual(query_results[3],{ 'id': 7 , 'val': 70})
        self.assertEqual(query_results[4],{ 'id': 9 , 'val': 90})

        self.cleanup(tmpfile)

    def test_join_with_qsql_file(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]

        header = [six.b('aa'), six.b('bb'), six.b('cc')]

        new_tmp_folder = self.create_folder_with_files({
            'some_csv_file': self.arrays_to_csv_file_content(six.b(','),header,numbers1),
            'some_qsql_database.qsql' : self.arrays_to_qsql_file_content(header,numbers2)
        },prefix='xx',suffix='yy')

        effective_filename1 = '%s/some_csv_file' % new_tmp_folder
        effective_filename2 = '%s/some_qsql_database.qsql' % new_tmp_folder

        cmd = Q_EXECUTABLE + ' -d , -H "select sum(large_file.aa),sum(small_file.aa) from %s large_file left join %s small_file on (small_file.aa == large_file.bb)"' % \
              (effective_filename1,effective_filename2)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('50005000,55'))

    # TODO RLRL Check if needed anymore

    # def test_creation_of_qsql_database(self):
    #     numbers = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]
    #     header = [six.b('aa'), six.b('bb'), six.b('cc')]
    #
    #     qsql_filename = self.create_qsql_file_with_content_and_return_filename(header,numbers)
    #
    #     conn = sqlite3.connect(qsql_filename)
    #     qcatalog = conn.execute('select temp_table_name,source_type,source from _qcatalog').fetchall()
    #     print(qcatalog)
    #
    #     cmd = '%s "select count(*) from %s" -A' % (Q_EXECUTABLE,qsql_filename)
    #     retcode, o, e = run_command(cmd)
    #     print(o)

    def test_join_with_qsql_file_and_save(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]

        header = [six.b('aa'), six.b('bb'), six.b('cc')]

        saved_qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        new_tmp_folder = self.create_folder_with_files({
            'some_csv_file': self.arrays_to_csv_file_content(six.b(','),header,numbers1),
            'some_qsql_database' : self.arrays_to_csv_file_content(six.b(','),header,numbers2)
        },prefix='xx',suffix='yy')
        cmd = '%s -d , -H "select count(*) from %s/some_qsql_database" -C readwrite' % (Q_EXECUTABLE,new_tmp_folder)
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode,0)
        os.remove('%s/some_qsql_database' % new_tmp_folder)

        effective_filename1 = '%s/some_csv_file' % new_tmp_folder
        effective_filename2 = '%s/some_qsql_database.qsql' % new_tmp_folder

        cmd = Q_EXECUTABLE + ' -d , -H "select sum(large_file.aa),sum(small_file.aa) from %s large_file left join %s small_file on (small_file.aa == large_file.bb)" -S %s' % \
              (effective_filename1,effective_filename2,saved_qsql_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)

        conn = sqlite3.connect(saved_qsql_with_multiple_tables)
        c1 = conn.execute('select count(*) from some_csv_file').fetchall()
        c2 = conn.execute('select count(*) from some_qsql_database').fetchall()

        self.assertEqual(c1[0][0],10000)
        self.assertEqual(c2[0][0],10)


    def test_saving_to_db_with_same_basename_files(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]

        header = [six.b('aa'), six.b('bb'), six.b('cc')]

        qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        new_tmp_folder = self.create_folder_with_files({
            'filename1': self.arrays_to_csv_file_content(six.b(','),header,numbers1),
            'otherfolder/filename1' : self.arrays_to_csv_file_content(six.b(','),header,numbers2)
        },prefix='xx',suffix='yy')

        effective_filename1 = '%s/filename1' % new_tmp_folder
        effective_filename2 = '%s/otherfolder/filename1' % new_tmp_folder

        expected_stored_table_name1 = 'filename1'
        expected_stored_table_name2 = 'filename1_2'

        cmd = Q_EXECUTABLE + ' -d , -H "select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s small_file left join %s large_file on (large_file.aa == small_file.bb)" -S %s' % \
              (effective_filename1,effective_filename2,qsql_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)
        self.assertEqual(e[0], six.b('Going to save data into a disk database: %s' % qsql_with_multiple_tables))
        self.assertTrue(e[1].startswith(six.b('Data has been saved into %s . Saving has taken' % qsql_with_multiple_tables)))
        self.assertEqual(e[2],six.b('Query to run on the database: select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s small_file left join %s large_file on (large_file.aa == small_file.bb);' % \
                                    (expected_stored_table_name1,expected_stored_table_name2)))
        self.assertEqual(e[3],six.b('You can run the query directly from the command line using the following command: echo "select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s small_file left join %s large_file on (large_file.aa == small_file.bb)" | sqlite3 %s' % \
                                    (expected_stored_table_name1,expected_stored_table_name2,qsql_with_multiple_tables)))

        #self.assertTrue(False) # pxpx - need to actually test reading from the saved db file
        conn = sqlite3.connect(qsql_with_multiple_tables)
        c1 = conn.execute('select count(*) from filename1').fetchall()
        c2 = conn.execute('select count(*) from filename1_2').fetchall()

        self.assertEqual(c1[0][0],10000)
        self.assertEqual(c2[0][0],10)


    def test_error_when_not_specifying_table_name_in_multi_table_qsql(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]

        header = [six.b('aa'), six.b('bb'), six.b('cc')]

        qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        new_tmp_folder = self.create_folder_with_files({
            'filename1': self.arrays_to_csv_file_content(six.b(','),header,numbers1),
            'otherfolder/filename1' : self.arrays_to_csv_file_content(six.b(','),header,numbers2)
        },prefix='xx',suffix='yy')

        effective_filename1 = '%s/filename1' % new_tmp_folder
        effective_filename2 = '%s/otherfolder/filename1' % new_tmp_folder

        expected_stored_table_name1 = 'filename1'
        expected_stored_table_name2 = 'filename1_2'

        cmd = Q_EXECUTABLE + ' -d , -H "select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s small_file left join %s large_file on (large_file.aa == small_file.bb)" -S %s' % \
              (effective_filename1,effective_filename2,qsql_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)

        # Actual tests

        cmd = '%s "select count(*) from %s"' % (Q_EXECUTABLE,qsql_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 87)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertEqual(e[0],six.b('Could not autodetect table name in sqlite file %s . Existing tables: %s,%s' % (qsql_with_multiple_tables,expected_stored_table_name1,expected_stored_table_name2)))

    def test_error_when_not_specifying_table_name_in_multi_table_sqlite(self):
        sqlite_with_multiple_tables = self.generate_tmpfile_name(suffix='.sqlite')

        c = sqlite3.connect(sqlite_with_multiple_tables)
        c.execute('create table my_table_1 (x int, y int)').fetchall()
        c.execute('create table my_table_2 (x int, y int)').fetchall()
        c.close()

        cmd = '%s "select count(*) from %s"' % (Q_EXECUTABLE,sqlite_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 87)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        print(e[0])
        self.assertEqual(e[0],six.b('Could not autodetect table name in sqlite file %s . Existing tables: my_table_1,my_table_2' % sqlite_with_multiple_tables))

    def test_querying_from_multi_table_sqlite_using_explicit_table_name(self):
        sqlite_with_multiple_tables = self.generate_tmpfile_name(suffix='.sqlite')

        c = sqlite3.connect(sqlite_with_multiple_tables)
        c.execute('create table my_table_1 (x int, y int)').fetchall()
        c.execute('insert into my_table_1 (x,y) values (100,200),(300,400)').fetchall()
        c.execute('commit').fetchall()
        c.execute('create table my_table_2 (x int, y int)').fetchall()
        c.close()

        cmd = '%s -d , "select * from %s:::my_table_1"' % (Q_EXECUTABLE,sqlite_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('100,200'))
        self.assertEqual(o[1],six.b('300,400'))

        # Check again, this time with a different output delimiter and with explicit column names
        cmd = '%s -t "select x,y from %s:::my_table_1"' % (Q_EXECUTABLE,sqlite_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('100\t200'))
        self.assertEqual(o[1],six.b('300\t400'))


    def test_error_when_specifying_nonexistent_table_name_in_multi_table_qsql(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]

        header = [six.b('aa'), six.b('bb'), six.b('cc')]

        qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        new_tmp_folder = self.create_folder_with_files({
            'filename1': self.arrays_to_csv_file_content(six.b(','),header,numbers1),
            'otherfolder/filename1' : self.arrays_to_csv_file_content(six.b(','),header,numbers2)
        },prefix='xx',suffix='yy')

        effective_filename1 = '%s/filename1' % new_tmp_folder
        effective_filename2 = '%s/otherfolder/filename1' % new_tmp_folder

        expected_stored_table_name1 = 'filename1'
        expected_stored_table_name2 = 'filename1_2'

        cmd = Q_EXECUTABLE + ' -d , -H "select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s small_file left join %s large_file on (large_file.aa == small_file.bb)" -S %s' % \
              (effective_filename1,effective_filename2,qsql_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)

        # Actual tests

        cmd = '%s "select count(*) from %s:::non_existent_table"' % (Q_EXECUTABLE,qsql_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 85)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertEqual(e[0],six.b('Table non_existent_table could not be found in sqlite file %s . Existing table names: %s,%s' % \
                                    (qsql_with_multiple_tables,expected_stored_table_name1,expected_stored_table_name2)))

    def test_querying_multi_table_qsql_file(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]

        header = [six.b('aa'), six.b('bb'), six.b('cc')]

        qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        new_tmp_folder = self.create_folder_with_files({
            'filename1': self.arrays_to_csv_file_content(six.b(','),header,numbers1),
            'otherfolder/filename1' : self.arrays_to_csv_file_content(six.b(','),header,numbers2)
        },prefix='xx',suffix='yy')

        effective_filename1 = '%s/filename1' % new_tmp_folder
        effective_filename2 = '%s/otherfolder/filename1' % new_tmp_folder

        expected_stored_table_name1 = 'filename1'
        expected_stored_table_name2 = 'filename1_2'

        cmd = Q_EXECUTABLE + ' -d , -H "select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s small_file left join %s large_file on (large_file.aa == small_file.bb)" -S %s' % \
              (effective_filename1,effective_filename2,qsql_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)

        # Actual tests

        cmd = '%s "select count(*) from %s:::%s"' % (Q_EXECUTABLE,qsql_with_multiple_tables,expected_stored_table_name1)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('10000'))

        cmd = '%s "select count(*) from %s:::%s"' % (Q_EXECUTABLE,qsql_with_multiple_tables,expected_stored_table_name2)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('10'))

    def test_preventing_db_overwrite(self):
        db_filename = self.random_tmp_filename('store-to-disk', 'db')
        self.assertFalse(os.path.exists(db_filename))

        retcode, o, e = run_command('seq 1 1000 | ' + Q_EXECUTABLE + ' "select count(*) from -" -c 1 -S %s' % db_filename)

        self.assertTrue(retcode == 0)
        self.assertTrue(os.path.exists(db_filename))

        retcode2, o2, e2 = run_command('seq 1 1000 | ' + Q_EXECUTABLE + ' "select count(*) from -" -c 1 -S %s' % db_filename)
        self.assertTrue(retcode2 != 0)
        self.assertTrue(e2[0].startswith(six.b('Going to save data into a disk database')))
        self.assertTrue(e2[1] == six.b('Disk database file {} already exists.'.format(db_filename)))

        os.remove(db_filename)


class BasicTests(AbstractQTestCase):

    def test_basic_aggregation(self):
        retcode, o, e = run_command(
            'seq 1 10 | ' + Q_EXECUTABLE + ' "select sum(c1),avg(c1) from -"')
        self.assertTrue(retcode == 0)
        self.assertTrue(len(o) == 1)
        self.assertTrue(len(e) == 0)

        s = sum(range(1, 11))
        self.assertTrue(o[0] == six.b('%s %s' % (s, s / 10.0)))

    def test_select_one_column(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(six.b(" ").join(o), six.b('a b c'))

        self.cleanup(tmpfile)

    def test_column_separation(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0])
        self.assertEqual(o[1], sample_data_rows[1])
        self.assertEqual(o[2], sample_data_rows[2])

        self.cleanup(tmpfile)

    def test_header_exception_on_numeric_header_data(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select * from %s" -A -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 3)
        self.assertTrue(
            six.b('Bad header row: Header must contain only strings') in e[0])
        self.assertTrue(six.b("Column name must be a string") in e[1])
        self.assertTrue(six.b("Column name must be a string") in e[2])

        self.cleanup(tmpfile)

    def test_different_header_in_second_file(self):
        folder_name = self.create_folder_with_files({
            'file1': self.arrays_to_csv_file_content(six.b(','),[six.b('a'),six.b('b')],[[six.b(str(x)),six.b(str(x))] for x in range(1,6)]),
            'file2': self.arrays_to_csv_file_content(six.b(','),[six.b('c'),six.b('d')],[[six.b(str(x)),six.b(str(x))] for x in range(1,6)])
        },prefix="xx",suffix="aa")

        cmd = Q_EXECUTABLE + ' -d , "select * from %s/*" -H' % (folder_name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 35)
        self.assertEqual(len(e),1)
        self.assertEqual(e[0],six.b("Bad header row: Extra header 'c,d' in file '%s/file2' mismatches original header 'a,b' from file '%s/file1'. Table name is '%s/*'" % (folder_name,folder_name,folder_name)))

    def test_data_with_header(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = Q_EXECUTABLE + ' -d , "select name from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(six.b(" ").join(o), six.b("a b c"))

        self.cleanup(tmpfile)

    def test_output_header_when_input_header_exists(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = Q_EXECUTABLE + ' -d , "select name from %s" -H -O' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 4)
        self.assertEqual(o[0],six.b('name'))
        self.assertEqual(o[1],six.b('a'))
        self.assertEqual(o[2],six.b('b'))
        self.assertEqual(o[3],six.b('c'))

        self.cleanup(tmpfile)

    def test_generated_column_name_warning_when_header_line_exists(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = Q_EXECUTABLE + ' -d , "select c3 from %s" -H' % tmpfile.name

        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 2)
        self.assertTrue(six.b('no such column: c3') in e[0])
        self.assertTrue(
            e[1].startswith(six.b('Warning - There seems to be a "no such column" error, and -H (header line) exists. Please make sure that you are using the column names from the header line and not the default (cXX) column names')))

        self.cleanup(tmpfile)

    def test_empty_data(self):
        tmpfile = self.create_file_with_data(six.b(''))
        cmd = Q_EXECUTABLE + ' -d , "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(six.b('Warning - data is empty') in e[0])

        self.cleanup(tmpfile)

    def test_empty_data_with_header_param(self):
        tmpfile = self.create_file_with_data(six.b(''))
        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        m = six.b("Header line is expected but missing in file %s" % tmpfile.name)
        self.assertTrue(m in e[0])

        self.cleanup(tmpfile)

    def test_one_row_of_data_without_header_param(self):
        tmpfile = self.create_file_with_data(header_row)
        cmd = Q_EXECUTABLE + ' -d , "select c2 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('value1'))

        self.cleanup(tmpfile)

    def test_one_row_of_data_with_header_param(self):
        tmpfile = self.create_file_with_data(header_row)
        cmd = Q_EXECUTABLE + ' -d , "select name from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(six.b('Warning - data is empty') in e[0])

        self.cleanup(tmpfile)

    def test_dont_leading_keep_whitespace_in_values(self):
        tmpfile = self.create_file_with_data(sample_data_with_spaces_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0], six.b('a'))
        self.assertEqual(o[1], six.b('b'))
        self.assertEqual(o[2], six.b('c'))

        self.cleanup(tmpfile)

    def test_keep_leading_whitespace_in_values(self):
        tmpfile = self.create_file_with_data(sample_data_with_spaces_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s" -k' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0], six.b('a'))
        self.assertEqual(o[1], six.b('   b'))
        self.assertEqual(o[2], six.b('c'))

        self.cleanup(tmpfile)

    def test_no_impact_of_keeping_leading_whitespace_on_integers(self):
        tmpfile = self.create_file_with_data(sample_data_with_spaces_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select c2 from %s" -k -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        f = open("/var/tmp/XXX","wb")
        f.write(six.b("\n").join(o))
        f.write(six.b("STDERR:"))
        f.write(six.b("\n").join(e))
        f.close()

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 7)


        self.assertEqual(o[0], six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1], six.b('  Sources:'))
        self.assertEqual(o[2], six.b('    source_type: file source: %s') % six.b(tmpfile.name))
        self.assertEqual(o[3], six.b('  Fields:'))
        self.assertEqual(o[4], six.b('    `c1` - text'))
        self.assertEqual(o[5], six.b('    `c2` - int'))
        self.assertEqual(o[6], six.b('    `c3` - int'))


        self.cleanup(tmpfile)

    def test_spaces_in_header_row(self):
        tmpfile = self.create_file_with_data(
            header_row_with_spaces + six.b("\n") + sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select name,\\`value 1\\` from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0], six.b('a,1'))
        self.assertEqual(o[1], six.b('b,2'))
        self.assertEqual(o[2], six.b('c,'))

        self.cleanup(tmpfile)

    def test_no_query_in_command_line(self):
        cmd = Q_EXECUTABLE + ' -d , ""'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertEqual(e[0],six.b('Query cannot be empty (query number 1)'))

    def test_empty_query_in_command_line(self):
        cmd = Q_EXECUTABLE + ' -d , "  "'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertEqual(e[0],six.b('Query cannot be empty (query number 1)'))

    def test_failure_in_query_stops_processing_queries(self):
        cmd = Q_EXECUTABLE + ' -d , "select 500" "select 300" "wrong-query" "select 8000"'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 2)
        self.assertEqual(o[0],six.b('500'))
        self.assertEqual(o[1],six.b('300'))

    def test_multiple_queries_in_command_line(self):
        cmd = Q_EXECUTABLE + ' -d , "select 500" "select 300+100" "select 300" "select 200"'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 4)

        self.assertEqual(o[0],six.b('500'))
        self.assertEqual(o[1],six.b('400'))
        self.assertEqual(o[2],six.b('300'))
        self.assertEqual(o[3],six.b('200'))

    def test_literal_calculation_query(self):
        cmd = Q_EXECUTABLE + ' -d , "select 1+40/6"'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 1)

        self.assertEqual(o[0],six.b('7'))

    def test_literal_calculation_query_float_result(self):
        cmd = Q_EXECUTABLE + ' -d , "select 1+40/6.0"'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 1)

        self.assertEqual(o[0],six.b('7.666666666666667'))

    def test_use_query_file(self):
        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select name from %s" % tmp_data_file.name))

        cmd = Q_EXECUTABLE + ' -d , -q %s -H' % tmp_query_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0], six.b('a'))
        self.assertEqual(o[1], six.b('b'))
        self.assertEqual(o[2], six.b('c'))

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)

    def test_use_query_file_with_incorrect_query_encoding(self):
        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select name,'Hr\xc3\xa1\xc4\x8d' from %s" % tmp_data_file.name),encoding=None)

        cmd = Q_EXECUTABLE + ' -d , -q %s -H -Q ascii' % tmp_query_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,3)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)

        self.assertTrue(e[0].startswith(six.b('Could not decode query number 1 using the provided query encoding (ascii)')))

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)

    def test_output_header_with_non_ascii_names(self):
        OUTPUT_ENCODING = 'utf-8'

        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select name,'Hr\xc3\xa1\xc4\x8d' Hr\xc3\xa1\xc4\x8d from %s" % tmp_data_file.name),encoding=None)

        cmd = Q_EXECUTABLE + ' -d , -q %s -H -Q utf-8 -O -E %s' % (tmp_query_file.name,OUTPUT_ENCODING)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),4)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0].decode(OUTPUT_ENCODING), u'name,Hr\xe1\u010d')
        self.assertEqual(o[1].decode(OUTPUT_ENCODING), u'a,Hr\xe1\u010d')
        self.assertEqual(o[2].decode(OUTPUT_ENCODING), u'b,Hr\xe1\u010d')
        self.assertEqual(o[3].decode(OUTPUT_ENCODING), u'c,Hr\xe1\u010d')

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)

    def test_use_query_file_with_query_encoding(self):
        OUTPUT_ENCODING = 'utf-8'

        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select name,'Hr\xc3\xa1\xc4\x8d' from %s" % tmp_data_file.name),encoding=None)

        cmd = Q_EXECUTABLE + ' -d , -q %s -H -Q utf-8 -E %s' % (tmp_query_file.name,OUTPUT_ENCODING)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0].decode(OUTPUT_ENCODING), u'a,Hr\xe1\u010d')
        self.assertEqual(o[1].decode(OUTPUT_ENCODING), u'b,Hr\xe1\u010d')
        self.assertEqual(o[2].decode(OUTPUT_ENCODING), u'c,Hr\xe1\u010d')

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)

    def test_use_query_file_and_command_line(self):
        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select name from %s" % tmp_data_file.name))

        cmd = Q_EXECUTABLE + ' -d , -q %s -H "select * from ppp"' % tmp_query_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertTrue(e[0].startswith(six.b("Can't provide both a query file and a query on the command line")))

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)

    def test_select_output_encoding(self):
        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select 'Hr\xc3\xa1\xc4\x8d' from %s" % tmp_data_file.name),encoding=None)

        for target_encoding in ['utf-8','ibm852']:
            cmd = Q_EXECUTABLE + ' -d , -q %s -H -Q utf-8 -E %s' % (tmp_query_file.name,target_encoding)
            retcode, o, e = run_command(cmd)

            self.assertEqual(retcode, 0)
            self.assertEqual(len(e), 0)
            self.assertEqual(len(o), 3)

            self.assertEqual(o[0].decode(target_encoding), u'Hr\xe1\u010d')
            self.assertEqual(o[1].decode(target_encoding), u'Hr\xe1\u010d')
            self.assertEqual(o[2].decode(target_encoding), u'Hr\xe1\u010d')

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)

    def test_select_failed_output_encoding(self):
        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select 'Hr\xc3\xa1\xc4\x8d' from %s" % tmp_data_file.name),encoding=None)

        cmd = Q_EXECUTABLE + ' -d , -q %s -H -Q utf-8 -E ascii' % tmp_query_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 3)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertTrue(e[0].startswith(six.b('Cannot encode data')))

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)


    def test_use_query_file_with_empty_query(self):
        tmp_query_file = self.create_file_with_data(six.b("   "))

        cmd = Q_EXECUTABLE + ' -d , -q %s -H' % tmp_query_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertTrue(e[0].startswith(six.b("Query cannot be empty")))

        self.cleanup(tmp_query_file)

    def test_use_non_existent_query_file(self):
        cmd = Q_EXECUTABLE + ' -d , -q non-existent-query-file -H'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertTrue(e[0].startswith(six.b("Could not read query from file")))

    def test_nonexistent_file(self):
        cmd = Q_EXECUTABLE + ' "select * from non-existent-file"'

        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)

        self.assertEqual(e[0],six.b("No files matching '%s/non-existent-file' have been found" % os.getcwd()))

    def test_default_column_max_length_parameter__short_enough(self):
        huge_text = six.b("x" * 131000)

        file_data = six.b("a,b,c\n1,{},3\n".format(huge_text))

        tmpfile = self.create_file_with_data(file_data)

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b('1'))

        self.cleanup(tmpfile)

    def test_default_column_max_length_parameter__too_long(self):
        huge_text = six.b("x") * 132000

        file_data = six.b("a,b,c\n1,{},3\n".format(huge_text))

        tmpfile = self.create_file_with_data(file_data)

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 31)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(e[0].startswith(six.b("Column length is larger than the maximum")))
        self.assertTrue(six.b("Offending file is '{}'".format(tmpfile.name)) in e[0])
        self.assertTrue(six.b('Line is 2') in e[0])

        self.cleanup(tmpfile)

    def test_column_max_length_parameter(self):
        file_data = six.b("a,b,c\nvery-long-text,2,3\n")
        tmpfile = self.create_file_with_data(file_data)

        cmd = Q_EXECUTABLE + ' -H -d , -M 3 "select a from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 31)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(e[0].startswith(six.b("Column length is larger than the maximum")))
        self.assertTrue((six.b("Offending file is '%s'" % tmpfile.name)) in e[0])
        self.assertTrue(six.b('Line is 2') in e[0])

        cmd2 = Q_EXECUTABLE + ' -H -d , -M 300 -H "select a from %s"' % tmpfile.name
        retcode2, o2, e2 = run_command(cmd2)

        self.assertEqual(retcode2, 0)
        self.assertEqual(len(o2), 1)
        self.assertEqual(len(e2), 0)

        self.assertEqual(o2[0],six.b('very-long-text'))

        self.cleanup(tmpfile)

    def test_invalid_column_max_length_parameter(self):
        file_data = six.b("a,b,c\nvery-long-text,2,3\n")
        tmpfile = self.create_file_with_data(file_data)

        cmd = Q_EXECUTABLE + ' -H -d , -M xx "select a from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 31)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertEqual(e[0],six.b('Max column length limit must be an integer larger than 2 (xx)'))

        self.cleanup(tmpfile)

    def test_duplicate_column_name_detection(self):
        file_data = six.b("a,b,a\n10,20,30\n30,40,50")
        tmpfile = self.create_file_with_data(file_data)

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 35)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 2)

        self.assertTrue(e[0].startswith(six.b('Bad header row:')))
        self.assertEqual(e[1],six.b("'a': Column name is duplicated"))

        self.cleanup(tmpfile)

    def test_join_with_stdin(self):
        x = [six.b(a) for a in map(str,range(1,101))]
        large_file_data = six.b("val\n") + six.b("\n").join(x)
        tmpfile = self.create_file_with_data(large_file_data)

        cmd = '(echo id ; seq 1 2 10) | %s -c 1 -H -O "select stdin.*,f.* from - stdin left join %s f on (stdin.id * 10 = f.val)"' % (Q_EXECUTABLE,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 6)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b('id val'))
        self.assertEqual(o[1],six.b('1 10'))
        self.assertEqual(o[2],six.b('3 30'))
        self.assertEqual(o[3],six.b('5 50'))
        self.assertEqual(o[4],six.b('7 70'))
        self.assertEqual(o[5],six.b('9 90'))

        self.cleanup(tmpfile)

    def test_concatenated_files(self):
        file_data1 = six.b("a,b,c\n10,11,12\n20,21,22")
        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        file_data2 = six.b("a,b,c\n30,31,32\n40,41,42")
        tmpfile2 = self.create_file_with_data(file_data2)
        tmpfile2_folder = os.path.dirname(tmpfile2.name)
        tmpfile2_filename = os.path.basename(tmpfile2.name)
        expected_cache_filename2 = os.path.join(tmpfile2_folder,tmpfile2_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -O -H -d , "select * from %s UNION ALL select * from %s" -C none' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 5)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('a,b,c'))
        self.assertEqual(o[1],six.b('10,11,12'))
        self.assertEqual(o[2],six.b('20,21,22'))
        self.assertEqual(o[3],six.b('30,31,32'))
        self.assertEqual(o[4],six.b('40,41,42'))

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_out_of_range_expected_column_count(self):
        cmd = '%s "select count(*) from some_table" -c -1' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 90)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0], six.b('Column count must be between 1 and 131072'))

    def test_out_of_range_expected_column_count__with_explicit_limit(self):
        cmd = '%s "select count(*) from some_table" -c -1 -M 100' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 90)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0], six.b('Column count must be between 1 and 100'))

    def test_other_out_of_range_expected_column_count__with_explicit_limit(self):
        cmd = '%s "select count(*) from some_table" -c 101 -M 100' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 90)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0], six.b('Column count must be between 1 and 100'))

    def test_explicit_limit_of_columns__data_is_ok(self):
        file_data1 = six.b("191\n192\n")
        tmpfile1 = self.create_file_with_data(file_data1)

        cmd = '%s "select count(*) from %s" -c 1 -M 3' % (Q_EXECUTABLE,tmpfile1.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0], six.b('2'))

        self.cleanup(tmpfile1)

class ManyOpenFilesTests(AbstractQTestCase):


    def test_multi_file_header_skipping(self):
        BATCH_SIZE = 50
        FILE_COUNT = 5

        numbers = list(range(1,1+BATCH_SIZE*FILE_COUNT))
        numbers_as_text = batch([str(x) for x in numbers],n=BATCH_SIZE)

        content_list = list(map(six.b,['a\n' + "\n".join(x)+'\n' for x in numbers_as_text]))

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','multi-header')

        cmd = '%s -d , -H -c 1 "select count(a),sum(a) from %s/*" -C none' % (Q_EXECUTABLE,tmpfolder)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b("%s,%s" % (BATCH_SIZE*FILE_COUNT,sum(numbers))))

        self.cleanup_folder(tmpfolder)

    def test_that_globs_dont_max_out_sqlite_attached_database_limits(self):
        BATCH_SIZE = 50
        FILE_COUNT = 40

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(six.b,["\n".join(x)+'\n' for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','attach-limit')
        #expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        cmd = 'cd %s && %s -c 1 "select count(*) from *" -C none --max-attached-sqlite-databases=10' % (tmpfolder,Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b(str(BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_maxing_out_max_attached_database_limits__regular_files(self):
        BATCH_SIZE = 50
        FILE_COUNT = 40

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(six.b,["\n".join(x)+'\n' for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','attach-limit')
        #expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        unioned_subquery = " UNION ALL ".join(["select * from %s/%s" % (tmpfolder,filename) for filename in filename_list])
        cmd = 'cd %s && %s -c 1 "select count(*) from (%s)" -C none --max-attached-sqlite-databases=10' % (tmpfolder,Q_EXECUTABLE,unioned_subquery)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b(str(BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_maxing_out_max_attached_database_limits__with_qsql_files_below_attached_limit(self):
        MAX_ATTACHED_SQLITE_DATABASES = 10

        BATCH_SIZE = 50
        FILE_COUNT = MAX_ATTACHED_SQLITE_DATABASES - 1

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(six.b,["\n".join(x)+'\n' for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','attach-limit')
        #expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        # Execute the query with -C readwrite, so all qsql files will be created
        unioned_subquery = " UNION ALL ".join(["select * from %s/%s" % (tmpfolder,filename) for filename in filename_list])
        cmd = 'cd %s && %s -c 1 "select count(*) from (%s)" -C readwrite --max-attached-sqlite-databases=%s' % (tmpfolder,Q_EXECUTABLE,unioned_subquery,MAX_ATTACHED_SQLITE_DATABASES)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b(str(BATCH_SIZE*FILE_COUNT)))

        # Now execute the same query with -C readwrite, so all files will be read directly from the qsql files
        cmd = 'cd %s && %s -c 1 "select count(*) from (%s)" -C readwrite' % (tmpfolder,Q_EXECUTABLE,unioned_subquery)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b(str(BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_maxing_out_max_attached_database_limits__with_qsql_files_above_attached_limit(self):
        MAX_ATTACHED_SQLITE_DATABASES = 10

        BATCH_SIZE = 50
        # Here's the difference from test_maxing_out_max_attached_database_limits__with_qsql_files_below_attached_limit
        # We're trying to cache 2 times the number of files than the number of databases that can be attached.
        # Expectation is that only a part of the files will be cached
        FILE_COUNT = MAX_ATTACHED_SQLITE_DATABASES * 2

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(six.b,["\n".join(x)+'\n' for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','attach-limit')
        #expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        # Execute the query with -C readwrite, so all qsql files will be created
        unioned_subquery = " UNION ALL ".join(["select * from %s/%s" % (tmpfolder,filename) for filename in filename_list])
        cmd = 'cd %s && %s -c 1 "select count(*) from (%s)" -C readwrite --max-attached-sqlite-databases=%s' % (tmpfolder,Q_EXECUTABLE,unioned_subquery,MAX_ATTACHED_SQLITE_DATABASES)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b(str(BATCH_SIZE*FILE_COUNT)))

        # Now execute the same query with -C readwrite, so all files will be read directly from the qsql files
        cmd = 'cd %s && %s -c 1 "select count(*) from (%s)" -C readwrite' % (tmpfolder,Q_EXECUTABLE,unioned_subquery)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b(str(BATCH_SIZE*FILE_COUNT)))

        from glob import glob
        files_in_folder = [os.path.basename(x) for x in glob('%s/*' % (tmpfolder))]

        expected_files_in_folder = filename_list + list(map(lambda x: 'file-%s.qsql' % x,range(MAX_ATTACHED_SQLITE_DATABASES-2)))

        self.assertEqual(sorted(files_in_folder),sorted(expected_files_in_folder))

        self.cleanup_folder(tmpfolder)

    def test_maxing_out_max_attached_database_limits__with_directly_using_qsql_files(self):
        MAX_ATTACHED_SQLITE_DATABASES = 10

        BATCH_SIZE = 50
        FILE_COUNT = MAX_ATTACHED_SQLITE_DATABASES * 2

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(six.b,["\n".join(x)+'\n' for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','attach-limit')
        #expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        # Prepare qsql for each of the files (separately, just for simplicity)
        for fn in filename_list:
            cmd = 'cd %s && %s -c 1 "select count(*) from %s" -C readwrite' % (tmpfolder,Q_EXECUTABLE,fn)
            retcode, o, e = run_command(cmd)

            self.assertEqual(retcode, 0)
            self.assertEqual(len(o), 1)
            self.assertEqual(len(e), 0)

        # Now execute a big query which uses the created qsql files
        unioned_subquery = " UNION ALL ".join(["select * from %s/%s.qsql" % (tmpfolder,filename) for filename in filename_list])

        cmd = 'cd %s && %s -c 1 "select count(*) from (%s)" -C readwrite' % (tmpfolder,Q_EXECUTABLE,unioned_subquery)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b(str(BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_too_many_open_files_for_one_table(self):
        # Previously file opening was parallel, causing too-many-open-files

        MAX_ALLOWED_FILES = 500

        BATCH_SIZE = 2
        FILE_COUNT = MAX_ALLOWED_FILES + 1

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(six.b,["\n".join(x) for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','attach-limit')

        cmd = 'cd %s && %s -c 1 "select count(*) from * where 1 = 1 or c1 != 2" -C none' % (tmpfolder,Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 82)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        x = six.b('Maximum source files for table must be %s. Table is name is %s/* Number of actual files is %s' % (MAX_ALLOWED_FILES,os.path.realpath(tmpfolder),FILE_COUNT))
        print(x)
        self.assertEqual(e[0],x)

        self.cleanup_folder(tmpfolder)

    def test_many_open_files_for_one_table(self):
        # Previously file opening was parallel, causing too-many-open-files

        BATCH_SIZE = 2
        FILE_COUNT = 500

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(six.b,["\n".join(x) for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','attach-limit')
        #expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        cmd = 'cd %s && %s -c 1 "select count(*) from * where 1 = 1 or c1 != 2" -C none' % (tmpfolder,Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b(str(BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_many_open_files_for_two_tables(self):
        BATCH_SIZE = 2
        FILE_COUNT = 500

        numbers_as_text = batch([str(x) for x in range(1, 1 + BATCH_SIZE * FILE_COUNT)], n=BATCH_SIZE)

        content_list = map(six.b, ["\n".join(x) for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x, range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder1 = self.create_folder_with_files(d, 'split-files1', 'blah')
        tmpfolder2 = self.create_folder_with_files(d, 'split-files1', 'blah')

        cmd = '%s -c 1 "select count(*) from %s/* a left join %s/* b on (a.c1 = b.c1)" -C none' % (
            Q_EXECUTABLE,
            tmpfolder1,
            tmpfolder2)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b(str(BATCH_SIZE * FILE_COUNT)))

        self.cleanup_folder(tmpfolder1)
        self.cleanup_folder(tmpfolder2)


class GzippingTests(AbstractQTestCase):

    def test_gzipped_file(self):
        tmpfile = self.create_file_with_data(
            six.b('\x1f\x8b\x08\x08\xf2\x18\x12S\x00\x03xxxxxx\x003\xe42\xe22\xe62\xe12\xe52\xe32\xe7\xb2\xe0\xb2\xe424\xe0\x02\x00\xeb\xbf\x8a\x13\x15\x00\x00\x00'))

        cmd = Q_EXECUTABLE + ' -z "select sum(c1),avg(c1) from %s"' % tmpfile.name

        retcode, o, e = run_command(cmd)
        self.assertTrue(retcode == 0)
        self.assertTrue(len(o) == 1)
        self.assertTrue(len(e) == 0)

        s = sum(range(1, 11))
        self.assertTrue(o[0] == six.b('%s %s' % (s, s / 10.0)))

        self.cleanup(tmpfile)


class DelimiterTests(AbstractQTestCase):

    def test_delimitation_mistake_with_header(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d " " "select * from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 2)

        self.assertTrue(e[0].startswith(six.b("Bad header row")))
        self.assertTrue(six.b("Column name cannot contain commas") in e[1])

        self.cleanup(tmpfile)

    def test_tab_delimitation_parameter(self):
        tmpfile = self.create_file_with_data(
            sample_data_no_header.replace(six.b(","), six.b("\t")))
        cmd = Q_EXECUTABLE + ' -t "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("\t")))

        self.cleanup(tmpfile)

    def test_pipe_delimitation_parameter(self):
        tmpfile = self.create_file_with_data(
            sample_data_no_header.replace(six.b(","), six.b("|")))
        cmd = Q_EXECUTABLE + ' -p "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("|")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("|")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("|")))

        self.cleanup(tmpfile)

    def test_tab_delimitation_parameter__with_manual_override_attempt(self):
        tmpfile = self.create_file_with_data(
            sample_data_no_header.replace(six.b(","), six.b("\t")))
        cmd = Q_EXECUTABLE + ' -t -d , "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 1)
        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("\t")))
        self.assertEqual(e[0],six.b('Warning: -t parameter overrides -d parameter (,)'))

        self.cleanup(tmpfile)

    def test_pipe_delimitation_parameter__with_manual_override_attempt(self):
        tmpfile = self.create_file_with_data(
            sample_data_no_header.replace(six.b(","), six.b("|")))
        cmd = Q_EXECUTABLE + ' -p -d , "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 1)
        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("|")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("|")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("|")))
        self.assertEqual(e[0],six.b('Warning: -p parameter overrides -d parameter (,)'))

        self.cleanup(tmpfile)

    def test_output_delimiter(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -D "|" "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("|")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("|")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("|")))

        self.cleanup(tmpfile)

    def test_output_delimiter_tab_parameter(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -T "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("\t")))

        self.cleanup(tmpfile)

    def test_output_delimiter_pipe_parameter(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -P "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("|")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("|")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("|")))

        self.cleanup(tmpfile)

    def test_output_delimiter_tab_parameter__with_manual_override_attempt(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -T -D "|" "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 1)

        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("\t")))
        self.assertEqual(e[0], six.b('Warning: -T parameter overrides -D parameter (|)'))

        self.cleanup(tmpfile)

    def test_output_delimiter_pipe_parameter__with_manual_override_attempt(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -P -D ":" "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 1)

        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("|")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("|")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("|")))
        self.assertEqual(e[0],six.b('Warning: -P parameter overrides -D parameter (:)'))

        self.cleanup(tmpfile)


class AnalysisTests(AbstractQTestCase):

    def test_analyze_result(self):
        d = "\n".join(['%s\t%s\t%s' % (x+1,x+1,x+1) for x in range(100)])
        tmpfile = self.create_file_with_data(six.b(d))

        cmd = Q_EXECUTABLE + ' -c 1 "select count(*) from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 5)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1], six.b('  Sources:'))
        self.assertEqual(o[2], six.b('    source_type: file source: %s' %(tmpfile.name)))
        self.assertEqual(o[3], six.b('  Fields:'))
        self.assertEqual(o[4], six.b('    `c1` - text'))

        self.cleanup(tmpfile)

    def test_analyze_result_with_data_stream(self):
        d = "\n".join(['%s\t%s\t%s' % (x+1,x+1,x+1) for x in range(100)])
        tmpfile = self.create_file_with_data(six.b(d))

        cmd = 'cat %s | %s  -c 1 "select count(*) from -" -A' % (tmpfile.name,Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 5)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('Table: -'))
        self.assertEqual(o[1], six.b('  Sources:'))
        self.assertEqual(o[2], six.b('    source_type: data-stream source: stdin'))
        self.assertEqual(o[3], six.b('  Fields:'))
        self.assertEqual(o[4], six.b('    `c1` - text'))

        self.cleanup(tmpfile)

    def test_column_analysis(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(o[0], six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4], six.b('    `c1` - text'))
        self.assertEqual(o[5], six.b('    `c2` - int'))
        self.assertEqual(o[6], six.b('    `c3` - int'))

        self.cleanup(tmpfile)

    def test_column_analysis_with_mixed_ints_and_floats(self):
        tmpfile = self.create_file_with_data(six.b("""planet_id,name,diameter_km,length_of_day_hours\n1000,Earth,12756,24\n2000,Mars,6792,24.7\n3000,Jupiter,142984,9.9"""))

        cmd = Q_EXECUTABLE + ' -d , -H "select * from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),8)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0], six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4], six.b('    `planet_id` - int'))
        self.assertEqual(o[5], six.b('    `name` - text'))
        self.assertEqual(o[6], six.b('    `diameter_km` - int'))
        self.assertEqual(o[7], six.b('    `length_of_day_hours` - real'))

        self.cleanup(tmpfile)

    def test_column_analysis_with_mixed_ints_and_floats_and_nulls(self):
        tmpfile = self.create_file_with_data(six.b("""planet_id,name,diameter_km,length_of_day_hours\n1000,Earth,12756,24\n2000,Mars,6792,24.7\n2500,Venus,,\n3000,Jupiter,142984,9.9"""))

        cmd = Q_EXECUTABLE + ' -d , -H "select * from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),8)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0], six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4], six.b('    `planet_id` - int'))
        self.assertEqual(o[5], six.b('    `name` - text'))
        self.assertEqual(o[6], six.b('    `diameter_km` - int'))
        self.assertEqual(o[7], six.b('    `length_of_day_hours` - real'))

        self.cleanup(tmpfile)

    def test_column_analysis_no_header(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(o[0], six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4], six.b('    `c1` - text'))
        self.assertEqual(o[5], six.b('    `c2` - int'))
        self.assertEqual(o[6], six.b('    `c3` - int'))

    def test_column_analysis_with_unexpected_header(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 7)
        self.assertEqual(len(e), 1)

        self.assertEqual(o[0], six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4],six.b('    `c1` - text'))
        self.assertEqual(o[5],six.b('    `c2` - text'))
        self.assertEqual(o[6],six.b('    `c3` - text'))

        self.assertEqual(
            e[0], six.b('Warning - There seems to be header line in the file, but -H has not been specified. All fields will be detected as text fields, and the header line will appear as part of the data'))

        self.cleanup(tmpfile)

    def test_column_analysis_for_spaces_in_header_row(self):
        tmpfile = self.create_file_with_data(
            header_row_with_spaces + six.b("\n") + sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select name,\\`value 1\\` from %s" -H -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 7)

        self.assertEqual(o[0], six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4], six.b('    `name` - text'))
        self.assertEqual(o[5], six.b('    `value 1` - int'))
        self.assertEqual(o[6], six.b('    `value2` - int'))

        self.cleanup(tmpfile)

    def test_column_analysis_with_header(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s" -A -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o),7)
        self.assertEqual(len(e),2)
        self.assertEqual(o[0], six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4], six.b('    `name` - text'))
        self.assertEqual(o[5], six.b('    `value1` - int'))
        self.assertEqual(o[6], six.b('    `value2` - int'))

        self.assertEqual(e[0],six.b('query error: no such column: c1'))
        self.assertTrue(e[1].startswith(six.b('Warning - There seems to be a ')))

        self.cleanup(tmpfile)



class StdInTests(AbstractQTestCase):

    def test_stdin_input(self):
        cmd = six.b('printf "%s" | ' + Q_EXECUTABLE + ' -d , "select c1,c2,c3 from -"') % sample_data_no_header
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0])
        self.assertEqual(o[1], sample_data_rows[1])
        self.assertEqual(o[2], sample_data_rows[2])

    def test_attempt_to_unzip_stdin(self):
        tmpfile = self.create_file_with_data(
            six.b('\x1f\x8b\x08\x08\xf2\x18\x12S\x00\x03xxxxxx\x003\xe42\xe22\xe62\xe12\xe52\xe32\xe7\xb2\xe0\xb2\xe424\xe0\x02\x00\xeb\xbf\x8a\x13\x15\x00\x00\x00'))

        cmd = 'cat %s | ' % tmpfile.name + Q_EXECUTABLE + ' -z "select sum(c1),avg(c1) from -"'

        retcode, o, e = run_command(cmd)
        self.assertTrue(retcode != 0)
        self.assertTrue(len(o) == 0)
        self.assertTrue(len(e) == 1)

        self.assertEqual(e[0],six.b('Cannot decompress standard input. Pipe the input through zcat in order to decompress.'))

        self.cleanup(tmpfile)

class QuotingTests(AbstractQTestCase):
    def test_non_quoted_values_in_quoted_data(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data)

        cmd = Q_EXECUTABLE + ' -d " " "select c1 from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)


        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),4)

        self.assertTrue(o[0],'non_quoted')
        self.assertTrue(o[1],'control-value-1')
        self.assertTrue(o[2],'non-quoted-value')
        self.assertTrue(o[3],'control-value-1')

        self.cleanup(tmp_data_file)

    def test_regular_quoted_values_in_quoted_data(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data)

        cmd = Q_EXECUTABLE + ' -d " " "select c2 from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),4)

        self.assertTrue(o[0],'regular_double_quoted')
        self.assertTrue(o[1],'control-value-2')
        self.assertTrue(o[2],'this is a quoted value')
        self.assertTrue(o[3],'control-value-2')

        self.cleanup(tmp_data_file)

    def test_double_double_quoted_values_in_quoted_data(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data)

        cmd = Q_EXECUTABLE + ' -d " " "select c3 from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),4)

        self.assertTrue(o[0],'double_double_quoted')
        self.assertTrue(o[1],'control-value-3')
        self.assertTrue(o[2],'this is a "double double" quoted value')
        self.assertTrue(o[3],'control-value-3')

        self.cleanup(tmp_data_file)

    def test_escaped_double_quoted_values_in_quoted_data(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data)

        cmd = Q_EXECUTABLE + ' -d " " "select c4 from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),4)

        self.assertTrue(o[0],'escaped_double_quoted')
        self.assertTrue(o[1],'control-value-4')
        self.assertTrue(o[2],'this is an escaped "quoted value"')
        self.assertTrue(o[3],'control-value-4')

        self.cleanup(tmp_data_file)

    def test_none_input_quoting_mode_in_relaxed_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -m relaxed -D , -w none -W none "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted,data",23'))
        self.assertEqual(o[1],six.b('unquoted-data,54,'))

        self.cleanup(tmp_data_file)

    def test_none_input_quoting_mode_in_strict_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -m strict -D , -w none "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(e),1)
        self.assertEqual(len(o),0)

        self.assertTrue(e[0].startswith(six.b('Strict mode. Column Count is expected to identical')))

        self.cleanup(tmp_data_file)

    def test_minimal_input_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w minimal "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_all_input_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w all "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_incorrect_input_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w unknown_wrapping_mode "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(e),1)
        self.assertEqual(len(o),0)

        self.assertTrue(e[0].startswith(six.b('Input quoting mode can only be one of all,minimal,none')))
        self.assertTrue(six.b('unknown_wrapping_mode') in e[0])

        self.cleanup(tmp_data_file)

    def test_none_output_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w all -W none "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_minimal_output_quoting_mode__without_need_to_quote_in_output(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w all -W minimal "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_minimal_output_quoting_mode__with_need_to_quote_in_output_due_to_delimiter(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        # output delimiter is set to space, so the output will contain it
        cmd = Q_EXECUTABLE + ' -d " " -D " " -w all -W minimal "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted data" 23'))
        self.assertEqual(o[1],six.b('unquoted-data 54'))

        self.cleanup(tmp_data_file)

    def test_minimal_output_quoting_mode__with_need_to_quote_in_output_due_to_newline(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2_with_newline)

        # Delimiter is set to colon (:), so it will not be inside the data values (this will make sure that the newline is the one causing the quoting)
        cmd = Q_EXECUTABLE + " -d ':' -w all -W minimal \"select c1,c2,replace(c1,'with' || x'0a' || 'a new line inside it','NEWLINE-REMOVED') from %s\"" % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),3)

        self.assertEqual(o[0],six.b('"quoted data with'))
        # Notice that the third column here is not quoted, because we replaced the newline with something else
        self.assertEqual(o[1],six.b('a new line inside it":23:quoted data NEWLINE-REMOVED'))
        self.assertEqual(o[2],six.b('unquoted-data:54:unquoted-data'))

        self.cleanup(tmp_data_file)

    def test_nonnumeric_output_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w all -W nonnumeric "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted data",23'))
        self.assertEqual(o[1],six.b('"unquoted-data",54'))

        self.cleanup(tmp_data_file)

    def test_all_output_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w all -W all "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted data","23"'))
        self.assertEqual(o[1],six.b('"unquoted-data","54"'))

        self.cleanup(tmp_data_file)

    def _internal_test_consistency_of_chaining_output_to_input(self,input_data,input_wrapping_mode,output_wrapping_mode):

        tmp_data_file = self.create_file_with_data(input_data)

        basic_cmd = Q_EXECUTABLE + ' -w %s -W %s "select * from -"' % (input_wrapping_mode,output_wrapping_mode)
        chained_cmd = 'cat %s | %s | %s | %s' % (tmp_data_file.name,basic_cmd,basic_cmd,basic_cmd)

        retcode, o, e = run_command(chained_cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(six.b("\n").join(o),input_data)

        self.cleanup(tmp_data_file)

    def test_consistency_of_chaining_minimal_wrapping_to_minimal_wrapping(self):
        input_data = six.b('"quoted data" 23\nunquoted-data 54')
        self._internal_test_consistency_of_chaining_output_to_input(input_data,'minimal','minimal')

    def test_consistency_of_chaining_all_wrapping_to_all_wrapping(self):
        input_data = six.b('"quoted data" "23"\n"unquoted-data" "54"')
        self._internal_test_consistency_of_chaining_output_to_input(input_data,'all','all')

    def test_input_field_quoting_and_data_types_with_encoding(self):
        OUTPUT_ENCODING = 'utf-8'

        # Checks combination of minimal input field quoting, with special characters that need to be decoded -
        # Both content and proper data types are verified
        data = six.b('111,22.22,"testing text with special characters - citt\xc3\xa0 ",http://somekindofurl.com,12.13.14.15,12.1\n')
        tmp_data_file = self.create_file_with_data(data)

        cmd = Q_EXECUTABLE + ' -d , "select * from %s" -E %s' % (tmp_data_file.name,OUTPUT_ENCODING)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),1)

        self.assertEqual(o[0].decode('utf-8'),u'111,22.22,testing text with special characters - citt\xe0 ,http://somekindofurl.com,12.13.14.15,12.1')

        cmd = Q_EXECUTABLE + ' -d , "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),10)

        self.assertEqual(o[0],six.b('Table: %s' % tmp_data_file.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s' % tmp_data_file.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4],six.b('    `c1` - int'))
        self.assertEqual(o[5],six.b('    `c2` - real'))
        self.assertEqual(o[6],six.b('    `c3` - text'))
        self.assertEqual(o[7],six.b('    `c4` - text'))
        self.assertEqual(o[8],six.b('    `c5` - text'))
        self.assertEqual(o[9],six.b('    `c6` - real'))

        self.cleanup(tmp_data_file)

    def test_multiline_double_double_quoted_values_in_quoted_data(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data)

        # FIXME Need to convert \0a to proper encoding suitable for the person running the tests.
        cmd = Q_EXECUTABLE + ' -d " " "select replace(c5,X\'0A\',\'::\') from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),4)

        self.assertTrue(o[0],six.b('multiline_double_double_quoted'))
        self.assertTrue(o[1],six.b('control-value-5'))
        self.assertTrue(o[2],six.b('this is a double double quoted "multiline\n value".'))
        self.assertTrue(o[3],six.b('control-value-5'))

        self.cleanup(tmp_data_file)

    def test_multiline_escaped_double_quoted_values_in_quoted_data(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data)

        # FIXME Need to convert \0a to proper encoding suitable for the person running the tests.
        cmd = Q_EXECUTABLE + ' -d " " "select replace(c6,X\'0A\',\'::\') from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),4)

        self.assertTrue(o[0],'multiline_escaped_double_quoted')
        self.assertTrue(o[1],'control-value-6')
        self.assertTrue(o[2],'this is an escaped "multiline:: value".')
        self.assertTrue(o[3],'control-value-6')

        self.cleanup(tmp_data_file)

    def test_disable_double_double_quoted_data_flag__values(self):
        # This test (and flag) is meant to verify backward compatibility only. It is possible that
        # this flag will be removed completely in the future

        tmp_data_file = self.create_file_with_data(double_double_quoted_data)

        cmd = Q_EXECUTABLE + ' -d " " --disable-double-double-quoting "select c2 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('double_double_quoted'))
        self.assertEqual(o[1],six.b('this is a quoted value with "double'))

        cmd = Q_EXECUTABLE + ' -d " " --disable-double-double-quoting "select c3 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b(''))
        self.assertEqual(o[1],six.b('double'))

        cmd = Q_EXECUTABLE + ' -d " " --disable-double-double-quoting "select c4 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b(''))
        self.assertEqual(o[1],six.b('quotes"""'))

        self.cleanup(tmp_data_file)

    def test_disable_escaped_double_quoted_data_flag__values(self):
        # This test (and flag) is meant to verify backward compatibility only. It is possible that
        # this flag will be removed completely in the future

        tmp_data_file = self.create_file_with_data(escaped_double_quoted_data)

        cmd = Q_EXECUTABLE + ' -d " " --disable-escaped-double-quoting "select c2 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('escaped_double_quoted'))
        self.assertEqual(o[1],six.b('this is a quoted value with \\escaped'))

        cmd = Q_EXECUTABLE + ' -d " " --disable-escaped-double-quoting "select c3 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b(''))
        self.assertEqual(o[1],six.b('double'))

        cmd = Q_EXECUTABLE + ' -d " " --disable-escaped-double-quoting "select c4 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b(''))
        self.assertEqual(o[1],six.b('quotes\\""'))

        self.cleanup(tmp_data_file)

    def test_combined_quoted_data_flags__number_of_columns_detected(self):
        # This test (and flags) is meant to verify backward compatibility only. It is possible that
        # these flags will be removed completely in the future
        tmp_data_file = self.create_file_with_data(combined_quoted_data)

        cmd = Q_EXECUTABLE + ' -d " " --disable-double-double-quoting --disable-escaped-double-quoting "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[o.index(six.b('  Fields:'))+1:]

        self.assertEqual(len(o),7) # found 7 fields

        cmd = Q_EXECUTABLE + ' -d " " --disable-escaped-double-quoting "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[o.index(six.b('  Fields:'))+1:]

        self.assertEqual(len(o),5) # found 5 fields

        cmd = Q_EXECUTABLE + ' -d " " --disable-double-double-quoting "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[o.index(six.b('  Fields:'))+1:]

        self.assertEqual(len(o),5) # found 5 fields

        cmd = Q_EXECUTABLE + ' -d " " "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[o.index(six.b('  Fields:'))+1:]

        self.assertEqual(len(o),3) # found only 3 fields, which is the correct amount

        self.cleanup(tmp_data_file)


class EncodingTests(AbstractQTestCase):

    def test_utf8_with_bom_encoding(self):
        utf_8_data_with_bom = six.b('\xef\xbb\xbf"typeid","limit","apcost","date","checkpointId"\n"1","2","5","1,2,3,4,5,6,7","3000,3001,3002"\n"2","2","5","1,2,3,4,5,6,7","3003,3004,3005"\n')
        tmp_data_file = self.create_file_with_data(utf_8_data_with_bom,encoding=None)

        cmd = Q_EXECUTABLE + ' -d , -H -O -e utf-8-sig "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),3)

        self.assertEqual(o[0],six.b('typeid,limit,apcost,date,checkpointId'))
        self.assertEqual(o[1],six.b('1,2,5,"1,2,3,4,5,6,7","3000,3001,3002"'))
        self.assertEqual(o[2],six.b('2,2,5,"1,2,3,4,5,6,7","3003,3004,3005"'))

        self.cleanup(tmp_data_file)


class QrcTests(AbstractQTestCase):

    def test_explicit_qrc_filename_not_found(self):
        non_existent_filename = str(uuid.uuid4())
        env_to_inject = { 'QRC_FILENAME': non_existent_filename}
        cmd = Q_EXECUTABLE + ' "select 1"'
        retcode, o, e = run_command(cmd, env_to_inject=env_to_inject)

        self.assertEqual(retcode, 244)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertTrue(e[0] == six.b('QRC_FILENAME env var exists, but cannot find qrc file at %s' % non_existent_filename))

    def test_explicit_qrc_filename_that_exists(self):
        tmp_qrc_file = self.create_file_with_data(six.b('''[options]
output_delimiter=|
'''))
        env_to_inject = { 'QRC_FILENAME': tmp_qrc_file.name}
        cmd = Q_EXECUTABLE + ' "select 1,2"'
        retcode, o, e = run_command(cmd, env_to_inject=env_to_inject)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0] == six.b('1|2'))

        self.cleanup(tmp_qrc_file)

    def test_all_default_options(self):
        # Create a qrc file that contains all default values inside the qrc file, but with some different values than the regular defaults
        tmp_qrc_file = self.create_file_with_data(six.b('''[options]
analyze_only=True
beautify=True
caching_mode=readwrite
column_count=32
delimiter=,
disable_column_type_detection=True
disable_double_double_quoting=False
disable_escaped_double_quoting=False
encoding=ascii
formatting=xxx
gzipped=True
input_quoting_mode=all
keep_leading_whitespace_in_values=True
list_user_functions=True
max_attached_sqlite_databases=888
max_column_length_limit=8888
mode=strict
output_delimiter=|
output_encoding=utf-8
output_header=True
output_quoting_mode=all
overwrite_qsql=False
pipe_delimited=True
pipe_delimited_output=True
query_encoding=ascii
query_filename=query-filename
save_db_to_disk_filename=save-db-to-disk-filename
skip_header=True
tab_delimited=True
tab_delimited_output=true
verbose=True
with_universal_newlines=True
'''))
        env_to_inject = { 'QRC_FILENAME': tmp_qrc_file.name}
        cmd = Q_EXECUTABLE + ' --dump-defaults'
        retcode, o, e = run_command(cmd, env_to_inject=env_to_inject)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 34)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b('[options]'))
        o = o[1:]

        m = {}
        for r in o:
            key,val = r.split(six.b("="),1)
            m[key] = val

        self.assertEqual(m[six.b('analyze_only')],six.b('True'))
        self.assertEqual(m[six.b('beautify')],six.b('True'))
        self.assertEqual(m[six.b('caching_mode')],six.b('readwrite'))
        self.assertEqual(m[six.b('column_count')],six.b('32'))
        self.assertEqual(m[six.b('delimiter')],six.b(','))
        self.assertEqual(m[six.b('disable_column_type_detection')],six.b('True'))
        self.assertEqual(m[six.b('disable_double_double_quoting')],six.b('False'))
        self.assertEqual(m[six.b('disable_escaped_double_quoting')],six.b('False'))
        self.assertEqual(m[six.b('encoding')],six.b('ascii'))
        self.assertEqual(m[six.b('formatting')],six.b('xxx'))
        self.assertEqual(m[six.b('gzipped')],six.b('True'))
        self.assertEqual(m[six.b('input_quoting_mode')],six.b('all'))
        self.assertEqual(m[six.b('keep_leading_whitespace_in_values')],six.b('True'))
        self.assertEqual(m[six.b('list_user_functions')],six.b('True'))
        self.assertEqual(m[six.b('max_attached_sqlite_databases')],six.b('888'))
        self.assertEqual(m[six.b('max_column_length_limit')],six.b('8888'))
        self.assertEqual(m[six.b('mode')],six.b('strict'))
        self.assertEqual(m[six.b('output_delimiter')],six.b('|'))
        self.assertEqual(m[six.b('output_encoding')],six.b('utf-8'))
        self.assertEqual(m[six.b('output_header')],six.b('True'))
        self.assertEqual(m[six.b('output_quoting_mode')],six.b('all'))
        self.assertEqual(m[six.b('overwrite_qsql')],six.b('False'))
        self.assertEqual(m[six.b('pipe_delimited')],six.b('True'))
        self.assertEqual(m[six.b('pipe_delimited_output')],six.b('True'))
        self.assertEqual(m[six.b('query_encoding')],six.b('ascii'))
        self.assertEqual(m[six.b('query_filename')],six.b('query-filename'))
        self.assertEqual(m[six.b('save_db_to_disk_filename')],six.b('save-db-to-disk-filename'))
        self.assertEqual(m[six.b('skip_header')],six.b('True'))
        self.assertEqual(m[six.b('tab_delimited')],six.b('True'))
        self.assertEqual(m[six.b('tab_delimited_output')],six.b('True'))
        self.assertEqual(m[six.b('verbose')],six.b('True'))
        self.assertEqual(m[six.b('with_universal_newlines')],six.b('True'))

        self.cleanup(tmp_qrc_file)

    def test_caching_readwrite_using_qrc_file(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        tmpfile_folder = os.path.dirname(tmpfile.name)
        tmpfile_filename = os.path.basename(tmpfile.name)
        expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -d , "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),3)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('a,1,0'))
        self.assertEqual(o[1],six.b('b,2,0'))
        self.assertEqual(o[2],six.b('c,,0'))

        # Ensure default does not create a cache file
        self.assertTrue(not os.path.exists(expected_cache_filename))

        tmp_qrc_file = self.create_file_with_data(six.b('''[options]
caching_mode=readwrite
'''))
        env_to_inject = { 'QRC_FILENAME': tmp_qrc_file.name}
        cmd = Q_EXECUTABLE + ' -d , "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd, env_to_inject=env_to_inject)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),3)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('a,1,0'))
        self.assertEqual(o[1],six.b('b,2,0'))
        self.assertEqual(o[2],six.b('c,,0'))

        # Ensure that qrc file caching is being used and caching is activated (cache file should exist)
        self.assertTrue(os.path.exists(expected_cache_filename))

        self.cleanup(tmp_qrc_file)
        self.cleanup(tmpfile)


class QsqlUsageTests(AbstractQTestCase):

    def test_concatenate_same_qsql_file_with_single_table(self):
        numbers = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]

        qsql_file_data = self.arrays_to_qsql_file_content([six.b('aa'), six.b('bb'), six.b('cc')], numbers)

        tmpfile = self.create_file_with_data(qsql_file_data,suffix='.qsql')

        cmd = Q_EXECUTABLE + ' -t "select count(*) from (select * from %s union all select * from %s)"' % (tmpfile.name,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('20000'))

    def test_query_qsql_with_single_table(self):
        numbers = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]

        qsql_file_data = self.arrays_to_qsql_file_content([six.b('aa'), six.b('bb'), six.b('cc')], numbers)

        tmpfile = self.create_file_with_data(qsql_file_data)

        cmd = Q_EXECUTABLE + ' -t "select sum(aa),sum(bb),sum(cc) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('50005000\t50005000\t50005000'))

    def test_query_qsql_with_single_table_with_explicit_non_existent_tablename(self):
        numbers = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]

        qsql_file_data = self.arrays_to_qsql_file_content([six.b('aa'), six.b('bb'), six.b('cc')], numbers)

        tmpfile = self.create_file_with_data(qsql_file_data)

        c = sqlite3.connect(tmpfile.name)
        actual_table_name = c.execute('select temp_table_name from _qcatalog').fetchall()[0][0]
        c.close()


        cmd = '%s -t "select sum(aa),sum(bb),sum(cc) from %s:::non-existent"' % (Q_EXECUTABLE,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 84)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertEqual(e[0],six.b('Table non-existent could not be found in qsql file %s . Existing table names: %s' % (tmpfile.name,actual_table_name)))

    def test_query_qsql_with_single_table_with_explicit_table_name(self):
        numbers = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]

        qsql_file_data = self.arrays_to_qsql_file_content([six.b('aa'), six.b('bb'), six.b('cc')], numbers)

        tmpfile = self.create_file_with_data(qsql_file_data)

        c = sqlite3.connect(tmpfile.name)
        actual_table_name = c.execute('select temp_table_name from _qcatalog').fetchall()[0][0]
        c.close()


        cmd = '%s -t "select sum(aa),sum(bb),sum(cc) from %s:::%s"' % (Q_EXECUTABLE,tmpfile.name,actual_table_name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('50005000\t50005000\t50005000'))

    def test_query_multi_qsql_with_single_table(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        qsql_file_data1 = self.arrays_to_qsql_file_content([six.b('aa'), six.b('bb'), six.b('cc')], numbers1)
        tmpfile1 = self.create_file_with_data(qsql_file_data1,suffix='.qsql')

        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]
        qsql_file_data2 = self.arrays_to_qsql_file_content([six.b('aa'), six.b('bb'), six.b('cc')], numbers2)
        tmpfile2 = self.create_file_with_data(qsql_file_data2,suffix='.qsql')

        cmd = Q_EXECUTABLE + ' -t "select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s small_file left join %s large_file on (large_file.aa == small_file.bb)"' % (tmpfile2.name,tmpfile1.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('55\t55\t55'))

    def test_query_concatenated_qsqls_each_with_single_table(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        qsql_file_data1 = self.arrays_to_qsql_file_content([six.b('aa'), six.b('bb'), six.b('cc')], numbers1)
        tmpfile1 = self.create_file_with_data(qsql_file_data1,suffix='.qsql')

        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]
        qsql_file_data2 = self.arrays_to_qsql_file_content([six.b('aa'), six.b('bb'), six.b('cc')], numbers2)
        tmpfile2 = self.create_file_with_data(qsql_file_data2,suffix='.qsql')

        cmd = Q_EXECUTABLE + ' -t "select sum(aa),sum(bb),sum(cc) from (select * from %s union all select * from %s)"' % (tmpfile2.name,tmpfile1.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('50005055\t50005055\t50005055'))

    def test_concatenated_qsql_and_data_stream__column_names_mismatch(self):
        N1 = 10000
        N2 = 100

        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, N1 + 1)]
        csv_file_data1 = self.arrays_to_csv_file_content(six.b('\t'),[six.b('aa'), six.b('bb'), six.b('cc')], numbers1)
        tmpfile1 = self.create_file_with_data(csv_file_data1)
        expected_cache_filename1 = '%s.qsql' % tmpfile1.name

        cmd = Q_EXECUTABLE + ' -H -t "select count(*) from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertTrue(os.path.exists(expected_cache_filename1))

        cmd = 'seq 1 %s | %s -c 1 "select count(*) from (select * from %s UNION ALL select * from -)"' % (N2, Q_EXECUTABLE,expected_cache_filename1)

        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 1)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertEqual(e[0],six.b('query error: SELECTs to the left and right of UNION ALL do not have the same number of result columns'))

    def test_concatenated_qsql_and_data_stream(self):
        N1 = 10000
        N2 = 100

        numbers1 = [[six.b(str(i))] for i in range(1, N1 + 1)]
        csv_file_data1 = self.arrays_to_csv_file_content(six.b('\t'),[six.b('c1')], numbers1)
        tmpfile1 = self.create_file_with_data(csv_file_data1)
        expected_cache_filename1 = '%s.qsql' % tmpfile1.name

        cmd = Q_EXECUTABLE + ' -H -t "select count(*) from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertTrue(os.path.exists(expected_cache_filename1))

        cmd = 'seq 1 %s | %s -t -c 1 "select count(*),sum(c1) from (select * from %s UNION ALL select * from -)"' % (N2, Q_EXECUTABLE,expected_cache_filename1)

        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('%s\t%s' % (N1+N2,sum(range(1,N1+1)) + sum(range(1,N2+1)))))

    def test_concatenated_qsql_and_data_stream__explicit_table_name(self):
        N1 = 10000
        N2 = 100

        numbers1 = [[six.b(str(i))] for i in range(1, N1 + 1)]
        csv_file_data1 = self.arrays_to_csv_file_content(six.b('\t'),[six.b('c1')], numbers1)
        tmpfile1 = self.create_file_with_data(csv_file_data1)
        tmpfile1_expected_table_name = os.path.basename(tmpfile1.name)

        expected_cache_filename1 = '%s.qsql' % tmpfile1.name

        cmd = Q_EXECUTABLE + ' -H -t "select count(*) from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertTrue(os.path.exists(expected_cache_filename1))

        cmd = 'seq 1 %s | %s -t -c 1 "select count(*),sum(c1) from (select * from %s:::%s UNION ALL select * from -)"' % (N2, Q_EXECUTABLE,expected_cache_filename1,tmpfile1_expected_table_name)

        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('%s\t%s' % (N1+N2,sum(range(1,N1+1)) + sum(range(1,N2+1)))))

    def test_write_to_qsql__check_chosen_table_name(self):
        numbers1 = [[six.b(str(i))] for i in range(1, 10001)]
        csv_file_data1 = self.arrays_to_csv_file_content(six.b('\t'),[six.b('c1')], numbers1)
        tmpfile1 = self.create_file_with_data(csv_file_data1)
        expected_cache_filename1 = '%s.qsql' % tmpfile1.name

        cmd = Q_EXECUTABLE + ' -c 1 -H -t "select count(*) from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertTrue(os.path.exists(expected_cache_filename1))

        c = sqlite3.connect(expected_cache_filename1)
        qcatalog_entries = c.execute('select temp_table_name from _qcatalog').fetchall()
        self.assertEqual(len(qcatalog_entries),1)
        self.assertEqual(qcatalog_entries[0][0],os.path.basename(tmpfile1.name))

    def test_concatenated_mixes_qsql_with_single_table_and_csv(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        csv_file_data1 = self.arrays_to_csv_file_content(six.b('\t'),[six.b('aa'), six.b('bb'), six.b('cc')], numbers1)
        tmpfile1 = self.create_file_with_data(csv_file_data1)
        expected_cache_filename1 = '%s.qsql' % tmpfile1.name

        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]
        csv_file_data2 = self.arrays_to_csv_file_content(six.b('\t'),[six.b('aa'), six.b('bb'), six.b('cc')], numbers2)
        tmpfile2 = self.create_file_with_data(csv_file_data2)
        expected_cache_filename2 = '%s.qsql' % tmpfile2.name


        cmd = Q_EXECUTABLE + ' -H -t "select count(*) from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertTrue(os.path.exists(expected_cache_filename1))

        cmd = Q_EXECUTABLE + ' -H -t "select count(*) from %s" -C readwrite' % tmpfile2.name
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertTrue(os.path.exists(expected_cache_filename2))

        # csv and qsql files prepared. now test all four combinations

        cmd = Q_EXECUTABLE + ' -O -H -t "select count(*) cnt,sum(aa) sum_aa,sum(bb) sum_bb,sum(cc) sum_cc from (select * from %s union all select * from %s)"' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),2)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('cnt\tsum_aa\tsum_bb\tsum_cc'))
        self.assertEqual(o[1],six.b('10010\t50005055\t50005055\t50005055'))

        cmd = Q_EXECUTABLE + ' -O -H -t "select count(*) cnt,sum(aa) sum_aa,sum(bb) sum_bb,sum(cc) sum_cc from (select * from %s union all select * from %s.qsql)"' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),2)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('cnt\tsum_aa\tsum_bb\tsum_cc'))
        self.assertEqual(o[1],six.b('10010\t50005055\t50005055\t50005055'))

        cmd = Q_EXECUTABLE + ' -O -H -t "select count(*) cnt,sum(aa) sum_aa,sum(bb) sum_bb,sum(cc) sum_cc from (select * from %s.qsql union all select * from %s)"' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),2)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('cnt\tsum_aa\tsum_bb\tsum_cc'))
        self.assertEqual(o[1],six.b('10010\t50005055\t50005055\t50005055'))

        cmd = Q_EXECUTABLE + ' -O -H -t "select count(*) cnt,sum(aa) sum_aa,sum(bb) sum_bb,sum(cc) sum_cc from (select * from %s.qsql union all select * from %s.qsql)"' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),2)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('cnt\tsum_aa\tsum_bb\tsum_cc'))
        self.assertEqual(o[1],six.b('10010\t50005055\t50005055\t50005055'))

    def test_analysis_of_concatenated_mixes_qsql_with_single_table_and_csv(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        csv_file_data1 = self.arrays_to_csv_file_content(six.b('\t'),[six.b('aa'), six.b('bb'), six.b('cc')], numbers1)
        tmpfile1 = self.create_file_with_data(csv_file_data1)
        expected_cache_filename1 = '%s.qsql' % tmpfile1.name

        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]
        csv_file_data2 = self.arrays_to_csv_file_content(six.b('\t'),[six.b('aa'), six.b('bb'), six.b('cc')], numbers2)
        tmpfile2 = self.create_file_with_data(csv_file_data2)
        expected_cache_filename2 = '%s.qsql' % tmpfile2.name

        cmd = Q_EXECUTABLE + ' -H -t "select count(*) from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertTrue(os.path.exists(expected_cache_filename1))

        cmd = Q_EXECUTABLE + ' -H -t "select count(*) from %s" -C readwrite' % tmpfile2.name
        retcode, o, e = run_command(cmd)
        self.assertEqual(retcode, 0)
        self.assertTrue(os.path.exists(expected_cache_filename2))

        # csv and qsql files prepared

        # Test function, will be used multiple times, each time with a different combination

        def do_check(caching_mode,
                     file1_source_type,file1_table_postfix,file1_postfix,
                     file2_source_type,file2_table_postfix,file2_postfix):
            cmd = '%s -C %s -O -H -t "select count(*) cnt,sum(aa) sum_aa,sum(bb) sum_bb,sum(cc) sum_cc from (select * from %s%s UNION ALL select * from %s%s)" -A' % (
                Q_EXECUTABLE,
                caching_mode,
                tmpfile1.name,
                file1_table_postfix,
                tmpfile2.name,
                file2_table_postfix)

            retcode, o, e = run_command(cmd)
            self.assertEqual(retcode, 0)
            self.assertEqual(len(o),14)
            self.assertEqual(len(e),0)
            self.assertEqual(o, [
                six.b('Table: %s%s' % (tmpfile1.name,file1_table_postfix)),
                six.b('  Sources:'),
                six.b('    source_type: %s source: %s%s' % (file1_source_type,tmpfile1.name,file1_postfix)),
                six.b('  Fields:'),
                six.b('    `aa` - int'),
                six.b('    `bb` - int'),
                six.b('    `cc` - int'),
                six.b('Table: %s%s' % (tmpfile2.name,file2_table_postfix)),
                six.b('  Sources:'),
                six.b('    source_type: %s source: %s%s' % (file2_source_type,tmpfile2.name,file2_postfix)),
                six.b('  Fields:'),
                six.b('    `aa` - int'),
                six.b('    `bb` - int'),
                six.b('    `cc` - int')])

        # now test *the analysis results* of all four combinations, adding `-C read`, so the
        # qsql will be used. Running with `-C none`, would have caused the qsql not to be used even if the qsql file exists

        do_check(caching_mode='read',
                 file1_source_type='qsql-file-with-original',file1_table_postfix='',file1_postfix='.qsql',
                 file2_source_type='qsql-file-with-original',file2_table_postfix='',file2_postfix='.qsql')
        do_check('read',
                 file1_source_type='qsql-file-with-original',file1_table_postfix='',file1_postfix='.qsql',
                 file2_source_type='qsql-file',file2_table_postfix='.qsql',file2_postfix='.qsql')
        do_check('read',
                 file1_source_type='qsql-file',file1_table_postfix='.qsql',file1_postfix='.qsql',
                 file2_source_type='qsql-file-with-original',file2_table_postfix='',file2_postfix='.qsql')
        do_check('read',
                 file1_source_type='qsql-file',file1_table_postfix='.qsql',file1_postfix='.qsql',
                 file2_source_type='qsql-file',file2_table_postfix='.qsql',file2_postfix='.qsql')

        # Now test the all combinations again, this time with `-C none`, to make sure that by
        # default, the qsql file is not used, and -A shows that fact

        do_check(caching_mode='none',
                 file1_source_type='file-with-unused-qsql',file1_table_postfix='',file1_postfix='',
                 file2_source_type='file-with-unused-qsql',file2_table_postfix='',file2_postfix='')
        do_check('none',
                 file1_source_type='file-with-unused-qsql',file1_table_postfix='',file1_postfix='',
                 file2_source_type='qsql-file',file2_table_postfix='.qsql',file2_postfix='.qsql')
        do_check('none',
                 file1_source_type='qsql-file',file1_table_postfix='.qsql',file1_postfix='.qsql',
                 file2_source_type='file-with-unused-qsql',file2_table_postfix='',file2_postfix='')
        do_check('none',
                 file1_source_type='qsql-file',file1_table_postfix='.qsql',file1_postfix='.qsql',
                 file2_source_type='qsql-file',file2_table_postfix='.qsql',file2_postfix='.qsql')

    def test_mixed_qsql_with_single_table_and_csv__missing_header_parameter_for_csv(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        qsql_file_data1 = self.arrays_to_qsql_file_content([six.b('aa'), six.b('bb'), six.b('cc')], numbers1)
        tmpfile1 = self.create_file_with_data(qsql_file_data1,suffix='.qsql')

        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]
        csv_file_data2 = self.arrays_to_csv_file_content(six.b('\t'),[six.b('aa'), six.b('bb'), six.b('cc')], numbers2)
        tmpfile2 = self.create_file_with_data(csv_file_data2)

        cmd = Q_EXECUTABLE + ' -t "select sum(aa),sum(bb),sum(cc) from (select * from %s union all select * from %s)"' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0],six.b('Warning - There seems to be header line in the file, but -H has not been specified. All fields will be detected as text fields, and the header line will appear as part of the data'))
        self.assertEqual(o[0],six.b('50005055.0\t50005055.0\t50005055.0'))

    def test_qsql_with_multiple_tables_direct_use(self):
        numbers1 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 10001)]
        qsql_filename1 = self.create_qsql_file_with_content_and_return_filename([six.b('aa'), six.b('bb'), six.b('cc')],numbers1)
        expected_stored_table_name1 = os.path.basename(qsql_filename1)[:-5]

        numbers2 = [[six.b(str(i)), six.b(str(i)), six.b(str(i))] for i in range(1, 11)]
        qsql_filename2 = self.create_qsql_file_with_content_and_return_filename([six.b('aa'), six.b('bb'), six.b('cc')],numbers2)
        expected_stored_table_name2 = os.path.basename(qsql_filename2)[:-5]

        qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        cmd = '%s -t "select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s large_file left join %s small_file on (large_file.aa == small_file.bb)" -S %s' % \
              (Q_EXECUTABLE,qsql_filename1,qsql_filename2,qsql_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)
        self.assertEqual(e[0], six.b('Going to save data into a disk database: %s' % qsql_with_multiple_tables))
        self.assertTrue(e[1].startswith(six.b('Data has been saved into %s . Saving has taken' % qsql_with_multiple_tables)))
        self.assertEqual(e[2],six.b('Query to run on the database: select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s large_file left join %s small_file on (large_file.aa == small_file.bb);' % \
                                    (expected_stored_table_name1,expected_stored_table_name2)))
        self.assertEqual(e[3],six.b('You can run the query directly from the command line using the following command: echo "select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s large_file left join %s small_file on (large_file.aa == small_file.bb)" | sqlite3 %s' % \
                                    (expected_stored_table_name1,expected_stored_table_name2,qsql_with_multiple_tables)))

        cmd = '%s -d , "select count(*) cnt,sum(aa),sum(bb),sum(cc) from %s:::%s"' % (Q_EXECUTABLE,qsql_with_multiple_tables,expected_stored_table_name1)
        r, o, e = run_command(cmd)

        self.assertEqual(r,0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('10000,50005000,50005000,50005000'))

    def test_direct_use_of_sqlite_db_with_one_table(self):
        tmpfile = self.create_file_with_data(six.b(''),suffix='.sqlite')
        os.remove(tmpfile.name)
        c = sqlite3.connect(tmpfile.name)
        c.execute(' create table mytable (x int, y int)').fetchall()
        c.execute(' insert into mytable (x,y) values (100,200),(300,400)').fetchall()
        c.commit()
        c.close()

        cmd = Q_EXECUTABLE + ' -t "select sum(x),sum(y) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('400\t600'))

        cmd = Q_EXECUTABLE + ' -t "select sum(x),sum(y) from %s:::mytable"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('400\t600'))

    def test_direct_use_of_sqlite_db_with_one_table__nonexistent_table(self):
        tmpfile = self.create_file_with_data(six.b(''),suffix='.sqlite')
        os.remove(tmpfile.name)
        c = sqlite3.connect(tmpfile.name)
        c.execute(' create table some_numbers (x int, y int)').fetchall()
        c.execute(' insert into some_numbers (x,y) values (100,200),(300,400)').fetchall()
        c.commit()
        c.close()

        cmd = Q_EXECUTABLE + ' -t "select sum(x),sum(y) from %s:::non_existent"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 85)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0],six.b('Table non_existent could not be found in sqlite file %s . Existing table names: some_numbers' % (tmpfile.name)))


    def test_qsql_creation_and_direct_use(self):
        numbers = [[six.b(str(i)),six.b(str(i)),six.b(str(i))] for i in range(1,10001)]

        file_data = self.arrays_to_csv_file_content(six.b('\t'),[six.b('aa'),six.b('bb'),six.b('cc')],numbers)

        tmpfile = self.create_file_with_data(file_data)
        tmpfile_folder = os.path.dirname(tmpfile.name)
        tmpfile_filename = os.path.basename(tmpfile.name)
        expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -t "select sum(aa),sum(bb),sum(cc) from %s" -H -C readwrite' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('50005000\t50005000\t50005000'))

        self.assertTrue(os.path.exists(expected_cache_filename))

        self.cleanup(tmpfile)

        # Get the data using a comma delimiter, to make sure that column parsing was done correctlyAdding to qcatalog table:
        cmd = Q_EXECUTABLE + ' -D , "select count(*),sum(aa),sum(bb),sum(cc) from %s"' % expected_cache_filename
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('10000,50005000,50005000,50005000'))

    def test_analysis_of_qsql_direct_usage(self):
        numbers = [[six.b(str(i)),six.b(str(i)),six.b(str(i))] for i in range(1,10001)]

        file_data = self.arrays_to_csv_file_content(six.b('\t'),[six.b('aa'),six.b('bb'),six.b('cc')],numbers)

        tmpfile = self.create_file_with_data(file_data)
        tmpfile_folder = os.path.dirname(tmpfile.name)
        tmpfile_filename = os.path.basename(tmpfile.name)
        expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -t "select sum(aa),sum(bb),sum(cc) from %s" -H -C readwrite' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('50005000\t50005000\t50005000'))

        self.assertTrue(os.path.exists(expected_cache_filename))

        self.cleanup(tmpfile)

        cmd = Q_EXECUTABLE + ' "select * from %s" -A' % expected_cache_filename
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 7)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('Table: %s' % expected_cache_filename))
        self.assertEqual(o[1],six.b("  Sources:"))
        self.assertEqual(o[2],six.b('    source_type: qsql-file source: %s' % expected_cache_filename))
        self.assertEqual(o[3],six.b("  Fields:"))
        self.assertEqual(o[4],six.b('    `aa` - int'))
        self.assertEqual(o[5],six.b('    `bb` - int'))
        self.assertEqual(o[6],six.b('    `cc` - int'))

    def test_analysis_of_qsql_direct_usage2(self):
        numbers = [[six.b(str(i)),six.b(str(i)),six.b(str(i))] for i in range(1,10001)]

        file_data = self.arrays_to_csv_file_content(six.b('\t'),[six.b('aa'),six.b('bb'),six.b('cc')],numbers)

        tmpfile = self.create_file_with_data(file_data)
        tmpfile_folder = os.path.dirname(tmpfile.name)
        tmpfile_filename = os.path.basename(tmpfile.name)
        expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -t "select sum(aa),sum(bb),sum(cc) from %s" -H -C readwrite' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('50005000\t50005000\t50005000'))

        self.assertTrue(os.path.exists(expected_cache_filename))

        self.cleanup(tmpfile)

        cmd = Q_EXECUTABLE + ' "select * from %s" -A' % expected_cache_filename
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 7)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('Table: %s' % expected_cache_filename))
        self.assertEqual(o[1],six.b("  Sources:"))
        self.assertEqual(o[2],six.b('    source_type: qsql-file source: %s' % expected_cache_filename))
        self.assertEqual(o[3],six.b("  Fields:"))
        self.assertEqual(o[4],six.b('    `aa` - int'))
        self.assertEqual(o[5],six.b('    `bb` - int'))
        self.assertEqual(o[6],six.b('    `cc` - int'))

    def test_direct_qsql_usage_for_single_table_qsql_file(self):
        disk_db_filename = self.random_tmp_filename('save-to-db','qsql')

        cmd = 'seq 1 10000 | %s -t "select sum(aa),sum(bb),sum(cc) from -" -S %s' % (Q_EXECUTABLE,disk_db_filename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)

        cmd = '%s -D, "select count(*),sum(c1) from %s:::data_stream_stdin"' % (Q_EXECUTABLE,disk_db_filename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('10000,50005000'))

    def test_direct_qsql_usage_for_single_table_qsql_file__nonexistent_table(self):
        disk_db_filename = self.random_tmp_filename('save-to-db','qsql')

        cmd = 'seq 1 10000 | %s -t "select sum(aa),sum(bb),sum(cc) from -" -S %s' % (Q_EXECUTABLE,disk_db_filename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)

        cmd = '%s -D, "select count(*),sum(c1) from %s:::unknown_table_name"' % (Q_EXECUTABLE,disk_db_filename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 85)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertEqual(e[0],six.b('Table unknown_table_name could not be found in sqlite file %s . Existing table names: data_stream_stdin' % (disk_db_filename)))

    def test_direct_qsql_usage_from_written_data_stream(self):
        disk_db_filename = self.random_tmp_filename('save-to-db','qsql')

        cmd = 'seq 1 10000 | %s -t "select sum(aa),sum(bb),sum(cc) from -" -S %s' % (Q_EXECUTABLE,disk_db_filename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)

        cmd = '%s -D, "select count(*),sum(c1) from %s:::data_stream_stdin"' % (Q_EXECUTABLE,disk_db_filename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('10000,50005000'))

    def test_direct_qsql_self_join(self):
        disk_db_filename = self.random_tmp_filename('save-to-db','qsql')

        N = 100
        cmd = 'seq 1 %s | %s -t "select count(*),sum(c1) from -" -S %s' % (N,Q_EXECUTABLE,disk_db_filename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)

        cmd = '%s -D, "select count(*),sum(a.c1),sum(b.c1) from %s:::data_stream_stdin a left join %s:::data_stream_stdin b"' % (Q_EXECUTABLE,disk_db_filename,disk_db_filename)
        retcode, o, e = run_command(cmd)

        expected_sum = sum(range(1,N+1))*N

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],six.b('10000,%s,%s' % (expected_sum,expected_sum)))


class CachingTests(AbstractQTestCase):

    def test_cache_empty_file(self):
        file_data = six.b("a,b,c")
        tmpfile = self.create_file_with_data(file_data)
        tmpfile_folder = os.path.dirname(tmpfile.name)
        tmpfile_filename = os.path.basename(tmpfile.name)
        tmpfile_expected_table_name = os.path.basename(tmpfile.name)
        expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C none' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0],six.b("Warning - data is empty"))

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0],six.b("Warning - data is empty"))

        # After readwrite caching has been activated, the cache file is expected to exist
        self.assertTrue(os.path.exists(expected_cache_filename))

        # Read the cache file directly, to make sure it's a valid sqlite file
        import sqlite3
        db = sqlite3.connect(expected_cache_filename)
        table_list = db.execute("select content_signature_key,temp_table_name,content_signature,creation_time,source_type,source from _qcatalog where temp_table_name == '%s'" % (tmpfile_expected_table_name)).fetchall()
        self.assertTrue(len(table_list) == 1)
        table_metadata = table_list[0]
        results = db.execute("select * from %s" % table_metadata[1]).fetchall()
        self.assertTrue(len(results) == 0)

        self.cleanup(tmpfile)

    def test_reading_the_wrong_cache__original_file_having_different_data(self):
        file_data1 = six.b("a,b,c\n10,20,30\n30,40,50")

        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], six.b('10'))
        self.assertEqual(o[1], six.b('30'))

        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename))

        # Overwrite the original file
        file_data2 = six.b("a,b,c\n10,20,30\n30,40,50\n50,60,70")
        self.write_file(tmpfile1.name,file_data2)

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 81)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0], six.b('%s vs %s.qsql: Content Signatures differ at inferer.rows (actual analysis data differs)' % \
                                     (tmpfile1.name,tmpfile1.name)))


    def test_reading_the_wrong_cache__original_file_having_different_delimiter(self):
        file_data1 = six.b("a,b,c\n10,20,30\n30,40,50")

        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], six.b('10'))
        self.assertEqual(o[1], six.b('30'))

        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename))

        # Overwrite the original file
        file_data2 = six.b("a\tb\tc\n10\t20\t30\n30\t40\t50")
        self.write_file(tmpfile1.name,file_data2)

        cmd = Q_EXECUTABLE + ' -H -t "select a from %s" -C read' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 80)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        x = six.b("%s vs %s.qsql: Content Signatures for table %s differ at input_delimiter (source value '\t' disk signature value ',')" % \
                                     (tmpfile1.name,tmpfile1.name,tmpfile1.name))
        self.assertEqual(e[0], x)

    def test_rename_cache_and_read_from_it(self):
        # create a file, along with its qsql
        file_data1 = six.b("a,b,c\n10,20,30\n30,40,50")

        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], six.b('10'))
        self.assertEqual(o[1], six.b('30'))
        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename1))

        tmp_fn = self.generate_tmpfile_name("aa","qsql")
        os.rename(expected_cache_filename1,tmp_fn)

        cmd = '%s "select a from %s"' % (Q_EXECUTABLE,tmp_fn)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], six.b('10'))
        self.assertEqual(o[1], six.b('30'))


    def test_reading_the_wrong_cache__qsql_file_not_having_a_matching_content_signature(self):
        # create a file, along with its qsql
        file_data1 = six.b("a,b,c\n10,20,30\n30,40,50")

        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], six.b('10'))
        self.assertEqual(o[1], six.b('30'))
        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename1))

        file_data2 = six.b("c,d,e\n10,20,30\n30,40,50")

        # create another file with a different header, along with its qsql
        tmpfile2 = self.create_file_with_data(file_data2)
        tmpfile2_folder = os.path.dirname(tmpfile2.name)
        tmpfile2_filename = os.path.basename(tmpfile2.name)
        expected_cache_filename2 = os.path.join(tmpfile2_folder,tmpfile2_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select c from %s" -C readwrite' % tmpfile2.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], six.b('10'))
        self.assertEqual(o[1], six.b('30'))
        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename2))

        # now take the second qsql file as if it was the first. Execution on file 1 should fail, since the qsql file
        # does not really contain the table we're after

        os.remove(expected_cache_filename1)
        os.rename(expected_cache_filename2,expected_cache_filename1)

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 80)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        x = six.b("%s vs %s.qsql: Content Signatures for table %s differ at inferer.header_row (source value '['a', 'b', 'c']' disk signature value '['c', 'd', 'e']')" % (tmpfile1.name,tmpfile1.name,tmpfile1.name))
        self.assertEqual(e[0], x)

    def test_reading_the_wrong_cache__qsql_file_not_having_any_content_signature(self):
        # create a file, along with its qsql
        file_data1 = six.b("a,b,c\n10,20,30\n30,40,50")

        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], six.b('10'))
        self.assertEqual(o[1], six.b('30'))
        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename1))

        file_data2 = six.b("c,d,e\n10,20,30\n30,40,50")

        # delete qcatalog content, so no entries will be available
        c = sqlite3.connect(expected_cache_filename1)
        c.execute('delete from _qcatalog').fetchall()
        c.commit()
        c.close()

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 97)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertEqual(e[0],six.b("Could not autodetect table name in qsql file. File contains no record of a table"))


    def test_cache_full_flow(self):
        file_data = six.b("a,b,c\n10,20,30\n30,40,50")
        tmpfile = self.create_file_with_data(file_data)
        tmpfile_folder = os.path.dirname(tmpfile.name)
        tmpfile_filename = os.path.basename(tmpfile.name)
        expected_tmpfile_table_name = tmpfile_filename
        expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C none' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0],six.b('10'))
        self.assertEqual(o[1],six.b('30'))

        # Ensure cache has not been created
        self.assertTrue(not os.path.exists(expected_cache_filename))

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0],six.b('10'))
        self.assertEqual(o[1],six.b('30'))

        # Ensure cache has not been created, as cache mode is "read" only
        self.assertTrue(not os.path.exists(expected_cache_filename))

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0],six.b('10'))
        self.assertEqual(o[1],six.b('30'))

        # After readwrite caching has been activated, the cache file is expected to exist
        self.assertTrue(os.path.exists(expected_cache_filename))

        # Read the cache file directly, to make sure it's a valid sqlite file
        db = sqlite3.connect(expected_cache_filename)
        table_list = db.execute("select content_signature_key,temp_table_name,content_signature,creation_time,source_type,source from _qcatalog where temp_table_name == '%s'" % expected_tmpfile_table_name).fetchall()
        self.assertTrue(len(table_list) == 1)
        table_metadata = table_list[0]
        results = db.execute("select * from %s" % table_metadata[1]).fetchall()
        self.assertEqual(results[0],(10,20,30))
        self.assertEqual(results[1],(30,40,50))

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0],six.b('10'))
        self.assertEqual(o[1],six.b('30'))

        # After readwrite caching has been activated, the cache file is expected to exist
        self.assertTrue(os.path.exists(expected_cache_filename))

        self.cleanup(tmpfile)

    def test_cache_full_flow_with_concatenated_files(self):
        file_data1 = six.b("a,b,c\n10,11,12\n20,21,22")
        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        file_data2 = six.b("a,b,c\n30,31,32\n40,41,42")
        tmpfile2 = self.create_file_with_data(file_data2)
        tmpfile2_folder = os.path.dirname(tmpfile2.name)
        tmpfile2_filename = os.path.basename(tmpfile2.name)
        expected_cache_filename2 = os.path.join(tmpfile2_folder,tmpfile2_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -O -H -d , "select * from (select * from %s UNION ALL select * from %s)" -C readwrite' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 5)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('a,b,c'))
        self.assertEqual(o[1],six.b('10,11,12'))
        self.assertEqual(o[2],six.b('20,21,22'))
        self.assertEqual(o[3],six.b('30,31,32'))
        self.assertEqual(o[4],six.b('40,41,42'))

        self.assertTrue(os.path.exists(expected_cache_filename1))
        self.assertTrue(os.path.exists(expected_cache_filename2))

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)


    def test_analyze_result_with_cache_file(self):
        file_data = six.b("a,b,c\n10,20,30\n30,40,50")
        tmpfile = self.create_file_with_data(file_data)
        tmpfile_folder = os.path.dirname(tmpfile.name)
        tmpfile_filename = os.path.basename(tmpfile.name)
        expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        # Ensure cache has not been created yet
        self.assertTrue(not os.path.exists(expected_cache_filename))

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0],six.b('10'))
        self.assertEqual(o[1],six.b('30'))

        # Ensure cache is now created
        self.assertTrue(os.path.exists(expected_cache_filename))

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),7)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: qsql-file-with-original source: %s.qsql' % tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4],six.b('    `a` - int'))
        self.assertEqual(o[5],six.b('    `b` - int'))
        self.assertEqual(o[6],six.b('    `c` - int'))

        # delete the newly created cache
        os.remove(expected_cache_filename)

        # Now rerun the analysis without the cache file
        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),7)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4],six.b('    `a` - int'))
        self.assertEqual(o[5],six.b('    `b` - int'))
        self.assertEqual(o[6],six.b('    `c` - int'))

        self.cleanup(tmpfile)

    def test_partial_caching_exists(self):
        file1_data = six.b("a,b,c\n10,20,30\n30,40,50\n60,70,80")
        tmpfile1 = self.create_file_with_data(file1_data)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        file2_data = six.b("b,x\n10,linewith10\n20,linewith20\n30,linewith30\n40,linewith40")
        tmpfile2 = self.create_file_with_data(file2_data)
        tmpfile2_folder = os.path.dirname(tmpfile2.name)
        tmpfile2_filename = os.path.basename(tmpfile2.name)
        expected_cache_filename2 = os.path.join(tmpfile2_folder,tmpfile2_filename + '.qsql')

        # Use only first file, and cache
        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0],six.b('10'))
        self.assertEqual(o[1],six.b('30'))

        # Ensure cache has been created for file 1
        self.assertTrue(os.path.exists(expected_cache_filename1))

        # Use both files with read caching, one should be read from cache, the other from the file
        cmd = Q_EXECUTABLE + ' -H -d , "select file1.a,file1.b,file1.c,file2.x from %s file1 left join %s file2 on (file1.b = file2.b)" -C read' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('10,20,30,linewith20'))
        self.assertEqual(o[1],six.b('30,40,50,linewith40'))
        self.assertEqual(o[2],six.b('60,70,80,'))

        # Ensure cache has NOT been created for file 2
        self.assertTrue(not os.path.exists(expected_cache_filename2))

        # Now rerun the query, this time with readwrite caching, so the second file cache will be written
        cmd = Q_EXECUTABLE + ' -H -d , "select file1.a,file1.b,file1.c,file2.x from %s file1 left join %s file2 on (file1.b = file2.b)" -C readwrite' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],six.b('10,20,30,linewith20'))
        self.assertEqual(o[1],six.b('30,40,50,linewith40'))
        self.assertEqual(o[2],six.b('60,70,80,'))

        # Ensure cache has now been created for file 2
        self.assertTrue(os.path.exists(expected_cache_filename2))

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)


class UserFunctionTests(AbstractQTestCase):
    def test_regexp_int_data_handling(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d , "select c2 from %s where regexp(\'^1\',c2)"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b("1"))

        self.cleanup(tmpfile)

    def test_percentile_func(self):
        cmd = 'seq 1000 1999 | %s "select substr(c1,0,3),percentile(c1,0),percentile(c1,0.5),percentile(c1,1) from - group by substr(c1,0,3)" -c 1' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 10)
        self.assertEqual(len(e), 0)

        output_table = [l.split(six.b(" ")) for l in o]
        group_labels = [int(row[0]) for row in output_table]
        minimum_values = [float(row[1]) for row in output_table]
        median_values = [float(row[2]) for row in output_table]
        max_values = [float(row[3]) for row in output_table]

        base_values = list(range(1000,2000,100))

        self.assertEqual(group_labels,list(range(10,20)))
        self.assertEqual(minimum_values,base_values)
        self.assertEqual(median_values,list(map(lambda x: x + 49.5,base_values)))
        self.assertEqual(max_values,list(map(lambda x: x + 99,base_values)))

    def test_regexp_null_data_handling(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d , "select count(*) from %s where regexp(\'^\',c2)"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b("2"))

        self.cleanup(tmpfile)

    def test_md5_function(self):
        cmd = 'seq 1 4 | %s -c 1 -d , "select c1,md5(c1,\'utf-8\') from -"' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),4)
        self.assertEqual(len(e),0)

        self.assertEqual(tuple(o[0].split(six.b(','),1)),(six.b('1'),six.b('c4ca4238a0b923820dcc509a6f75849b')))
        self.assertEqual(tuple(o[1].split(six.b(','),1)),(six.b('2'),six.b('c81e728d9d4c2f636f067f89cc14862c')))
        self.assertEqual(tuple(o[2].split(six.b(','),1)),(six.b('3'),six.b('eccbc87e4b5ce2fe28308fd9f2a7baf3')))
        self.assertEqual(tuple(o[3].split(six.b(','),1)),(six.b('4'),six.b('a87ff679a2f3e71d9181a67b7542122c')))

    def test_stddev_functions(self):
        tmpfile = self.create_file_with_data(six.b("\n".join(map(str,[234,354,3234,123,4234,234,634,56,65]))))

        cmd = '%s -c 1 -d , "select round(stddev_pop(c1),10),round(stddev_sample(c1),10) from %s"' % (Q_EXECUTABLE,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('1479.7015464838,1569.4604964764'))

        self.cleanup(tmpfile)

    def test_sqrt_function(self):
        cmd = 'seq 1 5 | %s -c 1 -d , "select round(sqrt(c1),10) from -"' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),5)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('1.0'))
        self.assertEqual(o[1],six.b('1.4142135624'))
        self.assertEqual(o[2],six.b('1.7320508076'))
        self.assertEqual(o[3],six.b('2.0'))
        self.assertEqual(o[4],six.b('2.2360679775'))

    def test_power_function(self):
        cmd = 'seq 1 5 | %s -c 1 -d , "select round(power(c1,2.5),10) from -"' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),5)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('1.0'))
        self.assertEqual(o[1],six.b('5.6568542495'))
        self.assertEqual(o[2],six.b('15.5884572681'))
        self.assertEqual(o[3],six.b('32.0'))
        self.assertEqual(o[4],six.b('55.9016994375'))

    def test_file_functions(self):
        filenames = [
            "file1",
            "file2.csv",
            "/var/tmp/file3",
            "/var/tmp/file4.gz",
            ""
        ]
        data = "\n".join(filenames)

        cmd = 'echo "%s" | %s -c 1 -d , "select file_folder(c1),file_ext(c1),file_basename(c1),file_basename_no_ext(c1) from -"' % (data,Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),5)
        self.assertEqual(len(e),0)
        self.assertEqual(o,[
            b',,file1,file1',
            b',.csv,file2.csv,file2',
            b'/var/tmp,,file3,file3',
            b'/var/tmp,.gz,file4.gz,file4',
            b',,,'
        ])


    def test_sha1_function(self):
        cmd = 'seq 1 4 | %s -c 1 -d , "select c1,sha1(c1) from -"' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),4)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('1,356a192b7913b04c54574d18c28d46e6395428ab'))
        self.assertEqual(o[1],six.b('2,da4b9237bacccdf19c0760cab7aec4a8359010b0'))
        self.assertEqual(o[2],six.b('3,77de68daecd823babbb58edb1c8e14d7106e83bb'))
        self.assertEqual(o[3],six.b('4,1b6453892473a467d07372d45eb05abc2031647a'))

    def test_regexp_extract_function(self):
        query = """
            select 
              regexp_extract('was ([0-9]+) seconds and ([0-9]+) ms',c1,0),
              regexp_extract('was ([0-9]+) seconds and ([0-9]+) ms',c1,1),
              regexp_extract('non-existent-(regexp)',c1,0) 
            from
              -
        """

        cmd = 'echo "Duration was 322 seconds and 240 ms" | %s -c 1 -d , "%s"' % (Q_EXECUTABLE,query)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('322,240,'))

    def test_sha_function(self):
        cmd = 'seq 1 4 | %s -c 1 -d , "select c1,sha(c1,1,\'utf-8\') as sha1,sha(c1,224,\'utf-8\') as sha224,sha(c1,256,\'utf-8\') as sha256 from -"' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),4)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('1,356a192b7913b04c54574d18c28d46e6395428ab,e25388fde8290dc286a6164fa2d97e551b53498dcbf7bc378eb1f178,6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b'))
        self.assertEqual(o[1],six.b('2,da4b9237bacccdf19c0760cab7aec4a8359010b0,58b2aaa0bfae7acc021b3260e941117b529b2e69de878fd7d45c61a9,d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35'))
        self.assertEqual(o[2],six.b('3,77de68daecd823babbb58edb1c8e14d7106e83bb,4cfc3a1811fe40afa401b25ef7fa0379f1f7c1930a04f8755d678474,4e07408562bedb8b60ce05c1decfe3ad16b72230967de01f640b7e4729b49fce'))
        self.assertEqual(o[3],six.b('4,1b6453892473a467d07372d45eb05abc2031647a,271f93f45e9b4067327ed5c8cd30a034730aaace4382803c3e1d6c2f,4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a'))


class MultiHeaderTests(AbstractQTestCase):
    def test_output_header_when_multiple_input_headers_exist(self):
        TMPFILE_COUNT = 5
        tmpfiles = [self.create_file_with_data(sample_data_with_header) for x in range(TMPFILE_COUNT)]

        tmpfilenames = " UNION ALL ".join(map(lambda x:"select * from %s" % x.name, tmpfiles))

        cmd = Q_EXECUTABLE + ' -d , "select name,value1,value2 from (%s) order by name" -H -O' % tmpfilenames
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), TMPFILE_COUNT*3+1)
        self.assertEqual(o[0], six.b("name,value1,value2"))

        for i in range (TMPFILE_COUNT):
            self.assertEqual(o[1+i],sample_data_rows[0])
        for i in range (TMPFILE_COUNT):
            self.assertEqual(o[TMPFILE_COUNT+1+i],sample_data_rows[1])
        for i in range (TMPFILE_COUNT):
            self.assertEqual(o[TMPFILE_COUNT*2+1+i],sample_data_rows[2])

        for oi in o[1:]:
            self.assertTrue(six.b('name') not in oi)

        for i in range(TMPFILE_COUNT):
            self.cleanup(tmpfiles[i])

    def test_output_header_when_extra_header_column_names_are_different__concatenation_replacement(self):
        tmpfile1 = self.create_file_with_data(sample_data_with_header)
        tmpfile2 = self.create_file_with_data(generate_sample_data_with_header(six.b('othername,value1,value2')))

        cmd = Q_EXECUTABLE + ' -d , "select name,value1,value2 from (select * from %s union all select * from %s) order by name" -H -O' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 7)
        self.assertEqual(len(e), 0)
        self.assertTrue(o, [
            six.b('name,value1,value2'),
            six.b('a,1,0'),
            six.b('a,1,0'),
            six.b('b,2,0'),
            six.b('b,2,0'),
            six.b('c,,0'),
            six.b('c,,0')
        ])

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_output_header_when_extra_header_has_different_number_of_columns(self):
        tmpfile1 = self.create_file_with_data(sample_data_with_header)
        tmpfile2 = self.create_file_with_data(generate_sample_data_with_header(six.b('name,value1')))

        cmd = Q_EXECUTABLE + ' -d , "select name,value1,value2 from (select * from %s UNION ALL select * from %s) order by name" -H -O' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 7)
        self.assertEqual(len(e), 0)
        self.assertTrue(o, [
            six.b('name,value1,value2'),
            six.b('a,1,0'),
            six.b('a,1,0'),
            six.b('b,2,0'),
            six.b('b,2,0'),
            six.b('c,,0'),
            six.b('c,,0')
        ])

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)


class ParsingModeTests(AbstractQTestCase):

    def test_strict_mode_column_count_mismatch_error(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = Q_EXECUTABLE + ' -m strict "select count(*) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(six.b("Column Count is expected to identical") in e[0])

        self.cleanup(tmpfile)

    def test_strict_mode_too_large_specific_column_count(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -m strict -c 4 "select count(*) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertEqual(
            e[0], six.b("Strict mode. Column count is expected to be 4 but is 3"))

        self.cleanup(tmpfile)

    def test_strict_mode_too_small_specific_column_count(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -m strict -c 2 "select count(*) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertEqual(
            e[0], six.b("Strict mode. Column count is expected to be 2 but is 3"))

        self.cleanup(tmpfile)

    def test_relaxed_mode_missing_columns_in_header(self):
        tmpfile = self.create_file_with_data(
            sample_data_with_missing_header_names)
        cmd = Q_EXECUTABLE + ' -d , -m relaxed "select count(*) from %s" -H -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 7)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s') % six.b(tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4],six.b('    `name` - text'))
        self.assertEqual(o[5],six.b('    `value1` - int'))
        self.assertEqual(o[6],six.b('    `c3` - int'))

        self.cleanup(tmpfile)

    def test_strict_mode_missing_columns_in_header(self):
        tmpfile = self.create_file_with_data(
            sample_data_with_missing_header_names)
        cmd = Q_EXECUTABLE + ' -d , -m strict "select count(*) from %s" -H -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertEqual(
            e[0], six.b('Strict mode. Header row contains less columns than expected column count(2 vs 3)'))

        self.cleanup(tmpfile)

    def test_output_delimiter_with_missing_fields(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select * from %s" -D ";"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('a;1;0'))
        self.assertEqual(o[1], six.b('b;2;0'))
        self.assertEqual(o[2], six.b('c;;0'))

        self.cleanup(tmpfile)

    def test_handling_of_null_integers(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select avg(c2) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('1.5'))

        self.cleanup(tmpfile)

    def test_empty_integer_values_converted_to_null(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select * from %s where c2 is null"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('c,,0'))

        self.cleanup(tmpfile)

    def test_empty_string_values_not_converted_to_null(self):
        tmpfile = self.create_file_with_data(
            sample_data_with_empty_string_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select * from %s where c2 == %s"' % (
            tmpfile.name, "''")
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('c,,0'))

        self.cleanup(tmpfile)

    def test_relaxed_mode_detected_columns(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = Q_EXECUTABLE + ' -m relaxed "select count(*) from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)

        column_rows = o[o.index(six.b('  Fields:'))+1:]

        self.assertEqual(len(column_rows), 11)

        column_tuples = [x.strip().split(six.b(" ")) for x in column_rows]
        column_info = [(x[0], x[2]) for x in column_tuples]
        column_names = [x[0] for x in column_tuples]
        column_types = [x[2] for x in column_tuples]

        self.assertEqual(column_names, [six.b('`c{}`'.format(x)) for x in range(1, 12)])
        self.assertEqual(column_types, list(map(lambda x:six.b(x),[
                          'text', 'int', 'text', 'text', 'int', 'text', 'int', 'int', 'text', 'text', 'text'])))

        self.cleanup(tmpfile)

    def test_relaxed_mode_detected_columns_with_specific_column_count(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = Q_EXECUTABLE + ' -m relaxed "select count(*) from %s" -A -c 9' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)

        column_rows = o[o.index(six.b('  Fields:'))+1:]

        self.assertEqual(len(column_rows), 9)

        column_tuples = [x.strip().split(six.b(" ")) for x in column_rows]
        column_info = [(x[0], x[2]) for x in column_tuples]
        column_names = [x[0] for x in column_tuples]
        column_types = [x[2] for x in column_tuples]

        self.assertEqual(column_names, [six.b('`c{}`'.format(x)) for x in range(1, 10)])
        self.assertEqual(
            column_types, list(map(lambda x:six.b(x),['text', 'int', 'text', 'text', 'int', 'text', 'int', 'int', 'text'])))

        self.cleanup(tmpfile)

    def test_relaxed_mode_last_column_data_with_specific_column_count(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c9 from %s" -c 9' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 9)
        self.assertEqual(len(e), 0)

        expected_output = list(map(lambda x:six.b(x),["/selinux", "/mnt", "/srv", "/lost+found", '"/initrd.img.old -> /boot/initrd.img-3.8.0-19-generic"',
                           "/cdrom", "/home", '"/vmlinuz -> boot/vmlinuz-3.8.0-19-generic"', '"/initrd.img -> boot/initrd.img-3.8.0-19-generic"']))

        self.assertEqual(o, expected_output)

        self.cleanup(tmpfile)

    def test_1_column_warning_in_relaxed_mode(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c1 from %s" -d ,' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('data without commas 1'))
        self.assertEqual(o[1],six.b('data without commas 2'))

        self.cleanup(tmpfile)

    def test_1_column_warning_in_strict_mode(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c1 from %s" -d , -m strict' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('data without commas 1'))
        self.assertEqual(o[1],six.b('data without commas 2'))

        self.cleanup(tmpfile)


    def test_1_column_warning_suppression_in_relaxed_mode_when_column_count_is_specific(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c1 from %s" -d , -m relaxed -c 1' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('data without commas 1'))
        self.assertEqual(o[1],six.b('data without commas 2'))

        self.cleanup(tmpfile)

    def test_1_column_warning_suppression_in_strict_mode_when_column_count_is_specific(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c1 from %s" -d , -m strict -c 1' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('data without commas 1'))
        self.assertEqual(o[1],six.b('data without commas 2'))

        self.cleanup(tmpfile)

    def test_fluffy_mode__as_relaxed_mode(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c9 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 9)
        self.assertEqual(len(e), 0)

        expected_output = list(map(lambda x:six.b(x),["/selinux", "/mnt", "/srv", "/lost+found",
                           "/initrd.img.old", "/cdrom", "/home", "/vmlinuz", "/initrd.img"]))

        self.assertEqual(o, expected_output)

        self.cleanup(tmpfile)

    def test_relaxed_mode_column_count_mismatch__was_previously_fluffy_mode_test(self):
        data_row = six.b("column1 column2 column3 column4")
        data_list = [data_row] * 1000
        data_list[950] = six.b("column1 column2 column3 column4 column5")
        tmpfile = self.create_file_with_data(six.b("\n").join(data_list))

        cmd = Q_EXECUTABLE + ' -m relaxed "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),1000)
        self.assertEqual(len(e),0)
        self.assertEqual(o[950],six.b('column1 column2 column3 "column4 column5"'))

        self.cleanup(tmpfile)

    def test_strict_mode_column_count_mismatch__less_columns(self):
        data_row = six.b("column1 column2 column3 column4")
        data_list = [data_row] * 1000
        data_list[750] = six.b("column1 column3 column4")
        tmpfile = self.create_file_with_data(six.b("\n").join(data_list))

        cmd = Q_EXECUTABLE + ' -m strict "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertTrue(e[0].startswith(six.b("Strict mode - Expected 4 columns instead of 3 columns")))
        self.assertTrue(six.b(' row 751.') in e[0])

        self.cleanup(tmpfile)

    def test_strict_mode_column_count_mismatch__more_columns(self):
        data_row = six.b("column1 column2 column3 column4")
        data_list = [data_row] * 1000
        data_list[750] = six.b("column1 column2 column3 column4 column5")
        tmpfile = self.create_file_with_data(six.b("\n").join(data_list))

        cmd = Q_EXECUTABLE + ' -m strict "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertTrue(e[0].startswith(six.b("Strict mode - Expected 4 columns instead of 5 columns")))
        self.assertTrue(six.b(' row 751.') in e[0])

        self.cleanup(tmpfile)


class FormattingTests(AbstractQTestCase):

    def test_column_formatting(self):
        # TODO Decide if this breaking change is reasonable
        #cmd = 'seq 1 10 | ' + Q_EXECUTABLE + ' -f 1=%4.3f,2=%4.3f "select sum(c1),avg(c1) from -" -c 1'
        cmd = 'seq 1 10 | ' + Q_EXECUTABLE + ' -f 1={:4.3f},2={:4.3f} "select sum(c1),avg(c1) from -" -c 1'

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('55.000 5.500'))

    def test_column_formatting_with_output_header(self):
        perl_regex = "'s/1\n/column_name\n1\n/;'"
        # TODO Decide if this breaking change is reasonable
        #cmd = 'seq 1 10 | perl -pe ' + perl_regex + ' | ' + Q_EXECUTABLE + ' -f 1=%4.3f,2=%4.3f "select sum(column_name) mysum,avg(column_name) myavg from -" -c 1 -H -O'
        cmd = 'seq 1 10 | LANG=C perl -pe ' + perl_regex + ' | ' + Q_EXECUTABLE + ' -f 1={:4.3f},2={:4.3f} "select sum(column_name) mysum,avg(column_name) myavg from -" -c 1 -H -O'

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('mysum myavg'))
        self.assertEqual(o[1], six.b('55.000 5.500'))

    def py3_test_successfully_parse_universal_newlines_without_explicit_flag(self):
        def list_as_byte_list(l):
            return list(map(lambda x:six.b(x),l))

        expected_output = list(map(lambda x:list_as_byte_list(x),[['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-May-07', '6850000', 'USD', 'b'],
                           ['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-Oct-06', '6000000', 'USD', 'a'],
                           ['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-Jan-08', '25000000', 'USD', 'c'],
                           ['mycityfaces', 'MyCityFaces', '7', 'web', 'Scottsdale', 'AZ', '1-Jan-08', '50000', 'USD', 'seed'],
                           ['flypaper', 'Flypaper', '', 'web', 'Phoenix', 'AZ', '1-Feb-08', '3000000', 'USD', 'a'],
                           ['infusionsoft', 'Infusionsoft', '105', 'software', 'Gilbert', 'AZ', '1-Oct-07', '9000000', 'USD', 'a']]))

        data = six.b('permalink,company,numEmps,category,city,state,fundedDate,raisedAmt,raisedCurrency,round\rlifelock,LifeLock,,web,Tempe,AZ,1-May-07,6850000,USD,b\rlifelock,LifeLock,,web,Tempe,AZ,1-Oct-06,6000000,USD,a\rlifelock,LifeLock,,web,Tempe,AZ,1-Jan-08,25000000,USD,c\rmycityfaces,MyCityFaces,7,web,Scottsdale,AZ,1-Jan-08,50000,USD,seed\rflypaper,Flypaper,,web,Phoenix,AZ,1-Feb-08,3000000,USD,a\rinfusionsoft,Infusionsoft,105,software,Gilbert,AZ,1-Oct-07,9000000,USD,a')
        tmp_data_file = self.create_file_with_data(data)

        cmd = Q_EXECUTABLE + ' -d , -H "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 6)

        actual_output = list(map(lambda row: row.split(six.b(",")),o))

        self.assertEqual(actual_output,expected_output)

        self.cleanup(tmp_data_file)

    test_parsing_universal_newlines_without_explicit_flag = py3_test_successfully_parse_universal_newlines_without_explicit_flag

    def test_universal_newlines_parsing_flag(self):
        def list_as_byte_list(l):
            return list(map(lambda x:six.b(x),l))

        expected_output = list(map(lambda x:list_as_byte_list(x),[['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-May-07', '6850000', 'USD', 'b'],
                           ['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-Oct-06', '6000000', 'USD', 'a'],
                           ['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-Jan-08', '25000000', 'USD', 'c'],
                           ['mycityfaces', 'MyCityFaces', '7', 'web', 'Scottsdale', 'AZ', '1-Jan-08', '50000', 'USD', 'seed'],
                           ['flypaper', 'Flypaper', '', 'web', 'Phoenix', 'AZ', '1-Feb-08', '3000000', 'USD', 'a'],
                           ['infusionsoft', 'Infusionsoft', '105', 'software', 'Gilbert', 'AZ', '1-Oct-07', '9000000', 'USD', 'a']]))

        data = six.b('permalink,company,numEmps,category,city,state,fundedDate,raisedAmt,raisedCurrency,round\rlifelock,LifeLock,,web,Tempe,AZ,1-May-07,6850000,USD,b\rlifelock,LifeLock,,web,Tempe,AZ,1-Oct-06,6000000,USD,a\rlifelock,LifeLock,,web,Tempe,AZ,1-Jan-08,25000000,USD,c\rmycityfaces,MyCityFaces,7,web,Scottsdale,AZ,1-Jan-08,50000,USD,seed\rflypaper,Flypaper,,web,Phoenix,AZ,1-Feb-08,3000000,USD,a\rinfusionsoft,Infusionsoft,105,software,Gilbert,AZ,1-Oct-07,9000000,USD,a')
        tmp_data_file = self.create_file_with_data(data)

        cmd = Q_EXECUTABLE + ' -d , -H -U "select permalink,company,numEmps,category,city,state,fundedDate,raisedAmt,raisedCurrency,round from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)

        if len(e) == 2 or len(e) == 1:
            # In python 3.7, there's a deprecation warning for the 'U' file opening mode, which is ok for now
            self.assertIn(len(e), [1,2])
            self.assertTrue(b"DeprecationWarning: 'U' mode is deprecated" in e[0])
        elif len(e) != 0:
            # Nothing should be output to stderr in other versions
            self.assertTrue(False,msg='Unidentified output in stderr')

        self.assertEqual(len(o), 6)

        actual_output = list(map(lambda row: row.split(six.b(",")),o))

        self.assertEqual(actual_output,expected_output)

        self.cleanup(tmp_data_file)



class SqlTests(AbstractQTestCase):

    def test_find_example(self):
        tmpfile = self.create_file_with_data(find_output)
        cmd = Q_EXECUTABLE + ' "select c5,c6,sum(c7)/1024.0/1024 as total from %s group by c5,c6 order by total desc"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('mapred mapred 0.9389581680297852'))
        self.assertEqual(o[1], six.b('root root 0.02734375'))
        self.assertEqual(o[2], six.b('harel harel 0.010888099670410156'))

        self.cleanup(tmpfile)

    def test_join_example(self):
        cmd = Q_EXECUTABLE + ' "select myfiles.c8,emails.c2 from {0}/exampledatafile myfiles join {0}/group-emails-example emails on (myfiles.c4 = emails.c1) where myfiles.c8 = \'ppp\'"'.format(EXAMPLES)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)

        self.assertEqual(o[0], six.b('ppp dip.1@otherdomain.com'))
        self.assertEqual(o[1], six.b('ppp dip.2@otherdomain.com'))

    def test_join_example_with_output_header(self):
        cmd = Q_EXECUTABLE + ' -O "select myfiles.c8 aaa,emails.c2 bbb from {0}/exampledatafile myfiles join {0}/group-emails-example emails on (myfiles.c4 = emails.c1) where myfiles.c8 = \'ppp\'"'.format(EXAMPLES)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0], six.b('aaa bbb'))
        self.assertEqual(o[1], six.b('ppp dip.1@otherdomain.com'))
        self.assertEqual(o[2], six.b('ppp dip.2@otherdomain.com'))

    def test_self_join1(self):
        tmpfile = self.create_file_with_data(six.b("\n").join([six.b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = Q_EXECUTABLE + ' "select * from %s a1 join %s a2 on (a1.c1 = a2.c1)"' % (tmpfile.name,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 10)

        self.cleanup(tmpfile)

    def test_self_join_reuses_table(self):
        tmpfile = self.create_file_with_data(six.b("\n").join([six.b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = Q_EXECUTABLE + ' "select * from %s a1 join %s a2 on (a1.c1 = a2.c1)" -A' % (tmpfile.name,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 6)

        self.assertEqual(o[0],six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s') % six.b(tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4],six.b('    `c1` - int'))
        self.assertEqual(o[5],six.b('    `c2` - int'))

        self.cleanup(tmpfile)

    def test_self_join2(self):
        tmpfile1 = self.create_file_with_data(six.b("\n").join([six.b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = Q_EXECUTABLE + ' "select * from %s a1 join %s a2 on (a1.c2 = a2.c2)"' % (tmpfile1.name,tmpfile1.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 10*10)

        self.cleanup(tmpfile1)

        tmpfile2 = self.create_file_with_data(six.b("\n").join([six.b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = Q_EXECUTABLE + ' "select * from %s a1 join %s a2 on (a1.c2 = a2.c2) join %s a3 on (a1.c2 = a3.c2)"' % (tmpfile2.name,tmpfile2.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 10*10*10)

        self.cleanup(tmpfile2)

    def test_disable_column_type_detection(self):
        tmpfile = self.create_file_with_data(six.b('''regular_text,text_with_digits1,text_with_digits2,float_number
"regular text 1",67,"67",12.3
"regular text 2",067,"067",22.3
"regular text 3",123,"123",33.4
"regular text 4",-123,"-123",0122.2
'''))

        # Check original column type detection
        cmd = Q_EXECUTABLE + ' -A -d , -H "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 8)

        self.assertEqual(o[0],six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1], six.b('  Sources:'))
        self.assertEqual(o[2], six.b('    source_type: file source: %s') % six.b(tmpfile.name))
        self.assertEqual(o[3], six.b('  Fields:'))
        self.assertEqual(o[4], six.b('    `regular_text` - text'))
        self.assertEqual(o[5], six.b('    `text_with_digits1` - int'))
        self.assertEqual(o[6], six.b('    `text_with_digits2` - int'))
        self.assertEqual(o[7], six.b('    `float_number` - real'))

        # Check column types detected when actual detection is disabled
        cmd = Q_EXECUTABLE + ' -A -d , -H --as-text "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 8)

        self.assertEqual(o[0],six.b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s') % six.b(tmpfile.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4],six.b('    `regular_text` - text'))
        self.assertEqual(o[5],six.b('    `text_with_digits1` - text'))
        self.assertEqual(o[6],six.b('    `text_with_digits2` - text'))
        self.assertEqual(o[7],six.b('    `float_number` - text'))

        # Get actual data with regular detection
        cmd = Q_EXECUTABLE + ' -d , -H "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 4)

        self.assertEqual(o[0],six.b("regular text 1,67,67,12.3"))
        self.assertEqual(o[1],six.b("regular text 2,67,67,22.3"))
        self.assertEqual(o[2],six.b("regular text 3,123,123,33.4"))
        self.assertEqual(o[3],six.b("regular text 4,-123,-123,122.2"))

        # Get actual data without detection
        cmd = Q_EXECUTABLE + ' -d , -H --as-text "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 4)

        self.assertEqual(o[0],six.b("regular text 1,67,67,12.3"))
        self.assertEqual(o[1],six.b("regular text 2,067,067,22.3"))
        self.assertEqual(o[2],six.b("regular text 3,123,123,33.4"))
        self.assertEqual(o[3],six.b("regular text 4,-123,-123,0122.2"))

        self.cleanup(tmpfile)


class BasicModuleTests(AbstractQTestCase):

    def test_engine_isolation(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("d e f\n10 20 30\n40 50 60"))

        # Run file 1 on engine 1
        q1 = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r = q1.execute('select * from %s' % tmpfile1.name)
        print("QueryQuery",file=sys.stdout)

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r.data,[(1,2,3),(4,5,6)])
        self.assertTrue(tmpfile1.name in r.metadata.table_structures)
        self.assertTrue(tmpfile1.name in r.metadata.new_table_structures)
        self.assertEqual(r.metadata.table_structures[tmpfile1.name].atomic_fns,[tmpfile1.name])
        self.assertEqual(r.metadata.table_structures[tmpfile1.name].source_type,'file')
        self.assertEqual(r.metadata.table_structures[tmpfile1.name].source,tmpfile1.name)

        # run file 1 on engine 2
        q2 = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r2 = q2.execute('select * from %s' % tmpfile1.name)
        print("QueryQuery",file=sys.stdout)

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r2.warnings),0)
        self.assertEqual(len(r2.data),2)
        self.assertEqual(r2.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r2.data,[(1,2,3),(4,5,6)])
        self.assertTrue(tmpfile1.name in r2.metadata.table_structures)
        self.assertTrue(tmpfile1.name in r2.metadata.new_table_structures)
        self.assertEqual(r2.metadata.table_structures[tmpfile1.name].atomic_fns,[tmpfile1.name])
        self.assertEqual(r2.metadata.table_structures[tmpfile1.name].source_type,'file')
        self.assertEqual(r2.metadata.table_structures[tmpfile1.name].source,tmpfile1.name)

        # run file 2 on engine 1
        r3 = q1.execute('select * from %s' % tmpfile2.name)
        print("QueryQuery",file=sys.stdout)

        print(r3)
        self.assertTrue(r3.status == 'ok')
        self.assertEqual(len(r3.warnings),0)
        self.assertEqual(len(r3.data),2)
        self.assertEqual(r3.metadata.output_column_name_list,['d','e','f'])
        self.assertEqual(r3.data,[(10,20,30),(40,50,60)])
        self.assertTrue(tmpfile2.name in r3.metadata.table_structures)
        self.assertTrue(tmpfile2.name in r3.metadata.new_table_structures)
        self.assertEqual(r3.metadata.table_structures[tmpfile2.name].atomic_fns,[tmpfile2.name])
        self.assertEqual(r3.metadata.table_structures[tmpfile2.name].source,tmpfile2.name)
        self.assertEqual(r3.metadata.table_structures[tmpfile2.name].source_type,'file')

        q1.done()
        q2.done()

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_simple_query(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r = q.execute('select * from %s' % tmpfile.name)

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r.data,[(1,2,3),(4,5,6)])
        self.assertTrue(tmpfile.name in r.metadata.table_structures)
        self.assertTrue(tmpfile.name in r.metadata.new_table_structures)
        self.assertEqual(r.metadata.table_structures[tmpfile.name].atomic_fns,[tmpfile.name])
        self.assertEqual(r.metadata.table_structures[tmpfile.name].source_type,'file')
        self.assertEqual(r.metadata.table_structures[tmpfile.name].source,tmpfile.name)

        q.done()
        self.cleanup(tmpfile)

    def test_loaded_data_reuse(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r1 = q.execute('select * from %s' % tmpfile.name)

        r2 = q.execute('select * from %s' % tmpfile.name)

        self.assertTrue(r1.status == 'ok')
        self.assertEqual(len(r1.warnings),0)
        self.assertEqual(len(r1.data),2)
        self.assertEqual(r1.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r1.data,[(1,2,3),(4,5,6)])
        self.assertTrue(tmpfile.name in r1.metadata.table_structures)
        self.assertTrue(tmpfile.name in r1.metadata.new_table_structures)
        self.assertEqual(r1.metadata.table_structures[tmpfile.name].atomic_fns,[tmpfile.name])
        self.assertEqual(r1.metadata.table_structures[tmpfile.name].source_type,'file')
        self.assertEqual(r1.metadata.table_structures[tmpfile.name].source,tmpfile.name)

        self.assertTrue(r2.status == 'ok')
        self.assertTrue(tmpfile.name in r2.metadata.table_structures)
        self.assertTrue(tmpfile.name not in r2.metadata.new_table_structures)
        self.assertEqual(r2.data,r1.data)
        self.assertEqual(r2.metadata.output_column_name_list,r2.metadata.output_column_name_list)
        self.assertEqual(len(r2.warnings),0)

        q.done()

        self.cleanup(tmpfile)

    def test_stdin_injection(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        data_streams_dict = {
            '-': DataStream('stdin','-',codecs.open(tmpfile.name,'rb',encoding='utf-8'))
        }
        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '),data_streams_dict=data_streams_dict)
        r = q.execute('select * from -')

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r.data,[(1,2,3),(4,5,6)])
        self.assertEqual(r.metadata.new_table_structures['-'],r.metadata.table_structures['-'])
        self.assertEqual(r.metadata.table_structures['-'].column_names,['a','b','c'])
        self.assertEqual(r.metadata.table_structures['-'].python_column_types,[int,int,int])
        self.assertEqual(r.metadata.table_structures['-'].sqlite_column_types,['int','int','int'])
        self.assertEqual(r.metadata.table_structures['-'].source_type,'data-stream')
        self.assertEqual(r.metadata.table_structures['-'].source,'stdin')

        q.done()
        self.cleanup(tmpfile)

    def test_named_stdin_injection(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        data_streams_dict = {
            'my_stdin_data': DataStream('my_stdin_data','my_stdin_data',codecs.open(tmpfile.name,'rb',encoding='utf-8'))
        }

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '),data_streams_dict=data_streams_dict)
        r = q.execute('select a from my_stdin_data')

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a'])
        self.assertEqual(r.data,[(1,),(4,)])
        self.assertTrue('my_stdin_data' in r.metadata.table_structures)
        self.assertTrue('my_stdin_data' in r.metadata.new_table_structures)
        self.assertEqual(r.metadata.table_structures['my_stdin_data'].qtable_name,'my_stdin_data')

        q.done()
        self.cleanup(tmpfile)

    def test_data_stream_isolation(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("d e f\n7 8 9\n10 11 12"))

        data_streams_dict = {
            'a-': DataStream('a-','a-',codecs.open(tmpfile1.name, 'rb', encoding='utf-8')),
            'b-': DataStream('b-','b-',codecs.open(tmpfile2.name, 'rb', encoding='utf-8'))
        }

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '),data_streams_dict=data_streams_dict)
        r1 = q.execute('select * from a-')

        self.assertTrue(r1.status == 'ok')
        self.assertEqual(len(r1.warnings),0)
        self.assertEqual(len(r1.data),2)
        self.assertEqual(r1.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r1.data,[(1,2,3),(4,5,6)])
        self.assertTrue('a-' in r1.metadata.table_structures)
        self.assertEqual(len(r1.metadata.table_structures),1)
        self.assertEqual(r1.metadata.table_structures['a-'].source_type, 'data-stream')
        self.assertEqual(r1.metadata.table_structures['a-'].source, 'a-')
        self.assertEqual(r1.metadata.table_structures['a-'].column_names, ['a','b','c'])
        self.assertEqual(r1.metadata.table_structures['a-'].python_column_types, [int,int,int])
        self.assertEqual(r1.metadata.table_structures['a-'].sqlite_column_types, ['int','int','int'])

        r2 = q.execute('select * from b-')

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r2.warnings),0)
        self.assertEqual(len(r2.data),2)
        self.assertEqual(r2.metadata.output_column_name_list,['d','e','f'])
        self.assertEqual(r2.data,[(7,8,9),(10,11,12)])

        self.assertEqual(len(r1.metadata.table_structures),2)
        self.assertTrue('b-' in r1.metadata.table_structures)
        self.assertEqual(r1.metadata.table_structures['b-'].source_type, 'data-stream')
        self.assertEqual(r1.metadata.table_structures['b-'].source, 'b-')
        self.assertEqual(r1.metadata.table_structures['b-'].column_names, ['d','e','f'])
        self.assertEqual(r1.metadata.table_structures['b-'].python_column_types, [int,int,int])
        self.assertEqual(r1.metadata.table_structures['b-'].sqlite_column_types, ['int','int','int'])

        q.done()
        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_multiple_stdin_injection(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("d e f\n7 8 9\n10 11 12"))

        data_streams_dict = {
            'my_stdin_data1': DataStream('my_stdin_data1','my_stdin_data1',codecs.open(tmpfile1.name,'rb',encoding='utf-8')),
            'my_stdin_data2': DataStream('my_stdin_data2','my_stdin_data2',codecs.open(tmpfile2.name,'rb',encoding='utf-8'))
        }
        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '),data_streams_dict=data_streams_dict)
        r1 = q.execute('select * from my_stdin_data1')

        self.assertTrue(r1.status == 'ok')
        self.assertEqual(len(r1.warnings),0)
        self.assertEqual(len(r1.data),2)
        self.assertEqual(r1.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r1.data,[(1,2,3),(4,5,6)])
        self.assertTrue('my_stdin_data1' in r1.metadata.table_structures)
        self.assertTrue('my_stdin_data1' in r1.metadata.new_table_structures)
        self.assertEqual(r1.metadata.table_structures['my_stdin_data1'].qtable_name,'my_stdin_data1')

        r2 = q.execute('select * from my_stdin_data2')

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r2.warnings),0)
        self.assertEqual(len(r2.data),2)
        self.assertEqual(r2.metadata.output_column_name_list,['d','e','f'])
        self.assertEqual(r2.data,[(7,8,9),(10,11,12)])
        # There should be another data load, even though it's the same 'filename' as before
        self.assertTrue('my_stdin_data2' in r2.metadata.table_structures)
        self.assertTrue('my_stdin_data2' in r2.metadata.new_table_structures)
        self.assertEqual(r2.metadata.table_structures['my_stdin_data2'].qtable_name,'my_stdin_data2')

        r3 = q.execute('select aa.*,bb.* from my_stdin_data1 aa join my_stdin_data2 bb')

        self.assertTrue(r3.status == 'ok')
        self.assertEqual(len(r3.warnings),0)
        self.assertEqual(len(r3.data),4)
        self.assertEqual(r3.metadata.output_column_name_list,['a','b','c','d','e','f'])
        self.assertEqual(r3.data,[(1,2,3,7,8,9),(1,2,3,10,11,12),(4,5,6,7,8,9),(4,5,6,10,11,12)])
        self.assertTrue('my_stdin_data1' in r3.metadata.table_structures)
        self.assertTrue('my_stdin_data1' not in r3.metadata.new_table_structures)

        q.done()
        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_different_input_params_for_different_files(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("7\t8\t9\n10\t11\t12"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))

        q.load_data(tmpfile1.name,QInputParams(skip_header=True,delimiter=' '))
        q.load_data(tmpfile2.name,QInputParams(skip_header=False,delimiter='\t'))

        r = q.execute('select aa.*,bb.* from %s aa join %s bb' % (tmpfile1.name,tmpfile2.name))

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),4)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c','c1','c2','c3'])
        self.assertEqual(r.data,[(1,2,3,7,8,9),(1,2,3,10,11,12),(4,5,6,7,8,9),(4,5,6,10,11,12)])
        self.assertTrue(tmpfile1.name not in r.metadata.new_table_structures)
        self.assertTrue(tmpfile2.name not in r.metadata.new_table_structures)

        q.done()
        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_different_input_params_for_different_files_2(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("7\t8\t9\n10\t11\t12"))

        q = QTextAsData()

        q.load_data(tmpfile1.name,QInputParams(skip_header=True,delimiter=' '))
        q.load_data(tmpfile2.name,QInputParams(skip_header=False,delimiter='\t'))

        r = q.execute('select aa.*,bb.* from %s aa join %s bb' % (tmpfile1.name,tmpfile2.name))

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),4)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c','c1','c2','c3'])
        self.assertEqual(r.data,[(1,2,3,7,8,9),(1,2,3,10,11,12),(4,5,6,7,8,9),(4,5,6,10,11,12)])
        self.assertTrue(tmpfile1.name not in r.metadata.new_table_structures)
        self.assertTrue(tmpfile2.name not in r.metadata.new_table_structures)

        q.done()
        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_input_params_override(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        default_input_params = QInputParams()

        for k in default_input_params.__dict__.keys():
            setattr(default_input_params,k,'GARBAGE')

        q = QTextAsData(default_input_params)

        r = q.execute('select * from %s' % tmpfile.name)

        self.assertTrue(r.status == 'error')

        overwriting_input_params = QInputParams(skip_header=True,delimiter=' ')

        r2 = q.execute('select * from %s' % tmpfile.name,input_params=overwriting_input_params)

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r2.warnings),0)
        self.assertEqual(len(r2.data),2)
        self.assertEqual(r2.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r2.data,[(1,2,3),(4,5,6)])
        self.assertTrue(tmpfile.name in r2.metadata.table_structures)
        self.assertTrue(tmpfile.name in r2.metadata.new_table_structures)
        self.assertEqual(r2.metadata.table_structures[tmpfile.name].atomic_fns,[tmpfile.name])
        self.assertEqual(r2.metadata.table_structures[tmpfile.name].source,tmpfile.name)
        self.assertEqual(r2.metadata.table_structures[tmpfile.name].source_type,'file')

        q.done()
        self.cleanup(tmpfile)

    def test_input_params_merge(self):
        input_params = QInputParams()

        for k in input_params.__dict__.keys():
            setattr(input_params,k,'GARBAGE')

        merged_input_params = input_params.merged_with(QInputParams())

        for k in merged_input_params.__dict__.keys():
            self.assertTrue(getattr(merged_input_params,k) != 'GARBAGE')

        for k in input_params.__dict__.keys():
            self.assertTrue(getattr(merged_input_params,k) != 'GARBAGE')

    def test_table_analysis_with_syntax_error(self):

        q = QTextAsData()

        q_output = q.analyze("bad syntax")

        q.done()
        self.assertTrue(q_output.status == 'error')
        self.assertTrue(q_output.error.msg.startswith('query error'))

    def test_execute_response(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData()

        q_output = q.execute("select a,c from %s" % tmpfile.name,QInputParams(skip_header=True))

        self.assertTrue(q_output.status == 'ok')
        self.assertTrue(q_output.error is None)
        self.assertEqual(len(q_output.warnings),0)
        self.assertEqual(len(q_output.data),2)
        self.assertEqual(q_output.data,[ (1,3),(4,6) ])
        self.assertTrue(q_output.metadata is not None)

        metadata = q_output.metadata

        self.assertEqual(metadata.output_column_name_list, [ 'a','c'])
        self.assertTrue(tmpfile.name in metadata.new_table_structures)
        self.assertEqual(len(metadata.table_structures),1)

        table_structure = metadata.new_table_structures[tmpfile.name]

        self.assertEqual(table_structure.column_names,[ 'a','b','c'])
        self.assertEqual(table_structure.python_column_types,[ int,int,int])
        self.assertEqual(table_structure.sqlite_column_types,[ 'int','int','int'])
        self.assertEqual(table_structure.qtable_name, tmpfile.name)
        self.assertEqual(table_structure.atomic_fns,[tmpfile.name])
        self.assertEqual(table_structure.source_type,'file')
        self.assertEqual(table_structure.source,tmpfile.name)

        q.done()
        self.cleanup(tmpfile)

    def test_analyze_response(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData()

        q_output = q.analyze("select a,c from %s" % tmpfile.name,QInputParams(skip_header=True))

        self.assertTrue(q_output.status == 'ok')
        self.assertTrue(q_output.error is None)
        self.assertEqual(len(q_output.warnings),0)
        self.assertEqual(len(q_output.data),2)
        self.assertEqual(q_output.data,[ (1,3),(4,6) ])
        self.assertTrue(q_output.metadata is not None)

        metadata = q_output.metadata

        self.assertEqual(metadata.output_column_name_list, [ 'a','c'])
        self.assertEqual(len(metadata.table_structures),1)
        self.assertTrue(tmpfile.name in metadata.new_table_structures)

        table_structure = metadata.table_structures[tmpfile.name]

        self.assertEqual(table_structure.column_names,[ 'a','b','c'])
        self.assertEqual(table_structure.python_column_types,[ int,int,int])
        self.assertEqual(table_structure.sqlite_column_types,[ 'int','int','int'])
        self.assertEqual(table_structure.qtable_name, tmpfile.name)
        self.assertEqual(table_structure.atomic_fns,[tmpfile.name])
        self.assertEqual(table_structure.source_type,'file')
        self.assertEqual(table_structure.source,tmpfile.name)

        q.done()
        self.cleanup(tmpfile)

    def test_load_data_from_string_without_previous_data_load(self):
        input_str = six.u('column1,column2,column3\n') + six.u('\n').join([six.u('value1,2.5,value3')] * 1000)


        data_streams_dict = {
            'my_data': DataStream('my_data_stream_id','my_data',six.StringIO(input_str))
        }
        q = QTextAsData(default_input_params=QInputParams(skip_header=True,delimiter=','),data_streams_dict=data_streams_dict)

        q_output = q.execute('select column2,column3 from my_data')

        self.assertTrue(q_output.status == 'ok')
        self.assertTrue(q_output.error is None)
        self.assertEqual(len(q_output.warnings),0)
        self.assertTrue(len(q_output.data),1000)
        self.assertEqual(len(set(q_output.data)),1)
        self.assertEqual(list(set(q_output.data))[0],(2.5,'value3'))

        metadata = q_output.metadata

        self.assertTrue(metadata.output_column_name_list,['column2','column3'])
        self.assertTrue('my_data' in metadata.new_table_structures)
        self.assertEqual(len(metadata.table_structures),1)

        table_structure = metadata.table_structures['my_data']

        self.assertEqual(table_structure.column_names,['column1','column2','column3'])
        self.assertEqual(table_structure.sqlite_column_types,['text','real','text'])
        self.assertEqual(table_structure.python_column_types,[str,float,str])
        self.assertEqual(table_structure.qtable_name, 'my_data')
        self.assertEqual(table_structure.source_type, 'data-stream')
        self.assertEqual(table_structure.source, 'my_data_stream_id')

        q.done()

    def test_load_data_from_string_with_previous_data_load(self):
        input_str = six.u('column1,column2,column3\n') + six.u('\n').join([six.u('value1,2.5,value3')] * 1000)

        data_streams_dict = {
            'my_data': DataStream('a','my_data',six.StringIO(input_str))
        }
        q = QTextAsData(default_input_params=QInputParams(skip_header=True,delimiter=','),data_streams_dict=data_streams_dict)

        dl = q.load_data('my_data',QInputParams(skip_header=True,delimiter=','))

        q_output = q.execute('select column2,column3 from my_data')

        self.assertTrue(q_output.status == 'ok')
        self.assertTrue(q_output.error is None)
        self.assertEqual(len(q_output.warnings),0)
        self.assertTrue(len(q_output.data),1000)
        self.assertEqual(len(set(q_output.data)),1)
        self.assertEqual(list(set(q_output.data))[0],(2.5,'value3'))

        metadata = q_output.metadata

        self.assertTrue(metadata.output_column_name_list,['column2','column3'])
        self.assertTrue('my_data' not in metadata.new_table_structures)
        self.assertEqual(len(metadata.table_structures),1)

        table_structure = metadata.table_structures['my_data']

        self.assertEqual(table_structure.column_names,['column1','column2','column3'])
        self.assertEqual(table_structure.sqlite_column_types,['text','real','text'])
        self.assertEqual(table_structure.python_column_types,[str,float,str])
        self.assertEqual(table_structure.qtable_name, 'my_data')

        q.done()



class BenchmarkAttemptResults(object):
    def __init__(self, attempt, lines, columns, duration,return_code):
        self.attempt = attempt
        self.lines = lines
        self.columns = columns
        self.duration = duration
        self.return_code = return_code

    def __str__(self):
        return "{}".format(self.__dict__)
    __repr__ = __str__

class BenchmarkResults(object):
    def __init__(self, lines, columns, attempt_results, mean, stddev):
        self.lines = lines
        self.columns = columns
        self.attempt_results = attempt_results
        self.mean = mean
        self.stddev = stddev

    def __str__(self):
        return "{}".format(self.__dict__)
    __repr__ = __str__

@pytest.mark.benchmark
class BenchmarkTests(AbstractQTestCase):

    BENCHMARK_DIR = os.environ.get('Q_BENCHMARK_DATA_DIR')

    def _ensure_benchmark_data_dir_exists(self):
        try:
            os.mkdir(BenchmarkTests.BENCHMARK_DIR)
        except Exception as e:
            pass

    def _create_benchmark_file_if_needed(self):
        self._ensure_benchmark_data_dir_exists()

        if os.path.exists('{}/'.format(BenchmarkTests.BENCHMARK_DIR)):
            return

        g = GzipFile('unit-file.csv.gz')
        d = g.read().decode('utf-8')
        f = open('{}/benchmark-file.csv'.format(BenchmarkTests.BENCHMARK_DIR), 'w')
        for i in range(100):
            f.write(d)
        f.close()

    def _prepare_test_file(self, lines, columns):

        filename = '{}/_benchmark_data__lines_{}_columns_{}.csv'.format(BenchmarkTests.BENCHMARK_DIR,lines, columns)

        if os.path.exists(filename):
            return filename

        c = ['c{}'.format(x + 1) for x in range(columns)]

        # write a header line
        ff = open(filename,'w')
        ff.write(",".join(c))
        ff.write('\n')
        ff.close()

        r, o, e = run_command('head -{} {}/benchmark-file.csv | ' + Q_EXECUTABLE + ' -d , "select {} from -" >> {}'.format(lines, BenchmarkTests.BENCHMARK_DIR, ','.join(c), filename))
        self.assertEqual(r, 0)
        # Create file cache as part of preparation
        r, o, e = run_command(Q_EXECUTABLE + ' -C readwrite -d , "select count(*) from %s"' % filename)
        self.assertEqual(r, 0)
        return filename

    def _decide_result(self,attempt_results):

        failed = list(filter(lambda a: a.return_code != 0,attempt_results))

        if len(failed) == 0:
            mean = sum([x.duration for x in attempt_results]) / len(attempt_results)
            sum_squared = sum([(x.duration - mean)**2 for x in attempt_results])
            ddof = 0
            pvar = sum_squared / (len(attempt_results) - ddof)
            stddev = pvar ** 0.5
        else:
            mean = None
            stddev = None

        return BenchmarkResults(
            attempt_results[0].lines,
            attempt_results[0].columns,
            attempt_results,
            mean,
            stddev
        )

    def _perform_test_performance_matrix(self,name,generate_cmd_function):
        results = []

        benchmark_results_folder = os.environ.get("Q_BENCHMARK_RESULTS_FOLDER",'')
        if benchmark_results_folder == "":
            raise Exception("Q_BENCHMARK_RESULTS_FOLDER must be provided as an environment variable")

        self._create_benchmark_file_if_needed()
        for columns in [1, 5, 10, 20, 50, 100]:
            for lines in [1, 10, 100, 1000, 10000, 100000, 1000000]:
                attempt_results = []
                for attempt in range(10):
                    filename = self._prepare_test_file(lines, columns)
                    if DEBUG:
                        print("Testing {}".format(filename))
                    t0 = time.time()
                    r, o, e = run_command(generate_cmd_function(filename,lines,columns))
                    duration = time.time() - t0
                    attempt_result = BenchmarkAttemptResults(attempt, lines, columns, duration, r)
                    attempt_results += [attempt_result]
                    if DEBUG:
                        print("Results: {}".format(attempt_result.__dict__))
                final_result = self._decide_result(attempt_results)
                results += [final_result]

        series_fields = [six.u('lines'),six.u('columns')]
        value_fields = [six.u('mean'),six.u('stddev')]

        all_fields = series_fields + value_fields

        output_filename = '{}/{}.benchmark-results'.format(benchmark_results_folder,name)
        output_file = open(output_filename,'w')
        for columns,g in itertools.groupby(sorted(results,key=lambda x:x.columns),key=lambda x:x.columns):
            x = six.u("\t").join(series_fields + [six.u('{}_{}').format(name, f) for f in value_fields])
            print(x,file = output_file)
            for result in g:
                print(six.u("\t").join(map(str,[getattr(result,f) for f in all_fields])),file=output_file)
        output_file.close()

        print("results have been written to : {}".format(output_filename))
        if DEBUG:
            print("RESULTS FOR {}".format(name))
            print(open(output_filename,'r').read())

    def test_q_matrix(self):
        Q_BENCHMARK_NAME = os.environ.get('Q_BENCHMARK_NAME')
        if Q_BENCHMARK_NAME is None:
            raise Exception('Q_BENCHMARK_NAME must be provided as an env var')

        def generate_q_cmd(data_filename, line_count, column_count):
            Q_BENCHMARK_ADDITIONAL_PARAMS = os.environ.get('Q_BENCHMARK_ADDITIONAL_PARAMS') or ''
            additional_params = ''
            additional_params = additional_params + ' ' + Q_BENCHMARK_ADDITIONAL_PARAMS
            return '{} -d , {} "select count(*) from {}"'.format(Q_EXECUTABLE,additional_params, data_filename)
        self._perform_test_performance_matrix(Q_BENCHMARK_NAME,generate_q_cmd)

    def _get_textql_version(self):
        r,o,e = run_command("textql --version")
        if r != 0:
            raise Exception("Could not find textql")
        if len(e) != 0:
            raise Exception("Errors while getting textql version")
        return o[0]

    def _get_octosql_version(self):
        r,o,e = run_command("octosql --version")
        if r != 0:
            raise Exception("Could not find octosql")
        if len(e) != 0:
            raise Exception("Errors while getting octosql version")
        version = re.findall('v[0-9]+\\.[0-9]+\\.[0-9]+',str(o[0],encoding='utf-8'))[0]
        return version

    def test_textql_matrix(self):
        def generate_textql_cmd(data_filename,line_count,column_count):
            return 'textql -dlm , -sql "select count(*)" {}'.format(data_filename)

        name = 'textql_%s' % self._get_textql_version()
        self._perform_test_performance_matrix(name,generate_textql_cmd)

    def test_octosql_matrix(self):
        config_fn = self.random_tmp_filename('octosql', 'config')
        def generate_octosql_cmd(data_filename,line_count,column_count):
            j = """
dataSources:
  - name: bmdata
    type: csv
    config:
      path: "{}"
      headerRow: false
      batchSize: 10000
""".format(data_filename)[1:]
            f = open(config_fn,'w')
            f.write(j)
            f.close()
            return 'octosql -c {} -o batch-csv "select count(*) from bmdata a"'.format(config_fn)

        name = 'octosql_%s' % self._get_octosql_version()
        self._perform_test_performance_matrix(name,generate_octosql_cmd)

def suite():
    tl = unittest.TestLoader()
    basic_stuff = tl.loadTestsFromTestCase(BasicTests)
    parsing_mode = tl.loadTestsFromTestCase(ParsingModeTests)
    sql = tl.loadTestsFromTestCase(SqlTests)
    formatting = tl.loadTestsFromTestCase(FormattingTests)
    basic_module_stuff = tl.loadTestsFromTestCase(BasicModuleTests)
    save_db_to_disk_tests = tl.loadTestsFromTestCase(SaveDbToDiskTests)
    user_functions_tests = tl.loadTestsFromTestCase(UserFunctionTests)
    multi_header_tests = tl.loadTestsFromTestCase(MultiHeaderTests)
    return unittest.TestSuite([basic_module_stuff, basic_stuff, parsing_mode, sql, formatting,save_db_to_disk_tests,multi_header_tests,user_functions_tests])

if __name__ == '__main__':
    if len(sys.argv) > 1:
        suite = unittest.TestSuite()
        if '.' in sys.argv[1]:
            c,m = sys.argv[1].split(".")
            suite.addTest(globals()[c](m))
        else:
            tl = unittest.TestLoader()
            tc = tl.loadTestsFromTestCase(globals()[sys.argv[1]])
            suite = unittest.TestSuite([tc])
    else:
        suite = suite()

    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(suite)
    sys.exit(not result.wasSuccessful())
