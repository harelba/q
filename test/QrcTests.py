from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import sample_data_no_header, sample_data_rows
from test.base import AbstractQTestCase

from test.utils import DEBUG,b

import os
import sys
import uuid


class QrcTests(AbstractQTestCase):

    def test_explicit_qrc_filename_not_found(self):
        non_existent_filename = str(uuid.uuid4())
        env_to_inject = { 'QRC_FILENAME': non_existent_filename}
        cmd = Q_EXECUTABLE + ' "select 1"'
        retcode, o, e = run_command(cmd, env_to_inject=env_to_inject)

        self.assertEqual(retcode, 244)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertTrue(e[0] == b('QRC_FILENAME env var exists, but cannot find qrc file at %s' % non_existent_filename))

    def test_explicit_qrc_filename_that_exists(self):
        tmp_qrc_file = self.create_file_with_data(b('''[options]
output_delimiter=|
'''))
        env_to_inject = { 'QRC_FILENAME': tmp_qrc_file.name}
        cmd = Q_EXECUTABLE + ' "select 1,2"'
        retcode, o, e = run_command(cmd, env_to_inject=env_to_inject)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0] == b('1|2'))

        self.cleanup(tmp_qrc_file)

    def test_all_default_options(self):
        # Create a qrc file that contains all default values inside the qrc file, but with some different values than the regular defaults
        tmp_qrc_file = self.create_file_with_data(b('''[options]
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

        self.assertEqual(o[0],b('[options]'))
        o = o[1:]

        m = {}
        for r in o:
            key,val = r.split(b("="),1)
            m[key] = val

        self.assertEqual(m[b('analyze_only')],b('True'))
        self.assertEqual(m[b('beautify')],b('True'))
        self.assertEqual(m[b('caching_mode')],b('readwrite'))
        self.assertEqual(m[b('column_count')],b('32'))
        self.assertEqual(m[b('delimiter')],b(','))
        self.assertEqual(m[b('disable_column_type_detection')],b('True'))
        self.assertEqual(m[b('disable_double_double_quoting')],b('False'))
        self.assertEqual(m[b('disable_escaped_double_quoting')],b('False'))
        self.assertEqual(m[b('encoding')],b('ascii'))
        self.assertEqual(m[b('formatting')],b('xxx'))
        self.assertEqual(m[b('gzipped')],b('True'))
        self.assertEqual(m[b('input_quoting_mode')],b('all'))
        self.assertEqual(m[b('keep_leading_whitespace_in_values')],b('True'))
        self.assertEqual(m[b('list_user_functions')],b('True'))
        self.assertEqual(m[b('max_attached_sqlite_databases')],b('888'))
        self.assertEqual(m[b('max_column_length_limit')],b('8888'))
        self.assertEqual(m[b('mode')],b('strict'))
        self.assertEqual(m[b('output_delimiter')],b('|'))
        self.assertEqual(m[b('output_encoding')],b('utf-8'))
        self.assertEqual(m[b('output_header')],b('True'))
        self.assertEqual(m[b('output_quoting_mode')],b('all'))
        self.assertEqual(m[b('overwrite_qsql')],b('False'))
        self.assertEqual(m[b('pipe_delimited')],b('True'))
        self.assertEqual(m[b('pipe_delimited_output')],b('True'))
        self.assertEqual(m[b('query_encoding')],b('ascii'))
        self.assertEqual(m[b('query_filename')],b('query-filename'))
        self.assertEqual(m[b('save_db_to_disk_filename')],b('save-db-to-disk-filename'))
        self.assertEqual(m[b('skip_header')],b('True'))
        self.assertEqual(m[b('tab_delimited')],b('True'))
        self.assertEqual(m[b('tab_delimited_output')],b('True'))
        self.assertEqual(m[b('verbose')],b('True'))
        self.assertEqual(m[b('with_universal_newlines')],b('True'))

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
        self.assertEqual(o[0],b('a,1,0'))
        self.assertEqual(o[1],b('b,2,0'))
        self.assertEqual(o[2],b('c,,0'))

        # Ensure default does not create a cache file
        self.assertTrue(not os.path.exists(expected_cache_filename))

        tmp_qrc_file = self.create_file_with_data(b('''[options]
caching_mode=readwrite
'''))
        env_to_inject = { 'QRC_FILENAME': tmp_qrc_file.name }
        print(sys.version_info)
        cmd = Q_EXECUTABLE + ' -d , "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd, env_to_inject=env_to_inject)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),3)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],b('a,1,0'))
        self.assertEqual(o[1],b('b,2,0'))
        self.assertEqual(o[2],b('c,,0'))

        # Ensure that qrc file caching is being used and caching is activated (cache file should exist)
        self.assertTrue(os.path.exists(expected_cache_filename))

        self.cleanup(tmp_qrc_file)
        self.cleanup(tmpfile)