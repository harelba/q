from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import sample_data_no_header, sample_data_rows
from test.base import AbstractQTestCase

import six
from test.utils import DEBUG

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
        env_to_inject = { 'QRC_FILENAME': tmp_qrc_file.name }
        print(sys.version_info)
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