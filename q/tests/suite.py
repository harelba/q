#
# Simplistic test suite for q.
#

import unittest
import random
import json
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

import q
from q.q import QTextAsData,QOutput,QOutputPrinter,QInputParams

# q uses this encoding as the default output encoding. Some of the tests use it in order to 
# make sure that the output is correctly encoded
SYSTEM_ENCODING = locale.getpreferredencoding()

EXAMPLES = os.path.abspath(os.path.join(q.__file__, os.pardir, os.pardir, 'examples'))


DEBUG = False
if len(sys.argv) > 2 and sys.argv[2] == '-v':
    DEBUG = True


def run_command(cmd_to_run):
    global DEBUG
    if DEBUG:
        print("CMD: {}".format(cmd_to_run))

    p = Popen(cmd_to_run, stdout=PIPE, stderr=PIPE, shell=True)
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


class AbstractQTestCase(unittest.TestCase):

    def create_file_with_data(self, data, encoding=None):
        if encoding is not None:
            raise Exception('Deprecated: Encoding must be none')
        tmpfile = NamedTemporaryFile(delete=False)
        tmpfile.write(data)
        tmpfile.close()
        return tmpfile

    def cleanup(self, tmpfile):
        global DEBUG
        if not DEBUG:
            os.remove(tmpfile.name)

    def random_tmp_filename(self,prefix,postfix):
        # TODO Use more robust method for this
        path = '/var/tmp'
        return '%s/%s-%s.%s' % (path,prefix,random.randint(0,1000000000),postfix)


class SaveDbToDiskTests(AbstractQTestCase):

    def test_store_to_disk(self):
        db_filename = self.random_tmp_filename('store-to-disk','db')
        self.assertFalse(os.path.exists(db_filename))

        retcode, o, e = run_command('seq 1 1000 | q "select count(*) from -" -c 1 -S %s' % db_filename)

        self.assertTrue(retcode == 0)
        self.assertTrue(len(o) == 0)
        self.assertTrue(len(e) == 5)
        self.assertTrue(e[0].startswith(six.b('Going to save data')))
        self.assertTrue(db_filename.encode(sys.stdout.encoding or 'utf-8') in e[0])
        self.assertTrue(e[1].startswith(six.b('Data has been loaded in')))
        self.assertTrue(e[2].startswith(six.b('Saving data to db file')))
        self.assertTrue(e[3].startswith(six.b('Data has been saved into')))
        self.assertTrue(e[4] == six.b('Query to run on the database: select count(*) from `-`;'))

        self.assertTrue(os.path.exists(db_filename))

        sqlite_command = """echo 'select * from `-`;' | sqlite3 %s""" % db_filename
        sqlite_retcode,sqlite_o,sqlite_e = run_command(sqlite_command)
        self.assertTrue(sqlite_retcode == 0)
        self.assertTrue(len(sqlite_o) == 1000)
        self.assertTrue(len(sqlite_e) == 0)

        os.remove(db_filename)

    def test_preventing_db_overwrite(self):
        db_filename = self.random_tmp_filename('store-to-disk', 'db')
        self.assertFalse(os.path.exists(db_filename))

        retcode, o, e = run_command('seq 1 1000 | q "select count(*) from -" -c 1 -S %s' % db_filename)

        self.assertTrue(retcode == 0)
        self.assertTrue(os.path.exists(db_filename))

        retcode2, o2, e2 = run_command('seq 1 1000 | q "select count(*) from -" -c 1 -S %s' % db_filename)
        self.assertTrue(retcode2 != 0)
        self.assertTrue(e2[0].startswith(six.b('Going to save data into a disk database')))
        self.assertTrue(e2[1] == six.b('Disk database file {} already exists.'.format(db_filename)))

        os.remove(db_filename)


class BasicTests(AbstractQTestCase):

    def test_basic_aggregation(self):
        retcode, o, e = run_command(
            'seq 1 10 | q "select sum(c1),avg(c1) from -"')
        self.assertTrue(retcode == 0)
        self.assertTrue(len(o) == 1)
        self.assertTrue(len(e) == 1)

        s = sum(range(1, 11))
        self.assertTrue(o[0] == six.b('%s %s' % (s, s / 10.0)))
        self.assertTrue(one_column_warning(e))

    def test_gzipped_file(self):
        tmpfile = self.create_file_with_data(
            six.b('\x1f\x8b\x08\x08\xf2\x18\x12S\x00\x03xxxxxx\x003\xe42\xe22\xe62\xe12\xe52\xe32\xe7\xb2\xe0\xb2\xe424\xe0\x02\x00\xeb\xbf\x8a\x13\x15\x00\x00\x00'))

        cmd = 'q -z "select sum(c1),avg(c1) from %s"' % tmpfile.name

        retcode, o, e = run_command(cmd)
        self.assertTrue(retcode == 0)
        self.assertTrue(len(o) == 1)
        self.assertTrue(len(e) == 1)

        s = sum(range(1, 11))
        self.assertTrue(o[0] == six.b('%s %s' % (s, s / 10.0)))
        self.assertTrue(one_column_warning(e))

        self.cleanup(tmpfile)

    def test_attempt_to_unzip_stdin(self):
        tmpfile = self.create_file_with_data(
            six.b('\x1f\x8b\x08\x08\xf2\x18\x12S\x00\x03xxxxxx\x003\xe42\xe22\xe62\xe12\xe52\xe32\xe7\xb2\xe0\xb2\xe424\xe0\x02\x00\xeb\xbf\x8a\x13\x15\x00\x00\x00'))

        cmd = 'cat %s | q -z "select sum(c1),avg(c1) from -"' % tmpfile.name

        retcode, o, e = run_command(cmd)
        self.assertTrue(retcode != 0)
        self.assertTrue(len(o) == 0)
        self.assertTrue(len(e) == 1)

        self.assertEqual(e[0],six.b('Cannot decompress standard input. Pipe the input through zcat in order to decompress.'))

        self.cleanup(tmpfile)

    def test_delimition_mistake_with_header(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = 'q -d " " "select * from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 3)

        self.assertTrue(e[0].startswith(
            six.b("Warning: column count is one - did you provide the correct delimiter")))
        self.assertTrue(e[1].startswith(six.b("Bad header row")))
        self.assertTrue(six.b("Column name cannot contain commas") in e[2])

        self.cleanup(tmpfile)

    def test_regexp_int_data_handling(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = 'q -d , "select c2 from %s where regexp(\'^1\',c2)"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b("1"))

        self.cleanup(tmpfile)

    def test_regexp_null_data_handling(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = 'q -d , "select count(*) from %s where regexp(\'^\',c2)"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b("2"))

        self.cleanup(tmpfile)

    def test_select_one_column(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = 'q -d , "select c1 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(six.b(" ").join(o), six.b('a b c'))

        self.cleanup(tmpfile)

    def test_tab_delimition_parameter(self):
        tmpfile = self.create_file_with_data(
            sample_data_no_header.replace(six.b(","), six.b("\t")))
        cmd = 'q -t "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("\t")))

        self.cleanup(tmpfile)

    def test_tab_delimition_parameter__with_manual_override_attempt(self):
        tmpfile = self.create_file_with_data(
            sample_data_no_header.replace(six.b(","), six.b("\t")))
        cmd = 'q -t -d , "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("\t")))

        self.cleanup(tmpfile)

    def test_output_delimiter(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = 'q -d , -D "|" "select c1,c2,c3 from %s"' % tmpfile.name
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
        cmd = 'q -d , -T "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("\t")))

        self.cleanup(tmpfile)

    def test_output_delimiter_tab_parameter__with_manual_override_attempt(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = 'q -d , -T -D "|" "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(six.b(","), six.b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(six.b(","), six.b("\t")))

        self.cleanup(tmpfile)

    def test_stdin_input(self):
        cmd = six.b('printf "%s" | q -d , "select c1,c2,c3 from -"') % sample_data_no_header
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0])
        self.assertEqual(o[1], sample_data_rows[1])
        self.assertEqual(o[2], sample_data_rows[2])

    def test_column_separation(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = 'q -d , "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0])
        self.assertEqual(o[1], sample_data_rows[1])
        self.assertEqual(o[2], sample_data_rows[2])

        self.cleanup(tmpfile)

    def test_column_analysis(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = 'q -d , "select c1 from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(o[0], six.b('Table for file: %s' % tmpfile.name))
        self.assertEqual(o[1].strip(), six.b('`c1` - text'))
        self.assertEqual(o[2].strip(), six.b('`c2` - int'))
        self.assertEqual(o[3].strip(), six.b('`c3` - int'))

        self.cleanup(tmpfile)

    def test_column_analysis_no_header(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = 'q -d , "select c1 from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(o[0], six.b('Table for file: %s' % tmpfile.name))
        self.assertEqual(o[1].strip(), six.b('`c1` - text'))
        self.assertEqual(o[2].strip(), six.b('`c2` - int'))
        self.assertEqual(o[3].strip(), six.b('`c3` - int'))

    def test_header_exception_on_numeric_header_data(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = 'q -d , "select * from %s" -A -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 3)
        self.assertTrue(
            six.b('Bad header row: Header must contain only strings') in e[0])
        self.assertTrue(six.b("Column name must be a string") in e[1])
        self.assertTrue(six.b("Column name must be a string") in e[2])

        self.cleanup(tmpfile)

    def test_column_analysis_with_header(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = 'q -d , "select c1 from %s" -A -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o),4)
        self.assertEqual(len(e),2)
        self.assertEqual(o[0], six.b('Table for file: %s' % tmpfile.name))
        self.assertEqual(o[1].strip(), six.b('`name` - text'))
        self.assertEqual(o[2].strip(), six.b('`value1` - int'))
        self.assertEqual(o[3].strip(), six.b('`value2` - int'))
        self.assertEqual(e[0].strip(),six.b('query error: no such column: c1'))
        self.assertTrue(e[1].startswith(six.b('Warning - There seems to be a ')))

        self.cleanup(tmpfile)

    def test_data_with_header(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = 'q -d , "select name from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(six.b(" ").join(o), six.b("a b c"))

        self.cleanup(tmpfile)

    def test_output_header_when_input_header_exists(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = 'q -d , "select name from %s" -H -O' % tmpfile.name
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
        cmd = 'q -d , "select c3 from %s" -H' % tmpfile.name

        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 2)
        self.assertTrue(six.b('no such column: c3') in e[0])
        self.assertEqual(
            e[1], six.b('Warning - There seems to be a "no such column" error, and -H (header line) exists. Please make sure that you are using the column names from the header line and not the default (cXX) column names'))

        self.cleanup(tmpfile)

    def test_column_analysis_with_unexpected_header(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = 'q -d , "select c1 from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 4)
        self.assertEqual(len(e), 1)

        self.assertEqual(o[0], six.b('Table for file: %s' % tmpfile.name))
        self.assertEqual(o[1].strip(), six.b('`c1` - text'))
        self.assertEqual(o[2].strip(), six.b('`c2` - text'))
        self.assertEqual(o[3].strip(), six.b('`c3` - text'))

        self.assertEqual(
            e[0], six.b('Warning - There seems to be header line in the file, but -H has not been specified. All fields will be detected as text fields, and the header line will appear as part of the data'))

        self.cleanup(tmpfile)

    def test_empty_data(self):
        tmpfile = self.create_file_with_data(six.b(''))
        cmd = 'q -d , "select c1 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(six.b('Warning - data is empty') in e[0])

        self.cleanup(tmpfile)

    def test_empty_data_with_header_param(self):
        tmpfile = self.create_file_with_data(six.b(''))
        cmd = 'q -d , "select c1 from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        m = six.b("Header line is expected but missing in file %s" % tmpfile.name)
        self.assertTrue(m in e[0])

        self.cleanup(tmpfile)

    def test_one_row_of_data_without_header_param(self):
        tmpfile = self.create_file_with_data(header_row)
        cmd = 'q -d , "select c2 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('value1'))

        self.cleanup(tmpfile)

    def test_one_row_of_data_with_header_param(self):
        tmpfile = self.create_file_with_data(header_row)
        cmd = 'q -d , "select c2 from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(six.b('Warning - data is empty') in e[0])

        self.cleanup(tmpfile)

    def test_dont_leading_keep_whitespace_in_values(self):
        tmpfile = self.create_file_with_data(sample_data_with_spaces_no_header)
        cmd = 'q -d , "select c1 from %s"' % tmpfile.name
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
        cmd = 'q -d , "select c1 from %s" -k' % tmpfile.name
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
        cmd = 'q -d , "select c2 from %s" -k -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 4)

        self.assertEqual(o[0], six.b('Table for file: %s' % tmpfile.name))
        self.assertEqual(o[1].strip(), six.b('`c1` - text'))
        self.assertEqual(o[2].strip(), six.b('`c2` - int'))
        self.assertEqual(o[3].strip(), six.b('`c3` - int'))

        self.cleanup(tmpfile)

    def test_spaces_in_header_row(self):
        tmpfile = self.create_file_with_data(
            header_row_with_spaces + six.b("\n") + sample_data_no_header)
        cmd = 'q -d , "select name,\`value 1\` from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0], six.b('a,1'))
        self.assertEqual(o[1], six.b('b,2'))
        self.assertEqual(o[2], six.b('c,'))

        self.cleanup(tmpfile)

    def test_column_analysis_for_spaces_in_header_row(self):
        tmpfile = self.create_file_with_data(
            header_row_with_spaces + six.b("\n") + sample_data_no_header)
        cmd = 'q -d , "select name,\`value 1\` from %s" -H -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 4)

        self.assertEqual(o[0], six.b('Table for file: %s' % tmpfile.name))
        self.assertEqual(o[1].strip(), six.b('`name` - text'))
        self.assertEqual(o[2].strip(), six.b('`value 1` - int'))
        self.assertEqual(o[3].strip(), six.b('`value2` - int'))

        self.cleanup(tmpfile)

    def test_no_query_in_command_line(self):
        cmd = 'q -d , ""'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertEqual(e[0],six.b('Query cannot be empty (query number 1)'))

    def test_empty_query_in_command_line(self):
        cmd = 'q -d , "  "'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertEqual(e[0],six.b('Query cannot be empty (query number 1)'))

    def test_failure_in_query_stops_processing_queries(self):
        cmd = 'q -d , "select 500" "select 300" "wrong-query" "select 8000"'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 2)
        self.assertEqual(o[0],six.b('500'))
        self.assertEqual(o[1],six.b('300'))

    def test_multiple_queries_in_command_line(self):
        cmd = 'q -d , "select 500" "select 300+100" "select 300" "select 200"'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 4)

        self.assertEqual(o[0],six.b('500'))
        self.assertEqual(o[1],six.b('400'))
        self.assertEqual(o[2],six.b('300'))
        self.assertEqual(o[3],six.b('200'))

    def test_literal_calculation_query(self):
        cmd = 'q -d , "select 1+40/6"'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 1)

        self.assertEqual(o[0],six.b('7'))

    def test_literal_calculation_query_float_result(self):
        cmd = 'q -d , "select 1+40/6.0"'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 1)

        self.assertEqual(o[0],six.b('7.666666666666667'))

    def test_use_query_file(self):
        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select name from %s" % tmp_data_file.name))

        cmd = 'q -d , -q %s -H' % tmp_query_file.name
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

        cmd = 'q -d , -q %s -H -Q ascii' % tmp_query_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,3)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)

        self.assertTrue(e[0].startswith(six.b('Could not decode query number 1 using the provided query encoding (ascii)')))

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)

    def test_output_header_with_non_ascii_names(self):
        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select name,'Hr\xc3\xa1\xc4\x8d' Hr\xc3\xa1\xc4\x8d from %s" % tmp_data_file.name),encoding=None)

        cmd = 'q -d , -q %s -H -Q utf-8 -O' % tmp_query_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),4)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0].decode(SYSTEM_ENCODING), u'name,Hr\xe1\u010d')
        self.assertEqual(o[1].decode(SYSTEM_ENCODING), u'a,Hr\xe1\u010d')
        self.assertEqual(o[2].decode(SYSTEM_ENCODING), u'b,Hr\xe1\u010d')
        self.assertEqual(o[3].decode(SYSTEM_ENCODING), u'c,Hr\xe1\u010d')

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)

    def test_use_query_file_with_query_encoding(self):
        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select name,'Hr\xc3\xa1\xc4\x8d' from %s" % tmp_data_file.name),encoding=None)

        cmd = 'q -d , -q %s -H -Q utf-8' % tmp_query_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0].decode(SYSTEM_ENCODING), u'a,Hr\xe1\u010d')
        self.assertEqual(o[1].decode(SYSTEM_ENCODING), u'b,Hr\xe1\u010d')
        self.assertEqual(o[2].decode(SYSTEM_ENCODING), u'c,Hr\xe1\u010d')

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)

    def test_use_query_file_and_command_line(self):
        tmp_data_file = self.create_file_with_data(sample_data_with_header)
        tmp_query_file = self.create_file_with_data(six.b("select name from %s" % tmp_data_file.name))

        cmd = 'q -d , -q %s -H "select * from ppp"' % tmp_query_file.name
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
            cmd = 'q -d , -q %s -H -Q utf-8 -E %s' % (tmp_query_file.name,target_encoding)
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

        cmd = 'q -d , -q %s -H -Q utf-8 -E ascii' % tmp_query_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 3)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertTrue(e[0].startswith(six.b('Cannot encode data')))

        self.cleanup(tmp_data_file)
        self.cleanup(tmp_query_file)


    def test_use_query_file_with_empty_query(self):
        tmp_query_file = self.create_file_with_data(six.b("   "))

        cmd = 'q -d , -q %s -H' % tmp_query_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertTrue(e[0].startswith(six.b("Query cannot be empty")))

        self.cleanup(tmp_query_file)

    def test_use_non_existent_query_file(self):
        cmd = 'q -d , -q non-existent-query-file -H'
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 1)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertTrue(e[0].startswith(six.b("Could not read query from file")))

    def test_non_quoted_values_in_quoted_data(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data)

        cmd = 'q -d " " "select c1 from %s"' % tmp_data_file.name
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

        cmd = 'q -d " " "select c2 from %s"' % tmp_data_file.name
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

        cmd = 'q -d " " "select c3 from %s"' % tmp_data_file.name
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

        cmd = 'q -d " " "select c4 from %s"' % tmp_data_file.name
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

        cmd = 'q -d " " -m relaxed -D , -w none -W none "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted,data",23'))
        self.assertEqual(o[1],six.b('unquoted-data,54,'))

        self.cleanup(tmp_data_file)

    def test_none_input_quoting_mode_in_strict_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = 'q -d " " -m strict -D , -w none "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(e),1)
        self.assertEqual(len(o),0)

        self.assertTrue(e[0].startswith(six.b('Strict mode. Column Count is expected to identical')))

        self.cleanup(tmp_data_file)

    def test_minimal_input_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = 'q -d " " -D , -w minimal "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_all_input_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = 'q -d " " -D , -w all "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_incorrect_input_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = 'q -d " " -D , -w unknown_wrapping_mode "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(e),1)
        self.assertEqual(len(o),0)

        self.assertTrue(e[0].startswith(six.b('Input quoting mode can only be one of all,minimal,none')))
        self.assertTrue(six.b('unknown_wrapping_mode') in e[0])

        self.cleanup(tmp_data_file)

    def test_none_output_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = 'q -d " " -D , -w all -W none "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_minimal_output_quoting_mode__without_need_to_quote_in_output(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = 'q -d " " -D , -w all -W minimal "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_minimal_output_quoting_mode__with_need_to_quote_in_output(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        # output delimiter is set to space, so the output will contain it
        cmd = 'q -d " " -D " " -w all -W minimal "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted data" 23'))
        self.assertEqual(o[1],six.b('unquoted-data 54'))

        self.cleanup(tmp_data_file)

    def test_nonnumeric_output_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = 'q -d " " -D , -w all -W nonnumeric "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted data",23'))
        self.assertEqual(o[1],six.b('"unquoted-data",54'))

        self.cleanup(tmp_data_file)

    def test_all_output_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = 'q -d " " -D , -w all -W all "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted data","23"'))
        self.assertEqual(o[1],six.b('"unquoted-data","54"'))

        self.cleanup(tmp_data_file)

    def _internal_test_consistency_of_chaining_output_to_input(self,input_data,input_wrapping_mode,output_wrapping_mode):

        tmp_data_file = self.create_file_with_data(input_data)

        basic_cmd = 'q -w %s -W %s "select * from -"' % (input_wrapping_mode,output_wrapping_mode)
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

    def test_utf8_with_bom_encoding(self):
        utf_8_data_with_bom = six.b('\xef\xbb\xbf"typeid","limit","apcost","date","checkpointId"\n"1","2","5","1,2,3,4,5,6,7","3000,3001,3002"\n"2","2","5","1,2,3,4,5,6,7","3003,3004,3005"\n')
        tmp_data_file = self.create_file_with_data(utf_8_data_with_bom,encoding=None)

        cmd = 'q -d , -H -O -e utf-8-sig "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),3)

        self.assertEqual(o[0],six.b('typeid,limit,apcost,date,checkpointId'))
        self.assertEqual(o[1],six.b('1,2,5,"1,2,3,4,5,6,7","3000,3001,3002"'))
        self.assertEqual(o[2],six.b('2,2,5,"1,2,3,4,5,6,7","3003,3004,3005"'))

        self.cleanup(tmp_data_file)

    def test_input_field_quoting_and_data_types_with_encoding(self):
        # Checks combination of minimal input field quoting, with special characters that need to be decoded -
        # Both content and proper data types are verified
        data = six.b('111,22.22,"testing text with special characters - citt\xc3\xa0 ",http://somekindofurl.com,12.13.14.15,12.1\n')
        tmp_data_file = self.create_file_with_data(data)

        cmd = 'q -d , "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),1)

        self.assertEqual(o[0].decode('utf-8'),u'111,22.22,testing text with special characters - citt\xe0 ,http://somekindofurl.com,12.13.14.15,12.1')

        cmd = 'q -d , "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),7)

        self.assertTrue(o[0].startswith(six.b('Table for file')))
        self.assertEqual(o[1].strip(),six.b('`c1` - int'))
        self.assertEqual(o[2].strip(),six.b('`c2` - float'))
        self.assertEqual(o[3].strip(),six.b('`c3` - text'))
        self.assertEqual(o[4].strip(),six.b('`c4` - text'))
        self.assertEqual(o[5].strip(),six.b('`c5` - text'))
        self.assertEqual(o[6].strip(),six.b('`c6` - float'))

        self.cleanup(tmp_data_file)

    def test_multiline_double_double_quoted_values_in_quoted_data(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data)

        # FIXME Need to convert \0a to proper encoding suitable for the person running the tests.
        cmd = 'q -d " " "select replace(c5,X\'0A\',\'::\') from %s"' % tmp_data_file.name
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
        cmd = 'q -d " " "select replace(c6,X\'0A\',\'::\') from %s"' % tmp_data_file.name
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
        # This test (and flag) is meant to verify backward comptibility only. It is possible that
        # this flag will be removed completely in the future

        tmp_data_file = self.create_file_with_data(double_double_quoted_data)

        cmd = 'q -d " " --disable-double-double-quoting "select c2 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('double_double_quoted'))
        self.assertEqual(o[1],six.b('this is a quoted value with "double'))

        cmd = 'q -d " " --disable-double-double-quoting "select c3 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b(''))
        self.assertEqual(o[1],six.b('double'))

        cmd = 'q -d " " --disable-double-double-quoting "select c4 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b(''))
        self.assertEqual(o[1],six.b('quotes"""'))

        self.cleanup(tmp_data_file)

    def test_disable_escaped_double_quoted_data_flag__values(self):
        # This test (and flag) is meant to verify backward comptibility only. It is possible that
        # this flag will be removed completely in the future

        tmp_data_file = self.create_file_with_data(escaped_double_quoted_data)

        cmd = 'q -d " " --disable-escaped-double-quoting "select c2 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('escaped_double_quoted'))
        self.assertEqual(o[1],six.b('this is a quoted value with \\escaped'))

        cmd = 'q -d " " --disable-escaped-double-quoting "select c3 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b(''))
        self.assertEqual(o[1],six.b('double'))

        cmd = 'q -d " " --disable-escaped-double-quoting "select c4 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b(''))
        self.assertEqual(o[1],six.b('quotes\\""'))

        self.cleanup(tmp_data_file)

    def test_combined_quoted_data_flags__number_of_columns_detected(self):
        # This test (and flags) is meant to verify backward comptibility only. It is possible that
        # these flags will be removed completely in the future
        tmp_data_file = self.create_file_with_data(combined_quoted_data)

        cmd = 'q -d " " --disable-double-double-quoting --disable-escaped-double-quoting "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[1:]   # remove the first "Table for file..." line in the output

        self.assertEqual(len(o),7) # found 7 fields

        cmd = 'q -d " " --disable-escaped-double-quoting "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[1:]   # remove the first "Table for file..." line in the output

        self.assertEqual(len(o),5) # found 5 fields

        cmd = 'q -d " " --disable-double-double-quoting "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[1:]   # remove the first "Table for file..." line in the output

        self.assertEqual(len(o),5) # found 5 fields

        cmd = 'q -d " " "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[1:]   # remove the first "Table for file..." line in the output

        self.assertEqual(len(o),3) # found only 3 fields, which is the correct amount

        self.cleanup(tmp_data_file)

    def test_nonexistent_file(self):
        cmd = 'q "select * from non-existent-file"'

        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)

        self.assertEqual(e[0],six.b("No files matching 'non-existent-file' have been found"))

    def test_default_column_max_length_parameter__short_enough(self):
        huge_text = six.b("x" * 131000)

        file_data = six.b("a,b,c\n1,{},3\n".format(huge_text))

        tmpfile = self.create_file_with_data(file_data)

        cmd = 'q -H -d , "select a from %s"' % tmpfile.name
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

        cmd = 'q -H -d , "select a from %s"' % tmpfile.name
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

        cmd = 'q -H -d , -M 3 "select a from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 31)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(e[0].startswith(six.b("Column length is larger than the maximum")))
        self.assertTrue((six.b("Offending file is '%s'" % tmpfile.name)) in e[0])
        self.assertTrue(six.b('Line is 2') in e[0])

        cmd2 = 'q -H -d , -M 300 -H "select a from %s"' % tmpfile.name
        retcode2, o2, e2 = run_command(cmd2)

        self.assertEqual(retcode2, 0)
        self.assertEqual(len(o2), 1)
        self.assertEqual(len(e2), 0)

        self.assertEqual(o2[0],six.b('very-long-text'))

        self.cleanup(tmpfile)

    def test_invalid_column_max_length_parameter(self):
        file_data = six.b("a,b,c\nvery-long-text,2,3\n")
        tmpfile = self.create_file_with_data(file_data)

        cmd = 'q -H -d , -M 0 "select a from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 31)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(e[0].startswith(six.b('Max column length limit must be a positive integer')))


        self.cleanup(tmpfile)

    def test_duplicate_column_name_detection(self):
        file_data = six.b("a,b,a\n10,20,30\n30,40,50")
        tmpfile = self.create_file_with_data(file_data)

        cmd = 'q -H -d , "select a from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 35)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 2)

        self.assertTrue(e[0].startswith(six.b('Bad header row:')))
        self.assertEqual(e[1],six.b("'a': Column name is duplicated"))

        self.cleanup(tmpfile)


class ParsingModeTests(AbstractQTestCase):

    def test_strict_mode_column_count_mismatch_error(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = 'q -m strict "select count(*) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(six.b("Column Count is expected to identical") in e[0])

        self.cleanup(tmpfile)

    def test_strict_mode_too_large_specific_column_count(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = 'q -d , -m strict -c 4 "select count(*) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertEqual(
            e[0], six.b("Strict mode. Column count is expected to be 4 but is 3"))

        self.cleanup(tmpfile)

    def test_strict_mode_too_small_specific_column_count(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = 'q -d , -m strict -c 2 "select count(*) from %s"' % tmpfile.name
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
        cmd = 'q -d , -m relaxed "select count(*) from %s" -H -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 4)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('Table for file: %s' % tmpfile.name))
        self.assertEqual(o[1].strip(), six.b('`name` - text'))
        self.assertEqual(o[2].strip(), six.b('`value1` - int'))
        self.assertEqual(o[3].strip(), six.b('`c3` - int'))

        self.cleanup(tmpfile)

    def test_strict_mode_missing_columns_in_header(self):
        tmpfile = self.create_file_with_data(
            sample_data_with_missing_header_names)
        cmd = 'q -d , -m strict "select count(*) from %s" -H -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertEqual(
            e[0], six.b('Strict mode. Header row contains less columns than expected column count(2 vs 3)'))

        self.cleanup(tmpfile)

    def test_output_delimiter_with_missing_fields(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = 'q -d , "select * from %s" -D ";"' % tmpfile.name
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
        cmd = 'q -d , "select avg(c2) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('1.5'))

        self.cleanup(tmpfile)

    def test_empty_integer_values_converted_to_null(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = 'q -d , "select * from %s where c2 is null"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('c,,0'))

        self.cleanup(tmpfile)

    def test_empty_string_values_not_converted_to_null(self):
        tmpfile = self.create_file_with_data(
            sample_data_with_empty_string_no_header)
        cmd = 'q -d , "select * from %s where c2 == %s"' % (
            tmpfile.name, "''")
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('c,,0'))

        self.cleanup(tmpfile)

    def test_relaxed_mode_detected_columns(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = 'q -m relaxed "select count(*) from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)

        table_name_row = o[0]
        column_rows = o[1:]

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
        cmd = 'q -m relaxed "select count(*) from %s" -A -c 9' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)

        table_name_row = o[0]
        column_rows = o[1:]

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
        cmd = 'q -m relaxed "select c9 from %s" -c 9' % tmpfile.name
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
        cmd = 'q -m relaxed "select c1 from %s" -d ,' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o),2)

        self.assertEqual(e[0],six.b("Warning: column count is one - did you provide the correct delimiter?"))
        self.assertEqual(o[0],six.b('data without commas 1'))
        self.assertEqual(o[1],six.b('data without commas 2'))

        self.cleanup(tmpfile)

    def test_1_column_warning_in_strict_mode(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = 'q -m relaxed "select c1 from %s" -d , -m strict' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o),2)

        self.assertEqual(e[0],six.b("Warning: column count is one - did you provide the correct delimiter?"))
        self.assertEqual(o[0],six.b('data without commas 1'))
        self.assertEqual(o[1],six.b('data without commas 2'))

        self.cleanup(tmpfile)


    def test_1_column_warning_suppression_in_relaxed_mode_when_column_count_is_specific(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = 'q -m relaxed "select c1 from %s" -d , -m relaxed -c 1' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('data without commas 1'))
        self.assertEqual(o[1],six.b('data without commas 2'))

        self.cleanup(tmpfile)

    def test_1_column_warning_suppression_in_strict_mode_when_column_count_is_specific(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = 'q -m relaxed "select c1 from %s" -d , -m strict -c 1' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('data without commas 1'))
        self.assertEqual(o[1],six.b('data without commas 2'))

        self.cleanup(tmpfile)

    def test_fluffy_mode(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = 'q -m fluffy "select c9 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 9)
        self.assertEqual(len(e), 0)

        expected_output = list(map(lambda x:six.b(x),["/selinux", "/mnt", "/srv", "/lost+found",
                           "/initrd.img.old", "/cdrom", "/home", "/vmlinuz", "/initrd.img"]))

        self.assertEqual(o, expected_output)

        self.cleanup(tmpfile)

    def test_fluffy_mode_column_count_mismatch(self):
        data_row = six.b("column1 column2 column3 column4")
        data_list = [data_row] * 1000
        data_list[950] = six.b("column1 column2 column3 column4 column5")
        tmpfile = self.create_file_with_data(six.b("\n").join(data_list))

        cmd = 'q -m fluffy "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertTrue(e[0].startswith(six.b("Deprecated fluffy mode")))
        self.assertTrue(six.b(' row 951 ') in e[0])

        self.cleanup(tmpfile)

    def test_strict_mode_column_count_mismatch__less_columns(self):
        data_row = six.b("column1 column2 column3 column4")
        data_list = [data_row] * 1000
        data_list[750] = six.b("column1 column3 column4")
        tmpfile = self.create_file_with_data(six.b("\n").join(data_list))

        cmd = 'q -m strict "select * from %s"' % tmpfile.name
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

        cmd = 'q -m strict "select * from %s"' % tmpfile.name
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
        #cmd = 'seq 1 10 | q -f 1=%4.3f,2=%4.3f "select sum(c1),avg(c1) from -" -c 1'
        cmd = 'seq 1 10 | q -f 1={:4.3f},2={:4.3f} "select sum(c1),avg(c1) from -" -c 1'

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('55.000 5.500'))

    def test_column_formatting_with_output_header(self):
        perl_regex = "'s/1\n/column_name\n1\n/;'"
        # TODO Decide if this breaking change is reasonable
        #cmd = 'seq 1 10 | perl -pe ' + perl_regex + ' | q -f 1=%4.3f,2=%4.3f "select sum(column_name) mysum,avg(column_name) myavg from -" -c 1 -H -O'
        cmd = 'seq 1 10 | perl -pe ' + perl_regex + ' | q -f 1={:4.3f},2={:4.3f} "select sum(column_name) mysum,avg(column_name) myavg from -" -c 1 -H -O'

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('mysum myavg'))
        self.assertEqual(o[1], six.b('55.000 5.500'))

    def py2_test_failure_to_parse_universal_newlines_without_explicit_flag(self):
        data = six.b('permalink,company,numEmps,category,city,state,fundedDate,raisedAmt,raisedCurrency,round\rlifelock,LifeLock,,web,Tempe,AZ,1-May-07,6850000,USD,b\rlifelock,LifeLock,,web,Tempe,AZ,1-Oct-06,6000000,USD,a\rlifelock,LifeLock,,web,Tempe,AZ,1-Jan-08,25000000,USD,c\rmycityfaces,MyCityFaces,7,web,Scottsdale,AZ,1-Jan-08,50000,USD,seed\rflypaper,Flypaper,,web,Phoenix,AZ,1-Feb-08,3000000,USD,a\rinfusionsoft,Infusionsoft,105,software,Gilbert,AZ,1-Oct-07,9000000,USD,a')
        tmp_data_file = self.create_file_with_data(data)

        cmd = 'q -d , -H "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(len(o), 0)

        self.assertTrue(e[0].startswith(six.b('Data contains universal newlines')))

        self.cleanup(tmp_data_file)

    def py3_test_successfuly_parse_universal_newlines_without_explicit_flag(self):
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

        cmd = 'q -d , -H "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 6)

        actual_output = list(map(lambda row: row.split(six.b(",")),o))

        self.assertEqual(actual_output,expected_output)

        self.cleanup(tmp_data_file)

    if six.PY2:
        test_parsing_universal_newlines_without_explicit_flag = py2_test_failure_to_parse_universal_newlines_without_explicit_flag
    else:
        test_parsing_universal_newlines_without_explicit_flag = py3_test_successfuly_parse_universal_newlines_without_explicit_flag


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

        cmd = 'q -d , -H -U "select permalink,company,numEmps,category,city,state,fundedDate,raisedAmt,raisedCurrency,round from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)

        if len(e) == 2:
            # In python 3.7, there's a deprecation warning for the 'U' file opening mode, which is ok for now
            self.assertEqual(len(e), 2)
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
        cmd = 'q "select c5,c6,sum(c7)/1024.0/1024 as total from %s group by c5,c6 order by total desc"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('mapred mapred 0.9389581680297852'))
        self.assertEqual(o[1], six.b('root root 0.02734375'))
        self.assertEqual(o[2], six.b('harel harel 0.010888099670410156'))

        self.cleanup(tmpfile)

    def test_join_example(self):
        cmd = 'q "select myfiles.c8,emails.c2 from {0}/exampledatafile myfiles join {0}/group-emails-example emails on (myfiles.c4 = emails.c1) where myfiles.c8 = \'ppp\'"'.format(EXAMPLES)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)

        self.assertEqual(o[0], six.b('ppp dip.1@otherdomain.com'))
        self.assertEqual(o[1], six.b('ppp dip.2@otherdomain.com'))

    def test_join_example_with_output_header(self):
        cmd = 'q -O "select myfiles.c8 aaa,emails.c2 bbb from {0}/exampledatafile myfiles join {0}/group-emails-example emails on (myfiles.c4 = emails.c1) where myfiles.c8 = \'ppp\'"'.format(EXAMPLES)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0], six.b('aaa bbb'))
        self.assertEqual(o[1], six.b('ppp dip.1@otherdomain.com'))
        self.assertEqual(o[2], six.b('ppp dip.2@otherdomain.com'))

    def test_self_join1(self):
        tmpfile = self.create_file_with_data(six.b("\n").join([six.b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = 'q "select * from %s a1 join %s a2 on (a1.c1 = a2.c1)"' % (tmpfile.name,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 10)

        self.cleanup(tmpfile)

    def test_self_join_reuses_table(self):
        tmpfile = self.create_file_with_data(six.b("\n").join([six.b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = 'q "select * from %s a1 join %s a2 on (a1.c1 = a2.c1)" -A' % (tmpfile.name,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0],six.b('Table for file: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  `c1` - int'))
        self.assertEqual(o[2],six.b('  `c2` - int'))

        self.cleanup(tmpfile)

    def test_self_join2(self):
        tmpfile1 = self.create_file_with_data(six.b("\n").join([six.b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = 'q "select * from %s a1 join %s a2 on (a1.c2 = a2.c2)"' % (tmpfile1.name,tmpfile1.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 10*10)

        self.cleanup(tmpfile1)

        tmpfile2 = self.create_file_with_data(six.b("\n").join([six.b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = 'q "select * from %s a1 join %s a2 on (a1.c2 = a2.c2) join %s a3 on (a1.c2 = a3.c2)"' % (tmpfile2.name,tmpfile2.name,tmpfile2.name)
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
        cmd = 'q -A -d , -H "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 5)


        self.assertEqual(o[0],six.b('Table for file: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  `regular_text` - text'))
        self.assertEqual(o[2],six.b('  `text_with_digits1` - int'))
        self.assertEqual(o[3],six.b('  `text_with_digits2` - int'))
        self.assertEqual(o[4],six.b('  `float_number` - float'))

        # Check column types detected when actual detection is disabled
        cmd = 'q -A -d , -H --as-text "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 5)

        self.assertEqual(o[0],six.b('Table for file: %s' % tmpfile.name))
        self.assertEqual(o[1],six.b('  `regular_text` - text'))
        self.assertEqual(o[2],six.b('  `text_with_digits1` - text'))
        self.assertEqual(o[3],six.b('  `text_with_digits2` - text'))
        self.assertEqual(o[4],six.b('  `float_number` - text'))

        # Get actual data with regular detection
        cmd = 'q -d , -H "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 4)

        self.assertEqual(o[0],six.b("regular text 1,67,67,12.3"))
        self.assertEqual(o[1],six.b("regular text 2,67,67,22.3"))
        self.assertEqual(o[2],six.b("regular text 3,123,123,33.4"))
        self.assertEqual(o[3],six.b("regular text 4,-123,-123,122.2"))

        # Get actual data without detection
        cmd = 'q -d , -H --as-text "select * from %s"' % (tmpfile.name)

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

    def test_simple_query(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r = q.execute('select * from %s' % tmpfile.name)

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r.data,[(1,2,3),(4,5,6)])
        self.assertEqual(len(r.metadata.data_loads),1)
        self.assertEqual(r.metadata.data_loads[0].filename,tmpfile.name)

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
        self.assertEqual(r1.metadata.data_loads[0].filename,tmpfile.name)

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r1.metadata.data_loads),1)
        self.assertEqual(r1.metadata.data_loads[0].filename,tmpfile.name)
        self.assertEqual(len(r2.metadata.data_loads),0)
        self.assertEqual(r2.data,r1.data)
        self.assertEqual(r2.metadata.output_column_name_list,r2.metadata.output_column_name_list)
        self.assertEqual(len(r2.warnings),0)

        self.cleanup(tmpfile)

    def test_stdin_injection(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r = q.execute('select * from -',stdin_file=codecs.open(tmpfile.name,'rb',encoding='utf-8'))

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r.data,[(1,2,3),(4,5,6)])
        self.assertEqual(len(r.metadata.data_loads),1)
        self.assertEqual(r.metadata.data_loads[0].filename,'-')

        self.cleanup(tmpfile)

    def test_named_stdin_injection(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r = q.execute('select a from my_stdin_data',stdin_file=codecs.open(tmpfile.name,'rb',encoding='utf-8'),stdin_filename='my_stdin_data')

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a'])
        self.assertEqual(r.data,[(1,),(4,)])
        self.assertEqual(len(r.metadata.data_loads),1)
        self.assertEqual(r.metadata.data_loads[0].filename,'my_stdin_data')

        self.cleanup(tmpfile)

    def test_stdin_injection_isolation(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("d e f\n7 8 9\n10 11 12"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r1 = q.execute('select * from -',stdin_file=codecs.open(tmpfile1.name,'rb',encoding='utf-8'))

        self.assertTrue(r1.status == 'ok')
        self.assertEqual(len(r1.warnings),0)
        self.assertEqual(len(r1.data),2)
        self.assertEqual(r1.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r1.data,[(1,2,3),(4,5,6)])
        self.assertEqual(len(r1.metadata.data_loads),1)
        self.assertEqual(r1.metadata.data_loads[0].filename,'-')

        r2 = q.execute('select * from -',stdin_file=codecs.open(tmpfile2.name,'rb',encoding='utf-8'))

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r2.warnings),0)
        self.assertEqual(len(r2.data),2)
        self.assertEqual(r2.metadata.output_column_name_list,['d','e','f'])
        self.assertEqual(r2.data,[(7,8,9),(10,11,12)])
        # There should be another data load, even though it's the same 'filename' as before
        self.assertEqual(len(r2.metadata.data_loads),1)
        self.assertEqual(r2.metadata.data_loads[0].filename,'-')

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_multiple_stdin_injection(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("d e f\n7 8 9\n10 11 12"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r1 = q.execute('select * from my_stdin_data1',stdin_file=codecs.open(tmpfile1.name,'rb',encoding='utf-8'),stdin_filename='my_stdin_data1')

        self.assertTrue(r1.status == 'ok')
        self.assertEqual(len(r1.warnings),0)
        self.assertEqual(len(r1.data),2)
        self.assertEqual(r1.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r1.data,[(1,2,3),(4,5,6)])
        self.assertEqual(len(r1.metadata.data_loads),1)
        self.assertEqual(r1.metadata.data_loads[0].filename,'my_stdin_data1')

        r2 = q.execute('select * from my_stdin_data2',stdin_file=codecs.open(tmpfile2.name,'rb',encoding='utf-8'),stdin_filename='my_stdin_data2')

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r2.warnings),0)
        self.assertEqual(len(r2.data),2)
        self.assertEqual(r2.metadata.output_column_name_list,['d','e','f'])
        self.assertEqual(r2.data,[(7,8,9),(10,11,12)])
        # There should be another data load, even though it's the same 'filename' as before
        self.assertEqual(len(r2.metadata.data_loads),1)
        self.assertEqual(r2.metadata.data_loads[0].filename,'my_stdin_data2')

        r3 = q.execute('select aa.*,bb.* from my_stdin_data1 aa join my_stdin_data2 bb')

        self.assertTrue(r3.status == 'ok')
        self.assertEqual(len(r3.warnings),0)
        self.assertEqual(len(r3.data),4)
        self.assertEqual(r3.metadata.output_column_name_list,['a','b','c','d','e','f'])
        self.assertEqual(r3.data,[(1,2,3,7,8,9),(1,2,3,10,11,12),(4,5,6,7,8,9),(4,5,6,10,11,12)])
        self.assertEqual(len(r3.metadata.data_loads),0)

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
        self.assertEqual(len(r.metadata.data_loads),0)

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_different_input_params_for_different_files(self):
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
        self.assertEqual(len(r.metadata.data_loads),0)

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
        self.assertEqual(len(r2.metadata.data_loads),1)
        self.assertEqual(r2.metadata.data_loads[0].filename,tmpfile.name)

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
        self.assertEqual(len(metadata.data_loads),1)
        self.assertEqual(len(metadata.table_structures),1)

        table_structure = metadata.table_structures[0]

        self.assertEqual(table_structure.column_names,[ 'a','b','c'])
        self.assertEqual(table_structure.column_types,[ 'int','int','int'])
        self.assertEqual(table_structure.filenames_str,tmpfile.name)
        self.assertTrue(len(table_structure.materialized_files.keys()),1)
        self.assertTrue(table_structure.materialized_files[tmpfile.name].filename,tmpfile.name)
        self.assertFalse(table_structure.materialized_files[tmpfile.name].is_stdin)

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
        self.assertEqual(len(metadata.data_loads),1)
        self.assertEqual(len(metadata.table_structures),1)

        table_structure = metadata.table_structures[0]

        self.assertEqual(table_structure.column_names,[ 'a','b','c'])
        self.assertEqual(table_structure.column_types,[ 'int','int','int'])
        self.assertEqual(table_structure.filenames_str,tmpfile.name)
        self.assertTrue(len(table_structure.materialized_files.keys()),1)
        self.assertTrue(table_structure.materialized_files[tmpfile.name].filename,tmpfile.name)
        self.assertFalse(table_structure.materialized_files[tmpfile.name].is_stdin)

        self.cleanup(tmpfile)

    def test_load_data_from_string(self):
        input_str = six.u('column1,column2,column3\n') + six.u('\n').join([six.u('value1,2.5,value3')] * 1000)

        q = QTextAsData()

        q.load_data_from_string('my_data',input_str,QInputParams(skip_header=True,delimiter=','))

        q_output = q.execute('select column2,column3 from my_data')

        self.assertTrue(q_output.status == 'ok')
        self.assertTrue(q_output.error is None)
        self.assertEqual(len(q_output.warnings),0)
        self.assertTrue(len(q_output.data),1000)
        self.assertEqual(len(set(q_output.data)),1)
        self.assertEqual(list(set(q_output.data))[0],(2.5,'value3'))

        metadata = q_output.metadata

        self.assertTrue(metadata.output_column_name_list,['column2','column3'])
        self.assertEqual(len(metadata.data_loads),0)
        self.assertTrue(len(metadata.table_structures),1)

        table_structure = metadata.table_structures[0]

        self.assertEqual(table_structure.column_names,['column1','column2','column3'])
        self.assertEqual(table_structure.column_types,['text','float','text'])
        self.assertEqual(table_structure.filenames_str,'my_data')
        self.assertTrue(len(table_structure.materialized_files.keys()),1)
        self.assertTrue(table_structure.materialized_files['my_data'].filename,'my_data')
        self.assertTrue(table_structure.materialized_files['my_data'].is_stdin)
