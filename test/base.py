from test.utils import Q_EXECUTABLE, run_command


import six


import os
import random
import unittest
from tempfile import NamedTemporaryFile
from test.utils import DEBUG

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