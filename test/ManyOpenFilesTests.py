from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, batch, run_command
from test.test_data import sample_data_no_header, sample_data_rows
from test.base import AbstractQTestCase

from test.utils import b
from test.utils import DEBUG

import collections
import os


class ManyOpenFilesTests(AbstractQTestCase):


    def test_multi_file_header_skipping(self):
        BATCH_SIZE = 50
        FILE_COUNT = 5

        numbers = list(range(1,1+BATCH_SIZE*FILE_COUNT))
        numbers_as_text = batch([str(x) for x in numbers],n=BATCH_SIZE)

        content_list = list(map(b,['a\n' + "\n".join(x)+'\n' for x in numbers_as_text]))

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','multi-header')

        cmd = '%s -d , -H -c 1 "select count(a),sum(a) from %s/*" -C none' % (Q_EXECUTABLE,tmpfolder)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],b("%s,%s" % (BATCH_SIZE*FILE_COUNT,sum(numbers))))

        self.cleanup_folder(tmpfolder)

    def test_that_globs_dont_max_out_sqlite_attached_database_limits(self):
        BATCH_SIZE = 50
        FILE_COUNT = 40

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(b,["\n".join(x)+'\n' for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','attach-limit')
        #expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        cmd = 'cd %s && %s -c 1 "select count(*) from *" -C none --max-attached-sqlite-databases=10' % (tmpfolder,Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],b(str(BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_maxing_out_max_attached_database_limits__regular_files(self):
        BATCH_SIZE = 50
        FILE_COUNT = 40

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(b,["\n".join(x)+'\n' for x in numbers_as_text])

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

        self.assertEqual(o[0],b(str(BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_maxing_out_max_attached_database_limits__with_qsql_files_below_attached_limit(self):
        MAX_ATTACHED_SQLITE_DATABASES = 10

        BATCH_SIZE = 50
        FILE_COUNT = MAX_ATTACHED_SQLITE_DATABASES - 1

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(b,["\n".join(x)+'\n' for x in numbers_as_text])

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
        self.assertEqual(o[0],b(str(BATCH_SIZE*FILE_COUNT)))

        # Now execute the same query with -C readwrite, so all files will be read directly from the qsql files
        cmd = 'cd %s && %s -c 1 "select count(*) from (%s)" -C readwrite' % (tmpfolder,Q_EXECUTABLE,unioned_subquery)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],b(str(BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_maxing_out_max_attached_database_limits__with_qsql_files_above_attached_limit(self):
        MAX_ATTACHED_SQLITE_DATABASES = 10

        BATCH_SIZE = 50
        # Here's the difference from test_maxing_out_max_attached_database_limits__with_qsql_files_below_attached_limit
        # We're trying to cache 2 times the number of files than the number of databases that can be attached.
        # Expectation is that only a part of the files will be cached
        FILE_COUNT = MAX_ATTACHED_SQLITE_DATABASES * 2

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(b,["\n".join(x)+'\n' for x in numbers_as_text])

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
        self.assertEqual(o[0],b(str(BATCH_SIZE*FILE_COUNT)))

        # Now execute the same query with -C readwrite, so all files will be read directly from the qsql files
        cmd = 'cd %s && %s -c 1 "select count(*) from (%s)" -C readwrite' % (tmpfolder,Q_EXECUTABLE,unioned_subquery)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],b(str(BATCH_SIZE*FILE_COUNT)))

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

        content_list = map(b,["\n".join(x)+'\n' for x in numbers_as_text])

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
        self.assertEqual(o[0],b(str(BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_too_many_open_files_for_one_table(self):
        # Previously file opening was parallel, causing too-many-open-files

        MAX_ALLOWED_FILES = 500

        BATCH_SIZE = 2
        FILE_COUNT = MAX_ALLOWED_FILES + 1

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(b,["\n".join(x) for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','attach-limit')

        cmd = 'cd %s && %s -c 1 "select count(*) from * where 1 = 1 or c1 != 2" -C none' % (tmpfolder,Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 82)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        x = b('Maximum source files for table must be %s. Table is name is %s/* Number of actual files is %s' % (MAX_ALLOWED_FILES,os.path.realpath(tmpfolder),FILE_COUNT))
        print(x)
        self.assertEqual(e[0],x)

        self.cleanup_folder(tmpfolder)

    def test_many_open_files_for_one_table(self):
        # Previously file opening was parallel, causing too-many-open-files

        BATCH_SIZE = 2
        FILE_COUNT = 500

        numbers_as_text = batch([str(x) for x in range(1,1+BATCH_SIZE*FILE_COUNT)],n=BATCH_SIZE)

        content_list = map(b,["\n".join(x) for x in numbers_as_text])

        filename_list = list(map(lambda x: 'file-%s' % x,range(FILE_COUNT)))
        d = collections.OrderedDict(zip(filename_list, content_list))

        tmpfolder = self.create_folder_with_files(d,'split-files','attach-limit')
        #expected_cache_filename = os.path.join(tmpfile_folder,tmpfile_filename + '.qsql')

        cmd = 'cd %s && %s -c 1 "select count(*) from * where 1 = 1 or c1 != 2" -C none' % (tmpfolder,Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],b(str(BATCH_SIZE*FILE_COUNT)))

        self.cleanup_folder(tmpfolder)

    def test_many_open_files_for_two_tables(self):
        BATCH_SIZE = 2
        FILE_COUNT = 500

        numbers_as_text = batch([str(x) for x in range(1, 1 + BATCH_SIZE * FILE_COUNT)], n=BATCH_SIZE)

        content_list = map(b, ["\n".join(x) for x in numbers_as_text])

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

        self.assertEqual(o[0], b(str(BATCH_SIZE * FILE_COUNT)))

        self.cleanup_folder(tmpfolder1)
        self.cleanup_folder(tmpfolder2)