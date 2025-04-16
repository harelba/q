from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, batch, get_sqlite_table_list, partition, run_command


import six
from six.moves import range
from test.utils import DEBUG
from test.base import AbstractQTestCase
import collections
import glob
import os
import sqlite3


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