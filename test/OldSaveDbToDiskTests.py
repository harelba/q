from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command, sqlite_dict_factory

from test.utils import DEBUG, b
from test.test_data import sample_data_no_header, sample_data_rows
from test.base import AbstractQTestCase

import os
import re
import sqlite3


class OldSaveDbToDiskTests(AbstractQTestCase):

    def test_join_with_stdin_and_save(self):
        x = [b(a) for a in map(str,range(1,101))]
        large_file_data = b("val\n") + b("\n").join(x)
        tmpfile = self.create_file_with_data(large_file_data)
        tmpfile_expected_table_name = os.path.basename(tmpfile.name)

        disk_db_filename = self.random_tmp_filename('save-to-db','sqlite')

        cmd = '(echo id ; seq 1 2 10) | ' + Q_EXECUTABLE + ' -c 1 -H -O "select stdin.*,f.* from - stdin left join %s f on (stdin.id * 10 = f.val)" -S %s' % \
            (tmpfile.name,disk_db_filename)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 4)

        self.assertEqual(e[0],b('Going to save data into a disk database: %s' % disk_db_filename))
        self.assertTrue(e[1].startswith(b('Data has been saved into %s . Saving has taken ' % disk_db_filename)))
        self.assertEqual(e[2],b('Query to run on the database: select stdin.*,f.* from data_stream_stdin stdin left join %s f on (stdin.id * 10 = f.val);' % \
                         tmpfile_expected_table_name))
        self.assertEqual(e[3],b('You can run the query directly from the command line using the following command: echo "select stdin.*,f.* from data_stream_stdin stdin left join %s f on (stdin.id * 10 = f.val)" | sqlite3 %s' %
                                    (tmpfile_expected_table_name,disk_db_filename)))

        P = re.compile(b("^Query to run on the database: (?P<query_to_run_on_db>.*)$"))
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
        numbers1 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 10001)]
        numbers2 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 11)]

        header = [b('aa'), b('bb'), b('cc')]

        new_tmp_folder = self.create_folder_with_files({
            'some_csv_file': self.arrays_to_csv_file_content(b(','),header,numbers1),
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
        self.assertEqual(o[0],b('50005000,55'))

    # TODO RLRL Check if needed anymore

    # def test_creation_of_qsql_database(self):
    #     numbers = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 11)]
    #     header = [b('aa'), b('bb'), b('cc')]
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
        numbers1 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 10001)]
        numbers2 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 11)]

        header = [b('aa'), b('bb'), b('cc')]

        saved_qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        new_tmp_folder = self.create_folder_with_files({
            'some_csv_file': self.arrays_to_csv_file_content(b(','),header,numbers1),
            'some_qsql_database' : self.arrays_to_csv_file_content(b(','),header,numbers2)
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
        numbers1 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 10001)]
        numbers2 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 11)]

        header = [b('aa'), b('bb'), b('cc')]

        qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        new_tmp_folder = self.create_folder_with_files({
            'filename1': self.arrays_to_csv_file_content(b(','),header,numbers1),
            'otherfolder/filename1' : self.arrays_to_csv_file_content(b(','),header,numbers2)
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
        self.assertEqual(e[0], b('Going to save data into a disk database: %s' % qsql_with_multiple_tables))
        self.assertTrue(e[1].startswith(b('Data has been saved into %s . Saving has taken' % qsql_with_multiple_tables)))
        self.assertEqual(e[2],b('Query to run on the database: select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s small_file left join %s large_file on (large_file.aa == small_file.bb);' % \
                                    (expected_stored_table_name1,expected_stored_table_name2)))
        self.assertEqual(e[3],b('You can run the query directly from the command line using the following command: echo "select sum(large_file.aa),sum(large_file.bb),sum(large_file.cc) from %s small_file left join %s large_file on (large_file.aa == small_file.bb)" | sqlite3 %s' % \
                                    (expected_stored_table_name1,expected_stored_table_name2,qsql_with_multiple_tables)))

        #self.assertTrue(False) # pxpx - need to actually test reading from the saved db file
        conn = sqlite3.connect(qsql_with_multiple_tables)
        c1 = conn.execute('select count(*) from filename1').fetchall()
        c2 = conn.execute('select count(*) from filename1_2').fetchall()

        self.assertEqual(c1[0][0],10000)
        self.assertEqual(c2[0][0],10)


    def test_error_when_not_specifying_table_name_in_multi_table_qsql(self):
        numbers1 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 10001)]
        numbers2 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 11)]

        header = [b('aa'), b('bb'), b('cc')]

        qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        new_tmp_folder = self.create_folder_with_files({
            'filename1': self.arrays_to_csv_file_content(b(','),header,numbers1),
            'otherfolder/filename1' : self.arrays_to_csv_file_content(b(','),header,numbers2)
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
        self.assertEqual(e[0],b('Could not autodetect table name in sqlite file %s . Existing tables: %s,%s' % (qsql_with_multiple_tables,expected_stored_table_name1,expected_stored_table_name2)))

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
        self.assertEqual(e[0],b('Could not autodetect table name in sqlite file %s . Existing tables: my_table_1,my_table_2' % sqlite_with_multiple_tables))

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
        self.assertEqual(o[0],b('100,200'))
        self.assertEqual(o[1],b('300,400'))

        # Check again, this time with a different output delimiter and with explicit column names
        cmd = '%s -t "select x,y from %s:::my_table_1"' % (Q_EXECUTABLE,sqlite_with_multiple_tables)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],b('100\t200'))
        self.assertEqual(o[1],b('300\t400'))


    def test_error_when_specifying_nonexistent_table_name_in_multi_table_qsql(self):
        numbers1 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 10001)]
        numbers2 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 11)]

        header = [b('aa'), b('bb'), b('cc')]

        qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        new_tmp_folder = self.create_folder_with_files({
            'filename1': self.arrays_to_csv_file_content(b(','),header,numbers1),
            'otherfolder/filename1' : self.arrays_to_csv_file_content(b(','),header,numbers2)
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
        self.assertEqual(e[0],b('Table non_existent_table could not be found in sqlite file %s . Existing table names: %s,%s' % \
                                    (qsql_with_multiple_tables,expected_stored_table_name1,expected_stored_table_name2)))

    def test_querying_multi_table_qsql_file(self):
        numbers1 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 10001)]
        numbers2 = [[b(str(i)), b(str(i)), b(str(i))] for i in range(1, 11)]

        header = [b('aa'), b('bb'), b('cc')]

        qsql_with_multiple_tables = self.generate_tmpfile_name(suffix='.qsql')

        new_tmp_folder = self.create_folder_with_files({
            'filename1': self.arrays_to_csv_file_content(b(','),header,numbers1),
            'otherfolder/filename1' : self.arrays_to_csv_file_content(b(','),header,numbers2)
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
        self.assertEqual(o[0],b('10000'))

        cmd = '%s "select count(*) from %s:::%s"' % (Q_EXECUTABLE,qsql_with_multiple_tables,expected_stored_table_name2)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0],b('10'))

    def test_preventing_db_overwrite(self):
        db_filename = self.random_tmp_filename('store-to-disk', 'db')
        self.assertFalse(os.path.exists(db_filename))

        retcode, o, e = run_command('seq 1 1000 | ' + Q_EXECUTABLE + ' "select count(*) from -" -c 1 -S %s' % db_filename)

        self.assertTrue(retcode == 0)
        self.assertTrue(os.path.exists(db_filename))

        retcode2, o2, e2 = run_command('seq 1 1000 | ' + Q_EXECUTABLE + ' "select count(*) from -" -c 1 -S %s' % db_filename)
        self.assertTrue(retcode2 != 0)
        self.assertTrue(e2[0].startswith(b('Going to save data into a disk database')))
        self.assertTrue(e2[1] == b('Disk database file {} already exists.'.format(db_filename)))

        os.remove(db_filename)