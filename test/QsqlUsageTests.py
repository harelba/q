from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command


import six
from six.moves import range
from test.utils import DEBUG
from test.base import AbstractQTestCase
import os
import sqlite3


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