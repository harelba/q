from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command


from test.utils import DEBUG,b
import os
import sqlite3
from test.test_data import sample_data_with_header
from test.test_data import sample_data_no_header
from test.test_data import header_row_with_spaces
from test.base import AbstractQTestCase

class CachingTests(AbstractQTestCase):

    def test_cache_empty_file(self):
        file_data = b("a,b,c")
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
        self.assertEqual(e[0],b("Warning - data is empty"))

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0],b("Warning - data is empty"))

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
        file_data1 = b("a,b,c\n10,20,30\n30,40,50")

        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], b('10'))
        self.assertEqual(o[1], b('30'))

        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename))

        # Overwrite the original file
        file_data2 = b("a,b,c\n10,20,30\n30,40,50\n50,60,70")
        self.write_file(tmpfile1.name,file_data2)

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 81)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0], b('%s vs %s.qsql: Content Signatures differ at inferer.rows (actual analysis data differs)' % \
                                     (tmpfile1.name,tmpfile1.name)))


    def test_reading_the_wrong_cache__original_file_having_different_delimiter(self):
        file_data1 = b("a,b,c\n10,20,30\n30,40,50")

        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], b('10'))
        self.assertEqual(o[1], b('30'))

        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename))

        # Overwrite the original file
        file_data2 = b("a\tb\tc\n10\t20\t30\n30\t40\t50")
        self.write_file(tmpfile1.name,file_data2)

        cmd = Q_EXECUTABLE + ' -H -t "select a from %s" -C read' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 80)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)
        x = b("%s vs %s.qsql: Content Signatures for table %s differ at input_delimiter (source value '\t' disk signature value ',')" % \
                                     (tmpfile1.name,tmpfile1.name,tmpfile1.name))
        self.assertEqual(e[0], x)

    def test_rename_cache_and_read_from_it(self):
        # create a file, along with its qsql
        file_data1 = b("a,b,c\n10,20,30\n30,40,50")

        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], b('10'))
        self.assertEqual(o[1], b('30'))
        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename1))

        tmp_fn = self.generate_tmpfile_name("aa","qsql")
        os.rename(expected_cache_filename1,tmp_fn)

        cmd = '%s "select a from %s"' % (Q_EXECUTABLE,tmp_fn)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], b('10'))
        self.assertEqual(o[1], b('30'))


    def test_reading_the_wrong_cache__qsql_file_not_having_a_matching_content_signature(self):
        # create a file, along with its qsql
        file_data1 = b("a,b,c\n10,20,30\n30,40,50")

        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], b('10'))
        self.assertEqual(o[1], b('30'))
        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename1))

        file_data2 = b("c,d,e\n10,20,30\n30,40,50")

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
        self.assertTrue(o[0], b('10'))
        self.assertEqual(o[1], b('30'))
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
        x = b("%s vs %s.qsql: Content Signatures for table %s differ at inferer.header_row (source value '['a', 'b', 'c']' disk signature value '['c', 'd', 'e']')" % (tmpfile1.name,tmpfile1.name,tmpfile1.name))
        self.assertEqual(e[0], x)

    def test_reading_the_wrong_cache__qsql_file_not_having_any_content_signature(self):
        # create a file, along with its qsql
        file_data1 = b("a,b,c\n10,20,30\n30,40,50")

        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile1.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0], b('10'))
        self.assertEqual(o[1], b('30'))
        # Ensure cache has been created
        self.assertTrue(os.path.exists(expected_cache_filename1))

        file_data2 = b("c,d,e\n10,20,30\n30,40,50")

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
        self.assertEqual(e[0],b("Could not autodetect table name in qsql file. File contains no record of a table"))


    def test_cache_full_flow(self):
        file_data = b("a,b,c\n10,20,30\n30,40,50")
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
        self.assertTrue(o[0],b('10'))
        self.assertEqual(o[1],b('30'))

        # Ensure cache has not been created
        self.assertTrue(not os.path.exists(expected_cache_filename))

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0],b('10'))
        self.assertEqual(o[1],b('30'))

        # Ensure cache has not been created, as cache mode is "read" only
        self.assertTrue(not os.path.exists(expected_cache_filename))

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C readwrite' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)
        self.assertTrue(o[0],b('10'))
        self.assertEqual(o[1],b('30'))

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
        self.assertTrue(o[0],b('10'))
        self.assertEqual(o[1],b('30'))

        # After readwrite caching has been activated, the cache file is expected to exist
        self.assertTrue(os.path.exists(expected_cache_filename))

        self.cleanup(tmpfile)

    def test_cache_full_flow_with_concatenated_files(self):
        file_data1 = b("a,b,c\n10,11,12\n20,21,22")
        tmpfile1 = self.create_file_with_data(file_data1)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        file_data2 = b("a,b,c\n30,31,32\n40,41,42")
        tmpfile2 = self.create_file_with_data(file_data2)
        tmpfile2_folder = os.path.dirname(tmpfile2.name)
        tmpfile2_filename = os.path.basename(tmpfile2.name)
        expected_cache_filename2 = os.path.join(tmpfile2_folder,tmpfile2_filename + '.qsql')

        cmd = Q_EXECUTABLE + ' -O -H -d , "select * from (select * from %s UNION ALL select * from %s)" -C readwrite' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 5)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],b('a,b,c'))
        self.assertEqual(o[1],b('10,11,12'))
        self.assertEqual(o[2],b('20,21,22'))
        self.assertEqual(o[3],b('30,31,32'))
        self.assertEqual(o[4],b('40,41,42'))

        self.assertTrue(os.path.exists(expected_cache_filename1))
        self.assertTrue(os.path.exists(expected_cache_filename2))

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)


    def test_analyze_result_with_cache_file(self):
        file_data = b("a,b,c\n10,20,30\n30,40,50")
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
        self.assertTrue(o[0],b('10'))
        self.assertEqual(o[1],b('30'))

        # Ensure cache is now created
        self.assertTrue(os.path.exists(expected_cache_filename))

        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),7)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: qsql-file-with-original source: %s.qsql' % tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4],b('    `a` - int'))
        self.assertEqual(o[5],b('    `b` - int'))
        self.assertEqual(o[6],b('    `c` - int'))

        # delete the newly created cache
        os.remove(expected_cache_filename)

        # Now rerun the analysis without the cache file
        cmd = Q_EXECUTABLE + ' -H -d , "select a from %s" -C read -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),7)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4],b('    `a` - int'))
        self.assertEqual(o[5],b('    `b` - int'))
        self.assertEqual(o[6],b('    `c` - int'))

        self.cleanup(tmpfile)

    def test_partial_caching_exists(self):
        file1_data = b("a,b,c\n10,20,30\n30,40,50\n60,70,80")
        tmpfile1 = self.create_file_with_data(file1_data)
        tmpfile1_folder = os.path.dirname(tmpfile1.name)
        tmpfile1_filename = os.path.basename(tmpfile1.name)
        expected_cache_filename1 = os.path.join(tmpfile1_folder,tmpfile1_filename + '.qsql')

        file2_data = b("b,x\n10,linewith10\n20,linewith20\n30,linewith30\n40,linewith40")
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
        self.assertTrue(o[0],b('10'))
        self.assertEqual(o[1],b('30'))

        # Ensure cache has been created for file 1
        self.assertTrue(os.path.exists(expected_cache_filename1))

        # Use both files with read caching, one should be read from cache, the other from the file
        cmd = Q_EXECUTABLE + ' -H -d , "select file1.a,file1.b,file1.c,file2.x from %s file1 left join %s file2 on (file1.b = file2.b)" -C read' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],b('10,20,30,linewith20'))
        self.assertEqual(o[1],b('30,40,50,linewith40'))
        self.assertEqual(o[2],b('60,70,80,'))

        # Ensure cache has NOT been created for file 2
        self.assertTrue(not os.path.exists(expected_cache_filename2))

        # Now rerun the query, this time with readwrite caching, so the second file cache will be written
        cmd = Q_EXECUTABLE + ' -H -d , "select file1.a,file1.b,file1.c,file2.x from %s file1 left join %s file2 on (file1.b = file2.b)" -C readwrite' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0],b('10,20,30,linewith20'))
        self.assertEqual(o[1],b('30,40,50,linewith40'))
        self.assertEqual(o[2],b('60,70,80,'))

        # Ensure cache has now been created for file 2
        self.assertTrue(os.path.exists(expected_cache_filename2))

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)