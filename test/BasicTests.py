from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command


import six
from six.moves import range

from test.utils import DEBUG
import os
from test.base import AbstractQTestCase
from test.test_data import (uneven_ls_output,find_output,header_row,sample_data_rows,sample_data_rows_with_empty_string,sample_data_no_header,sample_data_with_empty_string_no_header,sample_data_with_header,sample_data_with_missing_header_names,generate_sample_data_with_header,sample_quoted_data,double_double_quoted_data,escaped_double_quoted_data,combined_quoted_data,sample_quoted_data2,sample_quoted_data2_with_newline,one_column_data,sample_data_rows_with_spaces,sample_data_with_spaces_no_header,header_row_with_spaces,sample_data_with_spaces_with_header,long_value1,int_value,sample_data_with_long_values)

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