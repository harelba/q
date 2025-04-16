from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import (uneven_ls_output,find_output,header_row,sample_data_rows,sample_data_rows_with_empty_string,sample_data_no_header,sample_data_with_empty_string_no_header,sample_data_with_header,sample_data_with_missing_header_names,generate_sample_data_with_header,sample_quoted_data,double_double_quoted_data,escaped_double_quoted_data,combined_quoted_data,sample_quoted_data2,sample_quoted_data2_with_newline,one_column_data,sample_data_rows_with_spaces,sample_data_with_spaces_no_header,header_row_with_spaces,sample_data_with_spaces_with_header,long_value1,int_value,sample_data_with_long_values)
from test.base import AbstractQTestCase

from test.utils import b
from test.utils import DEBUG

class ParsingModeTests(AbstractQTestCase):

    def test_strict_mode_column_count_mismatch_error(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = Q_EXECUTABLE + ' -m strict "select count(*) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertTrue(b("Column Count is expected to identical") in e[0])

        self.cleanup(tmpfile)

    def test_strict_mode_too_large_specific_column_count(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -m strict -c 4 "select count(*) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertEqual(
            e[0], b("Strict mode. Column count is expected to be 4 but is 3"))

        self.cleanup(tmpfile)

    def test_strict_mode_too_small_specific_column_count(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -m strict -c 2 "select count(*) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertEqual(
            e[0], b("Strict mode. Column count is expected to be 2 but is 3"))

        self.cleanup(tmpfile)

    def test_relaxed_mode_missing_columns_in_header(self):
        tmpfile = self.create_file_with_data(
            sample_data_with_missing_header_names)
        cmd = Q_EXECUTABLE + ' -d , -m relaxed "select count(*) from %s" -H -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 7)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s') % b(tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4],b('    `name` - text'))
        self.assertEqual(o[5],b('    `value1` - int'))
        self.assertEqual(o[6],b('    `c3` - int'))

        self.cleanup(tmpfile)

    def test_strict_mode_missing_columns_in_header(self):
        tmpfile = self.create_file_with_data(
            sample_data_with_missing_header_names)
        cmd = Q_EXECUTABLE + ' -d , -m strict "select count(*) from %s" -H -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 1)

        self.assertEqual(
            e[0], b('Strict mode. Header row contains less columns than expected column count(2 vs 3)'))

        self.cleanup(tmpfile)

    def test_output_delimiter_with_missing_fields(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select * from %s" -D ";"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], b('a;1;0'))
        self.assertEqual(o[1], b('b;2;0'))
        self.assertEqual(o[2], b('c;;0'))

        self.cleanup(tmpfile)

    def test_handling_of_null_integers(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select avg(c2) from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], b('1.5'))

        self.cleanup(tmpfile)

    def test_empty_integer_values_converted_to_null(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select * from %s where c2 is null"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], b('c,,0'))

        self.cleanup(tmpfile)

    def test_empty_string_values_not_converted_to_null(self):
        tmpfile = self.create_file_with_data(
            sample_data_with_empty_string_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select * from %s where c2 == %s"' % (
            tmpfile.name, "''")
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], b('c,,0'))

        self.cleanup(tmpfile)

    def test_relaxed_mode_detected_columns(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = Q_EXECUTABLE + ' -m relaxed "select count(*) from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)

        column_rows = o[o.index(b('  Fields:'))+1:]

        self.assertEqual(len(column_rows), 11)

        column_tuples = [x.strip().split(b(" ")) for x in column_rows]
        column_info = [(x[0], x[2]) for x in column_tuples]
        column_names = [x[0] for x in column_tuples]
        column_types = [x[2] for x in column_tuples]

        self.assertEqual(column_names, [b('`c{}`'.format(x)) for x in range(1, 12)])
        self.assertEqual(column_types, list(map(lambda x:b(x),[
                          'text', 'int', 'text', 'text', 'int', 'text', 'int', 'int', 'text', 'text', 'text'])))

        self.cleanup(tmpfile)

    def test_relaxed_mode_detected_columns_with_specific_column_count(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = Q_EXECUTABLE + ' -m relaxed "select count(*) from %s" -A -c 9' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)

        column_rows = o[o.index(b('  Fields:'))+1:]

        self.assertEqual(len(column_rows), 9)

        column_tuples = [x.strip().split(b(" ")) for x in column_rows]
        column_info = [(x[0], x[2]) for x in column_tuples]
        column_names = [x[0] for x in column_tuples]
        column_types = [x[2] for x in column_tuples]

        self.assertEqual(column_names, [b('`c{}`'.format(x)) for x in range(1, 10)])
        self.assertEqual(
            column_types, list(map(lambda x:b(x),['text', 'int', 'text', 'text', 'int', 'text', 'int', 'int', 'text'])))

        self.cleanup(tmpfile)

    def test_relaxed_mode_last_column_data_with_specific_column_count(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c9 from %s" -c 9' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 9)
        self.assertEqual(len(e), 0)

        expected_output = list(map(lambda x:b(x),["/selinux", "/mnt", "/srv", "/lost+found", '"/initrd.img.old -> /boot/initrd.img-3.8.0-19-generic"',
                           "/cdrom", "/home", '"/vmlinuz -> boot/vmlinuz-3.8.0-19-generic"', '"/initrd.img -> boot/initrd.img-3.8.0-19-generic"']))

        self.assertEqual(o, expected_output)

        self.cleanup(tmpfile)

    def test_1_column_warning_in_relaxed_mode(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c1 from %s" -d ,' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],b('data without commas 1'))
        self.assertEqual(o[1],b('data without commas 2'))

        self.cleanup(tmpfile)

    def test_1_column_warning_in_strict_mode(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c1 from %s" -d , -m strict' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],b('data without commas 1'))
        self.assertEqual(o[1],b('data without commas 2'))

        self.cleanup(tmpfile)


    def test_1_column_warning_suppression_in_relaxed_mode_when_column_count_is_specific(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c1 from %s" -d , -m relaxed -c 1' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],b('data without commas 1'))
        self.assertEqual(o[1],b('data without commas 2'))

        self.cleanup(tmpfile)

    def test_1_column_warning_suppression_in_strict_mode_when_column_count_is_specific(self):
        tmpfile = self.create_file_with_data(one_column_data)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c1 from %s" -d , -m strict -c 1' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],b('data without commas 1'))
        self.assertEqual(o[1],b('data without commas 2'))

        self.cleanup(tmpfile)

    def test_fluffy_mode__as_relaxed_mode(self):
        tmpfile = self.create_file_with_data(uneven_ls_output)
        cmd = Q_EXECUTABLE + ' -m relaxed "select c9 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 9)
        self.assertEqual(len(e), 0)

        expected_output = list(map(lambda x:b(x),["/selinux", "/mnt", "/srv", "/lost+found",
                           "/initrd.img.old", "/cdrom", "/home", "/vmlinuz", "/initrd.img"]))

        self.assertEqual(o, expected_output)

        self.cleanup(tmpfile)

    def test_relaxed_mode_column_count_mismatch__was_previously_fluffy_mode_test(self):
        data_row = b("column1 column2 column3 column4")
        data_list = [data_row] * 1000
        data_list[950] = b("column1 column2 column3 column4 column5")
        tmpfile = self.create_file_with_data(b("\n").join(data_list))

        cmd = Q_EXECUTABLE + ' -m relaxed "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),1000)
        self.assertEqual(len(e),0)
        self.assertEqual(o[950],b('column1 column2 column3 "column4 column5"'))

        self.cleanup(tmpfile)

    def test_strict_mode_column_count_mismatch__less_columns(self):
        data_row = b("column1 column2 column3 column4")
        data_list = [data_row] * 1000
        data_list[750] = b("column1 column3 column4")
        tmpfile = self.create_file_with_data(b("\n").join(data_list))

        cmd = Q_EXECUTABLE + ' -m strict "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertTrue(e[0].startswith(b("Strict mode - Expected 4 columns instead of 3 columns")))
        self.assertTrue(b(' row 751.') in e[0])

        self.cleanup(tmpfile)

    def test_strict_mode_column_count_mismatch__more_columns(self):
        data_row = b("column1 column2 column3 column4")
        data_list = [data_row] * 1000
        data_list[750] = b("column1 column2 column3 column4 column5")
        tmpfile = self.create_file_with_data(b("\n").join(data_list))

        cmd = Q_EXECUTABLE + ' -m strict "select * from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(o),0)
        self.assertEqual(len(e),1)
        self.assertTrue(e[0].startswith(b("Strict mode - Expected 4 columns instead of 5 columns")))
        self.assertTrue(b(' row 751.') in e[0])

        self.cleanup(tmpfile)