from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import sample_quoted_data
from test.test_data import sample_quoted_data2
from test.test_data import sample_quoted_data2_with_newline
from test.test_data import double_double_quoted_data
from test.test_data import escaped_double_quoted_data
from test.test_data import combined_quoted_data

import six
from test.utils import DEBUG

class QuotingTests(AbstractQTestCase):
    def test_non_quoted_values_in_quoted_data(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data)

        cmd = Q_EXECUTABLE + ' -d " " "select c1 from %s"' % tmp_data_file.name
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

        cmd = Q_EXECUTABLE + ' -d " " "select c2 from %s"' % tmp_data_file.name
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

        cmd = Q_EXECUTABLE + ' -d " " "select c3 from %s"' % tmp_data_file.name
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

        cmd = Q_EXECUTABLE + ' -d " " "select c4 from %s"' % tmp_data_file.name
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

        cmd = Q_EXECUTABLE + ' -d " " -m relaxed -D , -w none -W none "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted,data",23'))
        self.assertEqual(o[1],six.b('unquoted-data,54,'))

        self.cleanup(tmp_data_file)

    def test_none_input_quoting_mode_in_strict_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -m strict -D , -w none "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(e),1)
        self.assertEqual(len(o),0)

        self.assertTrue(e[0].startswith(six.b('Strict mode. Column Count is expected to identical')))

        self.cleanup(tmp_data_file)

    def test_minimal_input_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w minimal "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_all_input_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w all "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_incorrect_input_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w unknown_wrapping_mode "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode,0)
        self.assertEqual(len(e),1)
        self.assertEqual(len(o),0)

        self.assertTrue(e[0].startswith(six.b('Input quoting mode can only be one of all,minimal,none')))
        self.assertTrue(six.b('unknown_wrapping_mode') in e[0])

        self.cleanup(tmp_data_file)

    def test_none_output_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w all -W none "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_minimal_output_quoting_mode__without_need_to_quote_in_output(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w all -W minimal "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('quoted data,23'))
        self.assertEqual(o[1],six.b('unquoted-data,54'))

        self.cleanup(tmp_data_file)

    def test_minimal_output_quoting_mode__with_need_to_quote_in_output_due_to_delimiter(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        # output delimiter is set to space, so the output will contain it
        cmd = Q_EXECUTABLE + ' -d " " -D " " -w all -W minimal "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted data" 23'))
        self.assertEqual(o[1],six.b('unquoted-data 54'))

        self.cleanup(tmp_data_file)

    def test_minimal_output_quoting_mode__with_need_to_quote_in_output_due_to_newline(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2_with_newline)

        # Delimiter is set to colon (:), so it will not be inside the data values (this will make sure that the newline is the one causing the quoting)
        cmd = Q_EXECUTABLE + " -d ':' -w all -W minimal \"select c1,c2,replace(c1,'with' || x'0a' || 'a new line inside it','NEWLINE-REMOVED') from %s\"" % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),3)

        self.assertEqual(o[0],six.b('"quoted data with'))
        # Notice that the third column here is not quoted, because we replaced the newline with something else
        self.assertEqual(o[1],six.b('a new line inside it":23:quoted data NEWLINE-REMOVED'))
        self.assertEqual(o[2],six.b('unquoted-data:54:unquoted-data'))

        self.cleanup(tmp_data_file)

    def test_nonnumeric_output_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w all -W nonnumeric "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted data",23'))
        self.assertEqual(o[1],six.b('"unquoted-data",54'))

        self.cleanup(tmp_data_file)

    def test_all_output_quoting_mode(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data2)

        cmd = Q_EXECUTABLE + ' -d " " -D , -w all -W all "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('"quoted data","23"'))
        self.assertEqual(o[1],six.b('"unquoted-data","54"'))

        self.cleanup(tmp_data_file)

    def _internal_test_consistency_of_chaining_output_to_input(self,input_data,input_wrapping_mode,output_wrapping_mode):

        tmp_data_file = self.create_file_with_data(input_data)

        basic_cmd = Q_EXECUTABLE + ' -w %s -W %s "select * from -"' % (input_wrapping_mode,output_wrapping_mode)
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

    def test_input_field_quoting_and_data_types_with_encoding(self):
        OUTPUT_ENCODING = 'utf-8'

        # Checks combination of minimal input field quoting, with special characters that need to be decoded -
        # Both content and proper data types are verified
        data = six.b('111,22.22,"testing text with special characters - citt\xc3\xa0 ",http://somekindofurl.com,12.13.14.15,12.1\n')
        tmp_data_file = self.create_file_with_data(data)

        cmd = Q_EXECUTABLE + ' -d , "select * from %s" -E %s' % (tmp_data_file.name,OUTPUT_ENCODING)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),1)

        self.assertEqual(o[0].decode('utf-8'),u'111,22.22,testing text with special characters - citt\xe0 ,http://somekindofurl.com,12.13.14.15,12.1')

        cmd = Q_EXECUTABLE + ' -d , "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),10)

        self.assertEqual(o[0],six.b('Table: %s' % tmp_data_file.name))
        self.assertEqual(o[1],six.b('  Sources:'))
        self.assertEqual(o[2],six.b('    source_type: file source: %s' % tmp_data_file.name))
        self.assertEqual(o[3],six.b('  Fields:'))
        self.assertEqual(o[4],six.b('    `c1` - int'))
        self.assertEqual(o[5],six.b('    `c2` - real'))
        self.assertEqual(o[6],six.b('    `c3` - text'))
        self.assertEqual(o[7],six.b('    `c4` - text'))
        self.assertEqual(o[8],six.b('    `c5` - text'))
        self.assertEqual(o[9],six.b('    `c6` - real'))

        self.cleanup(tmp_data_file)

    def test_multiline_double_double_quoted_values_in_quoted_data(self):
        tmp_data_file = self.create_file_with_data(sample_quoted_data)

        # FIXME Need to convert \0a to proper encoding suitable for the person running the tests.
        cmd = Q_EXECUTABLE + ' -d " " "select replace(c5,X\'0A\',\'::\') from %s"' % tmp_data_file.name
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
        cmd = Q_EXECUTABLE + ' -d " " "select replace(c6,X\'0A\',\'::\') from %s"' % tmp_data_file.name
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

        cmd = Q_EXECUTABLE + ' -d " " --disable-double-double-quoting "select c2 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('double_double_quoted'))
        self.assertEqual(o[1],six.b('this is a quoted value with "double'))

        cmd = Q_EXECUTABLE + ' -d " " --disable-double-double-quoting "select c3 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b(''))
        self.assertEqual(o[1],six.b('double'))

        cmd = Q_EXECUTABLE + ' -d " " --disable-double-double-quoting "select c4 from %s" -W none' % tmp_data_file.name
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

        cmd = Q_EXECUTABLE + ' -d " " --disable-escaped-double-quoting "select c2 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b('escaped_double_quoted'))
        self.assertEqual(o[1],six.b('this is a quoted value with \\escaped'))

        cmd = Q_EXECUTABLE + ' -d " " --disable-escaped-double-quoting "select c3 from %s" -W none' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),2)

        self.assertEqual(o[0],six.b(''))
        self.assertEqual(o[1],six.b('double'))

        cmd = Q_EXECUTABLE + ' -d " " --disable-escaped-double-quoting "select c4 from %s" -W none' % tmp_data_file.name
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

        cmd = Q_EXECUTABLE + ' -d " " --disable-double-double-quoting --disable-escaped-double-quoting "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[o.index(six.b('  Fields:'))+1:]

        self.assertEqual(len(o),7) # found 7 fields

        cmd = Q_EXECUTABLE + ' -d " " --disable-escaped-double-quoting "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[o.index(six.b('  Fields:'))+1:]

        self.assertEqual(len(o),5) # found 5 fields

        cmd = Q_EXECUTABLE + ' -d " " --disable-double-double-quoting "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[o.index(six.b('  Fields:'))+1:]

        self.assertEqual(len(o),5) # found 5 fields

        cmd = Q_EXECUTABLE + ' -d " " "select * from %s" -A' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        o = o[o.index(six.b('  Fields:'))+1:]

        self.assertEqual(len(o),3) # found only 3 fields, which is the correct amount

        self.cleanup(tmp_data_file)