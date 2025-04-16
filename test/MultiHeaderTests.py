from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.base import AbstractQTestCase
from test.test_data import (uneven_ls_output,find_output,header_row,sample_data_rows,sample_data_rows_with_empty_string,sample_data_no_header,sample_data_with_empty_string_no_header,sample_data_with_header,sample_data_with_missing_header_names,generate_sample_data_with_header,sample_quoted_data,double_double_quoted_data,escaped_double_quoted_data,combined_quoted_data,sample_quoted_data2,sample_quoted_data2_with_newline,one_column_data,sample_data_rows_with_spaces,sample_data_with_spaces_no_header,header_row_with_spaces,sample_data_with_spaces_with_header,long_value1,int_value,sample_data_with_long_values)

import six
from six.moves import range
from test.utils import DEBUG

class MultiHeaderTests(AbstractQTestCase):
    def test_output_header_when_multiple_input_headers_exist(self):
        TMPFILE_COUNT = 5
        tmpfiles = [self.create_file_with_data(sample_data_with_header) for x in range(TMPFILE_COUNT)]

        tmpfilenames = " UNION ALL ".join(map(lambda x:"select * from %s" % x.name, tmpfiles))

        cmd = Q_EXECUTABLE + ' -d , "select name,value1,value2 from (%s) order by name" -H -O' % tmpfilenames
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), TMPFILE_COUNT*3+1)
        self.assertEqual(o[0], six.b("name,value1,value2"))

        for i in range (TMPFILE_COUNT):
            self.assertEqual(o[1+i],sample_data_rows[0])
        for i in range (TMPFILE_COUNT):
            self.assertEqual(o[TMPFILE_COUNT+1+i],sample_data_rows[1])
        for i in range (TMPFILE_COUNT):
            self.assertEqual(o[TMPFILE_COUNT*2+1+i],sample_data_rows[2])

        for oi in o[1:]:
            self.assertTrue(six.b('name') not in oi)

        for i in range(TMPFILE_COUNT):
            self.cleanup(tmpfiles[i])

    def test_output_header_when_extra_header_column_names_are_different__concatenation_replacement(self):
        tmpfile1 = self.create_file_with_data(sample_data_with_header)
        tmpfile2 = self.create_file_with_data(generate_sample_data_with_header(six.b('othername,value1,value2')))

        cmd = Q_EXECUTABLE + ' -d , "select name,value1,value2 from (select * from %s union all select * from %s) order by name" -H -O' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 7)
        self.assertEqual(len(e), 0)
        self.assertTrue(o, [
            six.b('name,value1,value2'),
            six.b('a,1,0'),
            six.b('a,1,0'),
            six.b('b,2,0'),
            six.b('b,2,0'),
            six.b('c,,0'),
            six.b('c,,0')
        ])

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_output_header_when_extra_header_has_different_number_of_columns(self):
        tmpfile1 = self.create_file_with_data(sample_data_with_header)
        tmpfile2 = self.create_file_with_data(generate_sample_data_with_header(six.b('name,value1')))

        cmd = Q_EXECUTABLE + ' -d , "select name,value1,value2 from (select * from %s UNION ALL select * from %s) order by name" -H -O' % (tmpfile1.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 7)
        self.assertEqual(len(e), 0)
        self.assertTrue(o, [
            six.b('name,value1,value2'),
            six.b('a,1,0'),
            six.b('a,1,0'),
            six.b('b,2,0'),
            six.b('b,2,0'),
            six.b('c,,0'),
            six.b('c,,0')
        ])

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)