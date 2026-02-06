from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command


from test.utils import DEBUG,b
from test.test_data import header_row_with_spaces, sample_data_no_header, sample_data_with_header
from test.base import AbstractQTestCase

class AnalysisTests(AbstractQTestCase):

    def test_analyze_result(self):
        d = "\n".join(['%s\t%s\t%s' % (x+1,x+1,x+1) for x in range(100)])
        tmpfile = self.create_file_with_data(b(d))

        cmd = Q_EXECUTABLE + ' -c 1 "select count(*) from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 5)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1], b('  Sources:'))
        self.assertEqual(o[2], b('    source_type: file source: %s' %(tmpfile.name)))
        self.assertEqual(o[3], b('  Fields:'))
        self.assertEqual(o[4], b('    `c1` - text'))

        self.cleanup(tmpfile)

    def test_analyze_result_with_data_stream(self):
        d = "\n".join(['%s\t%s\t%s' % (x+1,x+1,x+1) for x in range(100)])
        tmpfile = self.create_file_with_data(b(d))

        cmd = 'cat %s | %s  -c 1 "select count(*) from -" -A' % (tmpfile.name,Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 5)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], b('Table: -'))
        self.assertEqual(o[1], b('  Sources:'))
        self.assertEqual(o[2], b('    source_type: data-stream source: stdin'))
        self.assertEqual(o[3], b('  Fields:'))
        self.assertEqual(o[4], b('    `c1` - text'))

        self.cleanup(tmpfile)

    def test_column_analysis(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(o[0], b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4], b('    `c1` - text'))
        self.assertEqual(o[5], b('    `c2` - int'))
        self.assertEqual(o[6], b('    `c3` - int'))

        self.cleanup(tmpfile)

    def test_column_analysis_with_mixed_ints_and_floats(self):
        tmpfile = self.create_file_with_data(b("""planet_id,name,diameter_km,length_of_day_hours\n1000,Earth,12756,24\n2000,Mars,6792,24.7\n3000,Jupiter,142984,9.9"""))

        cmd = Q_EXECUTABLE + ' -d , -H "select * from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),8)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0], b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4], b('    `planet_id` - int'))
        self.assertEqual(o[5], b('    `name` - text'))
        self.assertEqual(o[6], b('    `diameter_km` - int'))
        self.assertEqual(o[7], b('    `length_of_day_hours` - real'))

        self.cleanup(tmpfile)

    def test_column_analysis_with_mixed_ints_and_floats_and_nulls(self):
        tmpfile = self.create_file_with_data(b("""planet_id,name,diameter_km,length_of_day_hours\n1000,Earth,12756,24\n2000,Mars,6792,24.7\n2500,Venus,,\n3000,Jupiter,142984,9.9"""))

        cmd = Q_EXECUTABLE + ' -d , -H "select * from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o),8)
        self.assertEqual(len(e),0)
        self.assertEqual(o[0], b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4], b('    `planet_id` - int'))
        self.assertEqual(o[5], b('    `name` - text'))
        self.assertEqual(o[6], b('    `diameter_km` - int'))
        self.assertEqual(o[7], b('    `length_of_day_hours` - real'))

        self.cleanup(tmpfile)

    def test_column_analysis_no_header(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(o[0], b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4], b('    `c1` - text'))
        self.assertEqual(o[5], b('    `c2` - int'))
        self.assertEqual(o[6], b('    `c3` - int'))

    def test_column_analysis_with_unexpected_header(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s" -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 7)
        self.assertEqual(len(e), 1)

        self.assertEqual(o[0], b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4],b('    `c1` - text'))
        self.assertEqual(o[5],b('    `c2` - text'))
        self.assertEqual(o[6],b('    `c3` - text'))

        self.assertEqual(
            e[0], b('Warning - There seems to be header line in the file, but -H has not been specified. All fields will be detected as text fields, and the header line will appear as part of the data'))

        self.cleanup(tmpfile)

    def test_column_analysis_for_spaces_in_header_row(self):
        tmpfile = self.create_file_with_data(
            header_row_with_spaces + b("\n") + sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , "select name,\\`value 1\\` from %s" -H -A' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 7)

        self.assertEqual(o[0], b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4], b('    `name` - text'))
        self.assertEqual(o[5], b('    `value 1` - int'))
        self.assertEqual(o[6], b('    `value2` - int'))

        self.cleanup(tmpfile)

    def test_column_analysis_with_header(self):
        tmpfile = self.create_file_with_data(sample_data_with_header)
        cmd = Q_EXECUTABLE + ' -d , "select c1 from %s" -A -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o),7)
        self.assertEqual(len(e),2)
        self.assertEqual(o[0], b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s' % tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4], b('    `name` - text'))
        self.assertEqual(o[5], b('    `value1` - int'))
        self.assertEqual(o[6], b('    `value2` - int'))

        self.assertEqual(e[0],b('query error: no such column: c1'))
        self.assertTrue(e[1].startswith(b('Warning - There seems to be a ')))

        self.cleanup(tmpfile)