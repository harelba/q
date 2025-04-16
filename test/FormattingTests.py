from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import sample_data_no_header, sample_data_rows
from test.base import AbstractQTestCase

import six
from test.utils import DEBUG

class FormattingTests(AbstractQTestCase):

    def test_column_formatting(self):
        # TODO Decide if this breaking change is reasonable
        #cmd = 'seq 1 10 | ' + Q_EXECUTABLE + ' -f 1=%4.3f,2=%4.3f "select sum(c1),avg(c1) from -" -c 1'
        cmd = 'seq 1 10 | ' + Q_EXECUTABLE + ' -f 1={:4.3f},2={:4.3f} "select sum(c1),avg(c1) from -" -c 1'

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('55.000 5.500'))

    def test_column_formatting_with_output_header(self):
        perl_regex = "'s/1\n/column_name\n1\n/;'"
        # TODO Decide if this breaking change is reasonable
        #cmd = 'seq 1 10 | perl -pe ' + perl_regex + ' | ' + Q_EXECUTABLE + ' -f 1=%4.3f,2=%4.3f "select sum(column_name) mysum,avg(column_name) myavg from -" -c 1 -H -O'
        cmd = 'seq 1 10 | LANG=C perl -pe ' + perl_regex + ' | ' + Q_EXECUTABLE + ' -f 1={:4.3f},2={:4.3f} "select sum(column_name) mysum,avg(column_name) myavg from -" -c 1 -H -O'

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], six.b('mysum myavg'))
        self.assertEqual(o[1], six.b('55.000 5.500'))

    def py3_test_successfuly_parse_universal_newlines_without_explicit_flag(self):
        def list_as_byte_list(l):
            return list(map(lambda x:six.b(x),l))

        expected_output = list(map(lambda x:list_as_byte_list(x),[['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-May-07', '6850000', 'USD', 'b'],
                           ['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-Oct-06', '6000000', 'USD', 'a'],
                           ['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-Jan-08', '25000000', 'USD', 'c'],
                           ['mycityfaces', 'MyCityFaces', '7', 'web', 'Scottsdale', 'AZ', '1-Jan-08', '50000', 'USD', 'seed'],
                           ['flypaper', 'Flypaper', '', 'web', 'Phoenix', 'AZ', '1-Feb-08', '3000000', 'USD', 'a'],
                           ['infusionsoft', 'Infusionsoft', '105', 'software', 'Gilbert', 'AZ', '1-Oct-07', '9000000', 'USD', 'a']]))

        data = six.b('permalink,company,numEmps,category,city,state,fundedDate,raisedAmt,raisedCurrency,round\rlifelock,LifeLock,,web,Tempe,AZ,1-May-07,6850000,USD,b\rlifelock,LifeLock,,web,Tempe,AZ,1-Oct-06,6000000,USD,a\rlifelock,LifeLock,,web,Tempe,AZ,1-Jan-08,25000000,USD,c\rmycityfaces,MyCityFaces,7,web,Scottsdale,AZ,1-Jan-08,50000,USD,seed\rflypaper,Flypaper,,web,Phoenix,AZ,1-Feb-08,3000000,USD,a\rinfusionsoft,Infusionsoft,105,software,Gilbert,AZ,1-Oct-07,9000000,USD,a')
        tmp_data_file = self.create_file_with_data(data)

        cmd = Q_EXECUTABLE + ' -d , -H "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 6)

        actual_output = list(map(lambda row: row.split(six.b(",")),o))

        self.assertEqual(actual_output,expected_output)

        self.cleanup(tmp_data_file)

    test_parsing_universal_newlines_without_explicit_flag = py3_test_successfuly_parse_universal_newlines_without_explicit_flag

    def test_universal_newlines_parsing_flag(self):
        def list_as_byte_list(l):
            return list(map(lambda x:six.b(x),l))

        expected_output = list(map(lambda x:list_as_byte_list(x),[['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-May-07', '6850000', 'USD', 'b'],
                           ['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-Oct-06', '6000000', 'USD', 'a'],
                           ['lifelock', 'LifeLock', '', 'web', 'Tempe', 'AZ', '1-Jan-08', '25000000', 'USD', 'c'],
                           ['mycityfaces', 'MyCityFaces', '7', 'web', 'Scottsdale', 'AZ', '1-Jan-08', '50000', 'USD', 'seed'],
                           ['flypaper', 'Flypaper', '', 'web', 'Phoenix', 'AZ', '1-Feb-08', '3000000', 'USD', 'a'],
                           ['infusionsoft', 'Infusionsoft', '105', 'software', 'Gilbert', 'AZ', '1-Oct-07', '9000000', 'USD', 'a']]))

        data = six.b('permalink,company,numEmps,category,city,state,fundedDate,raisedAmt,raisedCurrency,round\rlifelock,LifeLock,,web,Tempe,AZ,1-May-07,6850000,USD,b\rlifelock,LifeLock,,web,Tempe,AZ,1-Oct-06,6000000,USD,a\rlifelock,LifeLock,,web,Tempe,AZ,1-Jan-08,25000000,USD,c\rmycityfaces,MyCityFaces,7,web,Scottsdale,AZ,1-Jan-08,50000,USD,seed\rflypaper,Flypaper,,web,Phoenix,AZ,1-Feb-08,3000000,USD,a\rinfusionsoft,Infusionsoft,105,software,Gilbert,AZ,1-Oct-07,9000000,USD,a')
        tmp_data_file = self.create_file_with_data(data)

        cmd = Q_EXECUTABLE + ' -d , -H -U "select permalink,company,numEmps,category,city,state,fundedDate,raisedAmt,raisedCurrency,round from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)

        if len(e) == 2 or len(e) == 1:
            # In python 3.7, there's a deprecation warning for the 'U' file opening mode, which is ok for now
            self.assertIn(len(e), [1,2])
            self.assertTrue(b"DeprecationWarning: 'U' mode is deprecated" in e[0])
        elif len(e) != 0:
            # Nothing should be output to stderr in other versions
            self.assertTrue(False,msg='Unidentified output in stderr')

        self.assertEqual(len(o), 6)

        actual_output = list(map(lambda row: row.split(six.b(",")),o))

        self.assertEqual(actual_output,expected_output)

        self.cleanup(tmp_data_file)