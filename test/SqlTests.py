from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import (uneven_ls_output,find_output,header_row,sample_data_rows,sample_data_rows_with_empty_string,sample_data_no_header,sample_data_with_empty_string_no_header,sample_data_with_header,sample_data_with_missing_header_names,generate_sample_data_with_header,sample_quoted_data,double_double_quoted_data,escaped_double_quoted_data,combined_quoted_data,sample_quoted_data2,sample_quoted_data2_with_newline,one_column_data,sample_data_rows_with_spaces,sample_data_with_spaces_no_header,header_row_with_spaces,sample_data_with_spaces_with_header,long_value1,int_value,sample_data_with_long_values,EXAMPLES)
from test.base import AbstractQTestCase

from test.utils import b
from test.utils import DEBUG

class SqlTests(AbstractQTestCase):

    def test_find_example(self):
        tmpfile = self.create_file_with_data(find_output)
        cmd = Q_EXECUTABLE + ' "select c5,c6,sum(c7)/1024.0/1024 as total from %s group by c5,c6 order by total desc"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], b('mapred mapred 0.9389581680297852'))
        self.assertEqual(o[1], b('root root 0.02734375'))
        self.assertEqual(o[2], b('harel harel 0.010888099670410156'))

        self.cleanup(tmpfile)

    def test_join_example(self):
        cmd = Q_EXECUTABLE + ' "select myfiles.c8,emails.c2 from {0}/exampledatafile myfiles join {0}/group-emails-example emails on (myfiles.c4 = emails.c1) where myfiles.c8 = \'ppp\'"'.format(EXAMPLES)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 2)

        self.assertEqual(o[0], b('ppp dip.1@otherdomain.com'))
        self.assertEqual(o[1], b('ppp dip.2@otherdomain.com'))

    def test_join_example_with_output_header(self):
        cmd = Q_EXECUTABLE + ' -O "select myfiles.c8 aaa,emails.c2 bbb from {0}/exampledatafile myfiles join {0}/group-emails-example emails on (myfiles.c4 = emails.c1) where myfiles.c8 = \'ppp\'"'.format(EXAMPLES)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)

        self.assertEqual(o[0], b('aaa bbb'))
        self.assertEqual(o[1], b('ppp dip.1@otherdomain.com'))
        self.assertEqual(o[2], b('ppp dip.2@otherdomain.com'))

    def test_self_join1(self):
        tmpfile = self.create_file_with_data(b("\n").join([b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = Q_EXECUTABLE + ' "select * from %s a1 join %s a2 on (a1.c1 = a2.c1)"' % (tmpfile.name,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 10)

        self.cleanup(tmpfile)

    def test_self_join_reuses_table(self):
        tmpfile = self.create_file_with_data(b("\n").join([b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = Q_EXECUTABLE + ' "select * from %s a1 join %s a2 on (a1.c1 = a2.c1)" -A' % (tmpfile.name,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 6)

        self.assertEqual(o[0],b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s') % b(tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4],b('    `c1` - int'))
        self.assertEqual(o[5],b('    `c2` - int'))

        self.cleanup(tmpfile)

    def test_self_join2(self):
        tmpfile1 = self.create_file_with_data(b("\n").join([b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = Q_EXECUTABLE + ' "select * from %s a1 join %s a2 on (a1.c2 = a2.c2)"' % (tmpfile1.name,tmpfile1.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 10*10)

        self.cleanup(tmpfile1)

        tmpfile2 = self.create_file_with_data(b("\n").join([b("{} 9000".format(i)) for i in range(0,10)]))
        cmd = Q_EXECUTABLE + ' "select * from %s a1 join %s a2 on (a1.c2 = a2.c2) join %s a3 on (a1.c2 = a3.c2)"' % (tmpfile2.name,tmpfile2.name,tmpfile2.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 10*10*10)

        self.cleanup(tmpfile2)

    def test_disable_column_type_detection(self):
        tmpfile = self.create_file_with_data(b('''regular_text,text_with_digits1,text_with_digits2,float_number
"regular text 1",67,"67",12.3
"regular text 2",067,"067",22.3
"regular text 3",123,"123",33.4
"regular text 4",-123,"-123",0122.2
'''))

        # Check original column type detection
        cmd = Q_EXECUTABLE + ' -A -d , -H "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 8)

        self.assertEqual(o[0],b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1], b('  Sources:'))
        self.assertEqual(o[2], b('    source_type: file source: %s') % b(tmpfile.name))
        self.assertEqual(o[3], b('  Fields:'))
        self.assertEqual(o[4], b('    `regular_text` - text'))
        self.assertEqual(o[5], b('    `text_with_digits1` - int'))
        self.assertEqual(o[6], b('    `text_with_digits2` - int'))
        self.assertEqual(o[7], b('    `float_number` - real'))

        # Check column types detected when actual detection is disabled
        cmd = Q_EXECUTABLE + ' -A -d , -H --as-text "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 8)

        self.assertEqual(o[0],b('Table: %s' % tmpfile.name))
        self.assertEqual(o[1],b('  Sources:'))
        self.assertEqual(o[2],b('    source_type: file source: %s') % b(tmpfile.name))
        self.assertEqual(o[3],b('  Fields:'))
        self.assertEqual(o[4],b('    `regular_text` - text'))
        self.assertEqual(o[5],b('    `text_with_digits1` - text'))
        self.assertEqual(o[6],b('    `text_with_digits2` - text'))
        self.assertEqual(o[7],b('    `float_number` - text'))

        # Get actual data with regular detection
        cmd = Q_EXECUTABLE + ' -d , -H "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 4)

        self.assertEqual(o[0],b("regular text 1,67,67,12.3"))
        self.assertEqual(o[1],b("regular text 2,67,67,22.3"))
        self.assertEqual(o[2],b("regular text 3,123,123,33.4"))
        self.assertEqual(o[3],b("regular text 4,-123,-123,122.2"))

        # Get actual data without detection
        cmd = Q_EXECUTABLE + ' -d , -H --as-text "select * from %s"' % (tmpfile.name)

        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(e), 0)
        self.assertEqual(len(o), 4)

        self.assertEqual(o[0],b("regular text 1,67,67,12.3"))
        self.assertEqual(o[1],b("regular text 2,067,067,22.3"))
        self.assertEqual(o[2],b("regular text 3,123,123,33.4"))
        self.assertEqual(o[3],b("regular text 4,-123,-123,0122.2"))

        self.cleanup(tmpfile)