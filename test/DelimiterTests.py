from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import sample_data_no_header, sample_data_rows


from test.utils import DEBUG,b

class DelimiterTests(AbstractQTestCase):

    def test_delimition_mistake_with_header(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d " " "select * from %s" -H' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertNotEqual(retcode, 0)
        self.assertEqual(len(o), 0)
        self.assertEqual(len(e), 2)

        self.assertTrue(e[0].startswith(b("Bad header row")))
        self.assertTrue(b("Column name cannot contain commas") in e[1])

        self.cleanup(tmpfile)

    def test_tab_delimition_parameter(self):
        tmpfile = self.create_file_with_data(
            sample_data_no_header.replace(b(","), b("\t")))
        cmd = Q_EXECUTABLE + ' -t "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0], sample_data_rows[0].replace(b(","), b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(b(","), b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(b(","), b("\t")))

        self.cleanup(tmpfile)

    def test_pipe_delimition_parameter(self):
        tmpfile = self.create_file_with_data(
            sample_data_no_header.replace(b(","), b("|")))
        cmd = Q_EXECUTABLE + ' -p "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)
        self.assertEqual(o[0], sample_data_rows[0].replace(b(","), b("|")))
        self.assertEqual(o[1], sample_data_rows[1].replace(b(","), b("|")))
        self.assertEqual(o[2], sample_data_rows[2].replace(b(","), b("|")))

        self.cleanup(tmpfile)

    def test_tab_delimition_parameter__with_manual_override_attempt(self):
        tmpfile = self.create_file_with_data(
            sample_data_no_header.replace(b(","), b("\t")))
        cmd = Q_EXECUTABLE + ' -t -d , "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 1)
        self.assertEqual(o[0], sample_data_rows[0].replace(b(","), b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(b(","), b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(b(","), b("\t")))
        self.assertEqual(e[0],b('Warning: -t parameter overrides -d parameter (,)'))

        self.cleanup(tmpfile)

    def test_pipe_delimition_parameter__with_manual_override_attempt(self):
        tmpfile = self.create_file_with_data(
            sample_data_no_header.replace(b(","), b("|")))
        cmd = Q_EXECUTABLE + ' -p -d , "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 1)
        self.assertEqual(o[0], sample_data_rows[0].replace(b(","), b("|")))
        self.assertEqual(o[1], sample_data_rows[1].replace(b(","), b("|")))
        self.assertEqual(o[2], sample_data_rows[2].replace(b(","), b("|")))
        self.assertEqual(e[0],b('Warning: -p parameter overrides -d parameter (,)'))

        self.cleanup(tmpfile)

    def test_output_delimiter(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -D "|" "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0].replace(b(","), b("|")))
        self.assertEqual(o[1], sample_data_rows[1].replace(b(","), b("|")))
        self.assertEqual(o[2], sample_data_rows[2].replace(b(","), b("|")))

        self.cleanup(tmpfile)

    def test_output_delimiter_tab_parameter(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -T "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0].replace(b(","), b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(b(","), b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(b(","), b("\t")))

        self.cleanup(tmpfile)

    def test_output_delimiter_pipe_parameter(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -P "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0].replace(b(","), b("|")))
        self.assertEqual(o[1], sample_data_rows[1].replace(b(","), b("|")))
        self.assertEqual(o[2], sample_data_rows[2].replace(b(","), b("|")))

        self.cleanup(tmpfile)

    def test_output_delimiter_tab_parameter__with_manual_override_attempt(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -T -D "|" "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 1)

        self.assertEqual(o[0], sample_data_rows[0].replace(b(","), b("\t")))
        self.assertEqual(o[1], sample_data_rows[1].replace(b(","), b("\t")))
        self.assertEqual(o[2], sample_data_rows[2].replace(b(","), b("\t")))
        self.assertEqual(e[0], b('Warning: -T parameter overrides -D parameter (|)'))

        self.cleanup(tmpfile)

    def test_output_delimiter_pipe_parameter__with_manual_override_attempt(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)
        cmd = Q_EXECUTABLE + ' -d , -P -D ":" "select c1,c2,c3 from %s"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 1)

        self.assertEqual(o[0], sample_data_rows[0].replace(b(","), b("|")))
        self.assertEqual(o[1], sample_data_rows[1].replace(b(","), b("|")))
        self.assertEqual(o[2], sample_data_rows[2].replace(b(","), b("|")))
        self.assertEqual(e[0],b('Warning: -P parameter overrides -D parameter (:)'))

        self.cleanup(tmpfile)