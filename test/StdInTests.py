from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import sample_data_no_header, sample_data_rows
from test.base import AbstractQTestCase

from test.utils import b
from test.utils import DEBUG

class StdInTests(AbstractQTestCase):

    def test_stdin_input(self):
        cmd = b('printf "%s" | ' + Q_EXECUTABLE + ' -d , "select c1,c2,c3 from -"') % sample_data_no_header
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 3)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0], sample_data_rows[0])
        self.assertEqual(o[1], sample_data_rows[1])
        self.assertEqual(o[2], sample_data_rows[2])

    def test_attempt_to_unzip_stdin(self):
        tmpfile = self.create_file_with_data(
            b('\x1f\x8b\x08\x08\xf2\x18\x12S\x00\x03xxxxxx\x003\xe42\xe22\xe62\xe12\xe52\xe32\xe7\xb2\xe0\xb2\xe424\xe0\x02\x00\xeb\xbf\x8a\x13\x15\x00\x00\x00'))

        cmd = 'cat %s | ' % tmpfile.name + Q_EXECUTABLE + ' -z "select sum(c1),avg(c1) from -"'

        retcode, o, e = run_command(cmd)
        self.assertTrue(retcode != 0)
        self.assertTrue(len(o) == 0)
        self.assertTrue(len(e) == 1)

        self.assertEqual(e[0],b('Cannot decompress standard input. Pipe the input through zcat in order to decompress.'))

        self.cleanup(tmpfile)