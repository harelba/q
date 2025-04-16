from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import sample_data_no_header, sample_data_rows

from test.utils import b

from test.utils import DEBUG
class GzippingTests(AbstractQTestCase):

    def test_gzipped_file(self):
        tmpfile = self.create_file_with_data(
            b('\x1f\x8b\x08\x08\xf2\x18\x12S\x00\x03xxxxxx\x003\xe42\xe22\xe62\xe12\xe52\xe32\xe7\xb2\xe0\xb2\xe424\xe0\x02\x00\xeb\xbf\x8a\x13\x15\x00\x00\x00'))

        cmd = Q_EXECUTABLE + ' -z "select sum(c1),avg(c1) from %s"' % tmpfile.name

        retcode, o, e = run_command(cmd)
        self.assertTrue(retcode == 0)
        self.assertTrue(len(o) == 1)
        self.assertTrue(len(e) == 0)

        s = sum(range(1, 11))
        self.assertTrue(o[0] == b('%s %s' % (s, s / 10.0)))

        self.cleanup(tmpfile)