from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import sample_data_with_header
from test.test_data import sample_data_no_header
from test.test_data import header_row_with_spaces
from test.base import AbstractQTestCase

import six
from test.utils import DEBUG

class EncodingTests(AbstractQTestCase):

    def test_utf8_with_bom_encoding(self):
        utf_8_data_with_bom = six.b('\xef\xbb\xbf"typeid","limit","apcost","date","checkpointId"\n"1","2","5","1,2,3,4,5,6,7","3000,3001,3002"\n"2","2","5","1,2,3,4,5,6,7","3003,3004,3005"\n')
        tmp_data_file = self.create_file_with_data(utf_8_data_with_bom,encoding=None)

        cmd = Q_EXECUTABLE + ' -d , -H -O -e utf-8-sig "select * from %s"' % tmp_data_file.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(e),0)
        self.assertEqual(len(o),3)

        self.assertEqual(o[0],six.b('typeid,limit,apcost,date,checkpointId'))
        self.assertEqual(o[1],six.b('1,2,5,"1,2,3,4,5,6,7","3000,3001,3002"'))
        self.assertEqual(o[2],six.b('2,2,5,"1,2,3,4,5,6,7","3003,3004,3005"'))

        self.cleanup(tmp_data_file)