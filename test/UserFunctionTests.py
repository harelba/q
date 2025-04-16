from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command
from test.test_data import sample_data_no_header
from test.base import AbstractQTestCase

import six
from six.moves import range


class UserFunctionTests(AbstractQTestCase):
    def test_regexp_int_data_handling(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d , "select c2 from %s where regexp(\'^1\',c2)"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b("1"))

        self.cleanup(tmpfile)

    def test_percentile_func(self):
        cmd = 'seq 1000 1999 | %s "select substr(c1,0,3),percentile(c1,0),percentile(c1,0.5),percentile(c1,1) from - group by substr(c1,0,3)" -c 1' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 10)
        self.assertEqual(len(e), 0)

        output_table = [l.split(six.b(" ")) for l in o]
        group_labels = [int(row[0]) for row in output_table]
        minimum_values = [float(row[1]) for row in output_table]
        median_values = [float(row[2]) for row in output_table]
        max_values = [float(row[3]) for row in output_table]

        base_values = list(range(1000,2000,100))

        self.assertEqual(group_labels,list(range(10,20)))
        self.assertEqual(minimum_values,base_values)
        self.assertEqual(median_values,list(map(lambda x: x + 49.5,base_values)))
        self.assertEqual(max_values,list(map(lambda x: x + 99,base_values)))

    def test_regexp_null_data_handling(self):
        tmpfile = self.create_file_with_data(sample_data_no_header)

        cmd = Q_EXECUTABLE + ' -d , "select count(*) from %s where regexp(\'^\',c2)"' % tmpfile.name
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode, 0)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(e), 0)

        self.assertEqual(o[0],six.b("2"))

        self.cleanup(tmpfile)

    def test_md5_function(self):
        cmd = 'seq 1 4 | %s -c 1 -d , "select c1,md5(c1,\'utf-8\') from -"' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),4)
        self.assertEqual(len(e),0)

        self.assertEqual(tuple(o[0].split(six.b(','),1)),(six.b('1'),six.b('c4ca4238a0b923820dcc509a6f75849b')))
        self.assertEqual(tuple(o[1].split(six.b(','),1)),(six.b('2'),six.b('c81e728d9d4c2f636f067f89cc14862c')))
        self.assertEqual(tuple(o[2].split(six.b(','),1)),(six.b('3'),six.b('eccbc87e4b5ce2fe28308fd9f2a7baf3')))
        self.assertEqual(tuple(o[3].split(six.b(','),1)),(six.b('4'),six.b('a87ff679a2f3e71d9181a67b7542122c')))

    def test_stddev_functions(self):
        tmpfile = self.create_file_with_data(six.b("\n".join(map(str,[234,354,3234,123,4234,234,634,56,65]))))

        cmd = '%s -c 1 -d , "select round(stddev_pop(c1),10),round(stddev_sample(c1),10) from %s"' % (Q_EXECUTABLE,tmpfile.name)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('1479.7015464838,1569.4604964764'))

        self.cleanup(tmpfile)

    def test_sqrt_function(self):
        cmd = 'seq 1 5 | %s -c 1 -d , "select round(sqrt(c1),10) from -"' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),5)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('1.0'))
        self.assertEqual(o[1],six.b('1.4142135624'))
        self.assertEqual(o[2],six.b('1.7320508076'))
        self.assertEqual(o[3],six.b('2.0'))
        self.assertEqual(o[4],six.b('2.2360679775'))

    def test_power_function(self):
        cmd = 'seq 1 5 | %s -c 1 -d , "select round(power(c1,2.5),10) from -"' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),5)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('1.0'))
        self.assertEqual(o[1],six.b('5.6568542495'))
        self.assertEqual(o[2],six.b('15.5884572681'))
        self.assertEqual(o[3],six.b('32.0'))
        self.assertEqual(o[4],six.b('55.9016994375'))

    def test_file_functions(self):
        filenames = [
            "file1",
            "file2.csv",
            "/var/tmp/file3",
            "/var/tmp/file4.gz",
            ""
        ]
        data = "\n".join(filenames)

        cmd = 'echo "%s" | %s -c 1 -d , "select file_folder(c1),file_ext(c1),file_basename(c1),file_basename_no_ext(c1) from -"' % (data,Q_EXECUTABLE)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),5)
        self.assertEqual(len(e),0)
        self.assertEqual(o,[
            b',,file1,file1',
            b',.csv,file2.csv,file2',
            b'/var/tmp,,file3,file3',
            b'/var/tmp,.gz,file4.gz,file4',
            b',,,'
        ])


    def test_sha1_function(self):
        cmd = 'seq 1 4 | %s -c 1 -d , "select c1,sha1(c1) from -"' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),4)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('1,356a192b7913b04c54574d18c28d46e6395428ab'))
        self.assertEqual(o[1],six.b('2,da4b9237bacccdf19c0760cab7aec4a8359010b0'))
        self.assertEqual(o[2],six.b('3,77de68daecd823babbb58edb1c8e14d7106e83bb'))
        self.assertEqual(o[3],six.b('4,1b6453892473a467d07372d45eb05abc2031647a'))

    def test_regexp_extract_function(self):
        query = """
            select 
              regexp_extract('was ([0-9]+) seconds and ([0-9]+) ms',c1,0),
              regexp_extract('was ([0-9]+) seconds and ([0-9]+) ms',c1,1),
              regexp_extract('non-existent-(regexp)',c1,0) 
            from
              -
        """

        cmd = 'echo "Duration was 322 seconds and 240 ms" | %s -c 1 -d , "%s"' % (Q_EXECUTABLE,query)
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),1)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('322,240,'))

    def test_sha_function(self):
        cmd = 'seq 1 4 | %s -c 1 -d , "select c1,sha(c1,1,\'utf-8\') as sha1,sha(c1,224,\'utf-8\') as sha224,sha(c1,256,\'utf-8\') as sha256 from -"' % Q_EXECUTABLE
        retcode, o, e = run_command(cmd)

        self.assertEqual(retcode,0)
        self.assertEqual(len(o),4)
        self.assertEqual(len(e),0)

        self.assertEqual(o[0],six.b('1,356a192b7913b04c54574d18c28d46e6395428ab,e25388fde8290dc286a6164fa2d97e551b53498dcbf7bc378eb1f178,6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b'))
        self.assertEqual(o[1],six.b('2,da4b9237bacccdf19c0760cab7aec4a8359010b0,58b2aaa0bfae7acc021b3260e941117b529b2e69de878fd7d45c61a9,d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35'))
        self.assertEqual(o[2],six.b('3,77de68daecd823babbb58edb1c8e14d7106e83bb,4cfc3a1811fe40afa401b25ef7fa0379f1f7c1930a04f8755d678474,4e07408562bedb8b60ce05c1decfe3ad16b72230967de01f640b7e4729b49fce'))
        self.assertEqual(o[3],six.b('4,1b6453892473a467d07372d45eb05abc2031647a,271f93f45e9b4067327ed5c8cd30a034730aaace4382803c3e1d6c2f,4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a'))