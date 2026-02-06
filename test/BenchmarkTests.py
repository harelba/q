from test.base import AbstractQTestCase
from test.utils import Q_EXECUTABLE, run_command


import pytest

from test.utils import DEBUG,b
from test.base import AbstractQTestCase
import itertools
import os
import re
import time
from gzip import GzipFile


class BenchmarkResults(object):
    def __init__(self, lines, columns, attempt_results, mean, stddev):
        self.lines = lines
        self.columns = columns
        self.attempt_results = attempt_results
        self.mean = mean
        self.stddev = stddev

    def __str__(self):
        return "{}".format(self.__dict__)
    __repr__ = __str__


class BenchmarkAttemptResults(object):
    def __init__(self, attempt, lines, columns, duration,return_code):
        self.attempt = attempt
        self.lines = lines
        self.columns = columns
        self.duration = duration
        self.return_code = return_code

    def __str__(self):
        return "{}".format(self.__dict__)
    __repr__ = __str__


@pytest.mark.benchmark
class BenchmarkTests(AbstractQTestCase):

    BENCHMARK_DIR = os.environ.get('Q_BENCHMARK_DATA_DIR')

    def _ensure_benchmark_data_dir_exists(self):
        try:
            os.mkdir(BenchmarkTests.BENCHMARK_DIR)
        except Exception as e:
            pass

    def _create_benchmark_file_if_needed(self):
        self._ensure_benchmark_data_dir_exists()

        if os.path.exists('{}/'.format(BenchmarkTests.BENCHMARK_DIR)):
            return

        g = GzipFile('unit-file.csv.gz')
        d = g.read().decode('utf-8')
        f = open('{}/benchmark-file.csv'.format(BenchmarkTests.BENCHMARK_DIR), 'w')
        for i in range(100):
            f.write(d)
        f.close()

    def _prepare_test_file(self, lines, columns):

        filename = '{}/_benchmark_data__lines_{}_columns_{}.csv'.format(BenchmarkTests.BENCHMARK_DIR,lines, columns)

        if os.path.exists(filename):
            return filename

        c = ['c{}'.format(x + 1) for x in range(columns)]

        # write a header line
        ff = open(filename,'w')
        ff.write(",".join(c))
        ff.write('\n')
        ff.close()

        r, o, e = run_command('head -{} {}/benchmark-file.csv | ' + Q_EXECUTABLE + ' -d , "select {} from -" >> {}'.format(lines, BenchmarkTests.BENCHMARK_DIR, ','.join(c), filename))
        self.assertEqual(r, 0)
        # Create file cache as part of preparation
        r, o, e = run_command(Q_EXECUTABLE + ' -C readwrite -d , "select count(*) from %s"' % filename)
        self.assertEqual(r, 0)
        return filename

    def _decide_result(self,attempt_results):

        failed = list(filter(lambda a: a.return_code != 0,attempt_results))

        if len(failed) == 0:
            mean = sum([x.duration for x in attempt_results]) / len(attempt_results)
            sum_squared = sum([(x.duration - mean)**2 for x in attempt_results])
            ddof = 0
            pvar = sum_squared / (len(attempt_results) - ddof)
            stddev = pvar ** 0.5
        else:
            mean = None
            stddev = None

        return BenchmarkResults(
            attempt_results[0].lines,
            attempt_results[0].columns,
            attempt_results,
            mean,
            stddev
        )

    def _perform_test_performance_matrix(self,name,generate_cmd_function):
        results = []

        benchmark_results_folder = os.environ.get("Q_BENCHMARK_RESULTS_FOLDER",'')
        if benchmark_results_folder == "":
            raise Exception("Q_BENCHMARK_RESULTS_FOLDER must be provided as an environment variable")

        self._create_benchmark_file_if_needed()
        for columns in [1, 5, 10, 20, 50, 100]:
            for lines in [1, 10, 100, 1000, 10000, 100000, 1000000]:
                attempt_results = []
                for attempt in range(10):
                    filename = self._prepare_test_file(lines, columns)
                    if DEBUG:
                        print("Testing {}".format(filename))
                    t0 = time.time()
                    r, o, e = run_command(generate_cmd_function(filename,lines,columns))
                    duration = time.time() - t0
                    attempt_result = BenchmarkAttemptResults(attempt, lines, columns, duration, r)
                    attempt_results += [attempt_result]
                    if DEBUG:
                        print("Results: {}".format(attempt_result.__dict__))
                final_result = self._decide_result(attempt_results)
                results += [final_result]

        series_fields = ['lines','columns']
        value_fields = ['mean','stddev']

        all_fields = series_fields + value_fields

        output_filename = '{}/{}.benchmark-results'.format(benchmark_results_folder,name)
        output_file = open(output_filename,'w')
        for columns,g in itertools.groupby(sorted(results,key=lambda x:x.columns),key=lambda x:x.columns):
            x = "\t".join(series_fields + ['{}_{}'.format(name, f) for f in value_fields])
            print(x,file = output_file)
            for result in g:
                print("\t".join(map(str,[getattr(result,f) for f in all_fields])),file=output_file)
        output_file.close()

        print("results have been written to : {}".format(output_filename))
        if DEBUG:
            print("RESULTS FOR {}".format(name))
            print(open(output_filename,'r').read())

    def test_q_matrix(self):
        Q_BENCHMARK_NAME = os.environ.get('Q_BENCHMARK_NAME')
        if Q_BENCHMARK_NAME is None:
            raise Exception('Q_BENCHMARK_NAME must be provided as an env var')

        def generate_q_cmd(data_filename, line_count, column_count):
            Q_BENCHMARK_ADDITIONAL_PARAMS = os.environ.get('Q_BENCHMARK_ADDITIONAL_PARAMS') or ''
            additional_params = ''
            additional_params = additional_params + ' ' + Q_BENCHMARK_ADDITIONAL_PARAMS
            return '{} -d , {} "select count(*) from {}"'.format(Q_EXECUTABLE,additional_params, data_filename)
        self._perform_test_performance_matrix(Q_BENCHMARK_NAME,generate_q_cmd)

    def _get_textql_version(self):
        r,o,e = run_command("textql --version")
        if r != 0:
            raise Exception("Could not find textql")
        if len(e) != 0:
            raise Exception("Errors while getting textql version")
        return o[0]

    def _get_octosql_version(self):
        r,o,e = run_command("octosql --version")
        if r != 0:
            raise Exception("Could not find octosql")
        if len(e) != 0:
            raise Exception("Errors while getting octosql version")
        version = re.findall('v[0-9]+\\.[0-9]+\\.[0-9]+',str(o[0],encoding='utf-8'))[0]
        return version

    def test_textql_matrix(self):
        def generate_textql_cmd(data_filename,line_count,column_count):
            return 'textql -dlm , -sql "select count(*)" {}'.format(data_filename)

        name = 'textql_%s' % self._get_textql_version()
        self._perform_test_performance_matrix(name,generate_textql_cmd)

    def test_octosql_matrix(self):
        config_fn = self.random_tmp_filename('octosql', 'config')
        def generate_octosql_cmd(data_filename,line_count,column_count):
            j = """
dataSources:
  - name: bmdata
    type: csv
    config:
      path: "{}"
      headerRow: false
      batchSize: 10000
""".format(data_filename)[1:]
            f = open(config_fn,'w')
            f.write(j)
            f.close()
            return 'octosql -c {} -o batch-csv "select count(*) from bmdata a"'.format(config_fn)

        name = 'octosql_%s' % self._get_octosql_version()
        self._perform_test_performance_matrix(name,generate_octosql_cmd)