

NOTE: *Please don't use or publish this benchmark data yet. See below for details*

# Update
q now provides inherent automatic caching capabilities, writing the CSV/TSV file to a `.qsql` file that sits beside the original file. After the cache exists (created as part of an initial query on a file), q knows to use it behind the scenes without changing the query itself, speeding up performance significantly.

The following table shows the impact of using caching in q:

|    Rows   | Columns | File Size | Query time without caching | Query time with caching | Speed Improvement |
|:---------:|:-------:|:---------:|:--------------------------:|:-----------------------:|:-----------------:|
| 5,000,000 |   100   |   4.8GB   |    4 minutes, 47 seconds   |       1.92 seconds      |        x149       |
| 1,000,000 |   100   |   983MB   |        50.9 seconds        |      0.461 seconds      |        x110       |
| 1,000,000 |    50   |   477MB   |        27.1 seconds        |      0.272 seconds      |        x99        |
|  100,000  |   100   |    99MB   |         5.2 seconds        |      0.141 seconds      |        x36        |
|  100,000  |    50   |    48MB   |         2.7 seconds        |      0.105 seconds      |        x25        |


Effectively, `.qsql` files are just standard sqlite3 files, with an additional metadata table that is used for detecting changes in the original delimited file. This means that any tool that can read sqlite3 files can use these files directly. The tradeoff is of course the additional disk usage that the cache files take.

A good side-effect to this addition, is that q now knows how to directly query multi-file sqlite3 databases. This means that the user can query any sqlite3 database file, or the `.qsql` file itself, even when the original file doesn't exist anymore. For example:

```bash
q "select a.*,b.* from my_file.csv.qsql a left join some-sqlite3-database:::some_table_name b on (a.id = b.id)"
```

NOTE: In the current version, caching is not enabled by default - Use `-C readwrite` to enable reading+writing cache files, or `-C read` to just read any existing cache files. A `~/.qrc` file can be added in order to make these options the default if you want.

The benchmark results below reflect the performance without the caching, e.g. directly reading the delimited files, parsing them and performing the query.

I'll update benchmark results later on to provide cached results as well.

# Overview
This just a preliminary benchmark, originally created for validating performance optimizations and suggestions from users, and analyzing q's move to python3. After writing it, I thought it might be interesting to test its speed against textql and octosql as well.

The results I'm getting are somewhat surprising, to the point of me questioning them a bit, so it would be great to validate the further before finalizing the benchmark results.

The most surprising results are as follows:
* python3 vs python2 - A huge improvement (for large files, execution times with python 3 are around 40% of the times for python 2)
* python3 vs textql (written in golang) - Seems that textql becomes slower than the python3 q version as the data sizes grows (both rows and columns)

I would love to validate these results by having other people run the benchmark as well and send me their results. 

If you're interested, follow the instructions and run the benchmark on your machine. After the benchmark is finished, send me the final results file, along with some details about your hardware, and i'll add it to the spreadsheet. <harelba@gmail.com>

I've tried to make running the benchmark as seamless as possible, but there obviously might be errors/issues. Please contact me if you encounter any issue, or just open a ticket.

# Benchmark
This is an initial version of the benchmark, along with some results. The following is compared:
* q running on multiple python versions
* textql 2.0.3
* octosql v0.3.0

The specific python versions which are being tested are specified in `benchmark-config.sh`.

This is by no means a scientific benchmark, and it only focuses on the data loading time which is the only significant factor for comparison (e.g. the query itself is a very simple count query). Also, it does not try to provide any usability comparison between q and textql/octosql, an interesting topic on its own.

## Methodology
The idea was to compare the time sensitivity of row and column count. 

* Row counts: 1,10,100,1000,10000,100000,1000000
* Column counts: 1,5,10,20,50,100
* Iterations for each combination: 10

File sizes:
* 1M rows by 100 columns - 976MB (~1GB) - Largest file
* 1M rows by 50 columns - 477MB

The benchmark executes simple `select count(*) from <file>` queries for each combination, calculating the mean and stddev of each set of iterations. The stddev is used in order to measure the validity of the results.

The graphs below only compare the means of the results, the standard deviations are written into the google sheet itself, and can be viewed there if needed.

Instructions on how to run the benchmark are at the bottom section of this document, after the results section.

## Hardware
OSX Catalina on a 15" Macbook Pro from Mid 2015, with 16GB of RAM, and an internal Flash Drive of 256GB.

## Results
(Results are automatically updated from the baseline tab in the google spreadsheet).

Detailed results below.

Summary:
* All python 3 versions (3.6/3.7/3.8) provide similar results across all scales.
* python 3.x provides significantly better results than python2. Improvement grows as the file size grows (20% improvement for small files, up to ~70% improvement for the largest file)
* textql seems to provide faster results than q (py3) for smaller files, up to around 30MB of data. As the size grows further, it becomes slower than q, up to 80% (74 seconds vs 41 seconds) for the largest file
* The larger the files, textql becomes slower than q-py3 (up to 80% more time than q for the largest file)
* octosql is significantly slower than both q and textql, even for small files with a low number of rows and columns

### Data for 1M rows

#### Run time durations for 1M rows and different column counts:
|   rows  	| columns 	| File Size 	| python 2.7 	| python 3.6 	| python 3.7 	| python 3.8 	| textql 	| octosql 	|
|:-------:	|:-------:	|:---------:	|:----------:	|:----------:	|:----------:	|:----------:	|:------:	|:-------:	|
| 1000000 	|    1    	|    17M    	|    5.15    	|    4.24    	|    4.08    	|    3.98    	|  2.90  	|  49.95  	|
| 1000000 	|    5    	|    37M    	|    10.68   	|    5.37    	|    5.26    	|    5.14    	|  5.88  	|  54.69  	|
| 1000000 	|    10   	|    89M    	|    17.56   	|    7.25    	|    7.15    	|    7.01    	|  9.69  	|  65.32  	|
| 1000000 	|    20   	|    192M   	|    30.28   	|    10.96   	|    10.78   	|    10.64   	|  17.34 	|  83.94  	|
| 1000000 	|    50   	|    477M   	|    71.56   	|    21.98   	|    21.59   	|    21.70   	|  38.57 	|  158.26 	|
| 1000000 	|   100   	|    986M   	|   131.86   	|    41.71   	|    40.82   	|    41.02   	|  74.62 	|  289.58 	|

#### Comparison between python 3.x and python 2 run times (1M rows):
(>100% is slower than q-py2, <100% is faster than q-py2)

|   rows    | columns 	| file size 	| q-py2 runtime 	| q-py3.6 vs q-py2 runtime 	| q-py3.7 vs q-py2 runtime 	| q-py3.8 vs q-py2 runtime 	|
|:-------:	|:-------:	|:---------:	|:-------------:	|:------------------------:	|:------------------------:	|:------------------------:	|
| 1000000 	|    1    	|    17M    	|    100.00%    	|          82.34%          	|          79.34%          	|          77.36%          	|
| 1000000 	|    5    	|    37M    	|    100.00%    	|          50.25%          	|          49.22%          	|          48.08%          	|
| 1000000 	|    10   	|    89M    	|    100.00%    	|          41.30%          	|          40.69%          	|          39.93%          	|
| 1000000 	|    20   	|    192M   	|    100.00%    	|          36.18%          	|          35.59%          	|          35.14%          	|
| 1000000 	|    50   	|    477M   	|    100.00%    	|          30.71%          	|          30.17%          	|          30.32%          	|
| 1000000 	|   100   	|    986M   	|    100.00%    	|          31.63%          	|          30.96%          	|          31.11%          	|

#### textql and octosql comparison against q-py3 run time (1M rows):
(>100% is slower than q-py3, <100% is faster than q-py3)

|   rows  	| columns 	| file size 	| avg q-py3 runtime 	| textql vs q-py3 runtime 	| octosql vs q-py3 runtime 	|
|:-------:	|:-------:	|:---------:	|:-----------------:	|:-----------------------:	|:------------------------:	|
| 1000000 	|    1    	|    17M    	|      100.00%      	|          70.67%         	|         1217.76%         	|
| 1000000 	|    5    	|    37M    	|      100.00%      	|         111.86%         	|         1040.70%         	|
| 1000000 	|    10   	|    89M    	|      100.00%      	|         135.80%         	|          915.28%         	|
| 1000000 	|    20   	|    192M   	|      100.00%      	|         160.67%         	|          777.92%         	|
| 1000000 	|    50   	|    477M   	|      100.00%      	|         177.26%         	|          727.40%         	|
| 1000000 	|   100   	|    986M   	|      100.00%      	|         181.19%         	|          703.15%         	|

### Sensitivity to column count 
Based on a the largest file size of 1,000,000 rows.

![Sensitivity to column count](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1585602598&format=image)

### Sensitivity to line count (per column count)

#### 1 Column Table
![1 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1119350798&format=image)

#### 5 Column Table
![5 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=599223098&format=image)

#### 10 Column Table
![10 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=82695414&format=image)

#### 20 Column Table
![20 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1573199483&format=image)

#### 50 Column Table
![50 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=448568670&format=image)

#### 100 Column Table
![100 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=2101488258&format=image)

## Running the benchmark
Please note that the initial run generates large files, so you'd need more than 3GB of free space available. All the generated files reside in the `_benchmark_data/` folder.

Part of the preparation flow will download the benchmark data as needed.

### Preparations
* Prerequisites:
  * pyenv installed
  * pyenv-virtualenv installed
  * [`textql`](https://github.com/dinedal/textql#install)
  * [`octosql`](https://github.com/cube2222/octosql#installation)

Run `./prepare-benchmark-env`

### Execution
Run `./run-benchmark <benchmark-id>`.

Benchmark output files will be written to `./benchmark-results/<q-executable>/<benchmark-id>/`.

* `benchmark-id` is the id you wanna give the benchmark.
* `q-executable` is the name of the q executable being used for the benchmark. If none has been provided through Q_EXECUTABLE, then the value will be the last commit hash. Note that there is no checking of whether the working tree is clean. 

The summary of benchmark will be written to `./benchmark-results/<benchmark-id>/summary.benchmark-results``

By default, the benchmark will use the source python files inside the project. If you wanna run it on one of the standalone binary executable, the set Q_EXECUTABLE to the full path of the q binary.

For anyone helping with running the benchmark, don't use this parameter for now, just test against a clean checkout of the code using `./run-benchmark <benchmark-id>`.

## Benchmark Development info
### Running against the standalone binary
* `./run-benchmark` can accept a second parameter with the q executable. If it gets this parameter, it will use this path for running q. This provides a way to test the standalone q binaries in the new packaging format. When this parameter does not exist, the benchmark is executed directly from the source code.

### Updating the benchmark markdown document file
The results should reside in the following [google sheet](https://docs.google.com/spreadsheets/d/1Ljr8YIJwUQ5F4wr6ATga5Aajpu1CvQp1pe52KGrLkbY/edit?usp=sharing). 

add a new tab to the google sheet, and paste the content of `summary.benchmark-results` to the new sheet.

