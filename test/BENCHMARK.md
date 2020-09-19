

NOTE: *Please don't use or publish this benchmark data yet. See below for details*

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

The benchmark executes simple `select count(*) from <file>` queries for each combination, calculating the mean and stddev of each set of iterations. The stddev is used in order to measure the validity of the results.

The graphs below only compare the means of the results, the standard deviations are written into the google sheet itself, and can be viewed there if needed.

## Hardware
OSX Catalina on a 15" Macbook Pro from Mid 2015, with 16GB of RAM, and an internal Flash Drive of 256GB.

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

## Results
(Results are automatically updated from the baseline tab in the google spreadsheet).

### Sensitivity to column count 
Based on a the largest file size of 1,000,000 rows.

![Sensitivity to column count](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1585602598&format=image)

### Version comparison per column count

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

## Benchmark Development info
### Running against the standalone binary
* `./run-benchmark` can accept a second parameter with the q executable. If it gets this parameter, it will use this path for running q. This provides a way to test the standalone q binaries in the new packaging format. When this parameter does not exist, the benchmark is executed directly from the source code.

### Updating the benchmark markdown document file
The results should reside in the following [google sheet](https://docs.google.com/spreadsheets/d/1Ljr8YIJwUQ5F4wr6ATga5Aajpu1CvQp1pe52KGrLkbY/edit?usp=sharing). 

add a new tab to the google sheet, and paste the content of `summary.benchmark-results` to the new sheet.

