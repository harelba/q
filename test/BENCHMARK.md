

*Please don't use or publish this benchmark data yet, it's still alpha, i'm checking the validity of the results, and python 3 q version has not been merged yet.*

**NOTE**
This just a preliminary benchmark, and the results I got are somewhat surprising. I would love to validate these results by having other people run the benchmark as well and send me emails with their results. If you're interested, follow the "Running the benchmark" part. After the benchmark is finished, send me the final results file, along with some details about your hardware, and i'll add it to the spreadsheet. <harelba@gmail.com>

# Benchmark
This is an initial version of the benchmark, along with some results. The following is compared:
* q running on python 2.7.16
* q running on python 3.8.1
* textql 2.0.3
* octosql v0.3.0

This is by no means a scientific benchmark, and it only focuses on the data loading time. Also, it does not try to provide any usability comparison between q and textql. Actually, I've created this benchmark in order to compare q over python 2 and 3, and only then decided it would be interesting to compare the results to textql and octosql.

## Methodology
The idea was to compare the time sensitivity of row and column count. 

* Row counts: 1,10,100,1000,10000,100000,1000000
* Column counts: 1,5,10,20,50,100
* Iterations for each combination: 10

The benchmark executes simple `select count(*) from <file>` queries for each combination, calculating the mean and stddev of each set of iterations. The stddev is used in order to measure the validity of the results.

The graphs below only compare the means of the results, the standard deviations are written into the google sheet itself, and can be viewed there if needed.

## Hardware
OSX Sierra on a 15" Macbook Pro from Mid 2015, with 16GB of RAM, and an internal Flash Drive of 256GB.

## Running the benchmark
Please note that the initial run generates big files, so you'd need more than 3GB of free space available. All the generated files reside in the `_benchmark_data/` folder.

Part of the preparation flow will download the benchmark as needed.

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

`benchmark-id` is the id you wanna give the benchmark.
`q-executable` is the name of the q executable being used for the benchmark. If none has been provided through Q_EXECUTABLE, then the value will be the last commit hash. Note that there is no checking of whether the working tree is clean.

The summary of benchmark will be written to `./benchmark-results/<benchmark-id>/summary.benchmark-results``

By default, the benchmark will use the source python files inside the project. If you wanna run it on one of the standalone binary executable, the set Q_EXECUTABLE to the full path of the q binary.

## Updating the benchmark markdown document file
The results should reside in the following [google sheet](https://docs.google.com/spreadsheets/d/1Ljr8YIJwUQ5F4wr6ATga5Aajpu1CvQp1pe52KGrLkbY/edit?usp=sharing). 

add a new tab to the google sheet, and paste the content of `summary.benchmark-results` to the new sheet.

## Results
(Results are automatically updated from the baseline tab in the google spreadsheet).

### 1 Column Table
![1 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1119350798&format=interactive)

### 5 Column Table
![5 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=599223098&format=interactive)

### 10 Column Table
![10 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=82695414&format=interactive)

### 20 Column Table
![20 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1573199483&format=interactive)

### 50 Column Table
![50 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=448568670&format=interactive)

### 100 Column Table
![100 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=2101488258&format=interactive)

