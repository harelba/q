
# Benchmark
*Please don't use or publish this benchmark data yet, it's still alpha, i'm checking the validity of the results, and python 3 q version has not been merged yet.*

This is an initial version of the benchmark, along with some results. The following is compared:
* q running on python 2.7.11
* q running on python 3.6.4
* textql 2.0.3

The q version used for the benchmark is still on the python2/3 compatibility branch (hash f0b62b15b91583cd944ea2e8daf6f730198959fa)

This is by no means a scientific benchmark, and it only focuses on the data loading time. Also, it does not try to provide any usability comparison between q and textql. Actually, I've created this benchmark in order to compare q over python 2 and 3, and only then decided it would be nice to add a similar comparison to textql.

## Methodology
The idea was to compare the time sensitivity of row and column count. 

* Row counts: 1,10,100,1000,10000,100000,1000000
* Column counts: 1,5,10,20,50,100
* Iterations for each combination: 3

The benchmark executes simple `select count(*) from <file>` queries for each combination, calculating the mean and stddev of each set of iterations. The stddev is used in order to measure the validity of the results.

## Hardware
OSX Sierra on a 15" Macbook Pro from Mid 2015, with 16GB of RAM, and an internal Flash Drive of 256GB.


## Running the benchmark

Please note that the initial run generates big files, so you'd need more than 3GB of free space available. This also means that the first run will take much longer than additional runs. This is typical, and does not affect the benchmark results. All the generated files reside in the `_benchmark_data/` folder.

* Create and activate a python 2.7 virtual environment called `py2-q`, and `pip install -r requirements.txt`
* $ `./test-all BenchmarkTests.test_q_matrix`
* Create and avtivate a python 3.x virtual environment called `py3-q`, and `pip install -r requirements.txt`
* $ `./test-all BenchmarkTests.test_q_matrix`
* Ensure that textql is installed
* $ `./test-all BenchmarkTests.test_textql`

The results from each of the benchmarks will be written to `<virtual-env-name>.benchmark-results`, and `textql.benchmark-results` for the textql test.

* $ `paste py2-q.benchmark-results py3-q.benchmark-results textql.benchmark-results > all.benchmark-results`

## Updating the benchmark markdown document file
The results should reside in the following [google sheet](https://docs.google.com/spreadsheets/d/1Ljr8YIJwUQ5F4wr6ATga5Aajpu1CvQp1pe52KGrLkbY/edit?usp=sharing).

* Paste `all.benchmark-results` to the google sheet, near "Fil raw results here".

* All the graphs below will be updated automatically.

## Results

### 1 Column Table
![1 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1332039801&format=image)

### 5 Column Table
![5 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=693226704&format=image)

### 10 Column Table
![10 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1439130326&format=image)

### 20 Column Table
![20 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1648886784&format=image)

### 50 Column Table
![50 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1895066152&format=image)

### 100 Column Table
![100 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1125692157&format=image)

