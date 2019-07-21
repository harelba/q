

*Please don't use or publish this benchmark data yet, it's still alpha, i'm checking the validity of the results, and python 3 q version has not been merged yet.*

**NOTE**
This just a preliminary benchmark, and the results I got are somewhat surprising. I would love to validate these results by having other people run the benchmark as well and send me emails with their results. If you're interested, follow the "Running the benchmark" part. After the benchmark is finished, send me the `all.benchmark-results` file, along with some details about your hardware, and i'll add it to the spreadsheet. <harelba@gmail.com>

# Benchmark
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
* Iterations for each combination: 10

The benchmark executes simple `select count(*) from <file>` queries for each combination, calculating the mean and stddev of each set of iterations. The stddev is used in order to measure the validity of the results.

## Hardware
OSX Sierra on a 15" Macbook Pro from Mid 2015, with 16GB of RAM, and an internal Flash Drive of 256GB.


## Running the benchmark

Please note that the initial run generates big files, so you'd need more than 3GB of free space available. This also means that the first run will take much longer than additional runs. This is typical, and does not affect the benchmark results. All the generated files reside in the `_benchmark_data/` folder.

### Preparations
Make sure you have pyenv and pyenv-virtualenv installed.

* $ `git clone git@github.com:harelba/q.git`
* $ `git checkout q-benchmark`
* $ `cd test/`
* $ `pyenv install 2.7.11`
* $ `pyenv virtualenv 2.7.11 py2-q`
* $ `pyenv activate py2-q`
* $ `pip install -r ../requirements.txt`
* $ `pyenv install 3.6.4`
* $ `pyenv virtualenv 3.6.4 py3-q`
* $ `pyenv activate py3-q`
* $ `pip install -r ../requirements.txt`
* Install `textql` (brew/apt-get/whatever)
* $ `wget "https://s3.amazonaws.com/harelba-q-public/benchmark_data.tar.gz"`
* $ `tar xvzf benchmark_data.tar.gz`

### Execution
* $ `pyenv activate py2-q`
* $ `./test-all BenchmarkTests.test_q_matrix` 
* $ `pyenv activate py3-q`
* $ `./test-all BenchmarkTests.test_q_matrix`
* $ `./test-all BenchmarkTests.test_textql`

The results from each of the benchmarks will be written to `<virtual-env-name>.benchmark-results`, and `textql.benchmark-results` for the textql test.

* $ `paste py2-q.benchmark-results py3-q.benchmark-results textql.benchmark-results > all.benchmark-results`

## Updating the benchmark markdown document file
The results should reside in the following [google sheet](https://docs.google.com/spreadsheets/d/1Ljr8YIJwUQ5F4wr6ATga5Aajpu1CvQp1pe52KGrLkbY/edit?usp=sharing). 

* Duplicat the baseline tab inside the spreadsheet.
* Paste the content of `all.benchmark-results` to the new tab, near "Fill raw results here".

* All the graphs below will be updated automatically.

## Results
(Results are automatically updated from the baseline tab in the google spreadsheet).

### 1 Column Table
![1 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1119350798&format=image)

### 5 Column Table
![5 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=599223098&format=image)

### 10 Column Table
![10 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=82695414&format=image)

### 20 Column Table
![20 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1573199483&format=image)

### 50 Column Table
![50 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1895066152&format=image)

### 100 Column Table
![100 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=2101488258&format=image)

