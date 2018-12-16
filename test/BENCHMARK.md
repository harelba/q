
# Benchmark
This is an initial version of the benchmark, along with some results. The following is compared:
* q running on python 2.7.11
* q running on python 3.6.4
* textql 2.0.3

This is by no means a scientific benchmark, and it only focuses on the data loading time. Also, it does not try to provide any usability comparison between q and textql. Actually, I've created this benchmark in order to compare q over python 2 and 3, and only then decided it would be nice to add a similar comparison to textql.

## Methodology
The idea was to compare the time sensitivity of row and column count. 

Row counts: 1,10,100,1000,10000,100000,1000000
Column counts: 1,5,10,20,50,100
Iterations for each combination: 3

The benchmark executes simple `select count(*) from <file>` queries for each combination, calculating the mean and stddev of each set of iterations. The stddev is used in order to measure the validity of the results.

## Hardware
OSX Sierra on a 15" Macbook Pro from Mid 2015, with 16GB of RAM, and an internal Flash Drive of 256GB.

## Results

### 1 Column Table
![1 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1332039801&format=image)

### 5 Column Table
![5 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=693226704&format=image)

### 10 Column Table
![10 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1904328818&format=image)

### 20 Column Table
![20 column table](https://docs.google.com/spreadsheets/d/e/2PACX-1vQy9Zm4I322Tdf5uoiFFJx6Oi3Z4AMq7He3fUUtsEQVQIdTGfWgjxFD6k8PAy9wBjvFkqaG26oBgNTP/pubchart?oid=1648886784&format=image)
