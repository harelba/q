[![Build Status](https://travis-ci.org/harelba/q.svg?branch=master)](https://travis-ci.org/harelba/q)

# q - Text as Data
q is a command line tool that allows direct execution of SQL-like queries on CSVs/TSVs (and any other tabular text files).

q treats ordinary files as database tables, and supports all SQL constructs, such as `WHERE`, `GROUP BY`, `JOIN`s, etc. It supports automatic column name and type detection, and q provides full support for multiple character encodings.

q's web site is [http://harelba.github.io/q/](http://harelba.github.io/q/). It contains everything you need to download and use q immediately.

## Installation.
Extremely simple. 

Instructions for all OSs are [here](http://harelba.github.io/q/#installation). 

## Examples

```
q "SELECT COUNT(*) FROM ./clicks_file.csv WHERE c3 > 32.3"

ps -ef | q -H "SELECT UID, COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"
```

Go [here](http://harelba.github.io/q/#examples) for more examples.

## Benchmark
I have created a preliminary benchmark comparing q's speed between python2, python3, and comparing both to textql and octosql. 

Your input about the validity of the benchmark and about the results would be greatly appreciated. More details are [here](test/BENCHMARK.md).

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Linkedin: [Harel Ben Attia](https://www.linkedin.com/in/harelba/)

Twitter [@harelba](https://twitter.com/harelba)

Email [harelba@gmail.com](mailto:harelba@gmail.com)

q on twitter: #qtextasdata

