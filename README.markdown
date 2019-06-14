[![Build Status](https://travis-ci.org/harelba/q.svg?branch=master)](https://travis-ci.org/harelba/q)

# q - Text as Data
q is a command line tool that allows direct execution of SQL-like queries on CSVs/TSVs (and any other tabular text files).

q treats ordinary files as database tables, and supports all SQL constructs, such as `WHERE`, `GROUP BY`, `JOIN`s, etc. It supports automatic column name and type detection, and q provides full support for multiple character encodings.

q's web site is [http://harelba.github.io/q/](http://harelba.github.io/q/). It contains everything you need to download and use q immediately.

## Installation.
Extremely simple. 

Instructions for all OSs are [here](http://harelba.github.io/q/install.html). 

## Examples

```
q "SELECT COUNT(*) FROM ./clicks_file.csv WHERE c3 > 32.3"

ps -ef | q -H "SELECT UID, COUNT(*) cnt FROM - GROUP BY UID ORDER BY cnt DESC LIMIT 3"
```

Go [here](http://harelba.github.io/q/examples.html) for more examples.

## Python API
A development branch for exposing q's capabilities as a <strong>Python module</strong> can be viewed <a href="https://github.com/harelba/q/tree/generic-injected-streams/PYTHON-API.markdown">here</a>, along with examples of the alpha version of the API.<br/>Existing functionality as a command-line tool will not be affected by this. Your input will be most appreciated.

## Change log
Click [here](http://harelba.github.io/q/changelog.html) to see the change log.

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

q on twitter: #qtextasdata

