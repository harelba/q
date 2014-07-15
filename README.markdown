# q - Text as Data
q is a command line tool that allows direct execution of SQL-like queries on CSVs/TSVs (and any other tabular text files).

q treats ordinary files as database tables, and supports all SQL constructs, such as WHERE, GROUP BY, JOINs etc. It supports automatic column name and column type detection, and provides full support for multiple encodings.

q's web site is [http://harelba.github.io/q/](http://harelba.github.io/q/). It contains everything you need to download and use q in no time.

## Download
Download links for all OSs are [here](http://harelba.github.io/q/install.html). 

## Examples
A beginner's tutorial can be found [here](examples/EXAMPLES.markdown).

__Example 1:__

    q -H -t "select count(distinct(uuid)) from ./clicks.csv"
    
__Output 1:__
```bash
229
```

__Example 2:__

    q -H -t "select request_id,score from ./clicks.csv where score > 0.7 order by score desc limit 5"

__Output 2:__
```bash
2cfab5ceca922a1a2179dc4687a3b26e	1.0
f6de737b5aa2c46a3db3208413a54d64	0.986665809568
766025d25479b95a224bd614141feee5	0.977105183282
2c09058a1b82c6dbcf9dc463e73eddd2	0.703255121794
```

__Example 3:__

    q -t -H "select strftime('%H:%M',date_time) hour_and_minute,count(*) from ./clicks.csv group by hour_and_minute"

__Output 3:__
```bash
07:00	138148
07:01	140026
07:02	121826
```

__Usage Example 4:__

    q -t -H "select hashed_source_machine,count(*) from ./clicks.csv group by hashed_source_machine"
    
__Output 4:__
```bash
47d9087db433b9ba.domain.com	400000
```

__Example 5 (total size per user/group in the /tmp subtree):__

    sudo find /tmp -ls | q "select c5,c6,sum(c7)/1024.0/1024 as total from - group by c5,c6 order by total desc"

__Output 5:__
```bash
mapred hadoop   304.00390625
root   root     8.0431451797485
smith  smith    4.34389972687
```

__Example 6 (top 3 user ids with the largest number of owned processes, sorted in descending order):__

Note the usage of the autodetected column name UID in the query.

    ps -ef | q -H "select UID,count(*) cnt from - group by UID order by cnt desc limit 3"
    
__Output 6:__
```bash
root 152
harel 119
avahi 2
```

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

q on twitter: #qtextasdata

