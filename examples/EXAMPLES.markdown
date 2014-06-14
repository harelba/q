# q - Treating Text as a Database 

See below for a JOIN example.

## Tutorial
This is a tutorial for beginners. If you're familiar with the concept and just wanna see some full fledged examples, take a look [here](README.markdown#examples) in the main page.

Tutorial steps:

1.  We'll start with a simple example and work from there. The file `exampledatafile` contains the output of an `ls -l` command, a list of files in some directory. In this example we'll do some calculations on this file list.
  * The following commands will count the lines in the file *exampledatafile*, effectively getting the number of files in the directory. The output will be exactly as if we ran the `wc -l` command.  

            q "SELECT COUNT(1) FROM exampledatafile"    

            cat exampledatafile | q "SELECT COUNT(1) FROM -"   
        
  * Now, let's assume we want to know the number of files per date in the directory. Notice that the date is in column 6.

            q "SELECT c6,COUNT(1) FROM exampledatafile GROUP BY c6"   

  * The results will show the number of files per date. However, there's a lot of "noise" - dates in which there is only one file. Let's leave only the ones which have 3 files or more:  

            q "SELECT c6,COUNT(1) AS cnt FROM exampledatafile GROUP BY c6 HAVING cnt >= 3"   

  * Now, let's see if we can get something more interesting. The following command will provide the **total size** of the files for each date. Notice that the file size is in c5.  

            q "SELECT c6,SUM(c5) AS size FROM exampledatafile GROUP BY c6"   

  * We can see the results. However, the sums are in bytes. Let's show the same results but in KB:  

            q "SELECT c6,SUM(c5)/1024.0 AS size FROM exampledatafile GROUP BY c6"  

  * The last command provided us with a list of results, but there is no order and the list is too long. Let's get the Top 5 dates:  

            q "SELECT c6,SUM(c5)/1024.0 AS size FROM exampledatafile GROUP BY c6 ORDER BY size DESC LIMIT 5"   

  * Now we'll see how we can format the output itself, so it looks better:  

            q -f "2=%4.2f" "SELECT c6,SUM(c5)/1024.0 AS size FROM exampledatafile GROUP BY c6 ORDER BY size DESC LIMIT 5"  
        
  * (An example of using JOIN will be added here - In the mean time just remember you have to use table alias for JOINed "tables")
        
2. A more complicated example, showing time manipulation. Let's assume that we have a file with a timestamp as its first column. We'll show how it's possible to get the number of rows per full minute:  

        q "SELECT DATETIME(ROUND(c1/60000)*60000/1000,'unixepoch','-05:00') as min, COUNT(1) FROM datafile*.gz GROUP BY min"  
        
   There are several things to notice here:
   
   * The timestamp value is in the first column, hence c1.
   * The timestamp is assumed to be a unix epoch timestamp, but in ms, and DATETIME accepts seconds, so we need to divide by 1000
   * The full-minute rounding is done by dividing by 60000 (ms), rounding and then multiplying by the same amount. Rounding to an hour, for example, would be the same except for having 3600000 instead of 60000.
   * We use DATETIME's capability in order to output the time in localtime format. In that case, it's converted to New York time (hence the -5 hours)
   * The filename is actually all files matching "datafile*.gz" - Multiple files can be read, and since they have a .gz extension, they are decompressed on the fly.
   * **NOTE:** For non-SQL people, the date manipulation may seem odd at first, but this is standard SQL processing for timestamps and it's easy to get used to.

## JOIN example

__Command 1 (Join data from two files):__

The following command _joins_ an ls output (`exampledatafile`) and a file containing rows of **group-name,email**  (`group-emails-example`) and provides a row of **filename,email** for each of the emails of the group. For brevity of output, there is also a filter for a specific filename called `ppp` which is achieved using a WHERE clause.
```bash
q "select myfiles.c8,emails.c2 from exampledatafile myfiles join group-emails-example emails on (myfiles.c4 = emails.c1) where myfiles.c8 = 'ppp'"
```

__Output 1: (rows of filename,email):__
```bash
ppp dip.1@otherdomain.com
ppp dip.2@otherdomain.com
```

You can see that the ppp filename appears twice, each time matched to one of the emails of the group `dip` to which it belongs. Take a look at the files [`exampledatafile`](exampledatafile) and [`group-emails-example`](group-emails-example) for the data.

## Installation
Installation instructions can be found [here](../doc/INSTALL.markdown)

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

