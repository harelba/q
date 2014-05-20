# q - Treating Text as a Database 

## Implementation 

The current implementation is written in Python using an in-memory database, in order to prevent the need for external dependencies. The implementation itself supports SELECT statements, including JOINs (Subqueries are supported only in the WHERE clause for now).

Please note that there is currently no checks and bounds on data size - It's up to the user to make sure things don't get too big.

Please make sure to read the limitations section as well.

Code wise, I'm planning for a big refactoring, and I have added full test suite in the latest version, so it'll be easier to do properly.

## Contact
Any feedback/suggestions/complaints regarding this tool would be much appreciated. Contributions are most welcome as well, of course.

Harel Ben-Attia, harelba@gmail.com, [@harelba](https://twitter.com/harelba) on Twitter

