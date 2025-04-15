#!/usr/bin/env python

class CouldNotConvertStringToNumericValueException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class SqliteOperationalErrorException(Exception):

    def __init__(self, msg,original_error):
        self.msg = msg
        self.original_error = original_error

    def __str(self):
        return repr(self.msg) + "//" + repr(self.original_error)

class IncorrectDefaultValueException(Exception):

    def __init__(self, option_type,option,actual_value):
        self.option_type = option_type
        self.option = option
        self.actual_value = actual_value

    def __str__(self):
        return repr(self)

class NonExistentTableNameInQsql(Exception):

    def __init__(self, qsql_filename,table_name,existing_table_names):
        self.qsql_filename = qsql_filename
        self.table_name = table_name
        self.existing_table_names = existing_table_names

class NonExistentTableNameInSqlite(Exception):

    def __init__(self, qsql_filename,table_name,existing_table_names):
        self.qsql_filename = qsql_filename
        self.table_name = table_name
        self.existing_table_names = existing_table_names

class TooManyTablesInQsqlException(Exception):

    def __init__(self, qsql_filename,existing_table_names):
        self.qsql_filename = qsql_filename
        self.existing_table_names = existing_table_names

class NoTableInQsqlExcption(Exception):

    def __init__(self, qsql_filename):
        self.qsql_filename = qsql_filename

class TooManyTablesInSqliteException(Exception):

    def __init__(self, qsql_filename,existing_table_names):
        self.qsql_filename = qsql_filename
        self.existing_table_names = existing_table_names

class NoTablesInSqliteException(Exception):

    def __init__(self, sqlite_filename):
        self.sqlite_filename = sqlite_filename

class ColumnMaxLengthLimitExceededException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class CouldNotParseInputException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class BadHeaderException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class EncodedQueryException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)


class CannotUnzipDataStreamException(Exception):

    def __init__(self):
        pass

class UniversalNewlinesExistException(Exception):

    def __init__(self):
        pass

class EmptyDataException(Exception):

    def __init__(self):
        pass

class MissingHeaderException(Exception):

    def __init__(self,msg):
        self.msg = msg

class InvalidQueryException(Exception):

    def __init__(self,msg):
        self.msg = msg

class TooManyAttachedDatabasesException(Exception):

    def __init__(self,msg):
        self.msg = msg

class FileNotFoundException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)

class UnknownFileTypeException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str(self):
        return repr(self.msg)


class ColumnCountMismatchException(Exception):

    def __init__(self, msg):
        self.msg = msg

class ContentSignatureNotFoundException(Exception):

    def __init__(self, msg):
        self.msg = msg

class StrictModeColumnCountMismatchException(Exception):

    def __init__(self,atomic_fn, expected_col_count,actual_col_count,lines_read):
        self.atomic_fn = atomic_fn
        self.expected_col_count = expected_col_count
        self.actual_col_count = actual_col_count
        self.lines_read = lines_read

class FluffyModeColumnCountMismatchException(Exception):

    def __init__(self,atomic_fn, expected_col_count,actual_col_count,lines_read):
        self.atomic_fn = atomic_fn
        self.expected_col_count = expected_col_count
        self.actual_col_count = actual_col_count
        self.lines_read = lines_read

class ContentSignatureDiffersException(Exception):

    def __init__(self,original_filename, other_filename, filenames_str,key,source_value,signature_value):
        self.original_filename = original_filename
        self.other_filename = other_filename
        self.filenames_str = filenames_str
        self.key = key
        self.source_value = source_value
        self.signature_value = signature_value


class ContentSignatureDataDiffersException(Exception):

    def __init__(self,msg):
        self.msg = msg


class InvalidQSqliteFileException(Exception):

    def __init__(self,msg):
        self.msg = msg


class MaximumSourceFilesExceededException(Exception):

    def __init__(self,msg):
        self.msg = msg




