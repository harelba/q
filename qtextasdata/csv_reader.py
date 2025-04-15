#!/usr/bin/env python

import csv
from qtextasdata.logging import xprint
from qtextasdata.exceptions import (
    CouldNotParseInputException,
    CouldNotConvertStringToNumericValueException,
    ColumnMaxLengthLimitExceededException,
    UniversalNewlinesExistException
)

def py3_encoded_csv_reader(encoding, f, dialect,row_data_only=False,**kwargs):
    try:
        xprint("f is %s" % str(f))
        xprint("dialect is %s" % dialect)
        csv_reader = csv.reader(f, dialect, **kwargs)

        if row_data_only:
            for row in csv_reader:
                yield row
        else:
            for row in csv_reader:
                yield (f.filename(),f.isfirstline(),row)

    except UnicodeDecodeError as e1:
        raise CouldNotParseInputException(e1)
    except ValueError as e:
        # TODO Add test for this
        if str(e) is not None and str(e).startswith('could not convert string to'):
            raise CouldNotConvertStringToNumericValueException(str(e))
        else:
            raise CouldNotParseInputException(str(e))
    except Exception as e:
        if str(e).startswith("field larger than field limit"):
            raise ColumnMaxLengthLimitExceededException(str(e))
        elif 'universal-newline' in str(e):
            raise UniversalNewlinesExistException()
        else:
            raise

encoded_csv_reader = py3_encoded_csv_reader
