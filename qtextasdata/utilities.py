import os
import math
import re
import hashlib
import locale
import sys

def get_stdout_encoding(encoding_override=None):
    if encoding_override is not None and encoding_override != 'none':
       return encoding_override

    if sys.stdout.isatty():
        return sys.stdout.encoding
    else:
        return locale.getpreferredencoding()

sha_algorithms = {
    1 : hashlib.sha1,
    224: hashlib.sha224,
    256: hashlib.sha256,
    386: hashlib.sha384,
    512: hashlib.sha512
}

def sha(data,algorithm,encoding):
    try:
        f = sha_algorithms[algorithm]
        return f(str(data).encode(encoding)).hexdigest()
    except Exception as e:
        print(e)

# For backward compatibility only (doesn't handle encoding well enough)
def sha1(data):
    return hashlib.sha1(str(data).encode('utf-8')).hexdigest()

# TODO Add caching of compiled regexps - Will be added after benchmarking capability is baked in
def regexp(regular_expression, data):
    if data is not None:
        if not isinstance(data, str):
            data = str(data)
        return re.search(regular_expression, data) is not None
    else:
        return False

def regexp_extract(regular_expression, data,group_number):
    if data is not None:
        if not isinstance(data, str):
            data = str(data)
        m = re.search(regular_expression, data)
        if m is not None:
            return m.groups()[group_number]
    else:
        return False

def md5(data,encoding):
    m = hashlib.md5()
    m.update(str(data).encode(encoding))
    return m.hexdigest()

def sqrt(data):
    return math.sqrt(data)

def power(data,p):
    return data**p

def file_ext(data):
    if data is None:
        return None

    return os.path.splitext(data)[1]

def file_folder(data):
    if data is None:
        return None
    return os.path.split(data)[0]

def file_basename(data):
    if data is None:
        return None
    return os.path.split(data)[1]
    
def file_basename_no_ext(data):
    if data is None:
        return None

    return os.path.split(os.path.splitext(data)[0])[-1]

def percentile(l, p):
    # TODO Alpha implementation, need to provide multiple interpolation methods, and add tests
    if not l:
        return None
    k = p*(len(l) - 1)
    f = math.floor(k)
    c = math.ceil(k)
    if c == f:
        return l[int(k)]
    return (c-k) * l[int(f)] + (k-f) * l[int(c)]

# TODO Streaming Percentile to prevent memory consumption blowup for large datasets
class StrictPercentile(object):
    def __init__(self):
        self.values = []
        self.p = None

    def step(self,value,p):
        if self.p is None:
          self.p = p
        self.values.append(value)

    def finalize(self):
        if len(self.values) == 0 or (self.p < 0 or self.p > 1):
            return None
        else:
            return percentile(sorted(self.values),self.p)

class StdevPopulation(object):
    def __init__(self):
        self.M = 0.0
        self.S = 0.0
        self.k = 0

    def step(self, value):
        try:
            # Ignore nulls
            if value is None:
                return
            val = float(value) # if fails, skips this iteration, which also ignores nulls
            tM = self.M
            self.k += 1
            self.M += ((val - tM) / self.k)
            self.S += ((val - tM) * (val - self.M))
        except ValueError:
            # TODO propagate udf errors to console
            raise Exception("Data is not numeric when calculating stddev (%s)" % value)

    def finalize(self):
        if self.k <= 1: # avoid division by zero
            return None
        else:
            return math.sqrt(self.S / (self.k))

class StdevSample(object):
    def __init__(self):
        self.M = 0.0
        self.S = 0.0
        self.k = 0

    def step(self, value):
        try:
            # Ignore nulls
            if value is None:
                return
            val = float(value) # if fails, skips this iteration, which also ignores nulls
            tM = self.M
            self.k += 1
            self.M += ((val - tM) / self.k)
            self.S += ((val - tM) * (val - self.M))
        except ValueError:
            # TODO propagate udf errors to console
            raise Exception("Data is not numeric when calculating stddev (%s)" % value)

    def finalize(self):
        if self.k <= 1: # avoid division by zero
            return None
        else:
            return math.sqrt(self.S / (self.k-1))

class FunctionType(object):
    REGULAR = 1
    AGG = 2

class UserFunctionDef(object):
    def __init__(self,func_type,name,usage,description,func_or_obj,param_count):
        self.func_type = func_type
        self.name = name
        self.usage = usage
        self.description = description
        self.func_or_obj = func_or_obj
        self.param_count = param_count

user_functions = [
    UserFunctionDef(FunctionType.REGULAR,
                    "regexp","regexp(<regular_expression>,<expr>) = <1|0>",
                    "Find regexp in string expression. Returns 1 if found or 0 if not",
                    regexp,
                    2),
    UserFunctionDef(FunctionType.REGULAR,
                    "regexp_extract","regexp_extract(<regular_expression>,<expr>,group_number) = <substring|null>",
                    "Get regexp capture group content",
                    regexp_extract,
                    3),
    UserFunctionDef(FunctionType.REGULAR,
                    "sha","sha(<expr>,<encoding>,<algorithm>) = <hex-string-of-sha>",
                    "Calculate sha of some expression. Algorithm can be one of 1,224,256,384,512. For now encoding must be manually provided. Will use the input encoding automatically in the future.",
                    sha,
                    3),
    UserFunctionDef(FunctionType.REGULAR,
                    "sha1","sha1(<expr>) = <hex-string-of-sha>",
                    "Exists for backward compatibility only, since it doesn't handle encoding properly. Calculates sha1 of some expression",
                    sha1,
                    1),
    UserFunctionDef(FunctionType.REGULAR,
                    "md5","md5(<expr>,<encoding>) = <hex-string-of-md5>",
                    "Calculate md5 of expression. Returns a hex-string of the result. Currently requires to manually provide the encoding of the data. Will be taken automatically from the input encoding in the future.",
                    md5,
                    2),
    UserFunctionDef(FunctionType.REGULAR,
                    "sqrt","sqrt(<expr>) = <square-root>",
                    "Calculate the square root of the expression",
                    sqrt,
                    1),
    UserFunctionDef(FunctionType.REGULAR,
                    "power","power(<expr1>,<expr2>) = <expr1-to-the-power-of-expr2>",
                    "Raise expr1 to the power of expr2",
                    power,
                    2),
    UserFunctionDef(FunctionType.REGULAR,
                    "file_ext","file_ext(<expr>) = <filename-extension-or-empty-string>",
                    "Get the extension of a filename",
                    file_ext,
                    1),
    UserFunctionDef(FunctionType.REGULAR,
                    "file_folder","file_folder(<expr>) = <folder-name-of-filename>",
                    "Get the folder part of a filename",
                    file_folder,
                    1),
    UserFunctionDef(FunctionType.REGULAR,
                    "file_basename","file_basename(<expr>) = <basename-of-filename-including-extension>",
                    "Get the basename of a filename, including extension if any",
                    file_basename,
                    1),
    UserFunctionDef(FunctionType.REGULAR,
                    "file_basename_no_ext","file_basename_no_ext(<expr>) = <basename-of-filename-without-extension>",
                    "Get the basename of a filename, without the extension if there is one",
                    file_basename_no_ext,
                    1),
    UserFunctionDef(FunctionType.AGG,
                    "percentile","percentile(<expr>,<percentile-in-the-range-0-to-1>) = <percentile-value>",
                    "Calculate the strict percentile of a set of a values.",
                    StrictPercentile,
                    2),
    UserFunctionDef(FunctionType.AGG,
                    "stddev_pop","stddev_pop(<expr>) = <stddev-value>",
                    "Calculate the population standard deviation of a set of values",
                    StdevPopulation,
                    1),
    UserFunctionDef(FunctionType.AGG,
                    "stddev_sample","stddev_sample(<expr>) = <stddev-value>",
                    "Calculate the sample standard deviation of a set of values",
                    StdevSample,
                    1)
]

def print_user_functions():
    for udf in user_functions:
        print("Function: %s" % udf.name)
        print("     Usage: %s" % udf.usage)
        print("     Description: %s" % udf.description)

def get_sqlite_type_affinity(sqlite_type):
    sqlite_type = sqlite_type.upper()
    if 'INT' in sqlite_type:
        return 'INTEGER'
    elif 'CHAR' in sqlite_type or 'TEXT' in sqlite_type or 'CLOB' in sqlite_type:
        return 'TEXT'
    elif 'BLOB' in sqlite_type:
        return 'BLOB'
    elif 'REAL' in sqlite_type or 'FLOA' in sqlite_type or 'DOUB' in sqlite_type:
        return 'REAL'
    else:
        return 'NUMERIC'

def sqlite_type_to_python_type(sqlite_type):
    SQLITE_AFFINITY_TO_PYTHON_TYPE_NAMES = {
        'INTEGER': int,
        'TEXT': str,
        'BLOB': bytes,
        'REAL': float,
        'NUMERIC': float
    }
    return SQLITE_AFFINITY_TO_PYTHON_TYPE_NAMES[get_sqlite_type_affinity(sqlite_type)] 


def escape_double_quotes_if_needed(v):
    x = v.replace('"', '""')
    return x


def quote_none_func(output_delimiter,v):
    return v


def quote_minimal_func(output_delimiter,v):
    if v is None:
        return v
    if isinstance(v, str) and (output_delimiter in v or '\n' in v or '\r' in v):
        return '"{}"'.format(escape_double_quotes_if_needed(v))
    return v


def quote_nonnumeric_func(output_delimiter,v):
    if v is None:
        return v
    if isinstance(v, str):
        return '"{}"'.format(escape_double_quotes_if_needed(v))
    return v


def quote_all_func(output_delimiter,v):
    if isinstance(v, str):
        return '"{}"'.format(escape_double_quotes_if_needed(v))
    else:
        return str('"{}"').format(v)