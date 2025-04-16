# q uses this encoding as the default output encoding. Some of the tests use it in order to 
# make sure that the output is correctly encoded
import itertools
import locale
import os
from subprocess import PIPE, Popen
import sys
import six


SYSTEM_ENCODING = locale.getpreferredencoding()
Q_EXECUTABLE = os.getenv('Q_EXECUTABLE', 'q')

DEBUG = '-v' in sys.argv
if os.environ.get('Q_DEBUG'):
    DEBUG = True

def batch(iterable, n=1):
    r = []
    l = len(iterable)
    for ndx in range(0, l, n):
        r += [iterable[ndx:min(ndx + n, l)]]
    return r


def partition(pred, iterable):
    t1, t2 = itertools.tee(iterable)
    return list(itertools.filterfalse(pred, t1)), list(filter(pred, t2))


def run_command(cmd_to_run,env_to_inject=None):
    global DEBUG
    if DEBUG:
        print("CMD: {}".format(cmd_to_run))

    if env_to_inject is None:
        env = os.environ.copy()
    else:
        env = os.environ.copy()
        env.update(env_to_inject)

    p = Popen(cmd_to_run, stdout=PIPE, stderr=PIPE, shell=True,env=env)
    o, e = p.communicate()
    # remove last newline
    o = o.rstrip()
    e = e.strip()
    # split rows
    if o != six.b(''):
        o = o.split(six.b(os.linesep))
    else:
        o = []
    if e != six.b(''):
        e = e.split(six.b(os.linesep))
    else:
        e = []

    res = (p.returncode, o, e)
    if DEBUG:
        print("RESULT:{}".format(res))
    return res


def one_column_warning(e):
    return e[0].startswith(six.b('Warning: column count is one'))


def sqlite_dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_sqlite_table_list(c,exclude_qcatalog=True):
    if exclude_qcatalog:
        r = c.execute("select tbl_name from sqlite_master where type='table' and tbl_name != '_qcatalog'").fetchall()
    else:
        r = c.execute("select tbl_name from sqlite_master where type='table'").fetchall()

    return r