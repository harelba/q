import os
import sys
import datetime


DEBUG = bool(os.environ.get('Q_DEBUG', None)) or '-V' in sys.argv
SQL_DEBUG = False

if DEBUG:
    def xprint(*args,**kwargs):
        print(datetime.datetime.now(datetime.timezone.utc).isoformat()," DEBUG ",*args,file=sys.stderr,**kwargs)

    def iprint(*args,**kwargs):
        print(datetime.datetime.now(datetime.timezone.utc).isoformat()," INFO ",*args,file=sys.stderr,**kwargs)

    def sqlprint(*args,**kwargs):
        pass
else:
    def xprint(*args,**kwargs): pass
    def iprint(*args,**kwargs): pass
    def sqlprint(*args,**kwargs): pass

if SQL_DEBUG:
    def sqlprint(*args,**kwargs):
        print(datetime.datetime.now(datetime.timezone.utc).isoformat(), " SQL ", *args, file=sys.stderr, **kwargs)
