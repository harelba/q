import os
from distutils.core import setup

here = os.path.dirname(os.path.abspath(__file__))
f = open(os.path.join(here, 'README.markdown'))
long_description = f.read()
f.close()


setup(name='q',
      version='1.5.0',
      description='Run SQL directly on CSV or TSV files',
      long_description=long_description,
      author='Harel Ben-Attia',
      author_email='harelba [at] gmail.com,',
      url='https://github.com/harelba/q',
      # license='MIT license',  - not specified yet
      platforms=['unix', 'linux', 'cygwin', 'win32'],
      scripts=['bin/q'],
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          # 'License :: OSI Approved :: MIT License',
          'Operating System :: POSIX',
          'Operating System :: Microsoft :: Windows',
          'Programming Language :: Python',
          # 'Programming Language :: Python :: 3',
          'Intended Audience :: Developers',
          'Topic :: Software Development :: Libraries',
      ],)
