#!/usr/bin/env python

from setuptools import setup, find_packages
from qtextasdata import q_version

with open("README.markdown", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='qtextasdata',
    url='https://github.com/harelba/q',
    license='Apache License 2.0',
    version=q_version,
    author='Harel Ben-Attia',
    description="Run SQL directly on CSV or TSV files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author_email='harelba@gmail.com',
    python_requires='>=3.7',
    install_requires=[
    ],
    extras_require={
        'dev': [
            'pytest>=8.0',
            'pytest-xdist>=3.0',
            'flake8>=7.0',
        ],
    },
    packages=find_packages(exclude=['test', 'test.*']),
    entry_points={
        'console_scripts': [
            'q = qtextasdata.cli:run_standalone'
        ]
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: SQL',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Text Processing :: General',
        'Topic :: Utilities',
    ],
    keywords='csv tsv sql data analysis query',
    project_urls={
        'Bug Reports': 'https://github.com/harelba/q/issues',
        'Source': 'https://github.com/harelba/q',
        'Documentation': 'https://github.com/harelba/q/blob/master/doc/PYTHON-API.md',
        'Changelog': 'https://github.com/harelba/q/blob/master/CHANGELOG.md',
    },
)
