#!/usr/bin/python
# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name="qtextasdata",
    version="1.5.0",
    description="Command line tool that allows direct execution of SQL-like queries on CSVs/TSVs",
    url="https://github.com/harelba/q",
    license='GPL',
    author="Harel Ben-Attia",
    author_email="harelba@gmail.com",
    entry_points={
        'console_scripts': [
            'q=bin.q:main',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 2.7',
    ]
)
