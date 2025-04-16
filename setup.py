#!/usr/bin/env python

from setuptools import setup, find_packages
from qtextasdata import q_version

with open("README.markdown", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='q',
    url='https://github.com/harelba/q',
    license='LICENSE',
    version=q_version,
    author='Harel Ben-Attia',
    description="Run SQL directly on CSV or TSV files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author_email='harelba@gmail.com',
    install_requires=[
    ],
    packages=['qtextasdata'],
    entry_points={
        'console_scripts': [
            'q = qtextasdata.cli:run_standalone'
        ]
    }
)
