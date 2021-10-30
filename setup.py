#!/usr/bin/env python

from setuptools import setup
import setuptools

q_version = '3.1.2-beta'

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
        'six==1.11.0'
    ],
    package_dir={"": "bin"},
    packages=setuptools.find_packages(where="bin"),
    entry_points={
        'console_scripts': [
            'q = bin.q:run_standalone'
        ]
    }
)
