#############################################################################
# Copyright (c) 2018 Eli Polonsky. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   * See the License for the specific language governing permissions and
#   * limitations under the License.
#
#############################################################################

from setuptools import setup

setup(
    name='q',
    url='https://github.com/harelba/q',
    license='LICENSE',
    version='1.7.4',
    author='Harel Ben-Attia',
    description="Run SQL directly on CSV or TSV files",
    author_email='harelba@gmail.com',
    install_requires=[
        'six==1.11.0'
    ],
    packages=[
        'q'
    ],
    entry_points={
        'console_scripts': [
            'q = q.q:run_standalone'
        ]
    }
)
