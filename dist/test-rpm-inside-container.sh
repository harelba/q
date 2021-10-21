#!/bin/bash
set -x
set -e

yum install -y python38 sqlite perl gcc python3-devel sqlite-devel
pip3 install -r test-requirements.txt

rpm -i $1
Q_EXECUTABLE=q Q_SKIP_EXECUTABLE_VALIDATION=true ./run-tests.sh -v
