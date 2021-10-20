#!/bin/bash
set -x
set -e

yum install -y python38 sqlite perl
pip3 install -r test-requirements.txt

rpm -i $1
Q_EXECUTABLE=q Q_SKIP_EXECUTABLE_VALIDATION=true bash -x test/test-all
