#!/bin/bash

set -x
set -e

sudo dpkg -i $1
Q_EXECUTABLE=q Q_SKIP_EXECUTABLE_VALIDATION=true ./run-tests.sh -v

