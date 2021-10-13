#!/bin/bash

set -x
set -e

RPM_LOCATION=$1

docker run -it -v `pwd`:/q-sources -w /q-sources centos:8 /bin/bash -e -x ./dist/test-rpm-inside-container.sh ${RPM_LOCATION}
