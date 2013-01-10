#!/bin/bash

RPMBUILD=~/rpmbuild
SOURCE_DIR=${RPMBUILD}/SOURCES
SPEC_DIR=${RPMBUILD}/SPECS
[ -x /bin/rpmdev-setuptree ] || exit -1
[ -x /bin/rpmbuild ] || exit -1
echo "Creating RPM directory tree"
/bin/rpmdev-setuptree

[ -d ${SOURCE_DIR} ] || exit -1

tar czf ${SOURCE_DIR}/q-0.1.tar.gz src

sed -f version.txt q.spec > ${SPEC_DIR}/q.spec

cd ${SPEC_DIR}
/bin/rpmbuild -ba q.spec

