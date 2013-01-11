#!/bin/bash

VERSION=`cat version.txt`
RPMBUILD=~/rpmbuild
SOURCE_DIR=${RPMBUILD}/SOURCES
SPEC_DIR=${RPMBUILD}/SPECS
[ -x /bin/rpmdev-setuptree ] || exit -1
[ -x /bin/rpmbuild ] || exit -1
echo "Creating RPM directory tree"
/bin/rpmdev-setuptree

[ -d ${SOURCE_DIR} ] || exit -1

tar czf ${SOURCE_DIR}/q-${VERSION}.tar.gz src

sed "s/QVERSION/$VERSION/g" q.spec > ${SPEC_DIR}/q.spec

cd ${SPEC_DIR}
/bin/rpmbuild -ba q.spec

