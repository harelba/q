#!/bin/bash -x

set -e

VERSION=2.0.16

echo "Packing binary for $TRAVIS_OS_NAME"

if [[ "$TRAVIS_OS_NAME" == "osx" || "$TRAVIS_OS_NAME" == "linux" ]]
then
	echo "Packing $TRAVIS_OS_NAME installer - packing binary"
	pyci pack binary
	echo "Packing $TRAVIS_OS_NAME installer - uploading"
	pyci github upload-asset --asset q-$(uname -m)-$(uname -s) --release $VERSION
else
	echo "Packing windows installer - packing binary"
	pyci pack binary
	echo "Packing windows installer - listing files"
	find `pwd` -ls | grep -v \.git/
	echo "Packing windows installer - packing nsis"
	BINARY_LOCATION="c:\\Users\\travis\\build\\harelba\\q\\q-AMD64-Windows.exe"
	pyci pack nsis --program-files-dir q-TextAsData --binary-path $BINARY_LOCATION --version ${VERSION}.0
	echo "Packing windows installer - uploading"
	pyci github upload-asset --asset $BINARY_LOCATION --release $VERSION
	SETUP_LOCATION="c:\\Users\\travis\\build\\harelba\\q\\q-AMD64-Windows-installer.exe"
	pyci github upload-asset --asset $SETUP_LOCATION --release $VERSION
fi

echo "done"
