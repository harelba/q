#!/bin/bash -x

set -e


echo "Packing binary for $TRAVIS_OS_NAME"

if [[ "$TRAVIS_OS_NAME" == "osx" || "$TRAVIS_OS_NAME" == "linux" ]]
then
	pyci pack binary
	pyci github upload-asset --asset q-$(uname -m)-$(uname -s) --release 2.0.2
else
	echo "Packing windows installer"
	pyci pack binary
	find `pwd` -ls
	BINARY_LOCATION=`pwd`/q-AMD64-Windows.exe
	pyci pack nsis --binary-path $BINARY_LOCATION --version 2.0.2.0
	pyci github upload-asset --asset $BINARY_LOCATION --release 2.0.2
fi
