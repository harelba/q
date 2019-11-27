#!/bin/bash -x

set -e


echo "Packing binary for $TRAVIS_OS_NAME"

if [[ "$TRAVIS_OS_NAME" == "osx" || "$TRAVIS_OS_NAME" == "linux" ]]
then
	pyci pack binary
	pyci github upload-asset --asset q-$(uname -m)-$(uname -s) --release 2.0.2
else
	echo "Packing windows installer - packing binary"
	pyci pack binary
	echo "Packing windows installer - listing files"
	find `pwd` -ls | grep -v \.git/
	echo "Packing windows installer - packing nsis"
	BINARY_LOCATION="c:\\Users\\travis\\build\\harelba\\q\\q-AMD64-Windows.exe"
	pyci pack nsis --binary-path $BINARY_LOCATION --version 2.0.2.0
	echo "Packing windows installer - uploading"
	pyci github upload-asset --asset $BINARY_LOCATION --release 2.0.2
	SETUP_LOCATION="c:\\Users\\travis\\build\\harelba\\q\\q-AMD64-Windows-installer.exe"
	pyci github upload-asset --asset $SETUP_LOCATION --release 2.0.2
	echo "done"
fi
