#!/bin/bash

set -e


echo "Packing binary for $TRAVIS_OS_NAME"

if [[ "$TRAVIS_OS_NAME" == "osx" || "$TRAVIS_OS_NAME" == "linux" ]]
then
	pyci pack binary
	pyci github upload-asset --asset q-$(uname -m)-$(uname -s) --release 2.0.2
else
	echo "Packing windows installer"
	pyci pack nsis --binary-path ./q-AMD64-Windows.exe
	pyci github upload-asset --asset q-AMD64-Windows.exe --release 2.0.2
fi
