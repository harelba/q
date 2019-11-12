#!/bin/bash

set -e


echo "Packing binary"

pyci pack binary
pyci github upload-asset --asset q-$(uname -m)-$(uname -s) --release 2.0.2

if [ "x$TRAVIS_OS_NAME" == "xwindows" ]
then
	echo "Packing windows installer"
	pyci pack nsis
	pyci github upload-asset --asset q-$(uname -a)-$(uname -s)-installer.exe --release 2.0.2
else
	echo "Not in windows - not packing windows installer"
fi
