#!/bin/bash

tar cvfz q-sources.tar.gz --exclude=build --exclude=rpmbuild --exclude=rrr --exclude=pyox* *
