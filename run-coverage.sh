#!/bin/bash

set -e

rm -vf ./htmlcov/*

pytest -m "not benchmark" --cov --cov-report html "$@"

function cleanup() {
  kill %1
}

# TODO Fix

# python -m http.server 8000 
# open http://localhost:8000/htmlcov/


