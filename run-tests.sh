#!/bin/bash

pytest -m 'not benchmark' "$@"
