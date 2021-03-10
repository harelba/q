#!/bin/bash

# Can get unittest standard parameters, such as -k <partial-module-or-func-name> and -v for verbose mode
#
# If you're trying to run a partial module/func and you get a `no module named...` error, then it's probably because you just wrote the name instead of writing `-k name`

python -m unittest "$@"
