#!/bin/bash

eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

pytest -m 'not benchmark' "$@"
