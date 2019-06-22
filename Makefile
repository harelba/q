SHELL := /bin/bash

PROJECT_NAME=$(shell dirname "$0")
ROOT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

.PHONY: test help
.DEFAULT_GOAL := ci

ci: dep lint test ## Equivelant to 'make dep lint test'

help: ## Show this help message.

	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

dep: dep-test dep-project ## Equivelant to 'make dep-test dep-project'

dep-test: ## Install the dependent libraries needed for tests.

	pip install -r test-requirements.txt

dep-project: ## Install the dependent libraries needed for the project to run.

	pip install -e .

lint: dep ## Run lint validations.

	flake8 ./bin/q ./test/test-suite --count --select=E901,E999,F821,F822,F823 --show-source --statistics

test: dep ## Run the unit tests.

	py.test -rs -c pytest.ini -s -v q/tests/suite.py --rootdir .

release: dep-project ## Run release

	pyci release --no-wheel-publish --wheel-universal
