.PHONY: format format-ci circle_install install lint test test-coverage airgo airgo-ci

SRC_DIR := airgo
TEST_DIR := tests

format:
	black $(SRC_DIR) $(TEST_DIR)

format-ci:
	black --check $(SRC_DIR) $(TEST_DIR)

upgrade-pip:
	pip install pip==20.2.4

dev-requirements:
	pip install -r dev-requirements.txt

dev-install: dev-requirements
	python setup.py develop

circle-install: upgrade-pip
	pip3 install .

lint:
	flake8 $(SRC_DIR)
	mypy $(SRC_DIR)

test:
	PYTHONPATH=. pytest -v $(TEST_DIR)

test-coverage:
	coverage run --source=$(SRC_DIR) -m pytest -vv $(TEST_DIR)
	coverage report
	coverage html
