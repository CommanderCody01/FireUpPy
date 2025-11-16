include ../Vars.mk
include ../Common.mk

PYTHON := python3
PIP := pip3
PYTEST := pytest
COV_OPTS := --cov=cif --cov-branch --cov-report term-missing:skip-covered --no-cov-on-fail 
PYTEST_OPTS := -vv
PYTEST_FILES := tests
BLACK := black
BLACK_OPTS := --line-length 130
FLAKE8 := flake8
ISORT := isort
ISORT_OPTS := --profile black

venv:
	$(PYTHON) -m venv --prompt $(ROOT) $@ && . $@/bin/activate && $(PIP) install -r venv_requirements.txt

deps:: venv
	if [[ -z "$${VIRTUAL_ENV}" ]] ; then . venv/bin/activate ; fi && $(PIP) install --upgrade --extra-index-url https://nexus.mgmt.aig.aetna.com/repository/pypi-hosted/simple  -r requirements.txt -r dev_requirements.txt

test::
	if [[ -z "$${VIRTUAL_ENV}" ]] ; then . venv/bin/activate ; fi && $(PYTEST) $(COV_OPTS) $(PYTEST_OPTS) $(PYTEST_FILES)

realclean::
	if [[ -n "$${VIRTUAL_ENV}" ]] ; then deactivate ; fi && rm -rf venv

check-fmt::
	if [[ -z "$${VIRTUAL_ENV}" ]] ; then . venv/bin/activate ; fi && $(BLACK) $(BLACK_OPTS) --check $(BLACK_EXTRA_OPTS) cif tests && $(ISORT) $(ISORT_OPTS) cif tests

fmt::
	if [[ -z "$${VIRTUAL_ENV}" ]] ; then . venv/bin/activate ; fi && $(BLACK) $(BLACK_OPTS) $(BLACK_EXTRA_OPTS) cif tests && $(ISORT) $(ISORT_OPTS) cif tests

lint::
	if [[ -z "$${VIRTUAL_ENV}" ]] ; then . venv/bin/activate ; fi && $(FLAKE8) $(FLAKE8_OPTS)

clean::
	rm -rf *.egg-info dist

dist::
	if [[ -z "$${VIRTUAL_ENV}" ]] ; then . venv/bin/activate ; fi && $(PYTHON) -m build

info::
	@printf "%15s: %s\n" PYTHON_VERSION $(shell cat .python-version)
