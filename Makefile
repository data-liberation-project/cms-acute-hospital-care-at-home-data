.PHONY: README.md venv data

PYTHON_DIRS=scripts

requirements.txt: requirements.in
	pip-compile requirements.in

venv:
	python -m venv venv
	venv/bin/pip install -r requirements.txt

format:
	black $(PYTHON_DIRS)
	isort $(PYTHON_DIRS)

lint:
	black --check $(PYTHON_DIRS)
	isort --check $(PYTHON_DIRS)
	flake8 --max-line-length 88 --extend-ignore E203 $(PYTHON_DIRS)

data:
	rm -rf data/standardized/* data/cleaned/*
	python scripts/XX-redact.py
	python scripts/00-standardize.py
	python scripts/01-clean.py
