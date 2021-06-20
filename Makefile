SHELL = bash

types:
	mypy aiorsmq tests

format:
	black aiorsmq tests setup.py

format-check:
	black --check aiorsmq tests setup.py

test:
	docker-compose -f tests/docker-compose.yml up -d
	PYTHONPATH=. pytest tests
	docker-compose -f tests/docker-compose.yml down

lint:
	flake8 aiorsmq tests setup.py


html:
	cd docs && rm -rf build && make html
