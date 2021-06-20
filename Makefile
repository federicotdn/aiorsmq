SHELL = bash

types:
	mypy aiorsmq tests

format:
	black aiorsmq tests

format-check:
	black --check aiorsmq tests

test:
	docker-compose -f tests/docker-compose.yml up -d
	PYTHONPATH=. pytest tests
	docker-compose -f tests/docker-compose.yml down

lint:
	flake8 aiorsmq tests


html:
	cd docs && rm -rf build && make html
