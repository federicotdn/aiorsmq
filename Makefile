SHELL = bash

types:
	mypy aiorsmq tests

format:
	black aiorsmq tests

test:
	pytest tests
