SHELL = bash

types:
	mypy aiorsmq tests

format:
	black aiorsmq tests

test:
	docker-compose -f tests/docker-compose.yml up -d
	pytest tests
	docker-compose -f tests/docker-compose.yml down
