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
	rm -rf docs-src/build docs
	cd docs-src && make html
	cp -r docs-src/build/html docs
	touch docs/.nojekyll

package:
	mkdir -p dist
	rm -rf dist/*
	python3 setup.py sdist bdist_wheel

upload: package
	twine upload dist/*
