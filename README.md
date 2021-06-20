# aiorsmq
[![CI Status](https://github.com/federicotdn/aiorsmq/workflows/CI/badge.svg)](https://github.com/federicotdn/aiorsmq/actions)
![Black](https://img.shields.io/badge/code%20style-black-000000.svg)
![PyPI](https://img.shields.io/pypi/v/aiorsmq)
![GitHub](https://img.shields.io/github/license/federicotdn/aiorsmq)

This is the repository for aiorsmq (**A**sync **IO** **RSMQ**), an asynchronous (`async`/`await`) implementation of [RSMQ](https://github.com/smrchy/rsmq) for Python 3.6+. It aims to provide all the features that RSMQ provides, but for Python users.

## Features
Some of aiorsmq's features are:
- Fully compatible with RSMQ.
- Provides an API similar to that of RSMQ, with some changes done to achieve something more "pythonic".
- All public functions, methods and classes documented.
- Type-annotated and checked with [mypy](http://mypy-lang.org/).
- Tested against Redis, and against the original RSMQ library.

## Installation
To install aiorsmq, run:
```bash
$ pip install aiorsmq
```

## Documentation
For examples and API documentation please visit the [documentation pages](https://federicotdn.github.io/aiorsmq/).

## Related Projects
For a synchronous implementation of RSMQ for Python, see [PyRSMQ](https://github.com/mlasevich/PyRSMQ).

## License
Distributed under the MIT license.

See [LICENSE](LICENSE) for more details.
