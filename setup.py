from setuptools import setup
import pathlib

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text()
requirements = (here / "requirements.txt").read_text().splitlines()

setup(
    name="aiorsmq",
    version="0.1.0",
    description="Async Python implementation of RSMQ.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/federicotdn/aiorsmq",
    author="Federico Tedin",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
    ],
    keywords="redis, asyncio, async, message, queue, mq",
    python_requires=">=3.6, <4",
    install_requires=requirements,
    project_urls={"Bug Reports": "https://github.com/federicotdn/aiorsmq/issues"},
)
