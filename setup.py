from setuptools import setup
import pathlib

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text()
requirements = (here / "requirements.txt").read_text().splitlines()
about = {}
with open(here / "aiorsmq" / "__version__.py") as f:
    exec(f.read(), about)

setup(
    name="aiorsmq",
    version=about["__version__"],
    description="Async Python implementation of RSMQ.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/federicotdn/aiorsmq",
    author="Federico Tedin",
    packages=["aiorsmq"],
    author_email="federicotedin@gmail.com",
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
    download_url=f"https://github.com/federicotdn/aiorsmq"
    f"/archive/refs/tags/{about['__version__']}.zip",
)
