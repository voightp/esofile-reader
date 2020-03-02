import setuptools
import sys
from pathlib import Path
from Cython.Build import cythonize

with open("README.md", "r") as fh:
    long_description = fh.read()

CURRENT_DIR = Path(__file__).parent
sys.path.insert(0, str(CURRENT_DIR))  # for setuptools.build_meta

setuptools.setup(
    name="esofile_reader",
    author="Vojtech Panek",
    author_email="vojtechpanek@seznam.cz",
    description="Package to read and process E+ output files.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/voightp/esofile-reader.git",
    packages=setuptools.find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: ",
        "Operating System :: OS Independent",
    ],
    ext_modules=cythonize(
        "./esofile_reader/processor/esofile_processor.pyx",
        annotate=True,
        compiler_directives={"language_level": "3", "profile": True, "linetrace": True},
    ),
    zip_safe=False,
    install_requires=[
        "appdirs == 1.4.3",
        "attrs == 19.3.0",
        "black == 19.10b0",
        "Click == 7.0",
        "coverage == 5.0.3",
        "Cython == 0.29.15",
        "et-xmlfile == 1.0.1",
        "jdcal == 1.4.1",
        "llvmlite == 0.31.0",
        "numba == 0.48.0",
        "numpy == 1.18.1",
        "openpyxl == 3.0.3",
        "pandas == 1.0.1",
        "pathspec == 0.7.0",
        "profilehooks == 1.11.1",
        "pyarrow >= 0.15.1",
        "python-dateutil == 2.8.1",
        "pytz == 2019.3",
        "regex == 2020.1.8",
        "six == 1.14.0",
        "SQLAlchemy == 1.3.13",
        "thrift == 0.13.0",
        "toml == 0.10.0",
        "typed-ast == 1.4.1",
        "setuptools-scm == 3.5.0",
    ],
)
