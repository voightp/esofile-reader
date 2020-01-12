import setuptools
from Cython.Build import cythonize

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="esofile_reader",
    version="0.1.0",
    author="Vojtech Panek",
    author_email="vojtechpanek@seznam.cz",
    description="Package to read and process E+ output files.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/voightp/esofile-reader.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: ",
        "Operating System :: Windows",
    ],
    ext_modules=cythonize("./esofile_reader/processing/esofile_processor.pyx",
                          annotate=False),
    zip_safe=False,
)
