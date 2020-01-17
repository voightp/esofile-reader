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
                          annotate=False, compiler_directives={'language_level': "3"}),
    zip_safe=False,
    install_requires=[
        "Cython >= 0.29.14",
        "et - xmlfile >= 1.0.1",
        "jdcal >= 1.4.1",
        "numpy >= 1.18.0",
        "openpyxl >= 3.0.2",
        "pandas >= 0.25.3",
        "python - dateutil >= 2.8.1",
        "pytz >= 2019.3",
        "six >= 1.13.0",
        "coverage >= 5.0.3"
    ]
)
