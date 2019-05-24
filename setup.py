import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="eso_pie",
    version="0.0.1",
    author="Vojtech Panek",
    author_email="vojtechpanek@seznam.cz",
    description="A package to read and process E+ output files.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/voightp/eso_reader",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: ",
        "Operating System :: Windows",
    ],
)