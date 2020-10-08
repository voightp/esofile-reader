from distutils.command.build_ext import build_ext
from distutils.core import Extension

from Cython.Build import cythonize


def build(setup_kwargs):
    """ This function is mandatory in order to build the extensions. """
    extensions = cythonize(
        [
            Extension(
                "esofile_reader.processing.extensions.esofile",
                sources=["esofile_reader/processing/extensions/esofile.pyx"],
            ),
            Extension(
                "esofile_reader.processing.extensions.raw_tables",
                sources=["esofile_reader/processing/extensions/raw_tables.pyx"],
            ),
        ]
    )

    setup_kwargs.update(
        {"ext_modules": extensions, "cmdclass": {"build_ext": build_ext}, "zip_safe": False,}
    )
