import setuptools
from distutils.core import setup
from proton.version import __version__

with open("Readme.md", "r") as fh:
    long_description = fh.read()

setup(
    name='proton',
    author = "Maximilien Lehujeur",
    author_email = "maximilien.lehujeur@gmail.com",
    version=__version__,
    packages=setuptools.find_packages(),
    url='https://github.com/obsmax/proton',
    license='LICENCE',
    description='parallelization helper based on python-multiprocessing',
    long_description=long_description,
    long_description_content_type="text/markdown",
    scripts=[],
    classifiers=[
        "Programming Language :: Python :: 3"
        "Operating System :: Linux"],
    python_requires='>=3')
