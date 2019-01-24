from setuptools import setup
from os.path import abspath, dirname, join

setup(
    name="data_extension",
    version="0.0.1",
    static=join(abspath(dirname(__file__)), 'varInspector')
)
