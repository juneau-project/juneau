from setuptools import setup
from os.path import abspath, dirname, join

setup(
    name="juneau",
    version="0.0.1",
    static=join(abspath(dirname(__file__)), 'varInspector')
)
