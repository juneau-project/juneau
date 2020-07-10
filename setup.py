from setuptools import setup, find_packages

setup(
    name="juneau",
    version="0.0.1",
    description="Data Extension for Jupyter notebook",
    packages=find_packages(),
    scripts=[
        "juneau/jupyter/print_var.py",
        "juneau/jupyter/jupyter.py",
        "juneau/search/search.py",
        "juneau/db/connect_psql.py",
        "juneau/db/table_db.py",
    ],
    install_requires=["setuptools",],
)
