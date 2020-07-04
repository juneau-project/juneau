from distutils.core import setup

setup(
    name="juneau",
    version="0.0.1",
    description="Data Extension for Jupyter notebook",
    packages=['juneau'],
    scripts=[
        'juneau/print_var.py',
        'juneau/jupyter.py',
        'juneau/connect_psql.py',
        'juneau/search.py',
        'juneau/table_db.py'
    ],
    install_requires=[
        'setuptools',
    ]
)
