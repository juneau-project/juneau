from setuptools import setup, find_packages

setup(
    name="juneau",
    version="0.0.1",
    description="Data Extension for Jupyter notebook",
    packages=find_packages(),
    include_package_data=True,
    data_files=[
        (
            "share/jupyter/nbextensions/juneau",
            ["juneau/static/main.js"]
        ),
        (
            "etc/jupyter/nbconfig/notebook.d",
            ["jupyter-config/nbconfig/notebook.d/juneau.json"]
        ),
        (
            "etc/jupyter/jupyter_notebook_config.d",
            ["jupyter-config/jupyter_notebook_config.d/juneau.json"]
        )
    ],
    scripts=[
        "juneau/jupyter/print_var.py",
        "juneau/jupyter/jupyter.py",
        "juneau/search/search.py",
        "juneau/db/connect_psql.py",
        "juneau/db/table_db.py",
    ],
    install_requires=[
        "py2neo==5.0b1",  # Incompatible for versions pre 4.3.0
        "psycopg2",
        "SQLAlchemy",
        "networkx",
        "numpy",
        "pandas",
        "jupyter_client",
        "notebook",
    ],
    extras_require={
        'dev': [
            'black'
        ]
    },
    zip_safe=False,
)
