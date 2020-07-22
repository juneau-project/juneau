from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="juneau",
    version="0.0.2",
    description="Data Extension for Jupyter notebook",
    packages=find_packages(),
    include_package_data=True,
    python_requires='>=3.6',
    url="https://juneau.readthedocs.io/",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
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
        "dev": [
            "black",
            "pytest",
            "m2r2"
        ]
    },
    zip_safe=False,
)
