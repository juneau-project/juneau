from distutils.core import setup

setup(
    name="data_extension",
    version="0.0.1",
    description="Data Extension for Jupyter notebook",
    packages=['data_extension'],
    scripts=['data_extension/print_var.py', 'data_extension/search.py'],
    install_requires=[
        'setuptools',
        'nb_config_manager',
        'psycopg2'
    ]
)