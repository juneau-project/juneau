# Brief Developers' Guide to Juneau

Juneau consists of three components executing at runtime:

1. The server extension (running inside Tornado alongside Jupyter Notebook).
2. The kernel (running its own Python interpreter) -- to which we will sometimes send commands via the server extension.
3. The client (with GUI widgets) running in the browser.


## Installing

Run `pip install -e .[dev]` to get all the production
requirements plus the development requirements (testing, linting, etc).

## Docs

Documentation uses [Sphinx](https://www.sphinx-doc.org/en/master/) and
[m2r2](https://github.com/CrossNox/m2r2) to convert Markdown to restructured text.
To make changes to the docs, edit the markdown files and run 

```
make html
```

in the `docs` folder.

## Configuration

The Juneau extension has a `config.py` which allows us to specify (1) the database we are indexing (`sql_dbs`), (2) the database schemas for storing Juneau's information, (3) configuration info for Neo4J.

## Data Extension: Initialization

When Juneau is installed as a server extension:

`sudo -H jupyter serverextension enable --py juneau`

this triggers `juneau/handler.py` and the `load_jupyter_server_extension`() function -- which in turn calls `background_load` to load and index all tables in the data lake.

The extension also logs the `/juneau` path for REST services.

## Indexing Tables and Notebooks

See `WithProv_Optimized` in `search_withprov_opt` as the current method for indexing tables and notebooks from the data lake.

## Data Extension: Requests

The `post` method in the handler is invoked during search; `put` is called when a new table is created.

### Put: Index

To index a table, we invoke the **kernel** with the name of the variable, and hope to get back a JSON version of the variable.  We then store it in the Juneau PostgreSQL schema, using Pandas.

```
HTTP request --> put(table_var_name) --> jupyter.request_var --> kernel

SQLAlchemy <-- Pandas <-- store_table <-- JSON -- kernel
Store_Provenance
Store_Lineage
````

First we need to fetch the variable (`find_variable`), which is done through the `jupyter.request_var` method.  This sends a blocking request to the kernel, which retrieves the variable as a JSON object.

The table is stored in the server extension, after it is given a unique name based on the variable, cell, notebook, etc.  We us an async method (`store_table`) to do the actual storage.

### Post: Search

The search operation first finds the contents of the "search table", then calls `search.search_tables` with the `WithProv_Optimized` object, a mode, the table content, and any provenance.

### Helper Functions

Several Python files are used to communicate with the kernel.  These include:

* `jupyter.py`, which includes methods for requesting data from the Jupyter kernel.
* `connect_psql.py`, which is invoked at the kernel side to connect to the Juneau SQL instance.
* `print_var.py`, which uses a blocking connection to the kernel to request a JSON version of all variables.  (This is superseded by `jupyter.py`).
* `var_list.py`, which finds all Python variables that we are interested in indexing.

## User Interface

The user interface is a separate client-side extension in Javascript.  See `static/main.js`.