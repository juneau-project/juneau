# Brief Developers' Guide to Juneau

Juneau consists of three components executing at runtime:

1. The server extension (running inside Tornado alongside Jupyter Notebook.
2. The kernel (running its own Python interpreter) -- to which we will sometimes send commands via the server extension.
3. The client (with GUI widgets) running in the browser.


## Configuration

The Juneau extension has a `config.py` which allows us to specify (1) the database we are indexing (`sql_dbs`), (2) the database schemas for storing Juneau's information, (3) configuration info for Neo4J.

## Data Extension: Initialization

When Juneau is installed as a server extension:

`sudo -H jupyter serverextension enable --py data_extension`

this triggers `data_extension/handler.py` and the `load_jupyter_server_extension`() function -- which in turn calls `background_load` to load and index all tables in the data lake.

The extension also logs the `/juneau` path for REST services.

## Indexing Tables and Notebooks

See `WithProv_Optimized` in `search_withprov_opt` as the current method for indexing tables and notebooks from the data lake.

## Data Extension: Requests

The `post` method in the handler is invoked during search; `put` is called when a new table is created.

### Put: Index

To index a table, we invoke the **kernel** with the name of the variable, and hope to get back a JSON version of the variable.  This is done through the `exec_ipython` call.

The table is stored in the server extension, after it is given a unique name based on the variable, cell, notebook, etc.  We us an async method (`fn`) to do the actual storage.

### Post: Search

The search operation first finds the contents of the "search table", then calls `search.search_tables` with the `WithProv_Optimized` object, a mode, the table content, and any provenance.

### Helper Functions

Several Python files are used to communicate with the kernel.  These include:

* `print_var.py`, which uses a blocking connection to the kernel to request a JSON version of all variables.
* `var_list.py`, which finds all Python variables that we are interested in indexing.

## User Interface

The user interface is a separate client-side extension in Javascript.  See `dataset_inspector/main.js`.