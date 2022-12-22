# Dataset Inspector

Based on jfbercher's Variable Inspector, https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tree/master/src/jupyter_contrib_nbextensions/nbextensions/varInspector

## Description and main features

The Dataset Inspector lists tabular data (matrices and dataframes) in Python.  It provides search capabilities for similar, linkable, and related tables.


## Configuration
The initial configuration can be given using the IPython-contrib nbextensions facility. It includes:

- varInspector.window_display - Display at startup or not (default: false)
- varInspector.cols.lenName: (and .lenType, .lenVar) - Width of columns (actually the max number of character to display in each column)
- varInspector.kernels_config - json object defining the kernels specific code and commands.


## Notes
- The displayed size of variables use the `getsizeof()` python method. This method doesn't work for all types, so the reported size is to be considered with some caution. The extension includes some code to correctly return the size of numpy arrays, pandas Series and DataFrame but the size for some other types may be incorrect.
- The extension builds on some code provided [here](https://github.com/jupyter-widgets/ipywidgets/blob/master/docs/source/examples/Variable%20Inspector.ipynb)  (essentially the `_fill` method)
- The extension uses Christian Bach's [table sorter jquery plugin](https://github.com/christianbach/tablesorter). License file is included.


## History

- @jfbercher march 22, 2017 -- initial release
- @jfbercher april 03, 2017 -- multiple kernel support. added support for R kernels.
- @jfbercher june 30, 2017 -- fixed #1014 (use of `%reset` with IPython kernel) and #1015 printing with python 2 kernel.
