def clean_notebook_name(nb_name):
    """
    Cleans the notebook name by removing the .ipynb extension, removing hyphens,
    and removing underscores.
    Example:
        >>> nb = "My-Awesome-Notebook.ipynb"
        >>> handler = JuneauHandler()
        >>> # Receive a PUT with `nb`
        >>> print(handler._clean_notebook_name())
        >>> # prints "MyAwesomeNotebook"
    Returns:
        A string that is cleaned per the requirements above.
    """
    nb_name = nb_name.replace('.ipynb', '').replace('-', '').replace('_', '')
    nb_name = nb_name.split("/")
    if len(nb_name) > 2:
        nb_name = nb_name[-2:]
    nb_name = "".join(nb_name)
    return nb_name[-25:]
