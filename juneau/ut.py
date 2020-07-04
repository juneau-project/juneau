def clean_long_nbname(nb_name):
    """
    A utility function that cleans notebooks that have a long name.

    Args:
        nb_name: The notebook's name

    Returns:
        a cleaned, shorter notebook name.
    """
    nb_name = nb_name.split("/")
    if len(nb_name) > 2:
        nb_name = nb_name[-2:]
    nb_name = "".join(nb_name)
    return nb_name[-25:]
