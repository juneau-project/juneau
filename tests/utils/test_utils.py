from juneau.utils import utils


def test_clean_notebook_name():
    raw = "My-Awesome-Notebook.ipynb"
    expected = "MyAwesomeNotebook"
    assert utils.clean_notebook_name(raw) == expected