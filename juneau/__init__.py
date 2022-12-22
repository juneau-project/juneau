from juneau.handler.server_extension import load_jupyter_server_extension


def _jupyter_server_extension_paths():
    return [{"module": "juneau"}]


def _jupyter_nbextension_paths():
    return [
        {
            "section": "notebook",
            "src": "dataset_inspector",  # path relative to `juneau` directory
            "dest": "juneau",
            "require": "juneau/main"
        }
    ]
