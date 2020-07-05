from juneau.handler.server_extension import load_jupyter_server_extension


def _jupyter_server_extension_paths():
    return [{
        "module": "juneau"
    }]

