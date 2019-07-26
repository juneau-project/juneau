from data_extension.handler import load_jupyter_server_extension

def _jupyter_server_extension_paths():
    return [{
        "module": "data_extension"
    }]

