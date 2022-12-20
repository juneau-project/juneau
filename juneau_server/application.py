import os

from traitlets import Unicode

from jupyter_server.extension.application import ExtensionApp, ExtensionAppJinjaMixin

from .juneau_server import JuneauHandler

DEFAULT_STATIC_FILES_PATH = os.path.join(os.path.dirname(__file__), "static")
DEFAULT_TEMPLATE_FILES_PATH = os.path.join(os.path.dirname(__file__), "templates")


class JuneauServer(ExtensionAppJinjaMixin, ExtensionApp):

    # The name of the extension.
    name = "juneau_server"

    # Te url that your extension will serve its homepage.
    extension_url = "/juneau_server"

    # Should your extension expose other server extensions when launched directly?
    load_other_extensions = True

    # Local path to static files directory.
    static_paths = [DEFAULT_STATIC_FILES_PATH]

    # Local path to templates directory.
    template_paths = [DEFAULT_TEMPLATE_FILES_PATH]

    configD = Unicode("", config=True, help="Juneau server side extension.")  # noqa

    def initialize_handlers(self):
        self.handlers.extend(
            [
                (r"/juneau/(.*)", JuneauHandler),
            ]
        )

    def initialize_settings(self):
        self.log.info(f"Config {self.config}")


# -----------------------------------------------------------------------------
# Main entry point
# -----------------------------------------------------------------------------

main = launch_new_instance = JuneauHandler.launch_instance