"""Import utilities."""


import importlib
import importlib.util


def import_module_from_path(path):
    """Import a module from a path.

    Args:
        path (str): Path to a module.

    Returns:
        module: Imported module.
    """
    spec = importlib.util.spec_from_file_location("module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
