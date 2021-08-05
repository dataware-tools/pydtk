"""
This script will install Pydtk and its dependencies.

It does

    1. Downloads the virtualenv package to a temporary directory and add it to sys.path.
    2. Creates a virtual environment in the correct OS data dir which will be
        - `${XDG_DATA_HOME}/pydtk` (or `~/.local/share/pydtk` if it's not set) on UNIX systems
        - In `${PYDTK_HOME}` if it's set.
    3. Installs the latest or given version of pydtk inside this virtual environment.
    4. Installs a `pydtk` script in the Python user directory (or `${POETRY_HOME/bin}` if `POETRY_HOME` is set).

This script was created by modifying the following.
    https://github.com/python-poetry/poetry/blob/master/install-poetry.py
    Copyright (c) 2018 Sébastien Eustace
    Released under the MIT license
    https://github.com/python-poetry/poetry/blob/master/LICENSE

"""

import argparse
import os
import shutil
import site
import subprocess
import sys
import tempfile

from contextlib import contextmanager
from pathlib import Path
from typing import Optional


MACOS = sys.platform == "darwin"


def data_dir(version: Optional[str] = None) -> Path:
    if os.getenv("PYDTK_HOME"):
        return Path(os.getenv("PYDTK_HOME")).expanduser()

    if MACOS:
        path = os.path.expanduser("~/Library/Application Support/pydtk")
    else:
        path = os.getenv("XDG_DATA_HOME", os.path.expanduser("~/.local/share"))
        path = os.path.join(path, "pydtk")

    if version:
        path = os.path.join(path, version)

    return Path(path)


def bin_dir() -> Path:
    if os.getenv("PYDTK_HOME"):
        return Path(os.getenv("PYDTK_HOME"), "bin").expanduser()

    user_base = site.getuserbase()
    bin_dir = os.path.join(user_base, "bin")

    return Path(bin_dir)


@contextmanager
def temporary_directory(*args, **kwargs):
    try:
        from tempfile import TemporaryDirectory
    except ImportError:
        name = tempfile.mkdtemp(*args, **kwargs)

        yield name

        shutil.rmtree(name)
    else:
        with TemporaryDirectory(*args, **kwargs) as name:
            yield name

class Installer:
    def __init__(
        self,
        version: Optional[str] = None,
        git: Optional[str] = None,
        path: Optional[str] = None,
    ) -> None:
        self._version = version
        self._git = git
        self._path = path
        self._data_dir = data_dir()
        self._bin_dir = bin_dir()

    def run(self) -> int:
        self.install()

    def install(self):
        """Make env and install pydtk."""
        env_path = self.make_env()
        self.install_pydtk(env_path)
        self.make_bin()

    def make_env(self) -> Path:
        """Make virtual environment."""
        env_path = self._data_dir.joinpath("venv")

        with temporary_directory() as tmp_dir:
            subprocess.call(
                [sys.executable, "-m", "pip", "install", "virtualenv", "-t", tmp_dir],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

            sys.path.insert(0, tmp_dir)

            import virtualenv

            virtualenv.cli_run([str(env_path), "--clear"])

        return env_path

    def make_bin(self) -> None:
        self._bin_dir.mkdir(parents=True, exist_ok=True)

        script = "pydtk"
        target_script = "venv/bin/pydtk"
        if self._bin_dir.joinpath(script).exists():
            self._bin_dir.joinpath(script).unlink()

        try:
            self._bin_dir.joinpath(script).symlink_to(
                self._data_dir.joinpath(target_script)
            )
            print(
                "It will add the `pydtk` command to pydtk's bin directory, located at:\n\n"
                f"{self._bin_dir}"
            )
        except OSError:
            # This can happen if the user
            # does not have the correct permission on Windows
            shutil.copy(
                self._data_dir.joinpath(target_script), self._bin_dir.joinpath(script)
            )

    def install_pydtk(
            self,
            env_path: Path
        ) -> None:
        """Install pydtk."""
        python = env_path.joinpath("bin/python")
        specification = "pydtk"

        subprocess.run(
            [str(python), "-m", "pip", "install", specification],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Installs the latest (or given) version of pydtk"
    )
    parser.add_argument("--version", help="install named version", dest="version")
    # parser.add_argument(
    #     "--uninstall",
    #     help="uninstall poetry",
    #     dest="uninstall",
    #     action="store_true",
    #     default=False,
    # )
    parser.add_argument(
        "--path",
        dest="path",
        action="store",
        help=(
            "Install from a given path (file or directory) instead of "
            "fetching the latest version of pydtk available online."
        ),
    )
    parser.add_argument(
        "--git",
        dest="git",
        action="store",
        help=(
            "Install from a git repository instead of fetching the latest version "
            "of pydtk available online."
        ),
    )

    args = parser.parse_args()

    installer = Installer(
        version=args.version or os.getenv("PYDTK_VERSION"),
        path=args.path,
        git=args.git,
    )

    # if args.uninstall or string_to_bool(os.getenv("POETRY_UNINSTALL", "0")):
    #     return installer.uninstall()

    return installer.run()


if __name__ == "__main__":
    sys.exit(main())
