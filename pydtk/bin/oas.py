#!/usr/bin/env python3

"""Dump OpenAPI Specification."""

import logging
import os
import sys

import fire

import pydtk
from pydtk.db.schemas import SCHEMAS_BY_FILES, BaseSchema


class OpenAPISpecification(object):
    """Generate OpenAPI Specification."""

    def __init__(self, out_dir: str) -> None:
        assert os.path.isdir(out_dir), f"Directory '{out_dir}' is not found."
        self.out_dir = out_dir
        self._schema_dir = os.path.join(
            os.path.dirname(pydtk.__file__), "db", "schemas"
        )

    def dump(self) -> None:
        """Dump OpenAPI Specification."""
        for rel_file_path, schemas in SCHEMAS_BY_FILES.items():
            rel_out_path = rel_file_path.replace(".py", ".json")
            self.dump_schemas_as_oas(schemas, rel_out_path, self.out_dir)

    def dump_schemas_as_oas(
        self, schemas: list, rel_out_path: str, output_dir: str
    ) -> None:
        """Dump schema information in the form of OpenAPI Specification format.

        Args:
            schemas (list): List of target schemas.
            rel_out_path (str): Relative output path.
            output_dir (str): Output directory.
        """
        dump_dir = os.path.join(output_dir, os.path.dirname(rel_out_path))
        os.makedirs(dump_dir, exist_ok=True)
        json_path = os.path.join(dump_dir, os.path.basename(rel_out_path))
        oas = "\n".join([self._schema_to_oas(schema) for schema in schemas])
        logging.debug(f"File: {json_path}\n{oas}")
        with open(json_path, "w") as f:
            f.write(oas)

    def _schema_to_oas(self, schema: BaseSchema):
        return schema.schema_json(indent=2)


def dump_oas(out_dir: str) -> None:
    """Dump schema information in the form of OpenAPI Specification format.

    Args:
        out_dir (str): Output directory.
    """
    oas = OpenAPISpecification(out_dir=out_dir)
    oas.dump()


def script() -> None:
    """Function for tool.poetry.scripts."""
    verbose = False
    if "-v" in sys.argv:
        verbose = True
        del sys.argv[sys.argv.index("-v")]
    if "--verbose" in sys.argv:
        verbose = True
        del sys.argv[sys.argv.index("--verbose")]

    # set logger
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s (%(module)s:%(lineno)d) %(levelname)s: %(message)s",
        )
    else:
        logging.basicConfig(
            level=logging.ERROR,
            format="%(asctime)s (%(module)s:%(lineno)d) %(levelname)s: %(message)s",
        )
        sys.tracebacklimit = 0
    fire.Fire(dump_oas)


if __name__ == "__main__":
    script()
