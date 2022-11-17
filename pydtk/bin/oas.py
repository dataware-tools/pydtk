#!/usr/bin/env python3

"""Dump OpenAPI Specification."""

import glob
import logging
import os

import fire

import pydtk
from pydtk.db.schemas import BaseSchema, get_schema


class OpenAPISpecification(object):
    """Generate OpenAPI Specification."""

    def __init__(self, out_dir: str) -> None:
        assert os.path.isdir(out_dir), f"Directory '{out_dir}' is not found."
        self.out_dir = out_dir
        self._schema_dir = os.path.join(os.path.dirname(pydtk.__file__), "db", "schemas")
        pass

    def dump(self) -> None:
        """Dump OpenAPI Specification."""
        api_versions = self.get_api_versions()
        schemas = []
        for api_ver in api_versions:
            schemas.extend(self.get_schemas(api_ver))
        for schema in schemas:
            self.dump_schema_as_oas(schema, self.out_dir)

    def get_api_versions(self) -> list:
        """Get all API versions.

        Returns:
            list: List of API versions.
        """
        versions = [os.path.dirname(directory).replace(f"{self._schema_dir}/", "") for directory in glob.glob(f"{self._schema_dir}/*/*/", recursive=False)]
        return versions

    def get_schemas(self, api_version: str) -> BaseSchema:
        """Get all schemas.

        Args:
            api_version (str): Target schema's API version.

        Returns:
            BaseSchema (pydtk.db.schema.BaseSchema): All schema of the target API version.
        """
        schema_dir = os.path.join(self._schema_dir, api_version.replace("/", os.sep).lower())
        schema_files = [file for file in glob.glob(f"{schema_dir}/*.py", recursive=False) if os.path.basename(file) != "__init__.py"]
        schemas = []
        for file in schema_files:
            kind = os.path.basename(file).replace(".py", "").capitalize()
            schemas.append(get_schema(api_version=api_version, kind=kind))
        return schemas

    def dump_schema_as_oas(self, schema: BaseSchema, output_dir: str) -> None:
        """Dump schema information in the form of OpenAPI Specification format.

        Args:
            schema (BaseSchema): Target schema.
            output_dir (str): Output directory.
        """
        dump_dir = os.path.join(output_dir, schema._api_version)
        os.makedirs(dump_dir, exist_ok=True)
        json_path = os.path.join(dump_dir, f"{schema._kind.lower()}.json")
        oas = self._schema_to_oas(schema)
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
    fire.Fire(dump_oas)


if __name__ == "__main__":
    # set logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s (%(module)s:%(lineno)d) %(levelname)s: %(message)s",
    )
    fire.Fire(dump_oas)
