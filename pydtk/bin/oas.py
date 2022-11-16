#!/usr/bin/env python3

"""Dump OpenAPI Specification."""

import fire
import logging
import os
import pydtk
from pydtk.db.schemas import BaseSchema

from pydtk.db.schemas import get_schema

import glob
from pydantic import BaseModel


class OpenAPISpecification(object):
    def __init__(self, out_dir: str) -> None:
        self.out_dir = out_dir
        self._schema_dir = os.path.join(os.path.dirname(pydtk.__file__), "db", "schemas")
        pass

    def dump(self):
        api_versions = self.get_api_versions()
        schemas = []
        for api_ver in api_versions:
            schemas.extend(self.get_schemas(api_ver))
        # for schema in schemas:
        #     self.dump_schema_as_oas(schema)

    def get_api_versions(self):
        versions = [os.path.dirname(directory).replace(f"{self._schema_dir}/", "") for directory in glob.glob(f"{self._schema_dir}/*/*/", recursive=False)]
        return versions

    def get_schemas(self, api_version: str):
        schema_dir = os.path.join(self._schema_dir, api_version.replace("/", os.sep).lower())
        schema_files = [file for file in glob.glob(f"{schema_dir}/*.py", recursive=False) if os.path.basename(file) != "__init__.py"]
        schemas = []
        for file in schema_files:
            kind = os.path.basename(file).replace(".py", "").capitalize()
            schemas.append(get_schema(api_version=api_version, kind=kind))
        return schemas


    def dump_schema_as_oas(self, schema: BaseSchema, output_dir):
        dump_dir = os.path.join(output_dir, schema._api_verson)
        os.makedirs(dump_dir, exist_ok=True)
        json_path = os.path.join(dump_dir, f"{schema._kind.lower()}.json")
        oas = self._shcema_to_oas(schema)
        with open(json_path, "w") as f:
            f.write(oas)

    def _schema_to_oas(self, schema: BaseSchema):
        return schema._api_version


def dump_oas():
    out_dir = "./tmp"
    oas = OpenAPISpecification(out_dir=out_dir)
    oas.dump()


if __name__ == "__main__":
    # set logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s (%(module)s:%(lineno)d) %(levelname)s: %(message)s",
    )
    dump_oas()
