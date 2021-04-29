#!/usr/bin/env bash
#
# Entrypoint for container images
#
# Copyright toolkit authors
#

source /opt/pydtk/.venv/bin/activate

exec "$@"
