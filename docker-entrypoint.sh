#!/usr/bin/env bash
#
# Entrypoint for container images
#
# Copyright toolkit authors
#

source /opt/pydtk/.venv/bin/activate
[[ -f /ros_entrypoint.sh ]] && source /ros_entrypoint.sh

exec "$@"
