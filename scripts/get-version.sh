#!/usr/bin/env bash

[[ ! -z ${VERSION} ]] && echo ${VERSION} && exit 0

LATEST_TAG=$(git describe --abbrev=0 2>/dev/null | sed 's/[^0-9.]*//g')
NUM_FORWARDED_COMMITS=$(git rev-list ${LATEST_TAG}..HEAD --count 2>/dev/null)
VERSION=${LATEST_TAG:-0.0.0}-${NUM_FORWARDED_COMMITS}
echo ${VERSION}
