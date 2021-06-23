#!/usr/bin/env bash

[[ ! -z ${VERSION} ]] && echo ${VERSION} && exit 0

LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null)
LATEST_VERSION=$(echo ${LATEST_TAG} | sed 's/[^0-9.]*//g')
NUM_FORWARDED_COMMITS=$(git rev-list ${LATEST_TAG}..HEAD --count 2>/dev/null)
VERSION=${LATEST_VERSION:-0.0.0}-${NUM_FORWARDED_COMMITS}
echo ${VERSION}
