#!/usr/bin/env sh

set -e

if [ -z ${VERSION} ]; then
  echo "IMAGE_VERSION env var needs to be set"
  exit 1
fi

if [ -z ${IMAGE} ]; then
  echo "IMAGE env var needs to be set"
  exit 1
fi

if [ -z ${DOCKER_FILE_PATH} ]; then
  echo "DOCKER_FILE_PATH env var needs to be set"
  exit 1
fi

if [ -z ${BASE} ]; then
  echo "Building image ${IMAGE}:${VERSION} (using ${DOCKER_FILE_PATH}) ..."
else
  echo "Building image ${IMAGE}:${VERSION} (using ${DOCKER_FILE_PATH} based on ${BASE}) ..."
fi
docker build \
  --build-arg VERSION=${VERSION} \
  --build-arg BASE=${BASE} \
  -t ${IMAGE}:${VERSION} \
  -f ${DOCKER_FILE_PATH} .
