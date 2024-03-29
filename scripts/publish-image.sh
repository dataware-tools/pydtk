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

echo "Pushing image ${IMAGE}:${VERSION}..."
docker push ${IMAGE}:${VERSION}
