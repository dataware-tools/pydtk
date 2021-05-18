#
# Makefile for PyDTK
#

VERSION ?= $(shell ./scripts/get-version.sh)
COMMIT_ID := $(shell git rev-parse HEAD)

IMAGE_NAME := hdwlab/pydtk
BUILD_TARGET_FILE := ./docker/targets.txt
BUILD_LIST := $(shell cat ${BUILD_TARGET_FILE})
$(warning Available targets: $(BUILD_LIST))

help: ## Show this help
	@echo "Help"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[36m%-20s\033[93m %s\n", $$1, $$2}'

.PHONY: default
default: help

.PHONY: publish-runtime-images
.PHONY: publish.runtime.%
PUBLISH_RUNTIME_TARGETS := $(addprefix publish.runtime.,$(BUILD_LIST))
publish-runtime-images: $(PUBLISH_RUNTIME_TARGETS)	## Publishes runtime docker images.
publish.runtime.%: build.runtime.%
	@IMAGE=$(IMAGE_NAME) VERSION=${VERSION}-$* ./scripts/publish-image.sh

.PHONY: publish-dev-images
.PHONY: publish.dev.%
PUBLISH_DEV_TARGETS := $(addprefix publish.dev.,$(BUILD_LIST))
publish-dev-images: $(PUBLISH_DEV_TARGETS)	## Publishes dev docker images.
publish.dev.%: build.dev.%
	@IMAGE=$(IMAGE_NAME) VERSION=${VERSION}-$*-dev ./scripts/publish-image.sh

.PHONY: tests
.PHONY: test.%
TEST_TARGETS := $(addprefix test.,$(BUILD_LIST))
tests: $(TEST_TARGETS)  ## Runs tests.
test.%: build.dev.%
	@docker run -v ${PWD}/pydtk:/opt/pydtk/pydtk -v ${PWD}/test:/opt/pydtk/test --rm $(IMAGE_NAME):${VERSION}-$*-dev tox
test.ros.%: build.dev.%
	@docker run -v ${PWD}/pydtk:/opt/pydtk/pydtk -v ${PWD}/test:/opt/pydtk/test --rm $(IMAGE_NAME):${VERSION}-$*-dev tox -e py3-pytest-ros

.PHONY: build-base-images
.PHONY: build.base.%
BUILD_BASE_TARGETS := $(addprefix build.base.,$(BUILD_LIST))
build-base-images: $(BUILD_BASE_TARGETS)	## Builds base images.
build.base.%:
	@IMAGE=$(IMAGE_NAME) DOCKER_FILE_PATH=./docker/base/$*.Dockerfile VERSION=$*-base ./scripts/build-image.sh

.PHONY: build-runtime-images
.PHONY: build.runtime.%
BUILD_RUNTIME_TARGETS := $(addprefix build.runtime.,$(BUILD_LIST))
build-runtime-images: $(BUILD_RUNTIME_TARGETS)	## Builds runtime images.
build.runtime.%: fix-version build.base.%
	@IMAGE=$(IMAGE_NAME) DOCKER_FILE_PATH=./docker/runtime/$*.Dockerfile VERSION=${VERSION}-$* BASE=$(IMAGE_NAME):$*-base ./scripts/build-image.sh

.PHONY: build-dev-images
.PHONY: build.dev.%
BUILD_DEV_TARGETS := $(addprefix build.dev.,$(BUILD_LIST))
build-dev-images: $(BUILD_DEV_TARGETS)	## Builds dev images.
build.dev.%: fix-version build.base.%
	@IMAGE=$(IMAGE_NAME) DOCKER_FILE_PATH=./docker/dev/$*.Dockerfile VERSION=${VERSION}-$*-dev BASE=$(IMAGE_NAME):$*-base ./scripts/build-image.sh

.PHONY: fix-version
fix-version:	## Fix version in pyproject.toml and __init__.py
	@sed -i -r 's/^version\ =\ ".*"/version\ =\ "$(VERSION)"/' pyproject.toml
	@[ -f pyproject.toml-r ] && rm pyproject.toml-r || :
	@sed -i -r 's/^__version__\ =\ ".*"/__version__\ =\ "$(VERSION)"/' pydtk/__init__.py
	@[ -f pydtk/__init__.py-r ] && rm pydtk/__init__.py-r || :
	@sed -i -r 's/^__commit_id__\ =\ ".*"/__commit_id__\ =\ "$(COMMIT_ID)"/' pydtk/__init__.py
	@[ -f pydtk/__init__.py-r ] && rm pydtk/__init__.py-r || :
