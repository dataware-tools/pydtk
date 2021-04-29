#
# Makefile for PyDTK
#

VERSION ?= $(shell ./scripts/get-version.sh)

IMAGE_NAME := hdwlab/pydtk
BUILD_TARGET_FILE := ./docker/targets.txt
BUILD_LIST := $(shell cat ${BUILD_TARGET_FILE})
$(warning Available targets: $(BUILD_LIST))

help: ## Show this help
	@echo "Help"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[36m%-20s\033[93m %s\n", $$1, $$2}'

.PHONY: default
default: help

.PHONY: publish-images
PUBLISH_TARGETS := $(addprefix publish.,$(BUILD_LIST))
publish-images: $(PUBLISH_TARGETS)	## Publishes runtime docker images.
publish.%: build.runtime.%
	@IMAGE=$(IMAGE_NAME) VERSION=${VERSION}-$* ./scripts/publish-image.sh

.PHONY: tests
TEST_TARGETS := $(addprefix test.,$(BUILD_LIST))
tests: $(TEST_TARGETS)  ## Runs tests.
test.%: build.dev.%
	@docker run -v ${PWD}/pydtk:/opt/pydtk/pydtk -v ${PWD}/test:/opt/pydtk/test --rm -it $(IMAGE_NAME):$*-dev tox

.PHONY: build-base-images
BUILD_BASE_TARGETS := $(addprefix build.base.,$(BUILD_LIST))
build-base-images: $(BUILD_BASE_TARGETS)	## Builds base images.
build.base.%:
	@IMAGE=$(IMAGE_NAME) DOCKER_FILE_PATH=./docker/base/$*.Dockerfile VERSION=$*-base ./scripts/build-image.sh

.PHONY: build-runtime-images
BUILD_RUNTIME_TARGETS := $(addprefix build.runtime.,$(BUILD_LIST))
build-runtime-images: $(BUILD_RUNTIME_TARGETS)	## Builds runtime images.
build.runtime.%: fix-version build.base.%
	@IMAGE=$(IMAGE_NAME) DOCKER_FILE_PATH=./docker/runtime/$*.Dockerfile VERSION=${VERSION}-$* BASE=$(IMAGE_NAME):$*-base ./scripts/build-image.sh

.PHONY: build-dev-images
BUILD_DEV_TARGETS := $(addprefix build.dev.,$(BUILD_LIST))
build-dev-images: $(BUILD_DEV_TARGETS)	## Builds dev images.
build.dev.%: fix-version build.base.%
	@IMAGE=$(IMAGE_NAME) DOCKER_FILE_PATH=./docker/dev/$*.Dockerfile VERSION=$*-dev BASE=$(IMAGE_NAME):$*-base ./scripts/build-image.sh

.PHONY: fix-version
fix-version:	## Fix version in pyproject.toml and __init__.py
	@sed -i '' -r 's/^version\ =\ ".*"/version\ =\ "$(VERSION)"/' pyproject.toml
	@sed -i '' -r 's/^__version__\ =\ ".*"/__version__\ =\ "$(VERSION)"/' pydtk/__init__.py
