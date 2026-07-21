# Makefile

# --- Configuration ---
# Defaults that can be overridden via command line (e.g., make build VERSION=2027.0)
VERSION ?= 2026.3
BUILD_NUMBER ?= 1
VERSION_FULL ?= $(VERSION).$(BUILD_NUMBER)
ARCHIVE_NAME ?= midcompte_python

# --- Helpers ---
DATE_STR := $(shell date '+%Y-%m-%d %H:%M')

# --- Paths ---
ARTIFACTS_DIR = artifacts
DIST_DIR = dist
BUILD_ASSETS_DIR = build_assets
MANIFEST_FILE = $(BUILD_ASSETS_DIR)/manifest.build.json
STAGING_DIR = staging
DOCKER_IMAGE?=registry.millegrilles.com:5000/millegrilles/midcompte_python
DOCKER_IMAGE_NO_PORT=$(shell echo $(DOCKER_IMAGE) | sed 's/:[0-9]*//')

# --- Environment ---


# --- Targets ---

.PHONY: all docker-build archive-build package deploy clean

# Default target
all: package

# 1. Docker Build
docker-build:
	@echo "==> Building docker image..."
	@docker build -t $(DOCKER_IMAGE):$(VERSION_FULL) .

# 3. Package the artifacts
archive-build:
	@echo "==> Packaging artifacts..."
	@rm -rf $(ARTIFACTS_DIR) $(STAGING_DIR) $(BUILD_ASSETS_DIR)
	@mkdir -p $(ARTIFACTS_DIR)
	@for dir in catalogue/*; do \
		if [ -d "$$dir" ]; then \
			SUBDIR=$$(basename "$$dir"); \
			echo "==> Processing bundle: $$SUBDIR"; \
			rm -rf $(STAGING_DIR); \
			cp -r "$$dir"/. $(STAGING_DIR)/; \
			sed -i 's/"version": "[^"]*"/"version": "$(VERSION_FULL)"/' $(STAGING_DIR)/metadata.json; \
			sed -i 's|image: "replace_me"|image: "$(DOCKER_IMAGE_NO_PORT):$(VERSION_FULL)"|' $(STAGING_DIR)/docker-compose.yml; \
			if [ -f "$(STAGING_DIR)/metadata.json" ]; then \
				python3 -c 'import json, sys; \
					path = sys.argv[1]; \
					data = json.load(open(path)); \
					data["version"] = sys.argv[2]; \
					json.dump(data, open(path, "w"), indent=2)' $(STAGING_DIR)/metadata.json "$(VERSION_FULL)"; \
				NAME=$$(python3 -c 'import json; print(json.load(open("$(STAGING_DIR)/metadata.json"))["name"])'); \
			else \
				NAME=$$SUBDIR; \
			fi; \
			tar -C $(STAGING_DIR) -zcf "$(ARTIFACTS_DIR)/$$NAME.$(VERSION_FULL).tar.gz" .; \
			echo "==> Generating SHA256 digest for $$NAME"; \
			sha256sum "$(ARTIFACTS_DIR)/$$NAME.$(VERSION_FULL).tar.gz"; \
		fi; \
	done
	@rm -rf $(STAGING_DIR) ${BUILD_ASSETS_DIR}

package: docker-build archive-build

deploy: package
	@docker push "${DOCKER_IMAGE}:${VERSION_FULL}"
	@echo "==> Pushing docker image artifacts..."
	@echo "==> Deploying artifacts..."
	@for dir in catalogue/*; do \
		if [ -d "$$dir" ]; then \
			SUBDIR=$$(basename "$$dir"); \
			echo "==> Processing bundle: $$SUBDIR"; \
			if [ -f "$(dir)/metadata.json" ]; then \
				python3 -c 'import json, sys; \
					path = sys.argv[1]; \
					data = json.load(open(path)); \
					data["version"] = sys.argv[2]; \
					json.dump(data, open(path, "w"), indent=2)' $(dir)/metadata.json "$(VERSION_FULL)"; \
				NAME=$$(python3 -c 'import json; print(json.load(open("$(dir)/metadata.json"))["name"])'); \
			else \
				NAME=$$SUBDIR; \
			fi; \
			echo "==> Deploying $$NAME"; \
			rsync "$(ARTIFACTS_DIR)/$$NAME.$(VERSION_FULL).tar.gz" ${DEPLOY_RSYNC_WEBAPP_DEST}/midcompte/${NAME}/; \
			${DEPLOY_CATALOGUE_UPDATE_COMMAND} --baseurl https://libs.millegrilles.com/archives/midcompte --archive archives/midcompte/"$$NAME.$(VERSION_FULL).tar.gz"; \
		fi; \
	done

# Clean up build artifacts
clean:
	@echo "==> Cleaning..."
	@rm -rf $(ARTIFACTS_DIR)
	@rm -rf $(STAGING_DIR)
	@rm -rf $(BUILD_ASSETS_DIR)
	@rm -rf $(DIST_DIR)
	@rm -rf node_modules
