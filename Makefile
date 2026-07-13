.PHONY: help docker docker-load inspect status dev dev-stop dev-logs dev-down nas-stop nas-deploy test-tnas

TEST_SIZE_MB ?= 64
PING_COUNT ?= 10
SKIP_DB_PULL ?= 0
NO_DB_PULL ?= $(SKIP_DB_PULL)
SKIP_DB_PUSH ?= 0
NO_DB_PUSH ?= $(SKIP_DB_PUSH)

IMAGE = wattimize
TAG = amd64-latest
TIMESTAMP := $(or $(TIMESTAMP),$(shell date +%Y%m%d_%H%M%S))
VERSION_TAG := amd64-$(TIMESTAMP)
PLATFORM = linux/amd64
OUTPUT_DIR = dist/docker-images
TAR := $(OUTPUT_DIR)/wattimize_amd64_$(TIMESTAMP).tar
BUILDER = wattimize-builder

help:
	@echo "Available targets:"
	@echo "  make docker      Build linux/amd64 image and export a timestamped TAR to $(OUTPUT_DIR)"
	@echo "  make docker-load Load the newest TAR from $(OUTPUT_DIR) into local Docker"
	@echo "  make inspect     Inspect image architecture/os"
	@echo "  make status      Show local and NAS runtime/database status"
	@echo "  make dev         Switch to local dev mode and sync DB from NAS when needed"
	@echo "                  Use SKIP_DB_PULL=1 to preserve the local SQLite database"
	@echo "  make dev-stop    Stop local docker compose services without removing them"
	@echo "  make dev-logs    Follow logs for wattimize-api"
	@echo "  make dev-down    Stop local docker compose services"
	@echo "  make nas-stop    Stop the NAS container without touching local services"
	@echo "  make nas-deploy  Build and deploy to TerraMaster NAS over SSH"
	@echo "                  Use SKIP_DB_PUSH=1 to deploy without pushing the local SQLite database"
	@echo "                  Always syncs data/home-plan/electricity-plans.json to the NAS data volume"
	@echo "  make test-tnas   Test NAS latency, packet loss, upload speed, and download speed"

docker:
	@mkdir -p $(OUTPUT_DIR)
	@docker buildx create --use --name $(BUILDER) 2>/dev/null || true
	@docker buildx inspect --bootstrap >/dev/null
	docker buildx build \
		--platform $(PLATFORM) \
		-t $(IMAGE):$(TAG) \
		-t $(IMAGE):$(VERSION_TAG) \
		-o type=docker,dest=$(TAR) \
		.
	@((command -v shasum >/dev/null 2>&1 && shasum -a 256 $(TAR) > $(TAR).sha256) || (command -v sha256sum >/dev/null 2>&1 && sha256sum $(TAR) > $(TAR).sha256))
	@echo "Exported: $(TAR)"
	@echo "Checksum: $(TAR).sha256"
	@echo "Image tags: $(IMAGE):$(TAG), $(IMAGE):$(VERSION_TAG)"

docker-load:
	@latest="$$(ls -1t $(OUTPUT_DIR)/wattimize_amd64_*.tar 2>/dev/null | head -n 1)"; \
	if [ -z "$$latest" ]; then \
		echo "No TAR found in $(OUTPUT_DIR). Run 'make docker' first."; \
		exit 1; \
	fi; \
	echo "Loading $$latest"; \
	docker load -i "$$latest"

inspect:
	docker image inspect $(IMAGE):$(TAG) --format '{{.Architecture}}/{{.Os}}'

status:
	bash ./scripts/status.sh

dev:
	SKIP_DB_PULL="$(SKIP_DB_PULL)" NO_DB_PULL="$(NO_DB_PULL)" bash ./scripts/dev-local.sh

dev-stop:
	docker compose stop

dev-logs:
	docker logs -f wattimize-api

dev-down:
	docker compose down

nas-stop:
	bash ./scripts/stop-nas.sh

nas-deploy:
	SKIP_DB_PUSH="$(SKIP_DB_PUSH)" NO_DB_PUSH="$(NO_DB_PUSH)" bash ./scripts/deploy-nas.sh

test-tnas:
	TNAS_TEST_SIZE_MB="$(TEST_SIZE_MB)" TNAS_PING_COUNT="$(PING_COUNT)" bash ./scripts/test-tnas.sh
