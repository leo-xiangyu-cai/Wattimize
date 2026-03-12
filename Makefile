.PHONY: help docker docker-load inspect dev dev-logs dev-down deploy-nas

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
	@echo "  make dev         Start local docker compose in hot-reload mode"
	@echo "  make dev-logs    Follow logs for wattimize-api"
	@echo "  make dev-down    Stop local docker compose services"
	@echo "  make deploy-nas  Build and deploy to TerraMaster NAS over SSH"

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

dev:
	docker compose up -d --build

dev-logs:
	docker logs -f wattimize-api

dev-down:
	docker compose down

deploy-nas:
	./scripts/deploy-nas.sh
