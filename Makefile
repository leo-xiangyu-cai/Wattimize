.PHONY: help docker docker-load inspect

IMAGE = wattimize
TAG = amd64-latest
TIMESTAMP ?= $(shell date +%Y%m%d_%H%M%S)
VERSION_TAG = amd64-$(TIMESTAMP)
PLATFORM = linux/amd64
TAR = wattimize_amd64_$(TIMESTAMP).tar
LATEST_TAR = wattimize_amd64_latest.tar
BUILDER = wattimize-builder

help:
	@echo "Available targets:"
	@echo "  make docker      Build linux/amd64 image and keep only latest TAR"
	@echo "  make docker-load Load latest TAR into local Docker"
	@echo "  make inspect     Inspect image architecture/os"

docker:
	@docker buildx create --use --name $(BUILDER) 2>/dev/null || true
	@docker buildx inspect --bootstrap >/dev/null
	docker buildx build \
		--platform $(PLATFORM) \
		-t $(IMAGE):$(TAG) \
		-t $(IMAGE):$(VERSION_TAG) \
		-o type=docker,dest=./$(TAR) \
		.
	cp -f ./$(TAR) ./$(LATEST_TAR)
	@((command -v shasum >/dev/null 2>&1 && shasum -a 256 ./$(TAR) > ./$(TAR).sha256) || (command -v sha256sum >/dev/null 2>&1 && sha256sum ./$(TAR) > ./$(TAR).sha256))
	@find . -maxdepth 1 -type f -name 'wattimize_amd64_*.tar' ! -name 'wattimize_amd64_latest.tar' -delete
	@find . -maxdepth 1 -type f -name 'wattimize_amd64_*.tar.sha256' -delete
	@echo "Exported: ./$(TAR)"
	@echo "Kept: ./$(LATEST_TAR)"
	@echo "Image tags: $(IMAGE):$(TAG), $(IMAGE):$(VERSION_TAG)"

docker-load:
	docker load -i ./$(LATEST_TAR)

inspect:
	docker image inspect $(IMAGE):$(TAG) --format '{{.Architecture}}/{{.Os}}'
