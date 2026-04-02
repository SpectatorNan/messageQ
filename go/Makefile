IMAGE_NAME ?= messageq
IMAGE_TAG ?= local
DOCKERFILE ?= deploy/Dockerfile
BUILD_CONTEXT ?= .
PLATFORM ?= linux/amd64
PLATFORMS ?= linux/amd64,linux/arm64
OCI_OUTPUT ?= /tmp/$(IMAGE_NAME)-$(IMAGE_TAG).oci.tar

IMAGE_REF := $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: build-image
build-image:
	docker buildx build --platform $(PLATFORM) --load -f $(DOCKERFILE) -t $(IMAGE_REF) $(BUILD_CONTEXT)

.PHONY: build-image-multi
build-image-multi:
	docker buildx build --platform $(PLATFORMS) --push -f $(DOCKERFILE) -t $(IMAGE_REF) $(BUILD_CONTEXT)

.PHONY: build-image-multi-oci
build-image-multi-oci:
	docker buildx build --platform $(PLATFORMS) --output type=oci,dest=$(OCI_OUTPUT) -f $(DOCKERFILE) -t $(IMAGE_REF) $(BUILD_CONTEXT)

.PHONY: compose-up
compose-up:
	docker compose -f deploy/docker-compose.yml up -d --build

.PHONY: compose-down
compose-down:
	docker compose -f deploy/docker-compose.yml down

.PHONY: k8s-apply
k8s-apply:
	kubectl apply -f deploy/k8s.yaml

.PHONY: k8s-delete
k8s-delete:
	kubectl delete -f deploy/k8s.yaml
