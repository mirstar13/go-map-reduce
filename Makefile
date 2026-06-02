BUILDX_BUILDER := mapreduce-builder
DOCKER_REGISTRY := starpal
PLATFORMS := linux/amd64
TAG ?= testing

.PHONY: minikube-start test-coverage docker-build docker-build-push buildx-setup buildx-cleanup

minikube-start:
	minikube start --cpus=max --memory=max --disk-size=20g --driver=docker
	kubectl apply -f ./manifests/
	minikube addons enable ingress
	minikube addons enable ingress-dns

test-coverage:
	@go test ./... -coverprofile='coverage.out' || true
	@go tool cover -html='coverage.out'

buildx-setup:
	@docker buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 || \
	docker buildx create --name $(BUILDX_BUILDER) --driver docker-container --bootstrap
	@docker buildx use $(BUILDX_BUILDER)

buildx-cleanup:
	@docker buildx rm $(BUILDX_BUILDER) 2>/dev/null || true

docker-build: buildx-setup
	docker buildx build --load -f ./cmd/cli/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-cli:$(TAG) .
	docker buildx build --load -f ./cmd/migrate/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-migrate:$(TAG) .
	docker buildx build --load -f ./services/manager/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-manager-service:$(TAG) .
	docker buildx build --load -f ./services/ui/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-ui-service:$(TAG) .
	docker buildx build --load -f ./services/worker/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-worker:$(TAG) .
	docker buildx build --load -f ./services/builder/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-builder:$(TAG) .
	docker buildx build --platform $(PLATFORMS) --push -f ./services/shuffle/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-shuffle-service:beta .

docker-build-push: buildx-setup
	docker buildx build --platform $(PLATFORMS) --push -f ./cmd/cli/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-cli:$(TAG) .
	docker buildx build --platform $(PLATFORMS) --push -f ./cmd/migrate/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-migrate:$(TAG) .
	docker buildx build --platform $(PLATFORMS) --push -f ./services/manager/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-manager-service:$(TAG) .
	docker buildx build --platform $(PLATFORMS) --push -f ./services/ui/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-ui-service:$(TAG) .
	docker buildx build --platform $(PLATFORMS) --push -f ./services/worker/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-worker:$(TAG) .
	docker buildx build --platform $(PLATFORMS) --push -f ./services/builder/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-builder:latest .
	docker buildx build --platform $(PLATFORMS) --push -f ./services/shuffle/Dockerfile -t $(DOCKER_REGISTRY)/mapreduce-shuffle-service:$(TAG) .