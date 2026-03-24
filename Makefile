# Docker buildx configuration
BUILDX_BUILDER := mapreduce-builder
DOCKER_REGISTRY := starpal
PLATFORMS := linux/amd64

.PHONY: minikube-start minikube-start-prod test-coverage docker-build docker-build-push buildx-setup buildx-cleanup

minikube-start:
	minikube start --cpus=4 --memory=6144 --disk-size=20g --driver=docker
	kubectl apply -f ./manifests/00_namespace.yml
	kubectl apply -f ./manifests/01_secrets.yml
	kubectl apply -f ./manifests/02_configmap.yml
	kubectl apply -f ./manifests/03_postgres.yml
	kubectl apply -f ./manifests/04_keycloak.yml

minikube-start-prod:
	minikube start --cpus=4 --memory=6144 --disk-size=20g --driver=docker
	minikube addons enable ingress
	minikube addons enable ingress-dns
	@echo "Waiting for ingress controller..."
	@sleep 30
	kubectl apply -f ./prodmanifests/

test-coverage:
<<<<<<< Updated upstream
	go test ./... -coverprofile='coverage.out' || true
	go tool cover -html='coverage.out'
=======
	@go test ./... -coverprofile='coverage.out' || true
	@go tool cover -html='coverage.out'

buildx-setup:
	@docker buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 || \
		docker buildx create --name $(BUILDX_BUILDER) --driver docker-container --bootstrap
	@docker buildx use $(BUILDX_BUILDER)

buildx-cleanup:
	@docker buildx rm $(BUILDX_BUILDER) 2>/dev/null || true

docker-build: buildx-setup
	@docker buildx build --load -f ./cmd/cli/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-cli .
	@docker buildx build --load -f ./cmd/migrate/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-migrate .
	@docker buildx build --load -f ./services/manager/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-manager-service .
	@docker buildx build --load -f ./services/ui/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-ui-service .
	@docker buildx build --load -f ./services/worker/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-worker .
	@docker buildx build --load -f ./services/builder/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-builder .

docker-build-push: buildx-setup
	@docker buildx build --platform $(PLATFORMS) --push -f ./cmd/cli/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-cli .
	@docker buildx build --platform $(PLATFORMS) --push -f ./cmd/migrate/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-migrate .
	@docker buildx build --platform $(PLATFORMS) --push -f ./services/manager/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-manager-service .
	@docker buildx build --platform $(PLATFORMS) --push -f ./services/ui/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-ui-service .
	@docker buildx build --platform $(PLATFORMS) --push -f ./services/worker/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-worker .
	@docker buildx build --platform $(PLATFORMS) --push -f ./services/builder/devDockerfile -t $(DOCKER_REGISTRY)/mapreduce-builder .
>>>>>>> Stashed changes
