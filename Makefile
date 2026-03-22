minikube-start:
	minikube start --cpus=4 --memory=6144 --disk-size=20g --driver=docker
	kubectl apply -f ./manifests/

test-coverage:
	@go test ./... -coverprofile='coverage.out' || true
	@go tool cover -html='coverage.out'

docker-build:
	@docker build -f ./cmd/cli/devDockerfile -t starpal/mapreduce-cli .
	@docker build -f ./cmd/migrate/devDockerfile -t starpal/mapreduce-migrate .
	@docker build -f ./services/manager/devDockerfile -t starpal/mapreduce-manager-service .
	@docker build -f ./services/ui/devDockerfile -t starpal/mapreduce-ui-service .