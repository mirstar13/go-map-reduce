minikube-start:
	minikube start --cpus=4 --memory=6144 --disk-size=20g --driver=docker
	kubectl apply -f ./manifests/00_namespace.yml
	kubectl apply -f ./manifests/01_secrets.yml
	kubectl apply -f ./manifests/02_configmap.yml
	kubectl apply -f ./manifests/03_postgres.yml
	kubectl apply -f ./manifests/04_keycloak.yml

test-coverage:
	@go test ./... -coverprofile='coverage.out' || true
	@go tool cover -html='coverage.out'