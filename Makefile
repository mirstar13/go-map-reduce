minikube-start:
	minikube start --cpus=4 --memory=6144 --disk-size=20g --driver=docker --kubernetes-version=v1.35.0
	kubectl apply -f ./devManifests/00_namespace.yml
	kubectl apply -f ./devManifests/01_secrets.yml
	kubectl apply -f ./devManifests/02_configmap.yml
	kubectl apply -f ./devManifests/03_postgres.yml