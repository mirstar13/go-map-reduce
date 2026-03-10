To start the node on minikube and run tests on the db:
    1. run make minikube-start
    2. run kubectl port-forward -n mapreduce svc/postgres-nodeport 5433:5432
    3. use localhost:5433 and correct credentials to connect to the db with goose