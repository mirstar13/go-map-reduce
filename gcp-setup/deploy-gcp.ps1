# deploy-gcp.ps1
# Builds, pushes, and deploys the MapReduce stack to GKE.

$ProjectID = gcloud config get-value project
$Region = "us-central1"
$Repo = "mapreduce-repo"

Write-Host "Deploying to Project: $ProjectID in $Region"

# 1. Update kustomization.yaml with actual PROJECT_ID and REGION
$KustomizeFile = "kustomization.yaml"
(Get-Content $KustomizeFile) -replace 'PROJECT_ID', $ProjectID -replace 'REGION', $Region | Set-Content $KustomizeFile

# 2. Build and Push images
Write-Host "Building and pushing images..."
$Services = @("manager-service", "ui-service", "worker", "builder")

foreach ($Svc in $Services) {
    $ImageURL = "$Region-docker.pkg.dev/$ProjectID/$Repo/mapreduce-$Svc:latest"
    Write-Host "Processing $Svc -> $ImageURL"
    
    switch ($Svc) {
        "manager-service" { $Dockerfile = "./services/manager/Dockerfile" }
        "ui-service"      { $Dockerfile = "./services/ui/Dockerfile" }
        "worker"          { $Dockerfile = "./services/worker/Dockerfile" }
        "builder"         { $Dockerfile = "./services/builder/Dockerfile" }
    }

    docker build -t "$ImageURL" -f "$Dockerfile" .
    docker push "$ImageURL"
}

# 3. Deploy via Kustomize
Write-Host "Applying Kubernetes manifests via Kustomize..."
kubectl apply -k .

Write-Host "Deployment initiated. Monitor status with: kubectl get pods -n mapreduce"
Write-Host "Public UI IP can be found with: kubectl get svc ui-public -n mapreduce"
